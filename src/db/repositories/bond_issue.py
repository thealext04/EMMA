"""
repositories/bond_issue.py — Data-access layer for the bond_issues table.

Handles linking EMMA-discovered bond issues to tracked borrowers in the database.

Usage:
    from src.db.engine import Session
    from src.db.repositories.bond_issue import BondIssueRepository

    with Session() as session:
        repo = BondIssueRepository(session)
        issue = repo.upsert_from_emma(
            borrower_id=1,
            emma_issue_id="MS92245",
            series_name="REVENUE BONDS RIDER UNIVERSITY ISSUE 2012 SERIES A",
            state="NJ",
            issue_date=date(2012, 3, 15),
        )
        session.commit()
"""

import logging
from datetime import date
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import BondIssue

logger = logging.getLogger(__name__)


class BondIssueRepository:
    """CRUD operations for the bond_issues table."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def upsert_from_emma(
        self,
        borrower_id: int,
        emma_issue_id: str,
        series_name: Optional[str] = None,
        issuer_id: Optional[int] = None,
        par_amount: Optional[float] = None,
        issue_date: Optional[date] = None,
        bond_type: Optional[str] = None,
        tax_status: Optional[str] = None,
        state: Optional[str] = None,
        continuing_disclosure_url: Optional[str] = None,
    ) -> tuple[BondIssue, bool]:
        """
        Insert or update a bond issue by emma_issue_id.

        Returns:
            (bond_issue, created) where created=True if a new row was inserted.
        """
        stmt = select(BondIssue).where(BondIssue.emma_issue_id == emma_issue_id)
        existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            # Update mutable fields on existing row
            if series_name:
                existing.series_name = series_name
            if par_amount is not None:
                existing.par_amount = par_amount
            if issue_date:
                existing.issue_date = issue_date
            if continuing_disclosure_url:
                existing.continuing_disclosure_url = continuing_disclosure_url
            logger.debug("Updated bond_issue: %s", emma_issue_id)
            return existing, False

        issue = BondIssue(
            borrower_id=borrower_id,
            issuer_id=issuer_id,
            emma_issue_id=emma_issue_id,
            series_name=series_name,
            par_amount=par_amount,
            issue_date=issue_date,
            bond_type=bond_type,
            tax_status=tax_status,
            state=state.upper() if state else None,
            continuing_disclosure_url=continuing_disclosure_url,
        )
        self.session.add(issue)
        logger.info(
            "Queued new bond_issue: %s for borrower_id=%d", emma_issue_id, borrower_id
        )
        return issue, True

    def list_for_borrower(self, borrower_id: int) -> list[BondIssue]:
        """Return all bond issues for a given borrower, newest first."""
        stmt = (
            select(BondIssue)
            .where(BondIssue.borrower_id == borrower_id)
            .order_by(BondIssue.issue_date.desc().nulls_last())
        )
        return list(self.session.execute(stmt).scalars())

    def get_by_emma_id(self, emma_issue_id: str) -> Optional[BondIssue]:
        """Fetch by EMMA issue ID. Returns None if not found."""
        stmt = select(BondIssue).where(BondIssue.emma_issue_id == emma_issue_id)
        return self.session.execute(stmt).scalar_one_or_none()

    def update_disclosure_cursor(
        self,
        emma_issue_id: str,
        last_seen_doc_date: date,
    ) -> Optional[BondIssue]:
        """
        Update the incremental-update cursor after a successful disclosure check.
        This prevents re-fetching documents already seen in a previous run.
        """
        issue = self.get_by_emma_id(emma_issue_id)
        if not issue:
            return None
        issue.last_seen_doc_date = last_seen_doc_date
        from datetime import datetime
        issue.last_disclosure_check = datetime.utcnow()
        return issue
