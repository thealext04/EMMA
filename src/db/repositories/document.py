"""
repositories/document.py — Data-access layer for the documents table.

Stores disclosure document metadata (URL, type, dates) without downloading
the actual PDF.  local_path remains NULL until a document is explicitly
fetched for AI extraction in Phase 4.

Usage:
    from src.db.engine import Session
    from src.db.repositories.document import DocumentRepository

    with Session() as session:
        repo = DocumentRepository(session)
        doc, created = repo.upsert(
            issue_id=1,
            borrower_id=1,
            emma_doc_id="DOC-ABC123",
            doc_type="financial_statement",
            doc_url="https://emma.msrb.org/...",
            posted_date=date(2024, 11, 15),
        )
        session.commit()
"""

import logging
from datetime import date, datetime
from typing import Optional

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from src.db.models import Document

logger = logging.getLogger(__name__)

# Canonical doc_type values
VALID_DOC_TYPES = {
    "financial_statement",
    "event_notice",
    "operating_report",
    "budget",
    "rating_notice",
    "bond_issuance",
    "other",
}

# Keywords used to classify doc_type from EMMA's free-text document labels.
# Checked in order — first match wins.
#
# EMMA's actual continuing-disclosure label conventions (discovered from live data):
#   "Financial Operating Filing (...)"  → financial_statement
#   "Event Filing as of MM/DD/YYYY ..."  → event_notice
#   "Official Statement (...)"           → bond_issuance
#
# IMPORTANT: do NOT use bare "rating" as a keyword — it is a substring of
# "operating" and would misclassify every "Financial Operating Filing" as a
# rating_notice.  Use full phrases like "rating action" or "credit rating" instead.
_TYPE_KEYWORDS: list[tuple[str, list[str]]] = [
    ("financial_statement", [
        # EMMA's native continuing-disclosure label — highest priority
        "financial operating filing",
        # Other common patterns
        "annual financial", "audited financial", "financial statement",
        "audit report", "auditor's report", "cafr",
        "annual report", "comprehensive annual",
    ]),
    ("event_notice", [
        # EMMA's native event-notice label
        "event filing",
        # Other common patterns
        "material event", "event notice", "covenant",
        "going concern", "forbearance", "bankruptcy",
        "liquidity facility", "payment default",
    ]),
    ("operating_report", [
        "operating report", "management report", "quarterly report",
        "monthly report", "interim report", "continuing disclosure report",
    ]),
    ("budget", [
        "annual budget", "adopted budget", "proposed budget",
    ]),
    ("rating_notice", [
        # Precise multi-word phrases only — avoids the "oper-ating" collision
        "rating action", "rating change", "rating report", "rating letter",
        "credit rating", "rating agency", "rating upgrade", "rating downgrade",
        "moody", "standard & poor", "fitch",
    ]),
    ("bond_issuance", [
        "official statement", "preliminary official", "offering memorandum",
        "bond prospectus", "remarketing supplement",
    ]),
]


def classify_doc_type(title: str, emma_type_label: str = "") -> str:
    """
    Infer a canonical doc_type from EMMA's title and type label strings.

    Checks _TYPE_KEYWORDS in priority order; falls back to "other".

    Key design decisions:
    - "Financial Operating Filing" (EMMA's label for annual financial disclosures)
      is matched first and explicitly, before any shorter-phrase rules.
    - "Event Filing" (EMMA's label for material event notices) is matched first.
    - "rating" is NOT used as a bare substring keyword because it is a substring
      of "operating" — causing every financial filing to be misclassified.
    """
    combined = (title + " " + emma_type_label).lower()
    for doc_type, keywords in _TYPE_KEYWORDS:
        if any(kw in combined for kw in keywords):
            return doc_type
    return "other"


def reclassify_all_documents(session) -> tuple[int, dict[str, int]]:
    """
    Re-apply classify_doc_type() to every document in the database.

    Used after fixing the classifier to correct previously misclassified records.
    Caller must commit the session after this returns.

    Returns:
        (updated_count, final_type_distribution)
    """
    docs = session.execute(select(Document)).scalars().all()
    updated = 0
    distribution: dict[str, int] = {}

    for doc in docs:
        new_type = classify_doc_type(doc.title or "")
        if new_type != doc.doc_type:
            doc.doc_type = new_type
            updated += 1
        distribution[new_type] = distribution.get(new_type, 0) + 1

    return updated, distribution


class DocumentRepository:
    """CRUD operations for the documents table."""

    def __init__(self, session: Session) -> None:
        self.session = session

    # ------------------------------------------------------------------
    # Create / Upsert
    # ------------------------------------------------------------------

    def upsert(
        self,
        issue_id: int,
        borrower_id: int,
        emma_doc_id: str,
        doc_type: str,
        doc_url: str,
        title: str = "",
        doc_date: Optional[date] = None,
        posted_date: Optional[date] = None,
        fiscal_year: Optional[int] = None,
    ) -> tuple[Document, bool]:
        """
        Insert a document record or return the existing one if already known.

        Idempotent on emma_doc_id — safe to call repeatedly as new disclosure
        pages are scraped.  local_path is intentionally not set here; PDFs
        are only fetched on demand during AI extraction (Phase 4).

        Returns:
            (document, created) where created=True if a new row was inserted.
        """
        stmt = select(Document).where(Document.emma_doc_id == emma_doc_id)
        existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            # Update mutable fields in case metadata has been corrected
            existing.doc_url = doc_url
            if posted_date and not existing.posted_date:
                existing.posted_date = posted_date
            return existing, False

        # Infer fiscal_year from doc_date if not supplied
        if fiscal_year is None and doc_date:
            fiscal_year = doc_date.year

        doc = Document(
            issue_id=issue_id,
            borrower_id=borrower_id,
            emma_doc_id=emma_doc_id,
            doc_type=doc_type,
            title=title if title else None,
            doc_date=doc_date,
            posted_date=posted_date,
            fiscal_year=fiscal_year,
            doc_url=doc_url,
            extraction_status="pending",   # URL stored; extraction deferred to Phase 4
        )
        self.session.add(doc)
        logger.debug("Queued new document: %s [%s]", emma_doc_id, doc_type)
        return doc, True

    def upsert_batch(
        self,
        records: list[dict],
    ) -> tuple[int, int]:
        """
        Upsert a list of document dicts.  Each dict must contain the keys
        accepted by upsert().

        Returns:
            (added_count, skipped_count)
        """
        added = skipped = 0
        for rec in records:
            _, created = self.upsert(**rec)
            if created:
                added += 1
            else:
                skipped += 1
        return added, skipped

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def list_for_borrower(
        self,
        borrower_id: int,
        doc_type: Optional[str] = None,
        since_date: Optional[date] = None,
        limit: int = 200,
    ) -> list[Document]:
        """
        Return documents for a borrower, newest first.

        Args:
            borrower_id: Borrower primary key.
            doc_type:    Filter to one document type (e.g. "financial_statement").
            since_date:  Only return documents posted on or after this date.
            limit:       Maximum rows returned.
        """
        stmt = (
            select(Document)
            .where(Document.borrower_id == borrower_id)
            .order_by(Document.posted_date.desc().nulls_last())
            .limit(limit)
        )
        if doc_type:
            stmt = stmt.where(Document.doc_type == doc_type)
        if since_date:
            stmt = stmt.where(Document.posted_date >= since_date)

        return list(self.session.execute(stmt).scalars())

    def list_for_issue(self, issue_id: int) -> list[Document]:
        """Return all documents for a bond issue, newest first."""
        stmt = (
            select(Document)
            .where(Document.issue_id == issue_id)
            .order_by(Document.posted_date.desc().nulls_last())
        )
        return list(self.session.execute(stmt).scalars())

    def count_for_borrower(self, borrower_id: int) -> dict[str, int]:
        """
        Return a dict of {doc_type: count} for a borrower.
        Useful for a quick disclosure coverage summary.
        """
        from sqlalchemy import case
        stmt = (
            select(Document.doc_type, func.count().label("n"))
            .where(Document.borrower_id == borrower_id)
            .group_by(Document.doc_type)
        )
        rows = self.session.execute(stmt).all()
        return {row.doc_type: row.n for row in rows}

    def latest_financial_statement(self, borrower_id: int) -> Optional[Document]:
        """Return the most recently posted financial statement for a borrower."""
        stmt = (
            select(Document)
            .where(
                Document.borrower_id == borrower_id,
                Document.doc_type == "financial_statement",
            )
            .order_by(Document.posted_date.desc().nulls_last())
            .limit(1)
        )
        return self.session.execute(stmt).scalar_one_or_none()
