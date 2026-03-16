"""
distress/late_filing.py — Late Disclosure Detection (Phase 3)

Answers two key credit surveillance questions:
  1. "When did an entity last publish any financials?"
  2. "Is this entity behind on its reporting obligations?"

Logic:
  - Continuing disclosure agreements (CDAs) require annual financial filings
    within 180 days of fiscal year end (SEC Rule 15c2-12 standard).
  - We compare: FYE + 180 days  vs.  most recent financial_statement posted_date.
  - If the deadline has passed and no financial_statement has been posted since
    the most recently completed FYE, the borrower is flagged as late.

Limitations (pre-Phase 4):
  - FYEs are set manually per borrower (defaulting to 06-30 for higher ed).
  - We cannot yet read PDFs to confirm which fiscal year a filing covers, so
    we use posted_date as a proxy: any financial_statement posted after the FYE
    is assumed to cover that year.
  - False positive rate <10% is acceptable for Phase 3 (per PHASES.md).

Usage:
    from src.db.engine import Session
    from src.distress.late_filing import scan_all_watchlist, check_borrower

    with Session() as session:
        results = scan_all_watchlist(session)
        for status in results:
            if status.is_late:
                print(f"{status.borrower_name}: {status.days_overdue}d overdue")
"""

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Default filing window in days (SEC Rule 15c2-12 standard)
DEFAULT_DEADLINE_DAYS: int = 180


@dataclass
class LateFilingStatus:
    """
    Late-filing assessment for a single borrower.

    Fields:
        borrower_id           — Borrower primary key
        borrower_name         — Display name
        fiscal_year_end       — "MM-DD" string (e.g. "06-30")
        fye_date              — Most recently completed FYE (a real date)
        deadline              — fye_date + 180 days (the expected filing deadline)
        last_filed_date       — Effective date of most recent financial_statement (or None)
        last_filed_title      — Document title of most recent financial_statement (or None)
        total_fs_count        — Total number of financial_statement records on file
        is_late               — True if deadline passed, no post-FYE dated filing found,
                                 AND no undated filings exist either
        days_overdue          — Days past deadline (0 if not late)
        no_fye_set            — True if borrower has no fiscal_year_end configured
        has_undated_filings   — True if financial_statements exist but none have dates.
                                 Cannot confirm late or current; needs date backfill.
    """
    borrower_id: int
    borrower_name: str
    fiscal_year_end: Optional[str]
    fye_date: Optional[date]
    deadline: Optional[date]
    last_filed_date: Optional[date]
    last_filed_title: Optional[str]
    total_fs_count: int
    is_late: bool
    days_overdue: int
    no_fye_set: bool = False
    has_undated_filings: bool = False


def compute_most_recent_fye(fye_str: str, today: Optional[date] = None) -> date:
    """
    Given a fiscal year end string ("MM-DD"), return the most recently
    completed FYE date that is strictly before today.

    Examples (today = 2026-03-15):
        "06-30"  →  2025-06-30  (June 30, 2025 — already passed)
        "12-31"  →  2025-12-31  (December 31, 2025 — already passed)
        "03-31"  →  2025-03-31  (March 31, 2025 — already passed, not today's)

    Args:
        fye_str:  Fiscal year end as "MM-DD" (e.g. "06-30").
        today:    Reference date (defaults to date.today()).

    Returns:
        Most recently completed FYE date.
    """
    if today is None:
        today = date.today()

    month = int(fye_str[:2])
    day = int(fye_str[3:])

    # Try this calendar year's FYE first
    try:
        candidate = date(today.year, month, day)
    except ValueError:
        # Handles edge cases like Feb 29 in non-leap years
        candidate = date(today.year, month, day - 1)

    if candidate >= today:
        # FYE hasn't passed yet this year — use last year's
        try:
            candidate = date(today.year - 1, month, day)
        except ValueError:
            candidate = date(today.year - 1, month, day - 1)

    return candidate


def compute_deadline(
    fye_str: str,
    deadline_days: int = DEFAULT_DEADLINE_DAYS,
    today: Optional[date] = None,
) -> tuple[date, date]:
    """
    Compute the most recently completed FYE and the expected filing deadline.

    Args:
        fye_str:       Fiscal year end as "MM-DD".
        deadline_days: Days after FYE that filing is due (default: 180).
        today:         Reference date (defaults to date.today()).

    Returns:
        (fye_date, deadline_date) — the FYE and its deadline.
    """
    fye_date = compute_most_recent_fye(fye_str, today)
    deadline = fye_date + timedelta(days=deadline_days)
    return fye_date, deadline


def _severity_for_days(days_overdue: int) -> str:
    """Map days overdue to a severity label."""
    if days_overdue < 30:
        return "low"
    elif days_overdue < 90:
        return "medium"
    elif days_overdue < 180:
        return "high"
    else:
        return "critical"


def _distress_score_contribution(days_overdue: int) -> int:
    """Map days overdue to a distress score contribution (0–75)."""
    if days_overdue <= 0:
        return 0
    elif days_overdue <= 30:
        return 10
    elif days_overdue <= 90:
        return 25
    elif days_overdue <= 180:
        return 50
    else:
        return 75


def check_borrower(
    session: Session,
    borrower_id: int,
    today: Optional[date] = None,
    deadline_days: int = DEFAULT_DEADLINE_DAYS,
) -> LateFilingStatus:
    """
    Compute the late-filing status for a single borrower.

    Args:
        session:       SQLAlchemy session.
        borrower_id:   Borrower primary key.
        today:         Reference date (defaults to date.today()).
        deadline_days: Filing window in days after FYE (default: 180).

    Returns:
        LateFilingStatus dataclass.
    """
    from src.db.repositories.borrower import BorrowerRepository
    from src.db.repositories.document import DocumentRepository

    if today is None:
        today = date.today()

    borrower_repo = BorrowerRepository(session)
    doc_repo = DocumentRepository(session)

    borrower = borrower_repo.get(borrower_id)
    if not borrower:
        raise ValueError(f"Borrower #{borrower_id} not found")

    # No FYE set — cannot compute deadline
    if not borrower.fiscal_year_end:
        logger.debug(
            "Borrower %d (%s) has no fiscal_year_end set — skipping",
            borrower_id, borrower.borrower_name,
        )
        return LateFilingStatus(
            borrower_id=borrower_id,
            borrower_name=borrower.borrower_name,
            fiscal_year_end=None,
            fye_date=None,
            deadline=None,
            last_filed_date=None,
            last_filed_title=None,
            total_fs_count=0,
            is_late=False,
            days_overdue=0,
            no_fye_set=True,
        )

    fye_date, deadline = compute_deadline(
        borrower.fiscal_year_end, deadline_days, today
    )

    # Get most recent financial statement
    # Use posted_date if available; fall back to doc_date (extracted from title
    # as "as of MM/DD/YYYY") since the scraper may not populate posted_date.
    # NOTE: EMMA's "Financial Operating Filing" title format does NOT include
    # an inline date, so doc_date is NULL for most financial_statement records.
    # When doc_date is also NULL, we treat the filing as "on record but undated."
    latest_doc = doc_repo.latest_financial_statement(borrower_id)
    last_filed_date = (
        (latest_doc.posted_date or latest_doc.doc_date) if latest_doc else None
    )
    last_filed_title = latest_doc.title if latest_doc else None

    # Count total financial statements on file for this borrower
    counts = doc_repo.count_for_borrower(borrower_id)
    total_fs_count = counts.get("financial_statement", 0)

    # If filings exist but none have dates, we cannot determine timeliness.
    # This happens because EMMA's "Financial Operating Filing" link text
    # does not embed a date.  Flag for date backfill rather than as LATE.
    has_undated_filings = (total_fs_count > 0 and last_filed_date is None)

    # Determine if late:
    # - Deadline must have already passed
    # - No datable filing found after the most recent FYE
    # - No undated filings either (benefit of the doubt — they may have filed)
    if deadline >= today:
        # Deadline hasn't passed yet — not late
        is_late = False
        days_overdue = 0
    elif has_undated_filings:
        # Cannot confirm; filings exist but we can't date them
        is_late = False
        days_overdue = 0
    elif last_filed_date is not None and last_filed_date > fye_date:
        # Filed something after the FYE — assume it covers this fiscal year
        is_late = False
        days_overdue = 0
    else:
        # Deadline passed, no filings found (dated or undated)
        is_late = True
        days_overdue = (today - deadline).days

    logger.debug(
        "Borrower %d (%s): FYE=%s deadline=%s last_filed=%s fs_count=%d "
        "has_undated=%s is_late=%s days_overdue=%d",
        borrower_id, borrower.borrower_name, fye_date, deadline,
        last_filed_date, total_fs_count, has_undated_filings, is_late, days_overdue,
    )

    return LateFilingStatus(
        borrower_id=borrower_id,
        borrower_name=borrower.borrower_name,
        fiscal_year_end=borrower.fiscal_year_end,
        fye_date=fye_date,
        deadline=deadline,
        last_filed_date=last_filed_date,
        last_filed_title=last_filed_title,
        total_fs_count=total_fs_count,
        is_late=is_late,
        days_overdue=days_overdue,
        no_fye_set=False,
        has_undated_filings=has_undated_filings,
    )


def scan_all_watchlist(
    session: Session,
    today: Optional[date] = None,
    deadline_days: int = DEFAULT_DEADLINE_DAYS,
) -> list[LateFilingStatus]:
    """
    Run late-filing check for all on-watchlist borrowers.

    Borrowers without a fiscal_year_end set are included in results
    with no_fye_set=True so the report can flag them for manual attention.

    Args:
        session:       SQLAlchemy session.
        today:         Reference date (defaults to date.today()).
        deadline_days: Filing window in days after FYE (default: 180).

    Returns:
        List of LateFilingStatus, sorted by days_overdue DESC then name.
    """
    from src.db.repositories.borrower import BorrowerRepository

    if today is None:
        today = date.today()

    repo = BorrowerRepository(session)
    borrowers = repo.list_watchlist(order_by_score=False)

    results: list[LateFilingStatus] = []
    for b in borrowers:
        try:
            status = check_borrower(session, b.borrower_id, today, deadline_days)
            results.append(status)
        except Exception as exc:
            logger.warning(
                "Error checking borrower %d (%s): %s", b.borrower_id, b.borrower_name, exc
            )

    # Sort: confirmed late first (most overdue → least),
    #       then undated (can't confirm), then no-FYE, then current
    results.sort(key=lambda s: (
        0 if s.is_late else (1 if s.has_undated_filings else (2 if s.no_fye_set else 3)),
        -s.days_overdue,
        s.borrower_name,
    ))
    return results
