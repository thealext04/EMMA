"""
repositories/borrower.py — Data-access layer for the borrowers table.

All methods accept and return ORM model instances.  Callers are responsible
for session lifecycle (open, commit, close).

Usage:
    from src.db.engine import Session
    from src.db.repositories.borrower import BorrowerRepository

    with Session() as session:
        repo = BorrowerRepository(session)
        b = repo.add(
            borrower_name="Rider University",
            sector="higher_ed",
            state="NJ",
            fiscal_year_end="06-30",
        )
        session.commit()
        print(b.borrower_id)
"""

import logging
from datetime import date
from typing import Optional

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from src.db.models import Borrower

logger = logging.getLogger(__name__)

# Valid sector values
VALID_SECTORS = {
    "higher_ed",
    "healthcare",
    "general_government",
    "housing",
    "utility",
    "transportation",
    "other",
}

# Valid distress status values
VALID_DISTRESS_STATUSES = {"monitor", "watch", "distressed", "resolved"}


class BorrowerRepository:
    """CRUD operations for the borrowers table."""

    def __init__(self, session: Session) -> None:
        self.session = session

    # ------------------------------------------------------------------
    # Create
    # ------------------------------------------------------------------

    def add(
        self,
        borrower_name: str,
        sector: str,
        state: Optional[str] = None,
        city: Optional[str] = None,
        fiscal_year_end: Optional[str] = None,
        watchlist_notes: Optional[str] = None,
        distress_status: str = "monitor",
        on_watchlist: bool = True,
    ) -> Borrower:
        """
        Insert a new borrower and return the persisted instance.

        The caller must call session.commit() after this method.

        Args:
            borrower_name:  Full legal name (e.g. "Rider University").
            sector:         One of VALID_SECTORS.
            state:          Two-letter state code (e.g. "NJ").
            city:           City name.
            fiscal_year_end: Fiscal year end as "MM-DD" (e.g. "06-30").
            watchlist_notes: Free-text note for why this borrower is tracked.
            distress_status: Initial status (default: "monitor").
            on_watchlist:   Whether to track this borrower (default: True).

        Returns:
            Unsaved Borrower instance (not yet committed).

        Raises:
            ValueError: If sector or distress_status is not a valid value.
        """
        if sector not in VALID_SECTORS:
            raise ValueError(
                f"Invalid sector '{sector}'. Valid values: {sorted(VALID_SECTORS)}"
            )
        if distress_status not in VALID_DISTRESS_STATUSES:
            raise ValueError(
                f"Invalid distress_status '{distress_status}'. "
                f"Valid values: {sorted(VALID_DISTRESS_STATUSES)}"
            )

        borrower = Borrower(
            borrower_name=borrower_name.strip(),
            sector=sector,
            state=state.upper() if state else None,
            city=city,
            fiscal_year_end=fiscal_year_end,
            distress_status=distress_status,
            on_watchlist=on_watchlist,
            watchlist_since=date.today() if on_watchlist else None,
            watchlist_notes=watchlist_notes,
        )
        self.session.add(borrower)
        logger.info("Queued new borrower: %r [%s]", borrower_name, sector)
        return borrower

    def get_or_create(
        self,
        borrower_name: str,
        sector: str,
        state: Optional[str] = None,
        **kwargs,
    ) -> tuple["Borrower", bool]:
        """
        Return existing borrower by name+state or create a new one.

        Returns:
            (borrower, created) — created=True if a new row was inserted.
        """
        stmt = select(Borrower).where(
            func.lower(Borrower.borrower_name) == borrower_name.strip().lower()
        )
        if state:
            stmt = stmt.where(Borrower.state == state.upper())

        existing = self.session.execute(stmt).scalar_one_or_none()
        if existing:
            return existing, False

        new = self.add(borrower_name=borrower_name, sector=sector, state=state, **kwargs)
        return new, True

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def get(self, borrower_id: int) -> Optional[Borrower]:
        """Fetch a borrower by primary key. Returns None if not found."""
        return self.session.get(Borrower, borrower_id)

    def get_by_name(self, name: str) -> Optional[Borrower]:
        """Case-insensitive exact name lookup. Returns first match."""
        stmt = select(Borrower).where(
            func.lower(Borrower.borrower_name) == name.strip().lower()
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def list_watchlist(
        self,
        sector: Optional[str] = None,
        state: Optional[str] = None,
        distress_status: Optional[str] = None,
        order_by_score: bool = True,
    ) -> list[Borrower]:
        """
        Return all on_watchlist=True borrowers, with optional filters.

        Args:
            sector:          Filter by sector.
            state:           Filter by state.
            distress_status: Filter by distress status.
            order_by_score:  If True, sort by distress_score DESC (nulls last),
                             then alphabetically.

        Returns:
            List of Borrower instances.
        """
        stmt = select(Borrower).where(Borrower.on_watchlist == True)  # noqa: E712

        if sector:
            stmt = stmt.where(Borrower.sector == sector)
        if state:
            stmt = stmt.where(Borrower.state == state.upper())
        if distress_status:
            stmt = stmt.where(Borrower.distress_status == distress_status)

        if order_by_score:
            stmt = stmt.order_by(
                Borrower.distress_score.desc().nulls_last(),
                Borrower.borrower_name,
            )
        else:
            stmt = stmt.order_by(Borrower.borrower_name)

        return list(self.session.execute(stmt).scalars())

    def list_all(self) -> list[Borrower]:
        """Return all borrowers regardless of watchlist status."""
        stmt = select(Borrower).order_by(Borrower.borrower_name)
        return list(self.session.execute(stmt).scalars())

    def count(self) -> int:
        """Return total number of borrowers."""
        stmt = select(func.count()).select_from(Borrower)
        return self.session.execute(stmt).scalar_one()

    # ------------------------------------------------------------------
    # Update
    # ------------------------------------------------------------------

    def update_distress_status(
        self,
        borrower_id: int,
        status: str,
        score: Optional[int] = None,
    ) -> Optional[Borrower]:
        """
        Update distress_status and optionally distress_score for a borrower.

        Returns the updated Borrower, or None if not found.
        """
        if status not in VALID_DISTRESS_STATUSES:
            raise ValueError(f"Invalid distress_status: {status!r}")

        b = self.get(borrower_id)
        if not b:
            return None

        b.distress_status = status
        if score is not None:
            b.distress_score = max(0, min(100, score))
        logger.info(
            "Updated borrower %d distress_status=%s score=%s",
            borrower_id, status, score,
        )
        return b

    def set_fiscal_year_end(self, borrower_id: int, fye: str) -> Optional[Borrower]:
        """
        Set or update the fiscal year end (MM-DD format).
        Example: set_fiscal_year_end(1, "06-30") for June 30.
        """
        b = self.get(borrower_id)
        if not b:
            return None
        b.fiscal_year_end = fye
        return b

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    def remove_from_watchlist(self, borrower_id: int) -> Optional[Borrower]:
        """
        Set on_watchlist=False. Does not delete the row — preserves history.
        Returns the updated Borrower or None if not found.
        """
        b = self.get(borrower_id)
        if not b:
            return None
        b.on_watchlist = False
        logger.info("Removed borrower %d (%r) from watchlist", borrower_id, b.borrower_name)
        return b
