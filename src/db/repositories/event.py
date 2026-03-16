"""
repositories/event.py — Data-access layer for the events table.

Events represent distress signals detected at the borrower level.
Phase 3 writes late_filing events; Phase 5 will add rating_downgrade,
going_concern, covenant_violation events from AI-extracted disclosures.

Usage:
    from src.db.engine import Session
    from src.db.repositories.event import EventRepository

    with Session() as session:
        repo = EventRepository(session)
        event = repo.upsert_late_filing(
            borrower_id=5,
            event_date=date(2025, 12, 27),
            days_overdue=78,
            last_filed_date=date(2024, 11, 15),
        )
        session.commit()
"""

import logging
from datetime import date
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import Event

logger = logging.getLogger(__name__)

# Valid severity values (mirrors the Event model comment)
SEVERITY_LEVELS = ("low", "medium", "high", "critical")

# Distress score contribution by days overdue
_SCORE_THRESHOLDS = [
    (180, 75),
    (90,  50),
    (30,  25),
    (1,   10),
    (0,    0),
]


def _severity_for_days(days_overdue: int) -> str:
    if days_overdue < 30:
        return "low"
    elif days_overdue < 90:
        return "medium"
    elif days_overdue < 180:
        return "high"
    else:
        return "critical"


class EventRepository:
    """CRUD operations for the events table."""

    def __init__(self, session: Session) -> None:
        self.session = session

    # ------------------------------------------------------------------
    # Late filing events
    # ------------------------------------------------------------------

    def upsert_late_filing(
        self,
        borrower_id: int,
        event_date: date,
        days_overdue: int,
        last_filed_date: Optional[date] = None,
    ) -> Event:
        """
        Create or update a late_filing event for a borrower.

        Idempotent: deduplicates on (borrower_id, event_type='late_filing', event_date).
        If an event already exists for this deadline, updates its summary and severity.

        Args:
            borrower_id:     Borrower primary key.
            event_date:      The filing deadline date (FYE + 180 days).
            days_overdue:    How many days past the deadline.
            last_filed_date: Most recent financial_statement posted_date (or None).

        Returns:
            The upserted Event instance (not yet committed — caller must commit).
        """
        severity = _severity_for_days(days_overdue)

        if last_filed_date:
            summary = (
                f"Annual financial disclosure overdue by {days_overdue} day(s). "
                f"Deadline: {event_date}. "
                f"Last filing posted: {last_filed_date}."
            )
        else:
            summary = (
                f"Annual financial disclosure overdue by {days_overdue} day(s). "
                f"Deadline: {event_date}. "
                f"No financial statements on record."
            )

        # Check for existing event (idempotent dedup)
        stmt = select(Event).where(
            Event.borrower_id == borrower_id,
            Event.event_type == "late_filing",
            Event.event_date == event_date,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            # Update in place — days_overdue and summary change as time passes
            existing.severity = severity
            existing.summary = summary
            existing.detected_date = date.today()
            logger.debug(
                "Updated late_filing event for borrower %d (deadline=%s, %dd overdue)",
                borrower_id, event_date, days_overdue,
            )
            return existing

        event = Event(
            borrower_id=borrower_id,
            event_type="late_filing",
            event_date=event_date,
            detected_date=date.today(),
            severity=severity,
            summary=summary,
            confirmed=False,
        )
        self.session.add(event)
        logger.info(
            "Created late_filing event for borrower %d (deadline=%s, %dd overdue, severity=%s)",
            borrower_id, event_date, days_overdue, severity,
        )
        return event

    def resolve_late_filing(self, borrower_id: int, event_date: date) -> Optional[Event]:
        """
        Mark a late_filing event as confirmed=True (i.e. the filing was received).
        Returns the updated event, or None if not found.
        """
        stmt = select(Event).where(
            Event.borrower_id == borrower_id,
            Event.event_type == "late_filing",
            Event.event_date == event_date,
        )
        event = self.session.execute(stmt).scalar_one_or_none()
        if event:
            event.confirmed = True
            logger.info(
                "Resolved late_filing event for borrower %d (deadline=%s)",
                borrower_id, event_date,
            )
        return event

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def list_for_borrower(
        self,
        borrower_id: int,
        event_type: Optional[str] = None,
        unconfirmed_only: bool = False,
    ) -> list[Event]:
        """
        Return all events for a borrower, newest first.

        Args:
            borrower_id:     Borrower primary key.
            event_type:      Filter to one event type (e.g. "late_filing").
            unconfirmed_only: If True, exclude confirmed=True events.
        """
        stmt = (
            select(Event)
            .where(Event.borrower_id == borrower_id)
            .order_by(Event.event_date.desc())
        )
        if event_type:
            stmt = stmt.where(Event.event_type == event_type)
        if unconfirmed_only:
            stmt = stmt.where(Event.confirmed == False)  # noqa: E712

        return list(self.session.execute(stmt).scalars())

    def list_late_filings(self, unconfirmed_only: bool = True) -> list[Event]:
        """
        Return all late_filing events across all borrowers, newest deadline first.

        Args:
            unconfirmed_only: If True (default), exclude resolved events.
        """
        stmt = (
            select(Event)
            .where(Event.event_type == "late_filing")
            .order_by(Event.event_date.desc())
        )
        if unconfirmed_only:
            stmt = stmt.where(Event.confirmed == False)  # noqa: E712

        return list(self.session.execute(stmt).scalars())

    def count_for_borrower(self, borrower_id: int) -> dict[str, int]:
        """Return {event_type: count} breakdown for a borrower."""
        from sqlalchemy import func
        stmt = (
            select(Event.event_type, func.count().label("n"))
            .where(Event.borrower_id == borrower_id)
            .group_by(Event.event_type)
        )
        rows = self.session.execute(stmt).all()
        return {row.event_type: row.n for row in rows}
