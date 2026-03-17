"""
distress/scoring.py — Multi-signal borrower distress scoring.

Aggregates all available signals into a single 0–100 score per borrower.
Scores are stored on borrower.distress_score and borrower.distress_status
so the borrower list always reflects the latest intelligence.

Scoring model:
  going_concern opinion (events table)            → +40 pts
  dscr < 1.0  (most recent annual metric)        → +25 pts
  dscr < 0.8                                     → +35 pts (replaces above)
  late_filing (active event)                     → +10–20 pts (by days overdue)
  negative unrestricted_net_assets               → +10 pts
  3+ consecutive years of enrollment decline     → +10 pts  (higher_ed only)
  event_notice (other event types):
    critical severity                            → +15 pts  (max across notices)
    high severity                                → +10 pts
    medium severity                              → +5 pts

  Score is capped at 100.

distress_status thresholds:
  0–19   → monitor
  20–44  → watch
  45–74  → distressed
  75–100 → critical

Usage:
    from src.db.engine import Session
    from src.distress.scoring import score_all_watchlist, update_borrower_score

    with Session() as session:
        results = score_all_watchlist(session)
        for borrower_id, name, score, status, breakdown in results:
            print(f"{name}: {score} ({status}) — {breakdown}")
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Event types that don't contribute to the "event_notice" signal bucket
_NON_NOTICE_TYPES = frozenset({
    "late_filing", "going_concern", "dscr_breach", "financial_statement_filed",
})


@dataclass
class ScoreBreakdown:
    """Detailed explanation of a borrower's distress score."""
    total: int = 0
    going_concern: int = 0
    dscr: int = 0
    late_filing: int = 0
    negative_net_assets: int = 0
    enrollment_decline: int = 0
    event_notices: int = 0
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, int]:
        return {
            k: v for k, v in {
                "going_concern":      self.going_concern,
                "dscr":               self.dscr,
                "late_filing":        self.late_filing,
                "negative_net_assets": self.negative_net_assets,
                "enrollment_decline": self.enrollment_decline,
                "event_notices":      self.event_notices,
            }.items() if v > 0
        }


def _status_for_score(score: int) -> str:
    """Map a 0–100 score to a distress_status label."""
    if score >= 75:
        return "critical"
    elif score >= 45:
        return "distressed"
    elif score >= 20:
        return "watch"
    return "monitor"


def compute_distress_score(borrower_id: int, session: Session) -> tuple[int, ScoreBreakdown]:
    """
    Compute a distress score (0–100) for one borrower.

    Reads from the events table and extracted_metrics — does not make any
    external calls.  Safe to call frequently; no writes to DB.

    Returns:
        (score, ScoreBreakdown) — the integer score and detailed breakdown.
    """
    from src.db.models import Event, ExtractedMetrics
    from src.db.repositories.metrics import MetricsRepository

    bd = ScoreBreakdown()
    metrics_repo = MetricsRepository(session)

    # ------------------------------------------------------------------
    # Signal 1: Going concern
    # ------------------------------------------------------------------
    gc_events = session.execute(
        select(Event).where(
            Event.borrower_id == borrower_id,
            Event.event_type == "going_concern",
        ).order_by(Event.event_date.desc())
    ).scalars().all()

    if gc_events:
        bd.going_concern = 40
        bd.notes.append(
            f"Going concern opinion (event_date={gc_events[0].event_date})"
        )

    # ------------------------------------------------------------------
    # Signal 2: DSCR (use most recent annual metric row)
    # ------------------------------------------------------------------
    latest_annual = metrics_repo.latest_for_borrower(borrower_id, annual_only=True)

    if latest_annual and latest_annual.dscr is not None:
        dscr = float(latest_annual.dscr)
        if dscr < 0.8:
            bd.dscr = 35
            bd.notes.append(f"DSCR {dscr:.2f}x — below 0.8x (critical)")
        elif dscr < 1.0:
            bd.dscr = 25
            bd.notes.append(f"DSCR {dscr:.2f}x — below 1.0x (breach)")
    else:
        # Fall back to dscr_breach events if no metric row has DSCR
        dscr_events = session.execute(
            select(Event).where(
                Event.borrower_id == borrower_id,
                Event.event_type == "dscr_breach",
            ).order_by(Event.event_date.desc())
        ).scalars().all()
        if dscr_events:
            sev = dscr_events[0].severity or "high"
            bd.dscr = 35 if sev == "critical" else 25
            bd.notes.append(
                f"DSCR breach event (severity={sev}, date={dscr_events[0].event_date})"
            )

    # ------------------------------------------------------------------
    # Signal 3: Late filing
    # ------------------------------------------------------------------
    late_events = session.execute(
        select(Event).where(
            Event.borrower_id == borrower_id,
            Event.event_type == "late_filing",
        ).order_by(Event.event_date.desc())
    ).scalars().all()

    if late_events:
        summary = late_events[0].summary or ""
        match = re.search(r"(\d+)\s+days?\s+overdue", summary, re.IGNORECASE)
        days_overdue = int(match.group(1)) if match else 0

        if days_overdue > 180:
            late_pts = 20
        elif days_overdue > 90:
            late_pts = 15
        elif days_overdue > 30:
            late_pts = 12
        else:
            late_pts = 10

        bd.late_filing = late_pts
        bd.notes.append(f"Late filing ({days_overdue}d overdue)")

    # ------------------------------------------------------------------
    # Signal 4: Negative unrestricted net assets
    # ------------------------------------------------------------------
    if latest_annual and latest_annual.unrestricted_net_assets is not None:
        una = float(latest_annual.unrestricted_net_assets)
        if una < 0:
            bd.negative_net_assets = 10
            bd.notes.append(f"Negative UNA: ${una/1000:.1f}M (FY{latest_annual.fiscal_year})")

    # ------------------------------------------------------------------
    # Signal 5: Enrollment decline (higher-ed only — 3 consecutive years)
    # ------------------------------------------------------------------
    all_metrics = metrics_repo.list_for_borrower(borrower_id, limit=10)
    annual_with_enroll = [
        m for m in all_metrics
        if m.period_type == "annual"
        and m.total_enrollment is not None
        and m.period_end_date is not None
    ]
    annual_with_enroll.sort(key=lambda m: m.period_end_date)

    if len(annual_with_enroll) >= 3:
        recent = annual_with_enroll[-3:]
        e0, e1, e2 = (int(m.total_enrollment) for m in recent)
        if e0 > e1 > e2:
            bd.enrollment_decline = 10
            bd.notes.append(
                f"3-year enrollment decline: {e0:,} → {e1:,} → {e2:,}"
            )

    # ------------------------------------------------------------------
    # Signal 6: Event notice signals (covenant violations, bankruptcies, etc.)
    # ------------------------------------------------------------------
    notice_events = session.execute(
        select(Event).where(
            Event.borrower_id == borrower_id,
            Event.event_type.notin_(list(_NON_NOTICE_TYPES)),
        )
    ).scalars().all()

    notice_pts = 0
    for ev in notice_events:
        sev = ev.severity or "medium"
        if sev == "critical":
            notice_pts = max(notice_pts, 15)
        elif sev == "high":
            notice_pts = max(notice_pts, 10)
        elif sev == "medium":
            notice_pts = max(notice_pts, 5)

    if notice_pts > 0:
        bd.event_notices = notice_pts
        bd.notes.append(
            f"Event notices present (worst severity: "
            f"{'critical' if notice_pts >= 15 else 'high' if notice_pts >= 10 else 'medium'})"
        )

    # ------------------------------------------------------------------
    # Total (capped)
    # ------------------------------------------------------------------
    bd.total = min(
        100,
        bd.going_concern
        + bd.dscr
        + bd.late_filing
        + bd.negative_net_assets
        + bd.enrollment_decline
        + bd.event_notices,
    )

    logger.debug(
        "borrower=%d score=%d breakdown=%s",
        borrower_id, bd.total, bd.as_dict(),
    )
    return bd.total, bd


def update_borrower_score(borrower_id: int, session: Session) -> tuple[int, str]:
    """
    Compute the distress score for one borrower and persist it.

    Writes borrower.distress_score and borrower.distress_status.
    Does NOT call session.commit() — caller is responsible.

    Returns:
        (score, status) — the computed score and distress_status string.
    """
    from src.db.repositories.borrower import BorrowerRepository

    score, bd = compute_distress_score(borrower_id, session)
    status = _status_for_score(score)

    repo = BorrowerRepository(session)
    repo.update_distress_status(borrower_id, status, score)

    logger.info(
        "borrower=%d score=%d status=%s notes=%s",
        borrower_id, score, status, bd.notes,
    )
    return score, status


def score_all_watchlist(session: Session) -> list[tuple]:
    """
    Compute and persist distress scores for all on-watchlist borrowers.
    Commits once after all updates.

    Returns:
        List of (borrower_id, name, score, status, breakdown_dict) sorted by score DESC.
    """
    from src.db.repositories.borrower import BorrowerRepository

    repo = BorrowerRepository(session)
    borrowers = repo.list_watchlist(order_by_score=False)

    results = []
    for b in borrowers:
        try:
            score, bd = compute_distress_score(b.borrower_id, session)
            status = _status_for_score(score)
            repo.update_distress_status(b.borrower_id, status, score)
            results.append((b.borrower_id, b.borrower_name, score, status, bd.as_dict()))
        except Exception as exc:
            logger.warning(
                "Error scoring borrower %d (%s): %s", b.borrower_id, b.borrower_name, exc
            )

    session.commit()
    results.sort(key=lambda r: -r[2])
    return results
