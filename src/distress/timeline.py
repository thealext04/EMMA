"""
distress/timeline.py — Chronological credit event timeline for a borrower.

Combines events table entries with annual financial metric snapshots into a
single sorted narrative — the credit history of an issuer over time.

This is how distress always unfolds in municipal credit:
  Year 1: enrollment drops, revenue misses
  Year 2: net income turns negative, late filing
  Year 3: DSCR breaches covenant, going concern opinion

The timeline makes that trajectory visible at a glance.

Usage:
    from src.db.engine import Session
    from src.distress.timeline import get_borrower_timeline, print_timeline

    with Session() as session:
        entries = get_borrower_timeline(borrower_id=1, session=session)
        print_timeline(borrower_name="Rider University", entries=entries)
"""

import logging
from dataclasses import dataclass
from datetime import date
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Terminal color / severity styling
SEVERITY_LABEL = {
    "critical": "CRITICAL",
    "high":     "HIGH    ",
    "medium":   "MEDIUM  ",
    "low":      "LOW     ",
    "monitor":  "INFO    ",
}

SEVERITY_ICON = {
    "critical": "🔴",
    "high":     "🟠",
    "medium":   "🟡",
    "low":      "🟢",
    "monitor":  "⚪",
}

# Human-readable event type labels
EVENT_TYPE_LABEL = {
    "going_concern":                "Going Concern Opinion",
    "dscr_breach":                  "DSCR Breach",
    "late_filing":                  "Late Filing",
    "covenant_violation":           "Covenant Violation",
    "payment_default":              "Payment Default",
    "rating_change":                "Rating Change",
    "bankruptcy":                   "Bankruptcy / Chapter 9",
    "forbearance":                  "Forbearance Agreement",
    "debt_restructuring":           "Debt Restructuring",
    "liquidity_facility_termination": "Liquidity Facility Terminated",
    "financial_statement_filed":    "Financial Statement Filed",
    "annual_filing":                "Annual Financials",
    "interim_filing":               "Interim Filing",
    "other":                        "Event Notice",
}


@dataclass
class TimelineEntry:
    """One event in the borrower credit timeline."""
    entry_date: date
    event_type: str
    severity: str                        # critical | high | medium | low | monitor
    summary: str
    doc_url: Optional[str] = None
    source: str = "event"               # 'event' | 'metric'

    @property
    def label(self) -> str:
        return EVENT_TYPE_LABEL.get(self.event_type, self.event_type.replace("_", " ").title())


def get_borrower_timeline(
    borrower_id: int,
    session: Session,
    include_metrics: bool = True,
    min_severity: Optional[str] = None,
) -> list[TimelineEntry]:
    """
    Build a chronological credit event timeline for a borrower.

    Combines:
    - All rows in the events table (late_filing, going_concern, dscr_breach, notices)
    - Annual extracted_metrics snapshots (financial summary per FY)

    Args:
        borrower_id:     Borrower primary key.
        session:         SQLAlchemy session.
        include_metrics: Include annual financial metric summaries (default: True).
        min_severity:    If set, filter to events at or above this severity.
                         Order: monitor < low < medium < high < critical.

    Returns:
        List of TimelineEntry sorted chronologically (oldest first).
    """
    from src.db.models import Event, ExtractedMetrics, Document

    entries: list[TimelineEntry] = []
    severity_rank = {"monitor": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}
    min_rank = severity_rank.get(min_severity or "monitor", 0)

    # ------------------------------------------------------------------
    # Events table
    # ------------------------------------------------------------------
    events = session.execute(
        select(Event)
        .where(Event.borrower_id == borrower_id)
        .order_by(Event.event_date)
    ).scalars().all()

    for ev in events:
        sev = ev.severity or "medium"
        if severity_rank.get(sev, 2) < min_rank:
            continue
        entries.append(TimelineEntry(
            entry_date=ev.event_date,
            event_type=ev.event_type,
            severity=sev,
            summary=ev.summary or "",
            source="event",
        ))

    # ------------------------------------------------------------------
    # Annual metric snapshots — one per fiscal year
    # ------------------------------------------------------------------
    if include_metrics:
        rows = session.execute(
            select(ExtractedMetrics, Document.doc_url)
            .join(Document, Document.doc_id == ExtractedMetrics.doc_id, isouter=True)
            .where(
                ExtractedMetrics.borrower_id == borrower_id,
                ExtractedMetrics.period_type == "annual",
                ExtractedMetrics.period_end_date.isnot(None),
            )
            .order_by(ExtractedMetrics.period_end_date)
        ).all()

        for m, doc_url in rows:
            parts: list[str] = []

            if m.total_revenue is not None:
                parts.append(f"Revenue ${float(m.total_revenue)/1000:.1f}M")
            if m.net_income is not None:
                ni = float(m.net_income)
                sign = "+" if ni >= 0 else ""
                parts.append(f"Net income {sign}${ni/1000:.1f}M")
            if m.dscr is not None:
                parts.append(f"DSCR {float(m.dscr):.2f}x")
            if m.days_cash_on_hand is not None:
                parts.append(f"Days cash {float(m.days_cash_on_hand):.0f}")
            if m.total_enrollment is not None:
                parts.append(f"Enrollment {int(m.total_enrollment):,}")
            elif m.fte_enrollment is not None:
                parts.append(f"FTE {float(m.fte_enrollment):.0f}")
            if m.unrestricted_net_assets is not None:
                una = float(m.unrestricted_net_assets)
                sign = "+" if una >= 0 else ""
                parts.append(f"UNA {sign}${una/1000:.1f}M")

            if not parts:
                continue  # no meaningful data to show

            # Infer severity from the metrics themselves
            sev = _infer_metric_severity(m)
            if severity_rank.get(sev, 0) < min_rank:
                continue

            entries.append(TimelineEntry(
                entry_date=m.period_end_date,
                event_type="annual_filing",
                severity=sev,
                summary="  ".join(parts),
                doc_url=doc_url,
                source="metric",
            ))

    # Sort chronologically; within same date put events before metrics
    entries.sort(key=lambda e: (e.entry_date, 0 if e.source == "event" else 1))
    return entries


def _infer_metric_severity(m) -> str:
    """Estimate severity from extracted metric values."""
    if m.dscr is not None and float(m.dscr) < 0.8:
        return "critical"
    if m.dscr is not None and float(m.dscr) < 1.0:
        return "high"
    if m.unrestricted_net_assets is not None and float(m.unrestricted_net_assets) < 0:
        return "medium"
    if m.net_income is not None and float(m.net_income) < 0:
        return "medium"
    return "monitor"


def print_timeline(
    borrower_name: str,
    entries: list[TimelineEntry],
    show_urls: bool = False,
) -> None:
    """
    Print a formatted timeline to stdout.

    Args:
        borrower_name: Display name for the header.
        entries:       List from get_borrower_timeline().
        show_urls:     If True, print source PDF URL under each entry.
    """
    if not entries:
        print(f"\n  No timeline data for {borrower_name}.")
        return

    header = f"{borrower_name.upper()} — Credit Event Timeline"
    print(f"\n{header}")
    print("═" * max(len(header), 70))

    for e in entries:
        icon = SEVERITY_ICON.get(e.severity, "⚪")
        sev_label = SEVERITY_LABEL.get(e.severity, e.severity.upper().ljust(8))
        label = e.label

        # Truncate long summaries to fit terminal
        summary = e.summary
        if len(summary) > 90:
            summary = summary[:87] + "..."

        print(
            f"  {e.entry_date}  {icon} [{sev_label}]  "
            f"{label:<32}  {summary}"
        )
        if show_urls and e.doc_url:
            print(f"               {'':38}  → {e.doc_url}")

    print()
    # Summary counts
    event_count = sum(1 for e in entries if e.source == "event")
    metric_count = sum(1 for e in entries if e.source == "metric")
    critical_count = sum(1 for e in entries if e.severity == "critical")
    high_count = sum(1 for e in entries if e.severity == "high")

    print(
        f"  {len(entries)} entries  |  "
        f"{metric_count} annual filings  |  "
        f"{event_count} events  |  "
        f"{critical_count} critical  |  {high_count} high"
    )
    print()
