"""
models.py — Dataclass models for all structured EMMA scraper data.

All fields use type annotations. Optional fields reflect genuine uncertainty
from the EMMA data source — do not assume they will always be populated.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Search & Discovery
# ---------------------------------------------------------------------------

@dataclass
class IssuerSearchResult:
    """
    One result row returned by the EMMA QuickSearch endpoint.

    Lightweight — only metadata returned by the search endpoint.

    Scoring fields (populated by borrower_search, not issue_search):
        match_confidence  — 0.0–1.0. Fraction of borrower name key-tokens
                            that appear as whole words in issue_name.
                            1.0 = all tokens matched. 0.0 = no tokens matched.
        match_reason      — Human-readable explanation of the confidence score.
        potentially_matured — True when the age heuristic suggests this issue
                              may be fully matured or called. See
                              borrower_search._estimate_maturity() for details.
    """
    issue_id: str                          # EMMA internal issue ID
    issuer_name: str
    issue_name: str                        # Bond series name / description
    state: Optional[str]
    bond_type: Optional[str]
    par_amount: Optional[float]            # Original par amount in dollars
    issue_date: Optional[date]
    emma_url: str                          # Full EMMA URL for this issue
    # --- Populated by borrower_search (defaults keep raw search results valid) ---
    match_confidence: float = 1.0
    match_reason: str = ""
    potentially_matured: bool = False


@dataclass
class CUSIPDetail:
    """
    One CUSIP within a bond issue. Each maturity has its own CUSIP.
    """
    cusip: str
    maturity_date: Optional[date]
    coupon: Optional[float]                # Annual coupon rate (e.g., 0.045 for 4.5%)
    par_amount: Optional[float]
    rating_sp: Optional[str]              # S&P rating
    rating_moodys: Optional[str]          # Moody's rating
    rating_fitch: Optional[str]           # Fitch rating


@dataclass
class BondIssueDetail:
    """
    Full metadata for a bond issue from /IssueView/Details/{issueId}.
    This is where the borrower → issuer → CUSIP relationship is established.
    """
    issue_id: str
    series_name: str
    issuer_name: str
    issuer_id: Optional[str]
    borrower_name: Optional[str]           # Conduit obligor — the credit entity
    issue_date: Optional[date]
    dated_date: Optional[date]
    settlement_date: Optional[date]
    par_amount: Optional[float]
    bond_type: Optional[str]
    tax_status: Optional[str]             # Tax-exempt, taxable, AMT
    continuing_disclosure_url: Optional[str]
    cusips: list[CUSIPDetail] = field(default_factory=list)
    raw_html_path: Optional[str] = None   # Path to cached raw HTML for reprocessing


# ---------------------------------------------------------------------------
# Disclosure Documents
# ---------------------------------------------------------------------------

@dataclass
class DisclosureDocument:
    """
    One document from /IssueView/ContinuingDisclosure/{issueId}.
    Represents a single filing in EMMA's continuing disclosure system.
    """
    doc_id: str                            # EMMA document ID (from URL or parsed)
    issue_id: str
    doc_type: str                          # Financial Statement, Event Notice, etc.
    doc_date: Optional[date]              # Date of the document (e.g., fiscal year end)
    posted_date: Optional[datetime]       # Date/time posted to EMMA
    title: str
    doc_url: str                           # Direct URL to PDF
    submitter: Optional[str]
    file_size_kb: Optional[int] = None
    local_path: Optional[str] = None      # Set after download


# ---------------------------------------------------------------------------
# Download Queue
# ---------------------------------------------------------------------------

@dataclass
class QueueItem:
    """
    One item in the document download queue.
    Stored as JSON in data/queue/queue.json between runs.
    """
    doc_id: str
    doc_url: str
    issue_id: str
    borrower_name: str
    doc_type: str
    doc_date: Optional[str]               # ISO date string (YYYY-MM-DD)
    discovered_at: str                    # ISO datetime string
    status: str                           # pending | downloading | downloaded | failed
    priority: int = 5                     # Lower = higher priority (1–10)
    attempts: int = 0
    local_path: Optional[str] = None
    error: Optional[str] = None

    def is_retryable(self, max_attempts: int = 3) -> bool:
        return self.status == "failed" and self.attempts < max_attempts


# ---------------------------------------------------------------------------
# Event Notices
# ---------------------------------------------------------------------------

@dataclass
class EventNotice:
    """
    One material event notice from /api/Search/EventNotice.
    High-signal distress indicators.
    """
    notice_id: str
    issuer_name: str
    issue_id: Optional[str]
    event_type: str                        # E.g., "Covenant Violation", "Rating Change"
    event_date: Optional[date]
    posted_date: Optional[datetime]
    title: str
    doc_url: Optional[str]
    state: Optional[str]
    is_high_signal: bool = False           # Flagged if event_type matches distress keywords


# ---------------------------------------------------------------------------
# Trade Data
# ---------------------------------------------------------------------------

@dataclass
class TradeRecord:
    """
    One trade from /TradeHistory/{cusip}.
    Used to detect price distress signals.
    """
    cusip: str
    trade_date: date
    reported_yield: Optional[float]       # As decimal (e.g., 0.055 for 5.5%)
    price: Optional[float]
    par_amount_traded: Optional[float]
    trade_type: Optional[str]             # Customer Buy, Customer Sell, Dealer


# ---------------------------------------------------------------------------
# Run Metrics
# ---------------------------------------------------------------------------

@dataclass
class RunMetrics:
    """
    Metrics collected during a single scraper run.
    Logged to structured output for monitoring.
    """
    run_date: str                          # ISO datetime
    issues_checked: int = 0
    new_documents_discovered: int = 0
    documents_queued: int = 0
    documents_downloaded: int = 0
    download_failures: int = 0
    request_count: int = 0
    http_429_count: int = 0               # If > 0: reduce rate immediately
    http_503_count: int = 0
    avg_response_ms: float = 0.0

    def request_success_rate(self) -> float:
        """Returns success rate as a fraction (0.0–1.0). Alert if below 0.95."""
        failures = self.http_429_count + self.http_503_count + self.download_failures
        if self.request_count == 0:
            return 1.0
        return max(0.0, (self.request_count - failures) / self.request_count)
