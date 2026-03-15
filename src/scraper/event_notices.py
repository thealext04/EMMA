"""
event_notices.py — Material Event Notice fetcher.

Queries /api/Search/EventNotice for new material event filings across EMMA.
This is used by both:
  1. Watchlist monitoring — check for notices on specific issues
  2. Market-wide scan (Phase 5) — scan all new notices for distress signals

Material event notices are the highest-signal distress indicators in EMMA.
Examples: covenant violations, rating changes, payment defaults, bankruptcies.

NOTE: The /api/Search/EventNotice endpoint has NOT been validated via live
testing as of 2026-03-15. It may return 404 or require different parameters,
similar to how /api/Search/Issue was found to return 404. Before relying on
this module in production, verify the endpoint manually and update the
implementation if needed (e.g., by scraping the HTML search results page
rather than expecting a JSON API response).

Usage:
    from src.scraper.event_notices import fetch_event_notices, HIGH_SIGNAL_TYPES

    notices = fetch_event_notices(session, days_back=7)
    distress = [n for n in notices if n.is_high_signal]
"""

import json
import logging
import re
from datetime import date, datetime, timedelta
from typing import Optional
from urllib.parse import urljoin

import requests

from src.scraper.cache import cached_get, TTL_CONTINUING_DISCLOSURE
from src.scraper.models import EventNotice
from src.scraper.retry import fetch_with_retry

logger = logging.getLogger(__name__)

EMMA_BASE = "https://emma.msrb.org"
EVENT_NOTICE_URL = EMMA_BASE + "/api/Search/EventNotice"

DEFAULT_PAGE_SIZE = 25
MAX_PAGES = 20

# High-signal event types — any match triggers a distress flag
HIGH_SIGNAL_TYPES: set[str] = {
    "covenant violation",
    "covenant waiver",
    "forbearance agreement",
    "forbearance",
    "going concern",
    "payment default",
    "failure to pay",
    "failure to provide",
    "bankruptcy",
    "insolvency",
    "debt restructuring",
    "restructuring",
    "rating withdrawal",
    "rating suspension",
    "liquidity facility termination",
    "tender offer",
    "unscheduled draw",
    "defeasance",
}

# Medium-signal event types — notable but not immediately alarming
MEDIUM_SIGNAL_TYPES: set[str] = {
    "rating change",
    "downgrade",
    "outlook change",
    "amendment",
    "material change",
    "substitution",
}


def fetch_event_notices(
    session: requests.Session,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
    state: Optional[str] = None,
    event_type: Optional[str] = None,
    days_back: int = 7,
    use_cache: bool = True,
) -> list[EventNotice]:
    """
    Fetch material event notices from EMMA.

    Args:
        session:    Active requests.Session.
        from_date:  Start date filter. Defaults to `days_back` days ago.
        to_date:    End date filter. Defaults to today.
        state:      Two-letter state code filter (optional).
        event_type: Specific event type filter (optional).
        days_back:  How many days back to search if from_date not provided.
        use_cache:  Whether to use response cache (disable for real-time scans).

    Returns:
        List of EventNotice objects, newest first.
        High-signal notices have is_high_signal=True.
    """
    if to_date is None:
        to_date = date.today()
    if from_date is None:
        from_date = to_date - timedelta(days=days_back)

    all_notices: list[EventNotice] = []
    page = 1

    while page <= MAX_PAGES:
        params: dict = {
            "fromDate": from_date.strftime("%Y-%m-%d"),
            "toDate": to_date.strftime("%Y-%m-%d"),
            "page": page,
            "pageSize": DEFAULT_PAGE_SIZE,
        }
        if state:
            params["state"] = state.upper()
        if event_type:
            params["eventType"] = event_type

        try:
            content = cached_get(
                session,
                EVENT_NOTICE_URL,
                ttl_hours=TTL_CONTINUING_DISCLOSURE,
                params=params,
                bypass=not use_cache,
            )
        except requests.RequestException as exc:
            logger.error("Event notice fetch failed (page %d): %s", page, exc)
            break

        notices, total_count = _parse_event_notices_response(content)

        if not notices:
            break

        all_notices.extend(notices)
        logger.info(
            "Event notices: page %d — %d notices (total reported: %d)",
            page, len(notices), total_count,
        )

        if len(all_notices) >= total_count or len(notices) < DEFAULT_PAGE_SIZE:
            break

        page += 1

    # Tag high-signal notices
    for notice in all_notices:
        notice.is_high_signal = _is_high_signal(notice.event_type)

    high_count = sum(1 for n in all_notices if n.is_high_signal)
    logger.info(
        "Event notices: %d total (%d high-signal) from %s to %s",
        len(all_notices), high_count, from_date, to_date,
    )

    return all_notices


def fetch_event_notices_for_issue(
    session: requests.Session,
    issue_id: str,
    days_back: int = 365,
) -> list[EventNotice]:
    """
    Fetch all event notices for a specific bond issue.
    Used for watchlist monitoring of individual borrowers.
    """
    # Note: EMMA may not support issue_id filtering on this endpoint.
    # We fetch broadly and filter by issue_id after.
    from_date = date.today() - timedelta(days=days_back)
    all_notices = fetch_event_notices(session, from_date=from_date, use_cache=False)
    return [n for n in all_notices if n.issue_id == issue_id]


def filter_high_signal(notices: list[EventNotice]) -> list[EventNotice]:
    """Return only high-signal distress notices from a list."""
    return [n for n in notices if n.is_high_signal]


def _is_high_signal(event_type: str) -> bool:
    """Return True if the event type matches any known high-signal keyword."""
    lower = event_type.lower()
    return any(signal in lower for signal in HIGH_SIGNAL_TYPES)


# ---------------------------------------------------------------------------
# Response parsers
# ---------------------------------------------------------------------------

def _parse_event_notices_response(
    content: str,
) -> tuple[list[EventNotice], int]:
    """Parse JSON response from /api/Search/EventNotice."""
    try:
        data = json.loads(content)
    except json.JSONDecodeError as exc:
        logger.error("Failed to parse event notices as JSON: %s", exc)
        return [], 0

    items = (
        data.get("hits")
        or data.get("results")
        or data.get("notices")
        or data.get("data")
        or (data if isinstance(data, list) else [])
    )

    total_count = (
        data.get("totalCount")
        or data.get("total")
        or len(items)
    )

    notices: list[EventNotice] = []
    for item in items:
        if isinstance(item, dict):
            n = _parse_notice_item(item)
            if n:
                notices.append(n)

    notices.sort(key=lambda n: n.posted_date or datetime.min, reverse=True)
    return notices, int(total_count)


def _parse_notice_item(item: dict) -> Optional[EventNotice]:
    """Parse one event notice from the response array."""
    norm = {k.lower(): v for k, v in item.items()}

    notice_id = str(
        norm.get("noticeid") or norm.get("id") or norm.get("documentid") or ""
    )
    if not notice_id:
        return None

    issuer_name = str(
        norm.get("issuername") or norm.get("issuer") or "Unknown Issuer"
    ).strip()

    issue_id = str(norm.get("issueid") or norm.get("issue_id") or "").strip() or None

    event_type = str(
        norm.get("eventtype")
        or norm.get("event_type")
        or norm.get("type")
        or norm.get("category")
        or "Unknown"
    ).strip()

    title = str(
        norm.get("title") or norm.get("description") or norm.get("name") or event_type
    ).strip()

    doc_url = (
        norm.get("url")
        or norm.get("documenturl")
        or norm.get("docurl")
    )
    if doc_url and not str(doc_url).startswith("http"):
        doc_url = urljoin(EMMA_BASE, str(doc_url))

    state = str(norm.get("state") or norm.get("statecode") or "").upper() or None

    event_date = _parse_date(str(norm.get("eventdate") or norm.get("date") or ""))
    posted_date = _parse_datetime(str(norm.get("posteddate") or norm.get("createdate") or ""))

    return EventNotice(
        notice_id=notice_id,
        issuer_name=issuer_name,
        issue_id=issue_id,
        event_type=event_type,
        event_date=event_date,
        posted_date=posted_date,
        title=title,
        doc_url=str(doc_url) if doc_url else None,
        state=state,
        is_high_signal=False,  # Set by caller after all items parsed
    )


# ---------------------------------------------------------------------------
# Date utilities
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> Optional[date]:
    raw = raw.strip()
    if not raw:
        return None

    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        except ValueError:
            pass

    return None


def _parse_datetime(raw: str) -> Optional[datetime]:
    raw = raw.strip()
    if not raw:
        return None

    m = re.match(r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2})", raw)
    if m:
        try:
            return datetime.fromisoformat(f"{m.group(1)}T{m.group(2)}")
        except ValueError:
            pass

    d = _parse_date(raw)
    if d:
        return datetime(d.year, d.month, d.day)

    if re.match(r"^\d{10,13}$", raw):
        ts = int(raw)
        if ts > 1e10:
            ts //= 1000
        try:
            return datetime.utcfromtimestamp(ts)
        except (ValueError, OSError):
            pass

    return None
