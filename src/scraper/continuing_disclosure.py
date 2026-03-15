"""
continuing_disclosure.py — Continuing Disclosure document list fetcher.

NOTE: The /IssueView/ContinuingDisclosure/{issueId} endpoint returns 404.
The correct approach is to fetch /IssueView/Details/{issueId} (the same page
used by issue_details.py) and extract all PDF <a> links from the HTML.
All disclosure documents — financial filings and event notices — are embedded
directly in that page as relative PDF links.

This is the core monitoring endpoint — called daily for watchlist borrowers.

Key features:
  - Incremental update: stops collecting documents posted on or before
    last_seen_date (using doc_date as a proxy since HTML has no explicit
    posted_date)
  - Skips "(Archived)" links (superseded document versions)
  - Parses doc_type and doc_date from link text
  - Generates a stable doc_id from the PDF filename

PDF link text examples observed on EMMA:
  "Financial Operating Filing (323 KB)"
  "Event Filing as of 01/21/2026 (111 KB)"
  "Event Filing dated 11/20/2020 (332 KB)"

PDF URL format examples:
  /P22014304-P21534633-P21991287.pdf   (current 3-part format)
  /RE1375171-RE1067927-RE1477956.pdf   (older RE-prefix format)
  /SS1393115-SS1083841-SS1491874.pdf   (older SS-prefix format)
  /ER1323887-ER1031755-ER1438880.pdf   (older ER-prefix format)

Usage:
    from src.scraper.continuing_disclosure import fetch_disclosure_documents
    from datetime import datetime

    docs = fetch_disclosure_documents(session, issue_id="ABC123")
    new_docs = fetch_disclosure_documents(
        session, issue_id="ABC123",
        last_seen_date=datetime(2026, 1, 1)
    )
"""

import logging
import re
from datetime import date, datetime
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from src.scraper.cache import cached_get, TTL_CONTINUING_DISCLOSURE
from src.scraper.models import DisclosureDocument

logger = logging.getLogger(__name__)

EMMA_BASE = "https://emma.msrb.org"

# Real working endpoint: the Issue Details page embeds all disclosure PDF links.
# /IssueView/ContinuingDisclosure/{issueId} returns 404 — do not use it.
ISSUE_DETAILS_URL = EMMA_BASE + "/IssueView/Details/{issue_id}"

# Known high-signal document types that trigger immediate distress review
HIGH_SIGNAL_TYPES = {
    "material event notice",
    "event notice",
    "failure to pay",
    "covenant violation",
    "bankruptcy",
    "rating change",
    "default",
    "forbearance",
    "going concern",
}


def fetch_disclosure_documents(
    session: requests.Session,
    issue_id: str,
    last_seen_date: Optional[datetime] = None,
    use_cache: bool = True,
) -> list[DisclosureDocument]:
    """
    Fetch all (or new) continuing disclosure documents for a bond issue.

    Fetches /IssueView/Details/{issueId} and extracts all PDF <a> links
    embedded in the page. Archived (superseded) documents are skipped.

    Args:
        session:        Active requests.Session (must have Disclaimer6 cookie set).
        issue_id:       EMMA internal issue ID.
        last_seen_date: If provided, stop collecting documents whose doc_date is
                        on or before this date. This implements incremental updates.
                        Note: EMMA's Details page does not expose an explicit
                        posted_date in the HTML, so doc_date (parsed from the
                        link text) is used as a proxy.
        use_cache:      Whether to use the 24-hour response cache.
                        Set to False for real-time monitoring runs.

    Returns:
        List of DisclosureDocument objects, newest first.
        If last_seen_date is provided, only documents newer than that date.
    """
    url = ISSUE_DETAILS_URL.format(issue_id=issue_id)

    try:
        html = cached_get(
            session,
            url,
            ttl_hours=TTL_CONTINUING_DISCLOSURE,
            bypass=not use_cache,
        )
    except requests.RequestException as exc:
        logger.error("Failed to fetch disclosure list for issue %s: %s", issue_id, exc)
        return []

    docs = _extract_pdf_links(html, issue_id)

    # Apply incremental filter using doc_date as posted_date proxy
    if last_seen_date is not None:
        new_docs: list[DisclosureDocument] = []
        for doc in docs:
            # Use posted_date if set; fall back to doc_date converted to datetime
            comparison_dt: Optional[datetime] = doc.posted_date
            if comparison_dt is None and doc.doc_date is not None:
                comparison_dt = datetime(doc.doc_date.year, doc.doc_date.month, doc.doc_date.day)

            if comparison_dt is not None and comparison_dt <= last_seen_date:
                logger.debug(
                    "Incremental stop at %s (last_seen: %s) for issue %s",
                    comparison_dt,
                    last_seen_date,
                    issue_id,
                )
                break
            new_docs.append(doc)
        docs = new_docs

    logger.info(
        "Issue %s — %d disclosure documents%s",
        issue_id,
        len(docs),
        f" (new since {last_seen_date.date()})" if last_seen_date else "",
    )
    return docs


def get_latest_posted_date(docs: list[DisclosureDocument]) -> Optional[datetime]:
    """
    Return the most recent posted_date from a list of documents.

    Since the Details page does not expose a true posted_date, this returns
    the most recent doc_date converted to datetime (used as a proxy cursor).

    Use this to update the last_seen_date cursor after a successful fetch.
    """
    # Prefer explicit posted_date; fall back to doc_date
    candidates: list[datetime] = []
    for d in docs:
        if d.posted_date is not None:
            candidates.append(d.posted_date)
        elif d.doc_date is not None:
            candidates.append(datetime(d.doc_date.year, d.doc_date.month, d.doc_date.day))
    return max(candidates) if candidates else None


# ---------------------------------------------------------------------------
# PDF link extraction from /IssueView/Details HTML
# ---------------------------------------------------------------------------

def _extract_pdf_links(html: str, issue_id: str) -> list[DisclosureDocument]:
    """
    Extract all disclosure document PDF links from an /IssueView/Details page.

    EMMA embeds PDF links as <a href="/Pxxx-Pyyy-Pzzz.pdf">Link Text</a>.
    The link text encodes both the document type and the document date.

    Links containing "(Archived)" in their text are skipped — these are
    superseded versions of documents that have been replaced by later filings.

    Results are sorted newest first by doc_date (None-dated docs go last).
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find all <a> tags whose href ends in .pdf (case-insensitive)
    pdf_links = soup.find_all("a", href=re.compile(r"\.pdf$", re.I))

    docs: list[DisclosureDocument] = []
    seen_doc_ids: set[str] = set()

    for link in pdf_links:
        href = link.get("href", "")
        link_text = link.get_text(strip=True)

        # Skip archived (superseded) documents
        if "(Archived)" in link_text or "(archived)" in link_text:
            logger.debug("Skipping archived document: %s", link_text)
            continue

        # Make URL absolute
        if not href.startswith("http"):
            doc_url = urljoin(EMMA_BASE, href)
        else:
            doc_url = href

        # Derive a stable doc_id from the PDF filename (e.g., "P22014304-P21534633-P21991287")
        filename_match = re.search(r"/([^/]+)\.pdf$", href, re.I)
        if filename_match:
            doc_id = filename_match.group(1)
        else:
            # Fallback: hash the URL for a stable ID
            import hashlib
            doc_id = hashlib.md5(doc_url.encode()).hexdigest()[:16]

        # Deduplicate by doc_id
        if doc_id in seen_doc_ids:
            continue
        seen_doc_ids.add(doc_id)

        doc_type = _classify_doc_type(link_text)
        doc_date = _extract_doc_date(link_text)

        docs.append(DisclosureDocument(
            doc_id=doc_id,
            issue_id=issue_id,
            doc_type=doc_type,
            doc_date=doc_date,
            # The Details page HTML does not expose a separate posted_date.
            # doc_date (from link text) is used as the incremental cursor proxy.
            posted_date=None,
            title=link_text,
            doc_url=doc_url,
            submitter=None,
        ))

    # Sort newest first: docs with doc_date before docs without, then by date desc
    docs.sort(
        key=lambda d: d.doc_date or date.min,
        reverse=True,
    )

    logger.debug(
        "Extracted %d PDF disclosure links for issue %s", len(docs), issue_id
    )
    return docs


def _classify_doc_type(link_text: str) -> str:
    """
    Determine the document type from the PDF link text.

    EMMA link text patterns observed:
        "Financial Operating Filing (323 KB)"      → "Financial Operating Filing"
        "Event Filing as of 01/21/2026 (111 KB)"  → "Event Filing"
        "Event Filing dated 11/20/2020 (332 KB)"  → "Event Filing"
        "Annual Report (456 KB)"                   → "Annual Report"

    Falls back to "Other" if no known pattern matches.
    """
    text_lower = link_text.lower()

    if "financial operating" in text_lower:
        return "Financial Operating Filing"
    if "event filing" in text_lower or "event notice" in text_lower:
        return "Event Filing"
    if "annual report" in text_lower:
        return "Annual Report"
    if "audited" in text_lower or "audit" in text_lower:
        return "Audited Financial Statement"
    if "budget" in text_lower:
        return "Budget Filing"
    if "rating" in text_lower:
        return "Rating Notice"
    if "material" in text_lower:
        return "Material Event Notice"

    # Strip file size suffix (e.g., " (323 KB)") and use as-is if something remains
    clean = re.sub(r"\s*\(\d+\s*KB\)\s*$", "", link_text, flags=re.I).strip()
    # Strip date suffixes from the cleaned text
    clean = re.sub(r"\s+(?:as of|dated)\s+\d{1,2}/\d{1,2}/\d{4}", "", clean, flags=re.I).strip()
    return clean or "Other"


def _extract_doc_date(link_text: str) -> Optional[date]:
    """
    Extract a document date from PDF link text.

    EMMA link text contains date strings in these formats:
        "Event Filing as of 01/21/2026 (111 KB)"   → date(2026, 1, 21)
        "Event Filing dated 11/20/2020 (332 KB)"   → date(2020, 11, 20)

    Returns None if no date pattern is found.
    """
    # Pattern: "as of MM/DD/YYYY" or "dated MM/DD/YYYY"
    m = re.search(r"(?:as of|dated)\s+(\d{1,2}/\d{1,2}/\d{4})", link_text, re.I)
    if m:
        return _parse_date(m.group(1))

    # Fallback: any bare MM/DD/YYYY in the text
    m = re.search(r"\b(\d{1,2}/\d{1,2}/\d{4})\b", link_text)
    if m:
        return _parse_date(m.group(1))

    return None


# ---------------------------------------------------------------------------
# Date utilities
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> Optional[date]:
    """Parse a date string. Returns None on failure."""
    raw = raw.strip()
    if not raw:
        return None

    # ISO: YYYY-MM-DD
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # US: MM/DD/YYYY
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        except ValueError:
            pass

    return None


def _parse_datetime(raw: str) -> Optional[datetime]:
    """Parse a datetime string. Returns None on failure."""
    raw = raw.strip()
    if not raw:
        return None

    # ISO datetime
    m = re.match(r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2})", raw)
    if m:
        try:
            return datetime.fromisoformat(f"{m.group(1)}T{m.group(2)}")
        except ValueError:
            pass

    # Date only — convert to midnight datetime
    d = _parse_date(raw)
    if d:
        return datetime(d.year, d.month, d.day)

    return None
