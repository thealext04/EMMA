"""
continuing_disclosure.py — Continuing Disclosure document list fetcher.

Fetches /IssueView/ContinuingDisclosure/{issueId} and returns all
disclosure documents for a bond issue.

This is the core monitoring endpoint — called daily for watchlist borrowers.

Key features:
  - Incremental update: stops parsing when a document is older than last_seen_date
  - Handles both JSON API responses and HTML-rendered pages
  - Returns structured DisclosureDocument objects

Usage:
    from src.scraper.continuing_disclosure import fetch_disclosure_documents
    from datetime import datetime

    docs = fetch_disclosure_documents(session, issue_id="ABC123")
    new_docs = fetch_disclosure_documents(
        session, issue_id="ABC123",
        last_seen_date=datetime(2026, 1, 1)
    )
"""

import json
import logging
import re
from datetime import date, datetime
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from src.scraper.cache import cached_get, TTL_CONTINUING_DISCLOSURE
from src.scraper.models import DisclosureDocument
from src.scraper.retry import fetch_with_retry

logger = logging.getLogger(__name__)

EMMA_BASE = "https://emma.msrb.org"
DISCLOSURE_URL = EMMA_BASE + "/IssueView/ContinuingDisclosure/{issue_id}"

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

    Args:
        session:        Active requests.Session.
        issue_id:       EMMA internal issue ID.
        last_seen_date: If provided, stop collecting documents posted on or
                        before this date. This implements incremental updates.
        use_cache:      Whether to use the 24-hour response cache.
                        Set to False for real-time monitoring runs.

    Returns:
        List of DisclosureDocument objects, newest first.
        If last_seen_date is provided, only documents newer than that date.
    """
    url = DISCLOSURE_URL.format(issue_id=issue_id)

    try:
        content = cached_get(
            session,
            url,
            ttl_hours=TTL_CONTINUING_DISCLOSURE,
            bypass=not use_cache,
        )
    except requests.RequestException as exc:
        logger.error("Failed to fetch disclosure list for issue %s: %s", issue_id, exc)
        return []

    # Try JSON parse first, then fall back to HTML
    docs = _try_parse_as_json(content, issue_id)
    if docs is None:
        docs = _parse_html_disclosure_page(content, issue_id)

    # Apply incremental filter
    if last_seen_date is not None:
        new_docs: list[DisclosureDocument] = []
        for doc in docs:
            if doc.posted_date and doc.posted_date <= last_seen_date:
                logger.debug(
                    "Incremental stop at %s (last_seen: %s) for issue %s",
                    doc.posted_date,
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
    Use this to update the last_seen_date cursor after a successful fetch.
    """
    dated = [d.posted_date for d in docs if d.posted_date is not None]
    return max(dated) if dated else None


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def _try_parse_as_json(
    content: str,
    issue_id: str,
) -> Optional[list[DisclosureDocument]]:
    """
    Attempt to parse the response as JSON.
    Returns None if content is not valid JSON (signals HTML fallback needed).
    """
    stripped = content.strip()
    if not stripped.startswith(("{", "[")):
        return None

    try:
        data = json.loads(stripped)
    except json.JSONDecodeError:
        return None

    # Normalize to a list of items
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        items = (
            data.get("documents")
            or data.get("filings")
            or data.get("results")
            or data.get("hits")
            or data.get("data")
            or []
        )
    else:
        return None

    docs: list[DisclosureDocument] = []
    for item in items:
        if isinstance(item, dict):
            doc = _parse_json_document(item, issue_id)
            if doc:
                docs.append(doc)

    # Sort newest first by posted_date
    docs.sort(key=lambda d: d.posted_date or datetime.min, reverse=True)
    return docs


def _parse_json_document(item: dict, issue_id: str) -> Optional[DisclosureDocument]:
    """Parse one document from the JSON response."""
    norm = {k.lower(): v for k, v in item.items()}

    doc_id = (
        str(norm.get("documentid") or norm.get("docid") or norm.get("id") or "")
    )
    if not doc_id:
        return None

    doc_url = (
        norm.get("url")
        or norm.get("documenturl")
        or norm.get("docurl")
        or norm.get("downloadurl")
    )
    if not doc_url:
        # Construct from doc_id if possible
        doc_url = f"{EMMA_BASE}/FileViewer/ViewFile/{doc_id}"

    if not str(doc_url).startswith("http"):
        doc_url = urljoin(EMMA_BASE, str(doc_url))

    doc_type = (
        norm.get("documenttype")
        or norm.get("doctype")
        or norm.get("type")
        or norm.get("category")
        or "Unknown"
    )

    title = (
        norm.get("title")
        or norm.get("description")
        or norm.get("name")
        or str(doc_type)
    )

    doc_date = _parse_date(str(norm.get("documentdate") or norm.get("docdate") or ""))
    posted_date = _parse_datetime(str(norm.get("posteddate") or norm.get("createdate") or ""))

    submitter = norm.get("submitter") or norm.get("submittername") or None

    return DisclosureDocument(
        doc_id=doc_id,
        issue_id=issue_id,
        doc_type=str(doc_type).strip(),
        doc_date=doc_date,
        posted_date=posted_date,
        title=str(title).strip(),
        doc_url=str(doc_url),
        submitter=str(submitter).strip() if submitter else None,
    )


def _parse_html_disclosure_page(
    html: str,
    issue_id: str,
) -> list[DisclosureDocument]:
    """
    Parse the HTML-rendered continuing disclosure page.

    EMMA renders disclosure documents in a table. Each row contains:
    - Document type
    - Document date
    - Posted date
    - Title / description (often a link to the PDF)
    - Submitter name
    """
    soup = BeautifulSoup(html, "html.parser")
    docs: list[DisclosureDocument] = []

    # Find disclosure document tables
    # EMMA typically uses a table with class containing "disclosure" or specific IDs
    doc_tables = _find_disclosure_tables(soup)

    if not doc_tables:
        logger.warning(
            "No disclosure document table found for issue %s — HTML may have changed",
            issue_id,
        )
        return []

    for table in doc_tables:
        rows = table.find_all("tr")
        if not rows:
            continue

        # Detect column positions from header row
        col_map = _detect_columns(rows[0])

        for row in rows[1:]:  # Skip header
            cells = row.find_all(["td", "th"])
            if not cells or len(cells) < 2:
                continue

            doc = _parse_html_row(cells, col_map, issue_id)
            if doc:
                docs.append(doc)

    # Sort newest first
    docs.sort(key=lambda d: d.posted_date or datetime.min, reverse=True)

    logger.debug(
        "HTML parser found %d documents for issue %s", len(docs), issue_id
    )
    return docs


def _find_disclosure_tables(soup: BeautifulSoup) -> list:
    """
    Locate the document listing table(s) on the disclosure page.
    Tries multiple selector strategies.
    """
    # Strategy 1: table with ID or class containing "disclosure" or "document"
    for selector in [
        "table.disclosure-table",
        "table#disclosureDocuments",
        "table.continuing-disclosure",
        "table[id*='document']",
        "table[class*='document']",
        "table[class*='filing']",
    ]:
        found = soup.select(selector)
        if found:
            return found

    # Strategy 2: any table that contains PDF links
    tables_with_pdfs = []
    for table in soup.find_all("table"):
        if table.find("a", href=re.compile(r"\.(pdf|PDF)", re.I)):
            tables_with_pdfs.append(table)
    if tables_with_pdfs:
        return tables_with_pdfs

    # Strategy 3: largest table on the page (last resort)
    all_tables = soup.find_all("table")
    if all_tables:
        largest = max(all_tables, key=lambda t: len(t.find_all("tr")))
        if len(largest.find_all("tr")) > 2:
            return [largest]

    return []


def _detect_columns(header_row) -> dict[str, int]:
    """
    Detect column positions from the table header row.
    Returns a dict mapping logical name → column index.
    """
    col_map: dict[str, int] = {}
    headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]

    for i, h in enumerate(headers):
        if any(k in h for k in ["type", "category", "kind"]):
            col_map.setdefault("doc_type", i)
        elif any(k in h for k in ["document date", "doc date", "filing date", "report date"]):
            col_map.setdefault("doc_date", i)
        elif any(k in h for k in ["posted", "received", "upload"]):
            col_map.setdefault("posted_date", i)
        elif any(k in h for k in ["description", "title", "document", "filing"]):
            col_map.setdefault("title", i)
        elif any(k in h for k in ["submitter", "filer", "submitted by"]):
            col_map.setdefault("submitter", i)

    return col_map


def _parse_html_row(
    cells: list,
    col_map: dict[str, int],
    issue_id: str,
) -> Optional[DisclosureDocument]:
    """Parse one table row into a DisclosureDocument."""
    def cell_text(idx: int) -> str:
        if idx < len(cells):
            return cells[idx].get_text(strip=True)
        return ""

    # Extract PDF link and doc_id from any cell
    doc_url = None
    doc_id = None
    for cell in cells:
        link = cell.find("a", href=True)
        if link:
            href = link["href"]
            if not href.startswith("http"):
                href = urljoin(EMMA_BASE, href)
            doc_url = href
            # Extract doc ID from URL
            m = re.search(r"/([A-Za-z0-9_-]{6,})\.(pdf|PDF)$", href)
            if not m:
                m = re.search(r"[?&]id=([A-Za-z0-9_-]+)", href)
            if not m:
                m = re.search(r"/ViewFile/([A-Za-z0-9_-]+)", href)
            if m:
                doc_id = m.group(1)
            break

    if not doc_url:
        return None

    if not doc_id:
        # Generate a stable ID from the URL
        import hashlib
        doc_id = hashlib.md5(doc_url.encode()).hexdigest()[:12]

    doc_type_text = cell_text(col_map.get("doc_type", 0))
    doc_date = _parse_date(cell_text(col_map.get("doc_date", 1)))
    posted_date = _parse_datetime(cell_text(col_map.get("posted_date", 2)))
    title = cell_text(col_map.get("title", 3)) or doc_type_text
    submitter = cell_text(col_map.get("submitter", 4)) or None

    return DisclosureDocument(
        doc_id=doc_id,
        issue_id=issue_id,
        doc_type=doc_type_text or "Unknown",
        doc_date=doc_date,
        posted_date=posted_date,
        title=title,
        doc_url=doc_url,
        submitter=submitter,
    )


# ---------------------------------------------------------------------------
# Date utilities
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> Optional[date]:
    """Parse a date string. Returns None on failure."""
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

    # Epoch milliseconds
    if re.match(r"^\d{10,13}$", raw):
        ts = int(raw)
        if ts > 1e10:
            ts //= 1000
        try:
            return datetime.utcfromtimestamp(ts)
        except (ValueError, OSError):
            pass

    return None
