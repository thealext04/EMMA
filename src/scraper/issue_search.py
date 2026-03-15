"""
issue_search.py — Issue Search client.

Discovers bond issues on EMMA by borrower/issuer name.

NOTE: The /api/Search/Issue endpoint returns 404. The correct approach is to
use the QuickSearch HTML page, which embeds results as inline JavaScript in a
define("pageData", ...) block. Results are parsed directly from that block —
there is no separate JSON API needed.

Real endpoint: GET /QuickSearch/Results?quickSearchText={name}&cat=desc

The ISSUER field in results (e.g., "NEW JERSEY EDUCATIONAL FACILITIES AUTHORITY")
is the conduit issuer, NOT the borrower. The borrower name appears in IssueDesc
(e.g., "REVENUE BONDS RIDER UNIVERSITY ISSUE 2012 SERIES A"). Callers should
inspect IssueDesc to confirm the borrower.

Usage:
    from src.scraper.issue_search import search_all_pages, get_issue_ids_for_borrower
    from src.scraper.session import EMMAsession

    mgr = EMMAsession()
    session = mgr.get_session()

    results = search_all_pages(session, search_text="Manhattan College")
    for r in results:
        print(r.issue_id, r.issuer_name, r.issue_name)

    issue_ids = get_issue_ids_for_borrower(session, "Rider University")
"""

import json
import logging
import re
from datetime import date
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from src.scraper.cache import cached_get, TTL_SEARCH
from src.scraper.models import IssuerSearchResult
from src.scraper.retry import fetch_with_retry

logger = logging.getLogger(__name__)

EMMA_BASE = "https://emma.msrb.org"

# Real working endpoint (confirmed via live testing).
# The old /api/Search/Issue returns 404.
QUICK_SEARCH_URL = EMMA_BASE + "/QuickSearch/Results"

# EMMA embeds up to ~100 results per QuickSearch page.
# Pagination is generally not needed, but the constant is kept for documentation.
RESULTS_PER_PAGE = 100


def search_issues(
    session: requests.Session,
    search_text: str,
    state: Optional[str] = None,
    use_cache: bool = True,
) -> tuple[list[IssuerSearchResult], int]:
    """
    Search EMMA for bond issues matching a borrower or issuer name.

    Fetches /QuickSearch/Results?quickSearchText={search_text}&cat=desc and
    extracts results from the embedded pageData JavaScript block.

    Args:
        session:     Active requests.Session (must have Disclaimer6 cookie set).
        search_text: Borrower name, issuer name, or keyword to search.
        state:       Two-letter state code filter (optional). Applied client-side
                     since the QuickSearch endpoint does not accept a state param.
        use_cache:   Whether to use the response cache.

    Returns:
        Tuple of (results list, total_count).
        total_count equals len(results) — EMMA embeds all matches in one page.
    """
    params: dict = {
        "quickSearchText": search_text,
        "cat": "desc",  # Search bond descriptions (catches conduit borrowers)
    }

    try:
        content = cached_get(
            session,
            QUICK_SEARCH_URL,
            ttl_hours=TTL_SEARCH,
            params=params,
            bypass=not use_cache,
        )
    except requests.RequestException as exc:
        logger.error("Issue search failed for '%s': %s", search_text, exc)
        return [], 0

    results = _parse_quick_search_page(content, search_text)

    # Apply optional state filter client-side
    if state:
        state_upper = state.upper()
        results = [r for r in results if r.state == state_upper]

    return results, len(results)


def search_all_pages(
    session: requests.Session,
    search_text: str,
    state: Optional[str] = None,
    use_cache: bool = True,
) -> list[IssuerSearchResult]:
    """
    Return all results for a search query.

    EMMA embeds up to ~100 results per QuickSearch page with no pagination
    API, so a single request is sufficient in most cases. This function is
    provided for API compatibility and future-proofing.

    Returns:
        Flat list of all IssuerSearchResult objects.
    """
    results, total_count = search_issues(
        session,
        search_text=search_text,
        state=state,
        use_cache=use_cache,
    )

    logger.info(
        "Search '%s' complete — %d issues found", search_text, len(results)
    )
    return results


def get_issue_ids_for_borrower(
    session: requests.Session,
    borrower_name: str,
    use_cache: bool = True,
) -> list[str]:
    """
    Convenience function: search for a borrower and return just the issue IDs.

    This is the primary entry point for Phase 1 discovery — given a borrower
    name, returns all EMMA issue IDs that can then be passed to
    fetch_issue_details() and fetch_disclosure_documents().

    Args:
        session:       Active requests.Session.
        borrower_name: Name of the borrower (will appear in bond descriptions).
        use_cache:     Whether to use the response cache.

    Returns:
        List of EMMA issue ID strings (e.g., ["AB12345", "CD67890"]).
    """
    results = search_all_pages(session, search_text=borrower_name, use_cache=use_cache)
    issue_ids = [r.issue_id for r in results]
    logger.info(
        "get_issue_ids_for_borrower('%s') → %d issue IDs", borrower_name, len(issue_ids)
    )
    return issue_ids


# ---------------------------------------------------------------------------
# Response parsing — pageData inline JS block
# ---------------------------------------------------------------------------

def _parse_quick_search_page(
    html: str,
    search_text: str,
) -> list[IssuerSearchResult]:
    """
    Parse the /QuickSearch/Results HTML page and extract bond issue results.

    EMMA embeds results in a <script> tag using the AMD define() pattern:

        define("pageData", [], function() {
            var pdata = {};
            pdata.Data = {"Status":"data","Messages":[],"Category":"desc","Data":[...]}
            ...
        });

    We locate the `pdata.Data = {...}` assignment, parse the JSON value,
    and extract the inner "Data" array.

    Each item in the Data array has these relevant fields:
        IssueId     — EMMA internal issue ID (used for all subsequent lookups)
        IssuerName  — Conduit issuer name (NOT the borrower)
        IssuerId    — EMMA internal issuer ID
        IssueDesc   — Bond description string; borrower name appears here
        State       — Two-letter state code
        DatedDate   — Bond dated date (MM/DD/YYYY or YYYY-MM-DD)
        IssueUrl    — Relative URL to the IssueView/Details page

    Returns:
        List of IssuerSearchResult objects. Empty list if parsing fails.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find the script tag containing the pageData define() block
    target_script: Optional[str] = None
    for script in soup.find_all("script"):
        text = script.string or ""
        if "pageData" in text and "pdata.Data" in text:
            target_script = text
            break

    if not target_script:
        logger.warning(
            "Could not find pageData script block in QuickSearch response for '%s' "
            "— page may have changed or returned no results",
            search_text,
        )
        return []

    # Extract the JSON object assigned to pdata.Data
    # Pattern: pdata.Data = {...};   (value may span multiple lines)
    m = re.search(r"pdata\.Data\s*=\s*(\{.*?\});", target_script, re.DOTALL)
    if not m:
        logger.warning(
            "Found pageData block but could not match pdata.Data assignment for '%s'",
            search_text,
        )
        return []

    try:
        outer = json.loads(m.group(1))
    except json.JSONDecodeError as exc:
        logger.error(
            "Failed to parse pdata.Data JSON for '%s': %s", search_text, exc
        )
        return []

    # Validate response status
    status = outer.get("Status", "")
    if status != "data":
        logger.info(
            "QuickSearch returned Status='%s' for '%s' — no results",
            status,
            search_text,
        )
        return []

    items = outer.get("Data")
    if not isinstance(items, list):
        logger.warning(
            "pdata.Data['Data'] is not a list for '%s' — got %s",
            search_text,
            type(items).__name__,
        )
        return []

    results: list[IssuerSearchResult] = []
    for item in items:
        try:
            result = _parse_page_data_item(item)
            if result:
                results.append(result)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to parse pageData item: %s — %s", exc, item)

    logger.debug(
        "QuickSearch '%s' — parsed %d results from pageData", search_text, len(results)
    )
    return results


def _parse_page_data_item(item: dict) -> Optional[IssuerSearchResult]:
    """
    Parse one bond issue item from the pageData.Data array.

    Field mapping:
        IssueId    → issue_id
        IssuerName → issuer_name  (conduit issuer, not the borrower)
        IssueDesc  → issue_name   (bond description; contains borrower name)
        State      → state
        DatedDate  → issue_date
        IssueUrl   → emma_url (relative; will be made absolute)
    """
    issue_id = str(item.get("IssueId") or "").strip()
    if not issue_id:
        logger.debug("Skipping pageData item with no IssueId: %s", item)
        return None

    issuer_name = str(item.get("IssuerName") or "Unknown Issuer").strip()
    issue_name = str(item.get("IssueDesc") or "").strip()
    state = str(item.get("State") or "").strip().upper() or None

    # Parse DatedDate (may be MM/DD/YYYY or YYYY-MM-DD or absent)
    issue_date: Optional[date] = None
    raw_date = str(item.get("DatedDate") or "").strip()
    if raw_date:
        issue_date = _parse_date(raw_date)

    # Build absolute URL from relative IssueUrl
    raw_url = str(item.get("IssueUrl") or "").strip()
    if raw_url:
        emma_url = urljoin(EMMA_BASE, raw_url) if not raw_url.startswith("http") else raw_url
    else:
        emma_url = f"{EMMA_BASE}/IssueView/Details/{issue_id}"

    return IssuerSearchResult(
        issue_id=issue_id,
        issuer_name=issuer_name,
        issue_name=issue_name,
        state=state,
        bond_type=None,       # Not provided in QuickSearch results
        par_amount=None,      # Not provided in QuickSearch results
        issue_date=issue_date,
        emma_url=emma_url,
    )


# ---------------------------------------------------------------------------
# Date utilities
# ---------------------------------------------------------------------------

def _parse_date(raw: str) -> Optional[date]:
    """
    Parse a date string in various formats EMMA might return.
    Returns None on failure rather than raising.
    """
    raw = raw.strip()
    if not raw:
        return None

    # ISO format: YYYY-MM-DD or YYYY-MM-DDT...
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # US format: MM/DD/YYYY
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        except ValueError:
            pass

    # Epoch milliseconds (JavaScript timestamps)
    if re.match(r"^\d{10,13}$", raw):
        from datetime import datetime
        ts = int(raw)
        if ts > 1e10:
            ts //= 1000
        try:
            return datetime.utcfromtimestamp(ts).date()
        except (ValueError, OSError):
            pass

    return None
