"""
issue_search.py — Issue Search API client.

Discovers bond issues on EMMA by issuer name, state, or bond type.
Uses the internal JSON API endpoint: GET /api/Search/Issue

This is the entry point for building the bond universe.
Given a borrower name, returns a list of matching bond issues — including
the EMMA issue ID needed for all subsequent calls.

Usage:
    from src.scraper.issue_search import search_issues, search_all_pages
    from src.scraper.session import EMMAsession

    mgr = EMMAsession()
    session = mgr.get_session()

    results = search_all_pages(session, search_text="Manhattan College")
    for r in results:
        print(r.issue_id, r.issuer_name, r.issue_name)
"""

import logging
from datetime import date
from typing import Optional
from urllib.parse import urljoin

import requests

from src.scraper.cache import cached_get, TTL_SEARCH
from src.scraper.models import IssuerSearchResult
from src.scraper.retry import fetch_with_retry

logger = logging.getLogger(__name__)

EMMA_BASE = "https://emma.msrb.org"
SEARCH_ISSUE_URL = urljoin(EMMA_BASE, "/api/Search/Issue")
SEARCH_ISSUER_URL = urljoin(EMMA_BASE, "/api/Search/Issuer")

DEFAULT_PAGE_SIZE = 25
MAX_PAGES = 40  # 40 × 25 = 1,000 results max per search term


def search_issues(
    session: requests.Session,
    search_text: str,
    state: Optional[str] = None,
    bond_type: Optional[str] = None,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    use_cache: bool = True,
) -> tuple[list[IssuerSearchResult], int]:
    """
    Query /api/Search/Issue for one page of results.

    Args:
        session:     Active requests.Session.
        search_text: Issuer or bond name to search.
        state:       Two-letter state code filter (optional).
        bond_type:   Bond type category filter (optional).
        page:        1-based page number.
        page_size:   Results per page (max 25 recommended).
        use_cache:   Whether to use the response cache.

    Returns:
        Tuple of (results list, total_count).
        total_count is the total matches across all pages.
    """
    params: dict = {
        "searchText": search_text,
        "page": page,
        "pageSize": page_size,
    }
    if state:
        params["state"] = state.upper()
    if bond_type:
        params["category"] = bond_type

    # Build cache key from URL + params
    cache_bypass = not use_cache

    try:
        content = cached_get(
            session,
            SEARCH_ISSUE_URL,
            ttl_hours=TTL_SEARCH,
            params=params,
            bypass=cache_bypass,
        )
    except requests.RequestException as exc:
        logger.error("Issue search failed for '%s': %s", search_text, exc)
        return [], 0

    return _parse_issue_search_response(content, search_text)


def search_all_pages(
    session: requests.Session,
    search_text: str,
    state: Optional[str] = None,
    bond_type: Optional[str] = None,
    use_cache: bool = True,
) -> list[IssuerSearchResult]:
    """
    Paginate through all results for a search query.

    Stops when a page returns fewer results than page_size or
    when MAX_PAGES is reached (safety limit).

    Returns:
        Flat list of all IssuerSearchResult across all pages.
    """
    all_results: list[IssuerSearchResult] = []
    page = 1

    while page <= MAX_PAGES:
        results, total_count = search_issues(
            session,
            search_text=search_text,
            state=state,
            bond_type=bond_type,
            page=page,
            use_cache=use_cache,
        )

        if not results:
            break

        all_results.extend(results)
        logger.info(
            "Search '%s' — page %d: %d results (total reported: %d)",
            search_text, page, len(results), total_count,
        )

        # Stop if we've retrieved everything
        if len(all_results) >= total_count or len(results) < DEFAULT_PAGE_SIZE:
            break

        page += 1

    logger.info(
        "Search '%s' complete — %d total issues found", search_text, len(all_results)
    )
    return all_results


def search_by_issuer(
    session: requests.Session,
    issuer_name: str,
    use_cache: bool = True,
) -> list[IssuerSearchResult]:
    """
    Search the /api/Search/Issuer endpoint to find all bond issues
    for a specific issuer entity.

    This is an alternative discovery path when you know the exact issuer name.
    """
    params = {"searchText": issuer_name}

    try:
        content = cached_get(
            session,
            SEARCH_ISSUER_URL,
            ttl_hours=TTL_SEARCH,
            params=params,
            bypass=not use_cache,
        )
    except requests.RequestException as exc:
        logger.error("Issuer search failed for '%s': %s", issuer_name, exc)
        return []

    results, _ = _parse_issue_search_response(content, issuer_name)
    return results


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def _parse_issue_search_response(
    content: str,
    search_text: str,
) -> tuple[list[IssuerSearchResult], int]:
    """
    Parse the JSON response from /api/Search/Issue or /api/Search/Issuer.

    EMMA's search API returns JSON. The exact schema may vary; this parser
    is defensive and logs warnings for unexpected shapes rather than crashing.

    Returns:
        (list of IssuerSearchResult, total_count)
    """
    import json

    try:
        data = json.loads(content)
    except json.JSONDecodeError as exc:
        logger.error("Failed to parse search response as JSON: %s", exc)
        return [], 0

    # EMMA API wraps results in different keys depending on endpoint version.
    # Try common patterns.
    hits = (
        data.get("hits")
        or data.get("results")
        or data.get("Issues")
        or data.get("data")
        or (data if isinstance(data, list) else [])
    )

    total_count = (
        data.get("totalCount")
        or data.get("total")
        or data.get("TotalCount")
        or len(hits)
    )

    if not isinstance(hits, list):
        logger.warning(
            "Unexpected search response shape for '%s' — could not find results list",
            search_text,
        )
        return [], 0

    results: list[IssuerSearchResult] = []
    for item in hits:
        try:
            result = _parse_issue_hit(item)
            if result:
                results.append(result)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to parse issue hit: %s — %s", exc, item)

    return results, int(total_count)


def _parse_issue_hit(item: dict) -> Optional[IssuerSearchResult]:
    """
    Parse one issue hit from the search results array.

    Field names are case-normalized to handle EMMA's inconsistent casing.
    """
    # Normalize keys to lowercase for flexible matching
    norm = {k.lower(): v for k, v in item.items()}

    issue_id = (
        norm.get("issueid")
        or norm.get("id")
        or norm.get("emma_id")
    )
    if not issue_id:
        logger.debug("Skipping hit with no issue_id: %s", item)
        return None

    issue_id = str(issue_id)

    issuer_name = (
        norm.get("issuername")
        or norm.get("issuer_name")
        or norm.get("issuer")
        or "Unknown Issuer"
    )

    issue_name = (
        norm.get("issuename")
        or norm.get("issue_name")
        or norm.get("name")
        or norm.get("description")
        or ""
    )

    state = norm.get("state") or norm.get("statecode") or None

    bond_type = (
        norm.get("bondtype")
        or norm.get("bond_type")
        or norm.get("category")
        or None
    )

    par_amount: Optional[float] = None
    for key in ("paramt", "par_amount", "paramount", "originalamount"):
        raw = norm.get(key)
        if raw is not None:
            try:
                par_amount = float(str(raw).replace(",", ""))
                break
            except (ValueError, TypeError):
                pass

    issue_date: Optional[date] = None
    for key in ("issuedate", "issue_date", "saledate", "dated_date"):
        raw = norm.get(key)
        if raw:
            try:
                issue_date = _parse_date(str(raw))
                if issue_date:
                    break
            except ValueError:
                pass

    emma_url = (
        norm.get("url")
        or norm.get("emma_url")
        or f"https://emma.msrb.org/IssueView/Details/{issue_id}"
    )

    return IssuerSearchResult(
        issue_id=issue_id,
        issuer_name=str(issuer_name).strip(),
        issue_name=str(issue_name).strip(),
        state=str(state).upper() if state else None,
        bond_type=str(bond_type).strip() if bond_type else None,
        par_amount=par_amount,
        issue_date=issue_date,
        emma_url=str(emma_url),
    )


def _parse_date(raw: str) -> Optional[date]:
    """
    Parse a date string in various formats EMMA might return.
    Returns None on failure rather than raising.
    """
    import re

    raw = raw.strip()
    if not raw:
        return None

    # ISO format: YYYY-MM-DD or YYYY-MM-DDT...
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))

    # US format: MM/DD/YYYY
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw)
    if m:
        return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))

    # Epoch milliseconds (JavaScript timestamps)
    if re.match(r"^\d{10,13}$", raw):
        from datetime import datetime
        ts = int(raw)
        if ts > 1e10:
            ts //= 1000
        return datetime.utcfromtimestamp(ts).date()

    return None
