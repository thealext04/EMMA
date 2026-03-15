"""
issue_details.py — Issue Details fetcher and parser.

Fetches /IssueView/Details/{issueId} and extracts:
  - Bond series name
  - Issuer name and ID
  - Borrower name (conduit obligor — the credit entity)
  - Issue dates (issue, dated, settlement)
  - Par amount
  - Bond type, tax status
  - CUSIPs with maturities and coupons
  - Continuing disclosure URL

This is where the borrower → issuer → CUSIP hierarchy is established.

NOTE on session requirements: The session must have the Disclaimer6=msrborg
cookie set on emma.msrb.org (handled automatically by EMMAsession._create_session).
Without it, EMMA redirects to the Terms of Use page instead of the Details page.

NOTE on the continuing_disclosure_url field: The /IssueView/ContinuingDisclosure/
endpoint returns 404. Disclosure documents are instead extracted directly from
this same /IssueView/Details/ page by continuing_disclosure.py. The
continuing_disclosure_url field in BondIssueDetail is kept for compatibility but
callers should use fetch_disclosure_documents(session, issue_id) directly.

Usage:
    from src.scraper.issue_details import fetch_issue_details
    detail = fetch_issue_details(session, issue_id="ABC123")
    print(detail.borrower_name, detail.continuing_disclosure_url)
"""

import json
import logging
import re
from datetime import date
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag

from src.scraper.cache import cached_get, TTL_ISSUE_DETAILS
from src.scraper.models import BondIssueDetail, CUSIPDetail
from src.scraper.retry import fetch_with_retry

logger = logging.getLogger(__name__)

EMMA_BASE = "https://emma.msrb.org"
ISSUE_DETAILS_URL = EMMA_BASE + "/IssueView/Details/{issue_id}"
CONTINUING_DISCLOSURE_URL = EMMA_BASE + "/IssueView/ContinuingDisclosure/{issue_id}"


def fetch_issue_details(
    session: requests.Session,
    issue_id: str,
    use_cache: bool = True,
) -> Optional[BondIssueDetail]:
    """
    Fetch and parse the Issue Details page for a given EMMA issue ID.

    Args:
        session:   Active requests.Session.
        issue_id:  EMMA internal issue ID (from search results).
        use_cache: Whether to use the 30-day response cache.

    Returns:
        BondIssueDetail on success, None on parse failure.
    """
    url = ISSUE_DETAILS_URL.format(issue_id=issue_id)

    try:
        html = cached_get(
            session,
            url,
            ttl_hours=TTL_ISSUE_DETAILS,
            bypass=not use_cache,
        )
    except requests.RequestException as exc:
        logger.error("Failed to fetch issue details for %s: %s", issue_id, exc)
        return None

    return _parse_issue_detail_html(html, issue_id)


def fetch_cusip_to_issue(
    session: requests.Session,
    cusip: str,
) -> Optional[BondIssueDetail]:
    """
    Resolve a CUSIP to a full BondIssueDetail.

    Flow: CUSIP → QuickSearch → Security/Details page → IssueView/Details

    Args:
        session: Active requests.Session.
        cusip:   9-character CUSIP string.

    Returns:
        BondIssueDetail on success, None if CUSIP cannot be resolved.
    """
    cusip = cusip.strip().upper()
    quick_url = f"{EMMA_BASE}/QuickSearch/Results"

    try:
        resp = fetch_with_retry(session, quick_url, params={"quickSearchText": cusip})
    except requests.RequestException as exc:
        logger.error("QuickSearch failed for CUSIP %s: %s", cusip, exc)
        return None

    if "Security/Details" not in resp.url:
        logger.warning("CUSIP %s did not redirect to Security/Details page", cusip)
        return None

    # Extract issue ID from the security details page
    issue_id = _extract_issue_id_from_security_page(resp.text, resp.url)
    if not issue_id:
        logger.warning("Could not find issue ID on security page for CUSIP %s", cusip)
        return None

    return fetch_issue_details(session, issue_id)


# ---------------------------------------------------------------------------
# HTML Parsers
# ---------------------------------------------------------------------------

def _parse_issue_detail_html(html: str, issue_id: str) -> Optional[BondIssueDetail]:
    """
    Parse the /IssueView/Details/{issueId} HTML page.

    EMMA embeds some data in structured HTML tables and some in inline JSON
    (used by the page's JavaScript). We try both approaches.
    """
    soup = BeautifulSoup(html, "html.parser")

    # --- Try embedded JSON first (more reliable than HTML scraping) ---
    embedded = _extract_embedded_json(soup)

    series_name = _find_text(soup, embedded, ["IssueName", "issueName", "name"], "")
    issuer_name = _find_text(soup, embedded, ["IssuerName", "issuerName", "issuer"], "")
    issuer_id = _find_text(soup, embedded, ["IssuerId", "issuerId"], None)
    borrower_name = _find_text(
        soup, embedded,
        ["ObligorName", "obligorName", "BorrowerName", "borrowerName", "ConduitObligor"],
        None,
    )
    bond_type = _find_text(soup, embedded, ["BondType", "bondType", "TypeOfBond"], None)
    tax_status = _find_text(
        soup, embedded, ["TaxStatus", "taxStatus", "TaxExemptStatus"], None
    )

    par_amount = _extract_par_amount(soup, embedded)
    issue_date = _extract_date_field(soup, embedded, ["IssueDate", "issueDate", "SaleDate"])
    dated_date = _extract_date_field(soup, embedded, ["DatedDate", "datedDate"])
    settlement_date = _extract_date_field(soup, embedded, ["SettlementDate", "settlementDate"])

    # Continuing disclosure URL
    cd_url = (
        _find_text(soup, embedded, ["ContinuingDisclosureUrl", "continuingDisclosureUrl"], None)
        or CONTINUING_DISCLOSURE_URL.format(issue_id=issue_id)
    )

    # CUSIP list
    cusips = _extract_cusips(soup, embedded)

    # Fallback: try to extract issuer/borrower from HTML if embedded JSON failed
    if not issuer_name:
        issuer_name = _extract_from_html_table(soup, "Issuer") or ""
    if not borrower_name:
        borrower_name = _extract_from_html_table(
            soup, ["Obligor", "Borrower", "Conduit Obligor", "Obligated Person"]
        )

    if not series_name:
        # Try the page title as a last resort
        title_tag = soup.find("h1") or soup.find("title")
        series_name = title_tag.get_text(strip=True) if title_tag else f"Issue {issue_id}"

    logger.info(
        "Parsed issue %s — borrower: %s | issuer: %s | CUSIPs: %d",
        issue_id,
        borrower_name or "NOT FOUND",
        issuer_name,
        len(cusips),
    )

    return BondIssueDetail(
        issue_id=issue_id,
        series_name=series_name,
        issuer_name=issuer_name,
        issuer_id=issuer_id,
        borrower_name=borrower_name,
        issue_date=issue_date,
        dated_date=dated_date,
        settlement_date=settlement_date,
        par_amount=par_amount,
        bond_type=bond_type,
        tax_status=tax_status,
        continuing_disclosure_url=cd_url,
        cusips=cusips,
    )


def _extract_embedded_json(soup: BeautifulSoup) -> dict:
    """
    EMMA pages often embed data as a JavaScript variable in a <script> tag.
    Extract it and return as a dict (or empty dict on failure).
    """
    for script in soup.find_all("script"):
        text = script.string or ""
        # Look for patterns like: var issueData = {...}; or window.issueModel = {...};
        for pattern in [
            r"var\s+issueData\s*=\s*(\{.*?\});",
            r"window\.issueModel\s*=\s*(\{.*?\});",
            r"var\s+model\s*=\s*(\{.*?\});",
            r"initialState\s*[:=]\s*(\{.*?\})",
        ]:
            m = re.search(pattern, text, re.DOTALL)
            if m:
                try:
                    return json.loads(m.group(1))
                except json.JSONDecodeError:
                    pass
    return {}


def _find_text(
    soup: BeautifulSoup,
    embedded: dict,
    keys: list[str],
    default: Optional[str],
) -> Optional[str]:
    """Try embedded JSON keys first, then return default."""
    for key in keys:
        val = embedded.get(key)
        if val and str(val).strip():
            return str(val).strip()
    return default


def _extract_par_amount(soup: BeautifulSoup, embedded: dict) -> Optional[float]:
    """Extract par amount from embedded JSON or HTML."""
    for key in ["ParAmount", "parAmount", "OriginalPar", "originalPar", "TotalPar"]:
        raw = embedded.get(key)
        if raw is not None:
            try:
                return float(str(raw).replace(",", "").replace("$", ""))
            except (ValueError, TypeError):
                pass

    # Try HTML: look for dollar amounts near "Par Amount" label
    label = soup.find(string=re.compile(r"Par\s+Amount", re.I))
    if label and label.parent:
        sibling = label.parent.find_next_sibling()
        if sibling:
            text = sibling.get_text(strip=True)
            m = re.search(r"[\d,]+", text)
            if m:
                try:
                    return float(m.group().replace(",", ""))
                except ValueError:
                    pass
    return None


def _extract_date_field(
    soup: BeautifulSoup,
    embedded: dict,
    keys: list[str],
) -> Optional[date]:
    """Extract a date field from embedded JSON."""
    for key in keys:
        raw = embedded.get(key)
        if raw:
            parsed = _parse_date(str(raw))
            if parsed:
                return parsed
    return None


def _extract_cusips(soup: BeautifulSoup, embedded: dict) -> list[CUSIPDetail]:
    """
    Extract the list of CUSIPs from the bond issue page.
    CUSIPs are usually in an HTML table or embedded JSON array.
    """
    cusips: list[CUSIPDetail] = []

    # Try embedded JSON array first
    for key in ["Cusips", "cusips", "Securities", "securities", "Maturities"]:
        items = embedded.get(key)
        if isinstance(items, list):
            for item in items:
                c = _parse_cusip_item(item)
                if c:
                    cusips.append(c)
            if cusips:
                return cusips

    # Fall back to HTML table parsing
    # Look for a table containing CUSIP-like strings (9 alphanumeric chars)
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for row in rows:
            cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            # Find cells that look like CUSIPs
            for i, cell in enumerate(cells):
                if re.match(r"^[A-Z0-9]{9}$", cell):
                    c = _parse_cusip_row(cells, i)
                    if c:
                        cusips.append(c)

    return cusips


def _parse_cusip_item(item: dict) -> Optional[CUSIPDetail]:
    """Parse one CUSIP from the embedded JSON array."""
    norm = {k.lower(): v for k, v in item.items()}
    cusip = norm.get("cusip") or norm.get("cusipnumber") or norm.get("number")
    if not cusip:
        return None

    maturity_date = None
    for key in ["maturitydate", "maturity", "maturitydate"]:
        raw = norm.get(key)
        if raw:
            maturity_date = _parse_date(str(raw))
            if maturity_date:
                break

    coupon: Optional[float] = None
    for key in ["couponrate", "coupon", "interestrate"]:
        raw = norm.get(key)
        if raw is not None:
            try:
                coupon = float(str(raw))
                if coupon > 1:
                    coupon /= 100  # Convert percentage to decimal
                break
            except (ValueError, TypeError):
                pass

    par_amount: Optional[float] = None
    for key in ["paramt", "paramount", "amount", "originalamount"]:
        raw = norm.get(key)
        if raw is not None:
            try:
                par_amount = float(str(raw).replace(",", ""))
                break
            except (ValueError, TypeError):
                pass

    return CUSIPDetail(
        cusip=str(cusip).upper().strip(),
        maturity_date=maturity_date,
        coupon=coupon,
        par_amount=par_amount,
        rating_sp=norm.get("ratingsp") or norm.get("rating_sp"),
        rating_moodys=norm.get("ratingmoodys") or norm.get("rating_moodys"),
        rating_fitch=norm.get("ratingfitch") or norm.get("rating_fitch"),
    )


def _parse_cusip_row(cells: list[str], cusip_idx: int) -> Optional[CUSIPDetail]:
    """Parse a CUSIP from a table row given the CUSIP cell index."""
    cusip = cells[cusip_idx]
    maturity_date: Optional[date] = None
    coupon: Optional[float] = None

    for cell in cells:
        # Try to find a maturity date
        if not maturity_date:
            maturity_date = _parse_date(cell)
        # Try to find a coupon (number between 0.1 and 20)
        if coupon is None:
            m = re.match(r"^(\d+\.?\d*)%?$", cell)
            if m:
                val = float(m.group(1))
                if 0.1 <= val <= 20:
                    coupon = val / 100 if val > 1 else val

    return CUSIPDetail(
        cusip=cusip,
        maturity_date=maturity_date,
        coupon=coupon,
        par_amount=None,
        rating_sp=None,
        rating_moodys=None,
        rating_fitch=None,
    )


def _extract_from_html_table(
    soup: BeautifulSoup,
    labels: "str | list[str]",
) -> Optional[str]:
    """
    Search for a label in the page and return the adjacent value cell.
    Handles both single string and list of fallback labels.

    Also checks breadcrumb navigation links for issuer name. On EMMA's
    /IssueView/Details pages, the breadcrumb takes the form:
        Home > Issuers By State > Georgia > Issuer Homepage > Issue Details
    where "Issuer Homepage" is a link whose text is the issuer name. When
    searching for "Issuer", this breadcrumb link is used as a fallback.
    """
    if isinstance(labels, str):
        labels = [labels]

    for label_text in labels:
        pattern = re.compile(re.escape(label_text), re.I)
        label_el = soup.find(string=pattern)
        if not label_el:
            continue
        parent = label_el.parent
        if not isinstance(parent, Tag):
            continue
        # Try next sibling td
        sibling = parent.find_next_sibling("td")
        if sibling:
            val = sibling.get_text(strip=True)
            if val:
                return val
        # Try parent's next sibling row
        row = parent.find_parent("tr")
        if row:
            next_row = row.find_next_sibling("tr")
            if next_row:
                val = next_row.get_text(strip=True)
                if val:
                    return val

    # Breadcrumb fallback: look for "Issuer Homepage" link text, whose href
    # points to the issuer page. The link text itself is the issuer name.
    # This covers the common EMMA pattern:
    #   <a href="/IssuerHomePage/...">ISSUER NAME HERE</a>
    if any(
        re.search(r"issuer", lbl, re.I)
        for lbl in (labels if isinstance(labels, list) else [labels])
    ):
        issuer_link = soup.find("a", href=re.compile(r"/IssuerHomePage/", re.I))
        if issuer_link:
            val = issuer_link.get_text(strip=True)
            if val:
                return val

    return None


def _extract_issue_id_from_security_page(html: str, url: str) -> Optional[str]:
    """
    Extract the EMMA issue ID from a Security/Details page.
    The issue ID usually appears in a link to /IssueView/Details/{issueId}.
    """
    # Try URL pattern first
    m = re.search(r"/IssueView/Details/([A-Za-z0-9]+)", url)
    if m:
        return m.group(1)

    soup = BeautifulSoup(html, "html.parser")
    link = soup.find("a", href=re.compile(r"/IssueView/Details/"))
    if link:
        m = re.search(r"/IssueView/Details/([A-Za-z0-9]+)", link["href"])
        if m:
            return m.group(1)

    return None


def _parse_date(raw: str) -> Optional[date]:
    """Parse a date string in various EMMA formats. Returns None on failure."""
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

    # Epoch ms
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
