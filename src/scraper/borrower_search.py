"""
borrower_search.py — Borrower-centric bond issue discovery.

Wraps the low-level issue_search module to provide:

1. **Multi-variant name search** — generates abbreviated forms of the borrower
   name (e.g., "UNIVERSITY" → "UNIV") and runs searches for each variant,
   deduplicating results by issue_id.

2. **Token-based match scoring** — eliminates false positives caused by EMMA's
   fuzzy word matching.  Example: searching "RIDER UNIVERSITY" would otherwise
   return Embry-RIDDLE bonds because EMMA matches on partial words.  The scorer
   tokenises the borrower name and the bond description, then checks whether
   *all* key borrower tokens (or their known abbreviations) appear as whole
   words in the description.  A result that fails this test is excluded.

3. **Maturity heuristic** — flags issues whose dated-date + estimated maximum
   maturity < today as `potentially_matured`.  Refunding bonds ("RFDG",
   "REFUNDING") use a shorter estimated maturity (20 years) vs. standard
   revenue bonds (35 years).  This is an approximation; use
   verify_maturity=True to fetch each issue's detail page for an exact check.

Usage:
    from src.scraper.borrower_search import find_issues_for_borrower
    from src.scraper.session import EMMAsession

    mgr = EMMAsession()
    session = mgr.get_session()

    results = find_issues_for_borrower(session, "Rider University")
    for r in results:
        flag = " [MATURED?]" if r.potentially_matured else ""
        print(f"  [{r.match_confidence:.2f}] {r.issue_id}  {r.issue_name}{flag}")
"""

import logging
import re
from datetime import date, timedelta
from typing import Optional

import requests

from src.scraper.issue_search import search_issues
from src.scraper.models import IssuerSearchResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Abbreviation maps
# ---------------------------------------------------------------------------

# Primary abbreviations used in EMMA bond descriptions.
# Key = full canonical word (upper-case).
# Value = set of abbreviated forms that appear in EMMA data.
# Used both for name-variant search-term generation AND for match scoring.
WORD_ABBREVS: dict[str, set[str]] = {
    "UNIVERSITY":   {"UNIV", "U"},
    "COLLEGE":      {"COLL", "CLG", "COL"},
    "INSTITUTE":    {"INST"},
    "INSTITUTION":  {"INST"},
    "MEDICAL":      {"MED"},
    "CENTER":       {"CTR", "CNTR"},
    "CENTRE":       {"CTR", "CNTR"},
    "MEMORIAL":     {"MEM", "MEML"},
    "GENERAL":      {"GEN"},
    "HOSPITAL":     {"HOSP", "HSPTL"},
    "HEALTH":       {"HLTH", "HTH"},
    "SYSTEM":       {"SYS"},
    "AUTHORITY":    {"AUTH"},
    "CORPORATION":  {"CORP"},
    "FOUNDATION":   {"FDN", "FDTN"},
    "DEPARTMENT":   {"DEPT"},
    "ASSOCIATION":  {"ASSN", "ASSOC"},
    "COMMUNITY":    {"COMM", "CMNTY"},
    "REGIONAL":     {"REGL", "REG"},
    "DISTRICT":     {"DIST"},
    "TECHNICAL":    {"TECH"},
    "TECHNOLOGY":   {"TECH"},
    "ELEMENTARY":   {"ELEM"},
    "SAINT":        {"ST"},
    "MOUNT":        {"MT", "MNT"},
    "FORT":         {"FT"},
    "POINT":        {"PT"},
}

# Build a reverse map so we can look up canonical → all valid forms in one shot.
# e.g.  "UNIV" → {"UNIVERSITY", "UNIV", "U"}
_CANONICAL_PLUS_ABBREVS: dict[str, set[str]] = {
    canonical: {canonical} | abbrevs
    for canonical, abbrevs in WORD_ABBREVS.items()
}
# Also map abbreviation → same expanded set (needed when input is abbreviated)
for _canonical, _abbrevs in list(WORD_ABBREVS.items()):
    for _abbrev in _abbrevs:
        if _abbrev not in _CANONICAL_PLUS_ABBREVS:
            _CANONICAL_PLUS_ABBREVS[_abbrev] = {_canonical} | _abbrevs

# Words that appear frequently in bond descriptions but carry no borrower
# identity information.  These are removed from the key-token list so they
# don't inflate or deflate match scores.
_BOND_STOPWORDS: frozenset[str] = frozenset({
    # Generic bond terms
    "REVENUE", "BONDS", "BOND", "NOTES", "NOTE", "PROJECT", "ISSUE",
    "REFUNDING", "RFDG", "REF", "SERIES", "SER", "SENIOR", "SR",
    "TAX", "EXEMPT", "TAXABLE", "EDUCATIONAL", "FACILITIES", "AUTHORITY",
    "FINANCING", "FINANCE", "DEVELOPMENT", "IMPROVEMENT",
    # Common legal connectors
    "AND", "THE", "OF", "FOR", "IN", "A", "AN", "AT", "BY", "TO",
    # Short abbreviations that are too ambiguous alone
    "U", "A", "B", "C", "D",
})

# Keywords in a bond description that indicate it is a refunding (shorter life)
_REFUNDING_KEYWORDS: frozenset[str] = frozenset({"RFDG", "REFUNDING", "REF", "RFNDG"})

# Maximum estimated maturity in years for different bond structures.
# Used to flag potentially matured bonds without fetching detail pages.
_MAX_MATURITY_STANDARD_YRS: int = 35   # revenue / GO bonds
_MAX_MATURITY_REFUNDING_YRS: int = 20  # refunding bonds (refund earlier debt)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def find_issues_for_borrower(
    session: requests.Session,
    borrower_name: str,
    state: Optional[str] = None,
    min_confidence: float = 0.6,
    exclude_matured: bool = True,
    use_cache: bool = True,
) -> list[IssuerSearchResult]:
    """
    Primary entry point for Phase 1 borrower-centric discovery.

    Searches EMMA for all bond issues linked to *borrower_name*, applies
    name-variant expansion to catch abbreviated descriptions, scores each
    result for borrower-name match quality, and optionally filters out
    bonds that are likely fully matured or called.

    Args:
        session:        Active requests.Session (Disclaimer6 cookie must be set).
        borrower_name:  Full legal name of the borrower (e.g., "Rider University").
                        Case-insensitive.
        state:          Optional two-letter state filter (applied client-side).
        min_confidence: Results below this threshold are excluded as false
                        positives.  Range 0.0–1.0.  Default 0.6 means at least
                        60% of the borrower's key tokens must appear in the
                        bond description.  Use 1.0 for strictest matching.
        exclude_matured: When True, issues flagged as `potentially_matured` by
                        the age heuristic are removed from results.  Default True.
                        Set False to include all issues regardless of age.
        use_cache:      Whether to use the response cache for EMMA requests.

    Returns:
        Deduplicated list of IssuerSearchResult, sorted by match_confidence
        descending.  Each result has match_confidence, match_reason, and
        potentially_matured populated.
    """
    search_terms = _generate_search_terms(borrower_name)
    logger.info(
        "find_issues_for_borrower('%s') — search variants: %s",
        borrower_name,
        search_terms,
    )

    # --- Run searches for each variant and collect all raw results ---
    seen_ids: set[str] = set()
    raw_results: list[IssuerSearchResult] = []

    for term in search_terms:
        results, _ = search_issues(
            session,
            search_text=term,
            state=state,
            use_cache=use_cache,
        )
        for r in results:
            if r.issue_id not in seen_ids:
                seen_ids.add(r.issue_id)
                raw_results.append(r)

    logger.info(
        "Raw results before scoring: %d unique issues from %d search variant(s)",
        len(raw_results),
        len(search_terms),
    )

    # --- Score and annotate each result ---
    key_tokens = _extract_key_tokens(borrower_name)
    scored: list[IssuerSearchResult] = []

    for result in raw_results:
        confidence, reason = _score_borrower_match(result.issue_name, key_tokens)
        matured = _estimate_maturity(result.issue_name, result.issue_date)

        result.match_confidence = confidence
        result.match_reason = reason
        result.potentially_matured = matured

        if confidence < min_confidence:
            logger.debug(
                "Excluding '%s' [%s] — confidence %.2f < %.2f: %s",
                result.issue_name[:60],
                result.issue_id,
                confidence,
                min_confidence,
                reason,
            )
            continue

        if exclude_matured and matured:
            logger.debug(
                "Excluding '%s' [%s] — flagged as potentially matured",
                result.issue_name[:60],
                result.issue_id,
            )
            continue

        scored.append(result)

    # Sort by confidence descending, then by issue_date descending (newest first)
    scored.sort(
        key=lambda r: (
            -r.match_confidence,
            -(r.issue_date.toordinal() if r.issue_date else 0),
        )
    )

    logger.info(
        "find_issues_for_borrower('%s') → %d issues after scoring "
        "(min_confidence=%.2f, exclude_matured=%s)",
        borrower_name,
        len(scored),
        min_confidence,
        exclude_matured,
    )
    return scored


def get_issue_ids_for_borrower(
    session: requests.Session,
    borrower_name: str,
    state: Optional[str] = None,
    min_confidence: float = 0.6,
    exclude_matured: bool = True,
    use_cache: bool = True,
) -> list[str]:
    """
    Convenience wrapper: find_issues_for_borrower() → list of issue IDs only.

    Used downstream by continuing_disclosure.fetch_disclosure_documents()
    and by the CLI discover command.
    """
    results = find_issues_for_borrower(
        session,
        borrower_name=borrower_name,
        state=state,
        min_confidence=min_confidence,
        exclude_matured=exclude_matured,
        use_cache=use_cache,
    )
    return [r.issue_id for r in results]


# ---------------------------------------------------------------------------
# Name variant generation
# ---------------------------------------------------------------------------

def _generate_search_terms(borrower_name: str) -> list[str]:
    """
    Generate a deduplicated list of EMMA search terms for a borrower name.

    Always includes the original name.  Also generates a variant where known
    long-form words are replaced with their primary abbreviation (e.g.,
    "UNIVERSITY" → "UNIV").  Duplicates are removed while preserving order.

    >>> _generate_search_terms("Rider University")
    ['Rider University', 'RIDER UNIV']

    >>> _generate_search_terms("Manhattan College")
    ['Manhattan College', 'MANHATTAN COLL']

    >>> _generate_search_terms("Rider Univ")  # already abbreviated
    ['Rider Univ']
    """
    terms: list[str] = [borrower_name]

    upper_words = borrower_name.upper().split()
    abbrev_parts: list[str] = []
    any_changed = False

    for word in upper_words:
        if word in WORD_ABBREVS:
            # Use the longest abbreviation (e.g. UNIV, not U; COLL not COL).
            # Single-letter abbreviations like "U" are too ambiguous for EMMA search.
            primary_abbrev = max(WORD_ABBREVS[word], key=len)
            abbrev_parts.append(primary_abbrev)
            any_changed = True
        else:
            abbrev_parts.append(word)

    if any_changed:
        abbrev_term = " ".join(abbrev_parts)
        abbrev_term_upper = abbrev_term.upper()
        if abbrev_term_upper != borrower_name.upper():
            terms.append(abbrev_term)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for t in terms:
        key = t.upper()
        if key not in seen:
            seen.add(key)
            unique.append(t)

    return unique


# ---------------------------------------------------------------------------
# Token extraction and match scoring
# ---------------------------------------------------------------------------

def _extract_key_tokens(name: str) -> list[str]:
    """
    Extract the meaningful identifier tokens from a borrower name.

    Strips punctuation, upper-cases, removes bond stopwords, and returns
    the remaining tokens that carry borrower identity information.

    >>> _extract_key_tokens("Rider University")
    ['RIDER', 'UNIVERSITY']

    >>> _extract_key_tokens("New Jersey Educational Facilities Authority")
    ['NEW', 'JERSEY']
    """
    # Split on whitespace and non-alphanumeric characters
    raw_tokens = re.split(r"[^A-Za-z0-9]+", name.upper())
    key_tokens = [
        t for t in raw_tokens
        if t and t not in _BOND_STOPWORDS and len(t) > 1
    ]
    return key_tokens


def _tokenise_description(desc: str) -> frozenset[str]:
    """
    Split an EMMA bond description into a set of uppercase word tokens.
    Splits on any non-alphanumeric character (spaces, hyphens, parens, etc.)

    >>> _tokenise_description("RFDG-EMBRY-RIDDLE AERONTCL UNIV")
    frozenset({'RFDG', 'EMBRY', 'RIDDLE', 'AERONTCL', 'UNIV'})
    """
    tokens = re.split(r"[^A-Za-z0-9]+", desc.upper())
    return frozenset(t for t in tokens if t)


def _score_borrower_match(
    issue_desc: str,
    key_tokens: list[str],
) -> tuple[float, str]:
    """
    Score how well a bond description matches the borrower's key tokens.

    For each key token, we check whether the token itself OR any of its known
    abbreviations/synonyms appears as a whole word in the description.  A
    result where *all* key tokens match scores 1.0.  Partial matches score
    proportionally.  Zero-token borrower names score 0.0.

    Returns:
        (confidence, reason) where confidence is 0.0–1.0 and reason is a
        human-readable string explaining which tokens matched or failed.

    Examples:
        key_tokens = ["RIDER", "UNIVERSITY"]

        "REVENUE BONDS RIDER UNIVERSITY ISSUE 2012 SERIES A"
            → confidence 1.0  (RIDER ✓, UNIVERSITY ✓)

        "REV BDS RIDER UNIV 2004 A"
            → confidence 1.0  (RIDER ✓, UNIV=UNIVERSITY ✓)

        "RFDG-EMBRY-RIDDLE AERONTCL UNIV"
            → confidence 0.5  (RIDER ✗, UNIV=UNIVERSITY ✓)
            → excluded at default min_confidence=0.6
    """
    if not key_tokens:
        return 0.0, "no key tokens extracted from borrower name"

    desc_tokens = _tokenise_description(issue_desc)
    matched: list[str] = []
    unmatched: list[str] = []

    for token in key_tokens:
        # Expand token to all valid forms (canonical + all abbreviations)
        valid_forms = _CANONICAL_PLUS_ABBREVS.get(token, {token})
        if valid_forms & desc_tokens:
            matched.append(token)
        else:
            unmatched.append(token)

    confidence = len(matched) / len(key_tokens)

    if unmatched:
        reason = f"matched {matched}; missing {unmatched} in '{issue_desc[:60]}'"
    else:
        reason = f"all tokens matched {matched}"

    return confidence, reason


# ---------------------------------------------------------------------------
# Maturity heuristic
# ---------------------------------------------------------------------------

def _estimate_maturity(issue_desc: str, issue_date: Optional[date]) -> bool:
    """
    Estimate whether a bond issue is likely fully matured using an age
    heuristic based on the issue's dated-date and bond structure.

    Logic:
        - If dated_date + _MAX_MATURITY_REFUNDING_YRS < today  AND  the
          description contains refunding keywords ("RFDG", "REFUNDING") →
          flag as potentially matured.
        - If dated_date + _MAX_MATURITY_STANDARD_YRS < today  (any bond
          type) → flag as potentially matured.
        - Otherwise → not flagged.

    Returns:
        True if the heuristic suggests the issue may be fully matured.
        False if the issue is likely still active, or if no dated-date
        is available (cannot determine — defaults to not flagged).

    Limitations:
        This is a coarse approximation.  Set verify_maturity=True in
        find_issues_for_borrower() [Phase 2 feature] for an accurate check
        based on the actual CUSIP maturity schedule from IssueView/Details.
    """
    if issue_date is None:
        return False  # can't determine — don't exclude

    today = date.today()
    desc_upper = issue_desc.upper()
    desc_tokens = _tokenise_description(desc_upper)

    is_refunding = bool(_REFUNDING_KEYWORDS & desc_tokens)
    max_years = _MAX_MATURITY_REFUNDING_YRS if is_refunding else _MAX_MATURITY_STANDARD_YRS

    cutoff = issue_date + timedelta(days=max_years * 365)
    return cutoff < today
