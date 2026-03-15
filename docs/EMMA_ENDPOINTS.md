# EMMA API Endpoints Reference

## Overview

EMMA (emma.msrb.org) exposes internal JSON APIs used by its own website. These are the most reliable and lightweight endpoints for data discovery. They return structured data and are far preferable to parsing HTML.

All endpoints are under `https://emma.msrb.org`.

---

## Endpoint 1 — Issue Search API

**Purpose:** Search for bond issues by issuer name, state, sector, or bond type. Used to build and maintain the master bond universe.

**Pattern:**
```
GET /api/Search/Issue
```

**Key Parameters:**
- `searchText` — issuer or bond name
- `state` — two-letter state code
- `category` — bond type filter
- `page`, `pageSize` — pagination

**Returns:**
- Issuer name
- Issue name / series
- EMMA issue ID (critical — used in all subsequent calls)
- State
- Bond type
- Par amount
- Issue date

**Usage Notes:**
- Use this to discover all bond issues for a target borrower
- Issuer name search is fuzzy — try multiple variations (e.g., "Manhattan College", "Manhattan Col")
- Store the `issueId` for use with Endpoints 2 and 3

---

## Endpoint 2 — Issue Details

**Purpose:** Full metadata for a specific bond issue. Contains borrower name, issuer, and the complete maturity schedule.

**Pattern:**
```
GET /IssueView/Details/{issueId}
```

**Returns:**
- Bond series name
- Issuer name and ID
- **Borrower name** (the conduit obligor — what we care about)
- Issue date, sale date, settlement date
- Par amount
- Bond type, tax status
- Full CUSIP list with maturities and coupons
- Continuing disclosure agreement URL

**Usage Notes:**
- This is where the borrower → issuer → CUSIP relationship is established
- Extract and store the `continuingDisclosureUrl` for use with Endpoint 3
- Page is HTML — parse with BeautifulSoup; some data also in embedded JSON

---

## Endpoint 3 — Continuing Disclosure

**Purpose:** Lists all disclosure documents filed for a bond issue. The core monitoring endpoint.

**Pattern:**
```
GET /IssueView/ContinuingDisclosure/{issueId}
```

**Returns for each document:**
- Document type (financial statement, event notice, operating report, etc.)
- Document date
- Date posted to EMMA
- Document URL (direct PDF link)
- Filing description / title
- Submitter name

**Usage Notes:**
- Sort by `postedDate DESC` and stop when you hit `last_seen_doc_date`
- Documents are paginated — fetch all pages
- Both the discovery layer AND the queue feed from this endpoint
- This endpoint is called daily for watchlist borrowers

**Incremental Update Logic:**
```python
for doc in disclosure_docs:
    if doc.posted_date <= issue.last_seen_doc_date:
        break  # Stop — already have everything newer
    queue.add(doc)

issue.last_seen_doc_date = disclosure_docs[0].posted_date  # Update cursor
```

---

## Endpoint 4 — Security Details

**Purpose:** CUSIP-level detail including ratings, call features, and current market information.

**Pattern:**
```
GET /Security/Details/{securityId}
```

Also accessible via CUSIP:
```
GET /QuickSearch/Results?quickSearchText={cusip}
```
(redirects to security detail page)

**Returns:**
- CUSIP
- Issuer name
- Coupon rate
- Maturity date
- Par amount at issuance
- S&P, Moody's, Fitch ratings (current)
- Call provisions
- Bond type and tax status
- Issue ID (links back to Endpoint 2)

**Usage Notes:**
- Used to track rating changes over time
- Store rating history — changes are a key event trigger
- The redirect from CUSIP → security page → issue page establishes the CUSIP → issue → borrower chain

---

## Endpoint 5 — Trade History

**Purpose:** Historical trade data for a CUSIP. Useful for detecting price distress.

**Pattern:**
```
GET /TradeHistory/{cusip}
```

**Returns per trade:**
- Trade date
- Reported yield
- Price
- Par amount traded
- Trade type (customer buy, customer sell, dealer)

**Usage Notes:**
- Yield spikes (e.g., 300+ bps above par) are a distress signal
- Illiquidity (no trades for extended periods) can also signal distress
- Updated in near real-time by MSRB's trade reporting system
- Pull this weekly for watchlist CUSIPs

---

## Additional Useful Endpoints

### Material Event Notices Search
Used for market-wide distress scanning. Returns all recent event notices across all issuers.

```
GET /api/Search/EventNotice
```

**Parameters:**
- `eventType` — filter by event category
- `fromDate`, `toDate` — date range
- `state` — state filter

**High-value event types to monitor:**
- Covenant violations
- Rating changes
- Payment defaults
- Bankruptcy filings
- Debt restructurings

### Issuer Search
Find all bond issues for a specific issuer.

```
GET /api/Search/Issuer?searchText={issuer_name}
```

Returns the `issuerId` which can then be used to pull all issues.

---

## Session Management

EMMA sets a few cookies that help with session persistence:

- `Disclaimer6` — tracks that the user has accepted MSRB's disclaimer
- `AWSALB` / `AWSALBCORS` — AWS load balancer session cookies (expire periodically)

**Strategy:**
1. Use `requests.Session()` to persist cookies automatically
2. Start each session with a GET to `https://emma.msrb.org/` to pick up initial cookies
3. If `AWSALB` cookies are needed, obtain them through the session flow, not hardcoded
4. Re-initialize the session if you receive HTTP 403 or unexpected redirects

---

## Rate Limiting Observations

EMMA does not publish rate limits, but observed behavior:

| Behavior | Threshold |
|----------|-----------|
| Soft slowdown | ~5 req/sec sustained |
| Hard rate limit (429) | >10 req/sec |
| IP block (rare) | Aggressive crawler patterns |

Target: **1 req/sec or less** for discovery, **1 req/2–3 sec** for PDF downloads.

---

## URL Patterns Quick Reference

```
Bond search:           /api/Search/Issue?searchText=...
Issue details:         /IssueView/Details/{issueId}
Continuing disclosure: /IssueView/ContinuingDisclosure/{issueId}
Security details:      /Security/Details/{securityId}
CUSIP quick search:    /QuickSearch/Results?quickSearchText={cusip}
Trade history:         /TradeHistory/{cusip}
Event notice search:   /api/Search/EventNotice
Issuer search:         /api/Search/Issuer?searchText=...
```
