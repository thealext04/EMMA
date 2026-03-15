# Project Phases

## Overview

The system is built in six sequential phases. Each phase delivers standalone value while building toward the full platform. Phase 1 is the critical foundation — without reliable data collection, nothing else works.

---

## Phase 1 — EMMA Scraping & Discovery Engine

**Status:** Complete ✅ (endpoints corrected 2026-03-15 based on live testing)
**Goal:** Build a reliable, respectful scraper that discovers and downloads EMMA disclosures without getting blocked.

### Endpoint Corrections (discovered via live testing, 2026-03-15)

The scaffold was written against assumed EMMA API endpoints. Live testing revealed
the following corrections, which have been applied to the source files:

| Module | Original (broken) endpoint | Correct endpoint |
|--------|---------------------------|-----------------|
| `session.py` | Warm-up GET only | Warm-up GET + must set `Disclaimer6=msrborg` cookie; without it EMMA returns Terms of Use page |
| `issue_search.py` | `GET /api/Search/Issue` (returns 404) | `GET /QuickSearch/Results?quickSearchText={name}&cat=desc`; results are embedded in inline JS as `pdata.Data = {...}` |
| `continuing_disclosure.py` | `GET /IssueView/ContinuingDisclosure/{id}` (returns 404) | `GET /IssueView/Details/{id}`; PDF links are `<a href="/*.pdf">` tags in the HTML |
| `event_notices.py` | `GET /api/Search/EventNotice` | Not yet validated — see module docstring |

### Deliverables

- [x] EMMA session manager — `src/scraper/session.py` (browser-like headers, warm-up, cookie persistence)
- [x] Issue Search API client — `src/scraper/issue_search.py` (paginated search, issuer and issue search)
- [x] Issue Details fetcher — `src/scraper/issue_details.py` (borrower name, CUSIPs, continuing disclosure URL)
- [x] Continuing Disclosure page parser — `src/scraper/continuing_disclosure.py` (JSON + HTML parsing)
- [x] Document downloader — `src/scraper/document_fetcher.py` (queue-based, single and multi-threaded)
- [x] Material Event Notice fetcher — `src/scraper/event_notices.py` (high-signal distress tagging)
- [x] Incremental update logic — built into `continuing_disclosure.py` via `last_seen_date` cursor
- [x] Retry and error handling — `src/scraper/retry.py` (exponential backoff: 5s → 30s → 120s)
- [x] Rate limiter — `src/scraper/rate_limiter.py` (1 req/s discovery, 2.5s downloads)
- [x] Response cache — `src/scraper/cache.py` (file-based, TTL per page type)
- [x] Local document storage — `src/scraper/storage.py` (organized by year/month/borrower-slug)
- [x] Download queue — `src/scraper/document_queue.py` (JSON-backed, crash-safe, idempotent)
- [x] Structured logger — `src/scraper/logger.py` (JSON output, rotating file handler)
- [x] CLI interface — `src/scraper/cli.py` (search, discover, download, events, queue, stats)
- [x] Data models — `src/scraper/models.py` (typed dataclasses for all entities)

### Source Files

| File | Purpose |
|------|---------|
| `src/scraper/models.py` | Typed dataclass models for all EMMA entities |
| `src/scraper/session.py` | EMMA session manager with browser-like headers |
| `src/scraper/rate_limiter.py` | Thread-safe per-layer rate limiting |
| `src/scraper/retry.py` | Exponential backoff retry with 429/5xx handling |
| `src/scraper/cache.py` | File-based response caching with TTLs |
| `src/scraper/storage.py` | Raw PDF storage (data/raw_documents/YYYY/MM/borrower/) |
| `src/scraper/logger.py` | JSON structured logger with rotating file handler |
| `src/scraper/issue_search.py` | /QuickSearch/Results?cat=desc HTML parser (pdata.Data inline JS) |
| `src/scraper/issue_details.py` | /IssueView/Details/{id} HTML parser |
| `src/scraper/continuing_disclosure.py` | /IssueView/Details/{id} PDF link extractor (ContinuingDisclosure endpoint is 404) |
| `src/scraper/document_queue.py` | JSON-backed download queue (Phase 2 migrates to DB) |
| `src/scraper/document_fetcher.py` | Queue-based PDF downloader (1–3 workers) |
| `src/scraper/event_notices.py` | /api/Search/EventNotice client — NOT yet validated (see module docstring) |
| `src/scraper/cli.py` | argparse CLI: search, discover, download, events, stats |

### Key Design Decisions

- Separate discovery (lightweight JSON/HTML) from document download (PDFs)
- Store raw documents before parsing — allows reprocessing as AI improves
- Conservative rate limits: 1 req/sec discovery, 1 req/2–3 sec downloads
- Incremental updates keyed on `last_seen_document_date` per issue

### Success Criteria

- Can discover all continuing disclosure documents for a given CUSIP or issuer name
- Can download PDFs reliably without triggering rate limits
- Incremental update reduces re-scraping volume by >90%
- Failure rate <5% on retries

---

## Phase 2 — Database & Borrower-Centric Data Model

**Status:** Planned
**Goal:** Build the database schema that correctly organizes data around borrowers, not CUSIPs.

### Deliverables

- [ ] Database setup (PostgreSQL recommended, SQLite for development)
- [ ] Borrowers table with sector, state, distress status
- [ ] Issuers table (conduit issuers, separate from borrowers)
- [ ] Bond Issues table linking borrower → issuer → series
- [ ] CUSIPs table with ratings, maturity, coupon
- [ ] Documents table with type classification, dates, URLs
- [ ] Events table — distress signals with severity scoring
- [ ] Download queue table — tracks pending/active/failed fetches
- [ ] ORM models (SQLAlchemy or similar)
- [ ] Migration system

### Key Design Decisions

- Borrower is the primary entity; all other tables reference back
- Documents have both `doc_date` (when filed) and `posted_date` (when appeared on EMMA)
- Events table designed for timeline assembly
- Queue table enables safe resumption after crashes

### Success Criteria

- Can query "all documents for borrower X in the last 90 days"
- Can compute expected vs actual filing dates for any borrower
- Can assemble a complete event timeline for any borrower

---

## Phase 3 — Late Disclosure Detection

**Status:** Planned
**Goal:** Automatically detect borrowers who have missed their financial statement filing deadline.

### Deliverables

- [ ] Fiscal year end detection — infer from historical filing dates per borrower
- [ ] Expected filing date calculation (FYE + 180 days, adjustable per borrower)
- [ ] Late filing detector — daily scan comparing expected vs actual
- [ ] Alert generation — create Event records for delinquent borrowers
- [ ] Override/exception management — some borrowers have non-standard deadlines
- [ ] Dashboard view — sortable list of late filers with days outstanding

### Key Design Decisions

- Filing deadlines vary: most are 180 days, some covenants specify shorter windows
- FYE is not always disclosed directly — infer from prior filing dates
- A filing that arrives late should still be recorded with actual vs expected date

### Success Criteria

- Correctly computes expected filing date for 95%+ of watchlist borrowers
- Detects new delinquencies within 24 hours of deadline passing
- False positive rate <10%

---

## Phase 4 — AI Document Parsing Pipeline

**Status:** Planned
**Goal:** Extract structured financial metrics from PDF filings using AI.

### Deliverables

- [ ] Document classifier — categorize each document before AI processing
- [ ] PDF text extractor — handle text PDFs and scanned (OCR) PDFs
- [ ] Sector-specific extraction prompts (higher ed, healthcare, general government)
- [ ] Financial metric extraction — revenue, EBITDA, DSCR, cash, enrollment, etc.
- [ ] Going concern opinion detector
- [ ] Covenant violation detector (from text of financial statements)
- [ ] Structured output schema — validated JSON output per document
- [ ] Extracted metrics database tables
- [ ] Reprocessing pipeline — re-run extraction when prompts improve

### Document Classification Categories

| Category | AI Extraction | Distress Alert |
|----------|--------------|----------------|
| Audited Financial Statement | Yes — full extraction | Going concern, DSCR |
| Event Notice | Yes — event type + summary | Covenant, default, rating |
| Operating Report | Yes — operating metrics | Revenue trends |
| Budget Filing | Limited | Deficit signals |
| Bond Issuance Document | No | No |
| Rating Notice | Yes — rating, outlook | Downgrade |

### Key Financial Metrics Extracted

**Income / Revenue**
- Total revenue
- Net income / (deficit)
- Operating income
- EBITDA

**Liquidity**
- Days cash on hand
- Total cash and investments
- Unrestricted net assets

**Debt**
- Total long-term debt
- Debt service (annual)
- Debt service coverage ratio (DSCR)

**Education Sector Specific**
- Total enrollment (headcount and FTE)
- Tuition revenue
- Tuition discount rate
- Endowment market value

**Healthcare Sector Specific**
- Licensed beds / staffed beds
- Patient admissions
- Patient days
- Net patient service revenue
- Days in accounts receivable

### Success Criteria

- Correctly classifies document type >95% of the time
- Extracts revenue and net income from financial statements >85% accuracy
- Going concern opinion detected with >95% recall
- AI cost per document <$0.10 average

---

## Phase 5 — Market-Wide Distress Detection

**Status:** Planned
**Goal:** Scan all new EMMA filings daily to surface distress signals from issuers outside the current watchlist.

### Deliverables

- [ ] Full EMMA filing feed — monitor all new documents posted each day
- [ ] Event Notice classifier — detect type and severity of each notice
- [ ] Keyword/pattern scanner for high-signal terms
- [ ] New borrower discovery — automatically add distressed borrowers to review queue
- [ ] Deduplication — avoid alerting twice on same event
- [ ] Alert triage system — human review queue for new discoveries

### High-Signal Keywords

```
covenant violation
covenant waiver
forbearance agreement
going concern
debt restructuring
bankruptcy
default
failure to pay
liquidity facility termination
rating withdrawal
```

### Success Criteria

- Processes all new EMMA filings within 24 hours of posting
- Correctly identifies event type >90% of the time
- Surfaces new distressed borrowers before rating agency action in >50% of cases

---

## Phase 6 — Distress Scoring & Reporting

**Status:** Planned
**Goal:** Aggregate all signals into a distress score per borrower and generate readable reports.

### Deliverables

- [ ] Distress score model — weighted combination of signals
- [ ] Borrower dashboard — score, timeline, key metrics at a glance
- [ ] Executive credit report generator — PDF or HTML output
- [ ] Score history tracking — watch scores deteriorate or improve over time
- [ ] Weekly digest — summary of top distress signals across watchlist
- [ ] Alert notifications — email or webhook on score threshold crossings

### Distress Score Components

| Signal | Weight | Notes |
|--------|--------|-------|
| Late financial filing | High | Days overdue drives severity |
| Going concern opinion | High | Binary trigger |
| Covenant violation notice | High | |
| Rating downgrade | Medium-High | Number of notches matters |
| DSCR < 1.0x | High | Below coverage |
| DSCR declining trend | Medium | |
| Days cash on hand declining | Medium | |
| Enrollment declining (HE) | Medium | |
| Price distress (yield spike) | Medium | From trade data |
| Debt restructuring filed | High | |
| Forbearance agreement | Very High | |

### Success Criteria

- Distress score correctly ranks-orders known distressed borrowers vs healthy ones
- Report generation completes in <60 seconds per borrower
- Weekly digest delivered reliably with zero missed weeks

---

## Phase Dependencies

```
Phase 1 (Scraper)
    └── Phase 2 (Database)
            ├── Phase 3 (Late Filing Detection)
            ├── Phase 4 (AI Parsing)
            │       └── Phase 6 (Scoring & Reporting)
            └── Phase 5 (Market-Wide Detection)
                    └── Phase 6 (Scoring & Reporting)
```

Phases 3, 4, and 5 can be developed in parallel once Phase 2 is complete.
