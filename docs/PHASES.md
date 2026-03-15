# Project Phases

## Overview

The system is built in six sequential phases. Each phase delivers standalone value while building toward the full platform. Phase 1 is the critical foundation — without reliable data collection, nothing else works.

---

## Phase 1 — EMMA Scraping & Discovery Engine

**Status:** In Progress
**Goal:** Build a reliable, respectful scraper that discovers and downloads EMMA disclosures without getting blocked.

### Deliverables

- [ ] EMMA session manager (browser-like headers, persistent sessions, cookie handling)
- [ ] Issue Search API client — discover bond issues by issuer, state, sector
- [ ] Issue Details fetcher — pull bond series metadata, borrower name, issuer name
- [ ] Continuing Disclosure page parser — list all documents for an issue
- [ ] Document downloader — queue-based PDF fetcher with rate limiting
- [ ] Material Event Notice fetcher
- [ ] Trade data fetcher (optional, for price distress signals)
- [ ] Incremental update logic — only fetch documents newer than last seen
- [ ] Retry and error handling — exponential backoff, failure logging
- [ ] Local document storage — organized by year/month/borrower

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
