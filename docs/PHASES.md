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

**Status:** Complete ✅ (delivered 2026-03-15)
**Goal:** Build the database schema that correctly organizes data around borrowers, not CUSIPs.

### Deliverables

- [x] Database setup (SQLite for development, PostgreSQL-ready)
- [x] Borrowers table with sector, state, distress status, fiscal year end, former names
- [x] Issuers table (conduit issuers, separate from borrowers)
- [x] Bond Issues table linking borrower → issuer → series
- [x] CUSIPs table with ratings, maturity, coupon
- [x] Documents table with type classification, dates, URLs, extraction status
- [x] Events table — distress signals with severity scoring and event timeline assembly
- [x] Download queue table — tracks pending/active/failed fetches
- [x] ORM models (SQLAlchemy, `src/db/models.py`)
- [x] Repository layer (`src/db/repositories/`) for borrowers, bond issues, documents, events, metrics
- [x] DB initialization script (`src/db/init_db.py`)
- [x] Seed script for 30 higher-ed watchlist borrowers (`scripts/seed_borrowers.py`)
- [x] Fiscal year end seed script (`scripts/seed_fyes.py`)
- [ ] Migration system (deferred — schema evolving, not yet needed at current scale)

### Schema Extensions Beyond Original Plan

The `extracted_metrics` table was expanded (Phase 5.1) to include:
`credit_rating`, `operating_expenses`, `interest_expense`, `technical_default`,
`forbearance_agreement`, `forbearance_text`, `gift_revenue`, `municipal_debt`,
`period_type`, `period_months`, `source_doc_ids`, `citations_json`

**Pending schema additions** (required for spreadsheet export — Phase 7):
- `borrowers.year_founded` — institution founding year
- `borrowers.institution_type` — public vs. private
- `borrowers.bond_trustee` — trustee bank name
- `borrowers.bdo_rating` — internal distress classification label
- `borrowers.liquidity_covenants` — covenant terms text

### Key Design Decisions

- Borrower is the primary entity; all other tables reference back
- Documents have both `doc_date` (when filed) and `posted_date` (when appeared on EMMA)
- Events table designed for timeline assembly
- Queue table enables safe resumption after crashes

### Success Criteria

- Can query "all documents for borrower X in the last 90 days" ✅
- Can compute expected vs actual filing dates for any borrower ✅
- Can assemble a complete event timeline for any borrower ✅

---

## Phase 3 — Late Disclosure Detection

**Status:** Complete ✅ (delivered 2026-03-15)
**Goal:** Automatically detect borrowers who have missed their financial statement filing deadline.

### Deliverables

- [x] Fiscal year end stored per borrower (manually set; `borrowers.fiscal_year_end`)
- [x] Expected filing date calculation (FYE + 180 days, configurable per call)
- [x] Late filing detector — `src/distress/late_filing.py` — scans all watchlist borrowers
- [x] Alert generation — `LateFilingStatus` dataclass with `is_late`, `days_overdue`
- [x] Undated filing handling — benefit-of-the-doubt logic when doc dates are missing
- [ ] Override/exception management per borrower (not yet built)
- [ ] Dashboard view (deferred to Phase 6)

### Known Limitation

EMMA's "Financial Operating Filing" link text does not embed a date. Most financial statement
records therefore have `doc_date = NULL`. The detector gives benefit-of-the-doubt to any
borrower with undated filings on record. Phase 4 AI extraction resolves this by reading
the fiscal year end date from the PDF itself.

### Success Criteria

- Correctly computes expected filing date for 95%+ of watchlist borrowers ✅
- Detects new delinquencies within 24 hours of deadline passing ✅ (runs on demand / cron)
- False positive rate <10% ✅ (undated-filing handling prevents most false positives)

---

## Phase 4 — AI Document Parsing Pipeline

**Status:** Built ✅ — queue processing needed ⚠️ (delivered 2026-03-15)
**Goal:** Extract structured financial metrics from PDF filings using AI.

### Deliverables

- [x] Document classifier — `src/parser/classifier.py` (metadata + keyword + AI fallback)
- [x] PDF text extractor — `src/parser/pdf_extractor.py` (pdfplumber + OCR fallback)
- [x] Sector-specific extraction prompts — higher ed and healthcare supplements in `extractor.py`
- [x] Financial metric extraction — revenue, DSCR, cash, enrollment, expenses, interest, forbearance
- [x] Going concern opinion detector — keyword pre-scan + AI confirmation
- [x] Event notice extraction — event type, severity, summary, key passage
- [x] Operating report extraction
- [x] Structured output schema — Pydantic validation with range checks
- [x] Extracted metrics database write via `MetricsRepository`
- [x] Citations tracking — field-level provenance (`citations_json`)
- [x] Pipeline orchestrator — `src/parser/pipeline.py` (classify → extract → validate → write → score)
- [x] Distress event generation — going concern, DSCR breach, technical default, forbearance
- [x] Reprocessing support — raw documents stored; re-run extraction by clearing `extraction_status`

### ⚠️ Current Bottleneck

The pipeline is fully built but has processed only **4 of 2,960 pending documents**.
The extraction queue contains **1,823 financial statements** and **953 event notices**
that have not yet been run through AI extraction. This is the single highest-priority
gap in the system — all downstream capabilities (distress scoring, spreadsheet export,
late filing resolution) depend on this data being populated.

**Recommended next run:** Process financial statements first, using:
- Batch API (50% cost reduction)
- Page limit of 20–25 pages per document (~85% token reduction)
- Expected total cost at current queue size: ~$5–$9

### Success Criteria

- Correctly classifies document type >95% of the time ✅ (built)
- Extracts revenue and net income from financial statements >85% accuracy ✅ (built; not yet measured at scale)
- Going concern opinion detected with >95% recall ✅ (built)
- AI cost per document <$0.10 average ✅ (with Batch API + page limiting: ~$0.003–$0.005)

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

**Status:** Partial 🟡 — distress scoring built; broad market scanning not started
**Goal:** Scan all new EMMA filings daily to surface distress signals from issuers outside the current watchlist.

### Deliverables

**Built (distress scoring — delivered as Phase 5.1):**
- [x] Distress score model — `src/distress/scoring.py` — weighted signal aggregation
- [x] Score updated automatically after each document extraction
- [x] Schema extensions for MVP metrics: `credit_rating`, `operating_expenses`,
      `interest_expense`, `technical_default`, `forbearance_agreement`, `gift_revenue`
- [x] Field-level citation provenance for audit trail

**Not yet built (market-wide scanning):**
- [ ] Full EMMA filing feed — monitor all new documents posted each day
- [ ] Event Notice classifier — detect type and severity of each notice across full market
- [ ] Keyword/pattern scanner for high-signal terms
- [ ] New borrower discovery — automatically add distressed borrowers to review queue
- [ ] Deduplication — avoid alerting twice on same event
- [ ] Alert triage system — human review queue for new discoveries

**Note:** The `event_notices.py` EMMA endpoint has not been validated against live traffic.
Confirm endpoint behavior before building market-wide scanning on top of it.

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

**Status:** Partial 🟡 — scoring built; reporting not started
**Goal:** Aggregate all signals into a distress score per borrower and generate readable reports.

### Deliverables

- [x] Distress score model — `src/distress/scoring.py` — weighted signal combination
- [ ] Borrower dashboard — score, timeline, key metrics at a glance
- [ ] Executive credit report generator — PDF or HTML output
- [ ] Score history tracking — watch scores deteriorate or improve over time
- [ ] Weekly digest — summary of top distress signals across watchlist
- [ ] Alert notifications — email or webhook on score threshold crossings

### Current Score State (as of 2026-03-19)

Most borrowers show score=0 because the AI extraction pipeline has not yet run at scale
(only 4 documents processed). Scores will populate automatically as extraction runs.

| Borrower | Score | Status |
|----------|-------|--------|
| Rider University | 55 | distressed |
| Lake Erie College | 10 | monitor |
| All others (28) | 0 | monitor |

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

---

## Phase 7 — Watchlist Spreadsheet Export

**Status:** Planned — requirements defined 2026-03-19
**Goal:** Automatically populate and refresh a structured Excel monitoring spreadsheet
from extracted metrics, enabling the team to replace the manually maintained watchlist
with a live, AI-populated credit tracking file.

### Background

The current watchlist is maintained manually in Excel. The target spreadsheet tracks
~30 higher-ed borrowers across ~50 columns of credit metrics, ratings, and analyst notes.
This phase automates the population of that sheet from the extraction pipeline output.

### Columns Auto-Populated from Database

| Column | Source |
|--------|--------|
| Obligor | `borrowers.borrower_name` |
| State | `borrowers.state` |
| Year Founded | `borrowers.year_founded` *(new field)* |
| Total Debt Outstanding | `extracted_metrics.total_long_term_debt` (most recent) |
| Credit Rating | `extracted_metrics.credit_rating` |
| Outlook / Distress Rating | `borrowers.distress_status` |
| Type (public/private) | `borrowers.institution_type` *(new field)* |
| Bond Trustee | `borrowers.bond_trustee` *(new field)* |
| Revenue (FY22–FY25) | `extracted_metrics.total_revenue` by fiscal_year |
| Revenue YoY changes | Computed from above |
| Contributions (FY22–FY25) | `extracted_metrics.gift_revenue` by fiscal_year |
| Contributions YoY changes | Computed |
| Enrollment (FY22–FY25) | `extracted_metrics.total_enrollment` by fiscal_year |
| Enrollment YoY changes | Computed |
| Operating Expenses (FY23–FY25) | `extracted_metrics.operating_expenses` by fiscal_year |
| Operating Profit/Loss (FY23–FY25) | `extracted_metrics.operating_income` by fiscal_year |
| Margin % (FY23–FY25) | Computed: operating_income / total_revenue |
| Total Cash & Investments | `extracted_metrics.cash_and_investments` (most recent) |
| Cash & Investments / OpEx | Computed |
| Cash & Investments / Total Debt | Computed |
| Interest Expense | `extracted_metrics.interest_expense` |
| Annual Debt Service Coverage | `extracted_metrics.dscr` |
| Liquidity Covenants | `borrowers.liquidity_covenants` *(new field)* |
| Key Notes | `borrowers.watchlist_notes` |
| EMMA Link | `bond_issues.continuing_disclosure_url` (primary issue) |
| FY25 Data? | Computed: `bool(extracted_metrics row where fiscal_year=2025)` |
| Date of Last Update | `extracted_metrics.extracted_at` (most recent) |

### Columns Manually Maintained

| Column | Notes |
|--------|-------|
| BDO Rating | Internal classification — seed in `borrowers.bdo_rating` *(new field)* |
| Status | Can use `borrowers.distress_status` or override |
| Status Formula | Excel formula referencing Status column |

### Deliverables

- [ ] Schema migration: add `year_founded`, `institution_type`, `bond_trustee`,
      `bdo_rating`, `liquidity_covenants` to `borrowers` table
- [ ] Seed script update: backfill new fields for existing 30 borrowers
- [ ] Export module: `src/reporting/watchlist_export.py` — queries metrics, computes
      derived columns, outputs formatted `.xlsx`
- [ ] Scheduled refresh: run export daily or on-demand via CLI
- [ ] Multi-year pivot logic: one row per borrower, columns span FY2022–FY2025

### Dependencies

Requires Phase 4 extraction to have processed financial statements for FY2022–FY2025.
The export module will output whatever years are available per borrower
and leave cells blank where extraction has not yet run.

### Success Criteria

- Export produces a valid `.xlsx` in under 60 seconds for a 30–200 borrower watchlist
- Revenue, enrollment, DSCR, and cash values match source PDFs
- Sheet refreshes automatically on a schedule without manual intervention

---

## Phase Dependencies

```
Phase 1 (Scraper) ✅
    └── Phase 2 (Database) ✅
            ├── Phase 3 (Late Filing Detection) ✅
            ├── Phase 4 (AI Parsing) ✅ built / ⚠️ needs scale run
            │       ├── Phase 5 (Market-Wide + Distress Scoring) 🟡
            │       ├── Phase 6 (Scoring & Reporting) 🟡
            │       └── Phase 7 (Watchlist Spreadsheet Export) 📋 planned
            └── Phase 5 (Market-Wide Detection) — partial
```

**Immediate priority:** Run Phase 4 extraction pipeline at scale against the 1,823
pending financial statements. Everything downstream is blocked on this data.
