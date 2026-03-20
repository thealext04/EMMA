# CLAUDE.md — EMMA Municipal Distress Monitoring System

This file provides context for AI agents working on this project. Read it before making any changes.

---

## What This Project Is

A borrower-centric municipal credit monitoring platform that detects early distress signals from EMMA (Electronic Municipal Market Access), the SEC-designated repository for municipal securities disclosures.

This is NOT a bond data scraper. It is a credit intelligence platform organized around borrowers, not CUSIPs or issuers.

---

## Current Phase

**Phase 4 scale-up → Phase 7 Watchlist Spreadsheet Export**

Phases 1–3 are complete. Phase 4 (AI parsing) is fully built but has processed only
4 of 2,960 pending documents. The immediate priorities in order are:

1. **Run Phase 4 extraction at scale** — 1,823 financial statements are pending.
   Use Batch API + 20-page limit to keep cost to ~$5–$9 for the full backlog.
2. **Add Phase 7 schema fields** to the `borrowers` table:
   `year_founded`, `institution_type`, `bond_trustee`, `bdo_rating`, `liquidity_covenants`
3. **Build `src/reporting/watchlist_export.py`** — the spreadsheet auto-population module
   that reads extracted metrics and outputs a formatted `.xlsx` watchlist.

See [docs/PHASES.md](docs/PHASES.md) for detailed phase statuses and the full roadmap.

### Live Database State (as of 2026-03-19)

| Entity | Count |
|--------|-------|
| Borrowers (all `higher_ed`) | 30 |
| Bond Issues | 200 |
| Documents discovered | 2,964 |
| Financial statements (pending extraction) | 1,823 |
| Event notices (pending extraction) | 953 |
| Documents extracted via AI | 4 |
| Distress events logged | 3 |
| Borrowers with non-zero distress score | 2 (Rider=55, Lake Erie=10) |

---

## Critical Architectural Rules

### 1. Borrow-Centric Data Model
All data must be organized around the **borrower** entity, not the issuer or CUSIP. The borrower is the credit risk. The issuer is a financing conduit.

Hierarchy: `Borrower → Issuer → Bond Issue → CUSIPs → Documents`

### 2. Separate Discovery from Downloads
- Discovery layer: lightweight JSON/HTML, 1 req/sec max
- Document fetcher: PDFs only, 1 req/2–3 sec, queue-based
- Never mix these two concerns in the same code path

### 3. Incremental Updates Always
Every bond issue has a `last_seen_doc_date` cursor. Always stop parsing when you hit a document older than this date. Never re-scrape what you already have.

### 4. Store Raw Before Parsing
Always download and store PDFs before attempting extraction. Never parse-on-the-fly without storing the raw file. This enables reprocessing as AI models improve.

### 5. Browser-Like HTTP Behavior
All requests must use realistic Chrome User-Agent headers and `requests.Session()` for cookie persistence. Never use default Python-requests headers.

---

## Key Files

| Path | Description |
|------|-------------|
| `docs/PROJECT_OVERVIEW.md` | Full project context |
| `docs/PHASES.md` | Phase-by-phase build plan with current statuses |
| `docs/ARCHITECTURE.md` | System architecture |
| `docs/DATABASE_SCHEMA.md` | Database tables and SQL |
| `docs/EMMA_ENDPOINTS.md` | EMMA API endpoints reference (live-tested corrections) |
| `docs/SCRAPING_STRATEGY.md` | Rate limits, retry, caching |
| `docs/AI_PARSING.md` | AI extraction pipeline and cost optimization |
| `src/scraper/` | Phase 1 — discovery engine (complete) |
| `src/db/models.py` | SQLAlchemy ORM — all tables |
| `src/db/repositories/` | Repository layer per entity |
| `src/parser/pipeline.py` | Phase 4 — AI extraction orchestrator |
| `src/parser/extractor.py` | Claude API extraction functions |
| `src/parser/classifier.py` | Document type classifier |
| `src/parser/pdf_extractor.py` | PDF → text (pdfplumber + OCR) |
| `src/distress/late_filing.py` | Phase 3 — late filing detector |
| `src/distress/scoring.py` | Phase 5/6 — distress score model |
| `src/monitor/` | Phase 5 — market-wide monitoring (not yet built) |
| `src/reporting/` | Phase 7 — spreadsheet export (not yet built) |
| `scripts/seed_borrowers.py` | Seeds 30 higher-ed watchlist borrowers |
| `scripts/seed_fyes.py` | Fiscal year end backfill |
| `scripts/fix_sync_data.py` | One-time data repair utility |
| `data/emma.db` | Live SQLite database |
| `data/raw_documents/` | Downloaded PDFs (by year/month/borrower) |
| `data/queue/queue.json` | JSON-backed download queue (Phase 2 migrates to DB) |

---

## EMMA Endpoints Quick Reference

```
Bond search:           https://emma.msrb.org/api/Search/Issue
Issue details:         https://emma.msrb.org/IssueView/Details/{issueId}
Continuing disclosure: https://emma.msrb.org/IssueView/ContinuingDisclosure/{issueId}
Security details:      https://emma.msrb.org/Security/Details/{securityId}
CUSIP lookup:          https://emma.msrb.org/QuickSearch/Results?quickSearchText={cusip}
Trade history:         https://emma.msrb.org/TradeHistory/{cusip}
Event notices:         https://emma.msrb.org/api/Search/EventNotice
```

---

## Rate Limits

| Layer | Limit |
|-------|-------|
| Discovery (JSON/HTML) | 1 request/second |
| Document downloads (PDF) | 1 request/2–3 seconds |
| Max parallel workers | 3 |
| Total combined throughput | ~1.5 req/sec |

Do not exceed these. EMMA is a public service and we want to remain a respectful, long-term consumer.

---

## Technology Stack

- Language: Python 3.11+
- HTTP: `requests` with `Session()`
- HTML parsing: `BeautifulSoup4`
- PDF extraction: `pdfplumber` (OCR fallback: `pytesseract`)
- Database: PostgreSQL (SQLite acceptable for development)
- ORM: SQLAlchemy
- AI extraction: Claude API (`claude-sonnet-4-6` default, `claude-haiku-4-5` for classification)
- Scheduling: APScheduler or cron

---

## What the Existing Code Does

### Production Code (use and extend these)

- `src/scraper/` — Full Phase 1 discovery engine. Finds bond issues, discovers documents, downloads PDFs, manages rate limits and retries. Entry point: `src/scraper/cli.py`.
- `src/db/` — Complete database layer. Models, engine, repositories for all entities.
- `src/parser/pipeline.py` — Phase 4 AI extraction orchestrator. Run this against the queue.
- `src/parser/extractor.py` — Claude API extraction (financial statements, event notices, operating reports).
- `src/distress/late_filing.py` — Phase 3 late filing detector. Schedule daily.
- `src/distress/scoring.py` — Distress score model. Called automatically by the pipeline.

### Reference Material Only (do not extend)

- `emma_issuer.py` — Early prototype with hardcoded session cookies. Superseded by `src/scraper/session.py`.
- `main.py` — Stub/test file from early exploration.

---

## Coding Guidelines

- Write type annotations for all function signatures
- Use dataclasses or Pydantic models for structured data, not plain dicts
- Log to a structured logger, not print statements
- Handle HTTP errors with retry + exponential backoff (see `docs/SCRAPING_STRATEGY.md`)
- Every database write should be idempotent (safe to run twice)
- Prefer explicit over implicit — name things clearly

---

## What to Avoid

- Do not hardcode session cookies in source code
- Do not write scraping code that hammers the same endpoint in a loop without delays
- Do not parse PDFs inline during download — queue them for separate processing
- Do not skip storing raw documents even if extraction is happening immediately
- Do not model data around CUSIPs or issuers as the primary entity
