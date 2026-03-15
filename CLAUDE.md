# CLAUDE.md — EMMA Municipal Distress Monitoring System

This file provides context for AI agents working on this project. Read it before making any changes.

---

## What This Project Is

A borrower-centric municipal credit monitoring platform that detects early distress signals from EMMA (Electronic Municipal Market Access), the SEC-designated repository for municipal securities disclosures.

This is NOT a bond data scraper. It is a credit intelligence platform organized around borrowers, not CUSIPs or issuers.

---

## Current Phase

**Phase 1 — EMMA Scraping & Discovery Engine**

The immediate goal is building a reliable, respectful scraper that can:
1. Discover bond issues by borrower/issuer name
2. Fetch continuing disclosure document lists for tracked issues
3. Queue and download PDFs
4. Track incremental updates (only fetch new documents)

See [docs/PHASES.md](docs/PHASES.md) for the full roadmap.

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
| `docs/PHASES.md` | Phase-by-phase build plan |
| `docs/ARCHITECTURE.md` | System architecture |
| `docs/DATABASE_SCHEMA.md` | Database tables and SQL |
| `docs/EMMA_ENDPOINTS.md` | EMMA API endpoints reference |
| `docs/SCRAPING_STRATEGY.md` | Rate limits, retry, caching |
| `docs/AI_PARSING.md` | AI extraction pipeline |
| `src/scraper/` | Phase 1 scraping code |
| `src/db/` | Database models |
| `src/monitor/` | Disclosure monitoring |
| `src/parser/` | AI document parsing |
| `src/distress/` | Distress detection and scoring |

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

- `emma_issuer.py` — Early prototype. Given a CUSIP, looks up the issuer name via EMMA QuickSearch. Uses `requests.Session()` and browser-like headers. **Not production-ready** — hardcoded session cookies.
- `main.py` — Stub/test file from early exploration.

These files are reference material. Do not build Phase 1 on top of them directly — start fresh in `src/scraper/`.

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
