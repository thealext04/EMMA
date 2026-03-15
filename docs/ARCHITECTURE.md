# System Architecture

## Core Design Principle

**Separate discovery from document collection.**

The system has three independent layers that run at different speeds:

```
EMMA Website / APIs
        │
        ▼
┌─────────────────┐
│  Discovery Layer │  ← lightweight, fast (JSON/HTML only)
└────────┬────────┘
         │ queues new documents
         ▼
┌─────────────────┐
│  Document Queue  │  ← durable, crash-safe
└────────┬────────┘
         │ workers pull and fetch
         ▼
┌──────────────────────┐
│  Document Fetcher     │  ← slow, respectful (PDFs)
└────────┬─────────────┘
         │ stores raw files
         ▼
┌──────────────────────┐
│  Raw Document Store   │  ← local filesystem
└────────┬─────────────┘
         │ queues for AI
         ▼
┌──────────────────────┐
│  AI Extraction Layer  │  ← expensive, on-demand
└────────┬─────────────┘
         │ writes structured output
         ▼
┌──────────────────────┐
│  Database             │  ← borrower-centric
└────────┬─────────────┘
         │
         ▼
┌──────────────────────┐
│  Monitoring / Alerts  │
└──────────────────────┘
```

---

## Layer 1 — Discovery

**Purpose:** Find new documents and changes on EMMA. Does not download PDFs.

**Inputs:** Issue IDs, borrower names, EMMA search queries

**Outputs:** New document URLs added to the download queue

**Design:**
- Calls EMMA JSON endpoints (lightweight, 20–80 KB responses)
- Compares results against `last_seen_document_date` stored per issue
- Stops parsing once it hits documents older than the stored date
- Runs on a schedule: watchlist daily, market-wide weekly

**Rate limit:** 1 request/second

---

## Layer 2 — Document Queue

**Purpose:** Durable list of documents pending download. Survives crashes.

**Schema:**
```sql
doc_download_queue
  id            SERIAL PRIMARY KEY
  doc_url       TEXT NOT NULL UNIQUE
  issue_id      TEXT
  borrower_id   INTEGER
  doc_type_hint TEXT          -- from discovery metadata
  discovered_at TIMESTAMP
  status        TEXT          -- pending, downloading, downloaded, failed
  attempts      INTEGER DEFAULT 0
  last_error    TEXT
  priority      INTEGER DEFAULT 5
```

**Statuses:**
- `pending` — waiting for a worker
- `downloading` — claimed by a worker
- `downloaded` — file on disk, ready for classification
- `failed` — max retries exceeded

---

## Layer 3 — Document Fetcher

**Purpose:** Downloads PDFs from EMMA slowly and stores them on disk.

**Design:**
- Pulls from queue (status = `pending`, ordered by priority DESC)
- Downloads at 1 request per 2–3 seconds
- Stores to `data/raw_documents/YYYY/MM/<borrower_slug>/<filename>`
- Updates queue status on success or failure
- Exponential backoff on errors: 5s → 30s → 2min
- Max 3 retries

**Concurrency:** Up to 3 parallel workers = ~1.5 requests/sec total

---

## Layer 4 — AI Extraction

**Purpose:** Extract structured financial data from PDF documents.

**Design:**
- Triggered by downloaded documents that pass classification
- Only runs AI on high-value document types (financial statements, event notices, operating reports)
- Skips bond issuance documents, offering memoranda, etc.
- Outputs validated JSON per document
- Stores both raw extracted text and structured output

**Cost control:**
- Classify first (cheap/free), extract only on high-value docs
- Estimated 2–10 AI extractions per day for a 50–200 borrower watchlist
- Target: <$0.10/document average

---

## Scheduling

| Job | Frequency | Description |
|-----|-----------|-------------|
| Watchlist disclosure check | Daily | Scan disclosure pages for watchlist borrowers |
| Market-wide event scan | Daily | Check all new event notices |
| Late filing detector | Daily | Compare expected vs actual filing dates |
| Trade data fetch | Daily | Yield/price data for CUSIP watchlist |
| Full bond universe refresh | Monthly | Rebuild issue → borrower mappings |
| Distress score update | Daily | Recompute scores after new data |

---

## HTTP Behavior

All requests must look like a normal browser session:

```python
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://emma.msrb.org/",
}
```

Use `requests.Session()` for persistent cookies and connection pooling.

Never use default `python-requests` headers — trivially detected.

---

## Caching

Many EMMA pages are static or change infrequently. Cache aggressively:

| Page Type | Cache TTL |
|-----------|-----------|
| Issue details | 30 days |
| CUSIP details | 30 days |
| Disclosure document list | 24 hours |
| Search results | 6 hours |
| Trade data | 1 hour |

Cache key: `{endpoint}:{id}:{page_type}`

---

## Storage Layout

```
data/
└── raw_documents/
    └── YYYY/
        └── MM/
            └── {borrower_slug}/
                └── {doc_date}_{doc_type}_{doc_id}.pdf
```

Raw documents are always stored before parsing. This enables:
- Reprocessing with improved AI prompts
- Auditing what was downloaded vs extracted
- Recovery from database corruption

---

## Error Handling

```
Request failure
    └── Retry with exponential backoff
            ├── Attempt 1: wait 5s
            ├── Attempt 2: wait 30s
            └── Attempt 3: wait 2min
                    └── Mark as failed, alert operator
```

Monitor these metrics:
- Request success rate (alert if <95%)
- Average response latency
- HTTP 429 / 503 rate (rate limit signals)
- Documents discovered per run
- Documents downloaded per run
- AI extraction failures

---

## Technology Stack (Recommended)

| Component | Technology |
|-----------|-----------|
| Scraper | Python + requests + BeautifulSoup |
| Queue | PostgreSQL table (simple, durable) |
| Database | PostgreSQL |
| ORM | SQLAlchemy |
| PDF extraction | pdfplumber + pytesseract (OCR fallback) |
| AI extraction | Claude API (claude-opus-4-6 or claude-sonnet-4-6) |
| Scheduling | cron or APScheduler |
| Storage | Local filesystem (S3-compatible later) |

---

## What "Behave Like a Researcher" Means

From EMMA's perspective, this system should look like:

> A research analyst who opens EMMA each morning, checks their bookmarked issuers, downloads a few new PDFs, and browses for an hour.

Not like:

> A bot hammering 1,000 requests per minute across the entire site.

The system achieves this through:
- Low, predictable request rates
- Incremental updates (not full re-crawls)
- Persistent sessions with real-looking headers
- Human-scale scheduling (daily jobs, not continuous polling)
