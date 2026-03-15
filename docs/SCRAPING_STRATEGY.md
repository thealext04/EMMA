# Scraping Strategy

## Core Principle: Act Like a Researcher

The goal is traffic that looks like a research analyst browsing EMMA — not a bot crawling it. A human researcher:

- Opens a handful of bookmarked issuer pages each morning
- Downloads new PDFs that weren't there yesterday
- Searches for specific issuers by name
- Browses slowly and purposefully

This system replicates that behavior pattern through architecture, not stealth.

---

## The Two-Layer Approach

### Layer 1 — Discovery (Fast, Lightweight)

Discovers which documents exist. Never downloads PDFs here.

- Calls JSON/HTML endpoints only
- 20–80 KB responses
- 1 request/second
- Runs on schedule, not continuously

### Layer 2 — Fetch (Slow, PDF-Heavy)

Downloads actual documents from the queue.

- 1 request per 2–3 seconds
- Up to 3 concurrent workers (~1.5 req/sec total)
- Completely decoupled from discovery

**Why separate?** Discovery can run daily across hundreds of issues without much traffic. Downloading PDFs is expensive (in bandwidth and server load) and should only happen once per document.

---

## Request Configuration

### Headers

```python
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://emma.msrb.org/",
}
```

**Never use** Python-requests default headers (`python-urllib/3.x` or similar). These are trivially detected.

### Session Setup

```python
import requests

session = requests.Session()
session.headers.update(HEADERS)

# Warm up the session — picks up cookies naturally
session.get("https://emma.msrb.org/", timeout=15)
```

Use `requests.Session()` for all requests. Benefits:
- Cookies persist across calls (looks like a browser)
- TCP connections are reused (lower overhead)
- Consistent session identity

---

## Rate Limiting

```python
import time

# Discovery layer
DISCOVERY_DELAY = 1.0   # seconds between requests

# Document downloads
DOWNLOAD_DELAY = 2.5    # seconds between requests

def polite_get(session, url, params=None, is_download=False):
    delay = DOWNLOAD_DELAY if is_download else DISCOVERY_DELAY
    response = session.get(url, params=params, timeout=20)
    response.raise_for_status()
    time.sleep(delay)
    return response
```

---

## Incremental Updates (Critical for Scale)

**Never re-scrape documents you already have.**

Each bond issue in the database stores a `last_seen_doc_date` cursor. When scanning:

```python
def discover_new_documents(session, issue):
    docs = fetch_disclosure_page(session, issue.emma_issue_id)
    new_docs = []

    for doc in docs:  # assumed sorted newest-first
        if doc.posted_date <= issue.last_seen_doc_date:
            break  # Everything from here on is already known
        new_docs.append(doc)

    if new_docs:
        issue.last_seen_doc_date = new_docs[0].posted_date
        queue_for_download(new_docs)

    return new_docs
```

This reduces scraping volume by 90%+ after the initial load.

---

## Retry Strategy

```python
import time

def fetch_with_retry(session, url, max_attempts=3):
    delays = [5, 30, 120]  # seconds

    for attempt in range(max_attempts):
        try:
            response = session.get(url, timeout=20)

            if response.status_code == 429:
                # Rate limited — back off longer
                time.sleep(delays[attempt] * 2)
                continue

            response.raise_for_status()
            return response

        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt == max_attempts - 1:
                raise
            time.sleep(delays[attempt])

    raise Exception(f"Failed after {max_attempts} attempts: {url}")
```

**Backoff schedule:**
- Attempt 1: wait 5 seconds
- Attempt 2: wait 30 seconds
- Attempt 3: wait 2 minutes
- After max retries: mark as `failed` in queue, log error

---

## Caching

Cache discovery responses locally to avoid redundant network calls:

```python
import hashlib, json, os
from datetime import datetime, timedelta

CACHE_DIR = "data/.cache"

def cached_get(session, url, ttl_hours=24):
    cache_key = hashlib.md5(url.encode()).hexdigest()
    cache_path = os.path.join(CACHE_DIR, cache_key + ".json")

    if os.path.exists(cache_path):
        with open(cache_path) as f:
            entry = json.load(f)
        if datetime.fromisoformat(entry["expires"]) > datetime.now():
            return entry["content"]

    response = session.get(url, timeout=20)
    response.raise_for_status()

    content = response.text
    expires = (datetime.now() + timedelta(hours=ttl_hours)).isoformat()

    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(cache_path, "w") as f:
        json.dump({"content": content, "expires": expires}, f)

    return content
```

**Cache TTLs:**

| Page Type | TTL |
|-----------|-----|
| Issue details | 30 days |
| Continuing disclosure list | 24 hours |
| Search results | 6 hours |
| Trade data | 1 hour |
| Security / CUSIP details | 30 days |

---

## Document Storage

Store raw PDFs before any parsing:

```
data/raw_documents/{YYYY}/{MM}/{borrower_slug}/{date}_{type}_{id}.pdf
```

Example:
```
data/raw_documents/2026/03/manhattan-college/20260312_financial_statement_abc123.pdf
```

**Why store raw files?**
- AI models improve — you can reprocess historical documents
- Extraction failures can be retried without re-downloading
- Audit trail of what was collected and when
- Database can be rebuilt from raw files if corrupted

---

## Scheduling

Run jobs on a predictable schedule. Avoid random timing or continuous polling.

```
Daily jobs (run 6:00 AM ET):
  - Watchlist disclosure check (all watchlist borrowers)
  - Market-wide event notice scan
  - Late filing detector
  - Trade data update (watchlist CUSIPs)

Weekly job (run Sunday midnight):
  - Market-wide new issuer/issue scan

Monthly job (run 1st of month):
  - Full bond universe metadata refresh
  - Stale cache cleanup
```

---

## What to Monitor (Scraper Health)

Log these metrics per run:

```python
scraper_metrics = {
    "run_date": ...,
    "issues_checked": ...,
    "new_documents_discovered": ...,
    "documents_queued": ...,
    "documents_downloaded": ...,
    "download_failures": ...,
    "request_success_rate": ...,   # alert if < 95%
    "avg_response_ms": ...,
    "http_429_count": ...,         # alert if > 0
    "http_503_count": ...,
}
```

If `http_429_count > 0`, reduce rate immediately and investigate.

---

## What Not to Do

- **Don't** crawl the entire bond universe daily — use incremental updates
- **Don't** download PDFs you've already stored
- **Don't** run parallel workers at high concurrency (>3 workers max)
- **Don't** hardcode session cookies — derive them from session flow
- **Don't** ignore 429 responses — they signal you're going too fast
- **Don't** scrape from a residential IP at production scale — use a stable server IP
- **Don't** re-request identical pages more than once per cache TTL
