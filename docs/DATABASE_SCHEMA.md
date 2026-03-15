# Database Schema

## Design Philosophy

All tables reference back to `borrowers` as the primary entity. The hierarchy is:

```
borrowers
  └── bond_issues (via borrower_id)
        ├── cusips (via issue_id)
        └── documents (via issue_id)
              └── extracted_metrics (via doc_id)

events (via borrower_id)
doc_download_queue (via issue_id, borrower_id)
```

---

## Core Tables

### borrowers

The central entity. Represents the credit risk obligor, not the legal issuer.

```sql
CREATE TABLE borrowers (
    borrower_id     SERIAL PRIMARY KEY,
    borrower_name   TEXT NOT NULL,
    sector          TEXT,          -- higher_ed, healthcare, government, housing, other
    state           CHAR(2),
    city            TEXT,
    distress_status TEXT DEFAULT 'monitor',  -- monitor, watch, distressed, resolved
    distress_score  INTEGER,       -- 0–100
    fiscal_year_end TEXT,          -- MM-DD, e.g. '06-30' for June 30
    on_watchlist    BOOLEAN DEFAULT false,
    watchlist_since DATE,
    watchlist_notes TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);
```

**sector values:** `higher_ed`, `healthcare`, `general_government`, `housing`, `utility`, `transportation`, `other`

**distress_status values:**
- `monitor` — normal surveillance
- `watch` — elevated concern, increased monitoring frequency
- `distressed` — active distress signals present
- `resolved` — distress resolved or bond matured/defeased

---

### issuers

Conduit issuers — the legal entity that issues the bonds, not the borrower.

```sql
CREATE TABLE issuers (
    issuer_id       SERIAL PRIMARY KEY,
    issuer_name     TEXT NOT NULL,
    issuer_type     TEXT,          -- state_authority, county, city, housing_authority, etc.
    state           CHAR(2),
    emma_issuer_id  TEXT UNIQUE,   -- EMMA's internal issuer identifier
    created_at      TIMESTAMP DEFAULT NOW()
);
```

---

### bond_issues

A bond series (e.g., "Series 2019 Revenue Bonds"). One borrower can have many issues across many issuers.

```sql
CREATE TABLE bond_issues (
    issue_id                  SERIAL PRIMARY KEY,
    borrower_id               INTEGER REFERENCES borrowers(borrower_id),
    issuer_id                 INTEGER REFERENCES issuers(issuer_id),
    emma_issue_id             TEXT UNIQUE,   -- EMMA's internal issue ID
    series_name               TEXT,          -- e.g. "Series 2019A"
    par_amount                NUMERIC(18,2),
    issue_date                DATE,
    sale_date                 DATE,
    bond_type                 TEXT,          -- revenue, go, conduit, etc.
    tax_status                TEXT,          -- tax_exempt, taxable, amt
    state                     CHAR(2),
    continuing_disclosure_url TEXT,
    last_disclosure_check     TIMESTAMP,
    last_seen_doc_date        DATE,          -- incremental update cursor
    created_at                TIMESTAMP DEFAULT NOW(),
    updated_at                TIMESTAMP DEFAULT NOW()
);
```

---

### cusips

Individual CUSIP-level detail. One bond issue has one CUSIP per maturity.

```sql
CREATE TABLE cusips (
    cusip_id        SERIAL PRIMARY KEY,
    cusip           CHAR(9) UNIQUE NOT NULL,
    issue_id        INTEGER REFERENCES bond_issues(issue_id),
    maturity_date   DATE,
    coupon_rate     NUMERIC(6,4),  -- as decimal, e.g. 0.0450 = 4.50%
    par_amount      NUMERIC(18,2),
    rating_sp       TEXT,          -- S&P rating at last check
    rating_moodys   TEXT,
    rating_fitch    TEXT,
    callable        BOOLEAN,
    call_date       DATE,
    emma_security_id TEXT,
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);
```

---

### documents

Every disclosure document filed on EMMA for tracked issues.

```sql
CREATE TABLE documents (
    doc_id          SERIAL PRIMARY KEY,
    issue_id        INTEGER REFERENCES bond_issues(issue_id),
    borrower_id     INTEGER REFERENCES borrowers(borrower_id),
    emma_doc_id     TEXT UNIQUE,
    doc_type        TEXT NOT NULL,  -- see Document Types below
    doc_date        DATE,           -- date of the document itself
    posted_date     DATE,           -- date posted to EMMA
    fiscal_year     INTEGER,        -- fiscal year the doc covers, if known
    doc_url         TEXT,
    local_path      TEXT,           -- path to downloaded file
    file_size_bytes INTEGER,
    page_count      INTEGER,
    extraction_status TEXT DEFAULT 'pending',  -- pending, extracted, failed, skipped
    extracted_at    TIMESTAMP,
    created_at      TIMESTAMP DEFAULT NOW()
);
```

**doc_type values:**
- `financial_statement` — audited annual financials
- `event_notice` — material event notice
- `operating_report` — management/operating report
- `budget` — annual budget filing
- `rating_notice` — rating change notification
- `bond_issuance` — official statement or POS
- `other` — unclassified

---

### events

Distress signals and notable events at the borrower level. Drives timelines and scoring.

```sql
CREATE TABLE events (
    event_id        SERIAL PRIMARY KEY,
    borrower_id     INTEGER REFERENCES borrowers(borrower_id),
    doc_id          INTEGER REFERENCES documents(doc_id),  -- source document, if any
    event_type      TEXT NOT NULL,  -- see Event Types below
    event_date      DATE NOT NULL,
    detected_date   DATE DEFAULT CURRENT_DATE,
    severity        TEXT,           -- low, medium, high, critical
    summary         TEXT,           -- human-readable description
    raw_text        TEXT,           -- extracted passage from document
    confirmed       BOOLEAN DEFAULT false,
    created_at      TIMESTAMP DEFAULT NOW()
);
```

**event_type values:**
- `late_filing` — missed disclosure deadline
- `going_concern` — auditor going concern opinion
- `covenant_violation` — covenant breach notice
- `covenant_waiver` — covenant waiver granted
- `rating_downgrade` — rating lowered
- `rating_upgrade` — rating raised
- `rating_withdrawal` — rating withdrawn
- `payment_default` — missed debt service payment
- `forbearance` — forbearance agreement executed
- `debt_restructuring` — debt restructuring filed
- `bankruptcy` — bankruptcy filing
- `liquidity_facility_termination` — credit/liquidity facility terminated
- `financial_statement_filed` — routine filing (positive signal)
- `dscr_breach` — DSCR fallen below covenant threshold

---

### extracted_metrics

Structured financial data extracted from documents by AI.

```sql
CREATE TABLE extracted_metrics (
    metric_id       SERIAL PRIMARY KEY,
    doc_id          INTEGER REFERENCES documents(doc_id),
    borrower_id     INTEGER REFERENCES borrowers(borrower_id),
    fiscal_year     INTEGER,
    period_end_date DATE,

    -- Income / Revenue
    total_revenue           NUMERIC(18,2),
    operating_revenue       NUMERIC(18,2),
    net_income              NUMERIC(18,2),
    operating_income        NUMERIC(18,2),
    ebitda                  NUMERIC(18,2),

    -- Liquidity
    days_cash_on_hand       NUMERIC(8,2),
    cash_and_investments    NUMERIC(18,2),
    unrestricted_net_assets NUMERIC(18,2),

    -- Debt
    total_long_term_debt    NUMERIC(18,2),
    annual_debt_service     NUMERIC(18,2),
    dscr                    NUMERIC(8,4),  -- debt service coverage ratio

    -- Higher Ed Specific
    total_enrollment        INTEGER,
    fte_enrollment          NUMERIC(10,2),
    tuition_revenue         NUMERIC(18,2),
    tuition_discount_rate   NUMERIC(6,4),
    endowment_value         NUMERIC(18,2),

    -- Healthcare Specific
    licensed_beds           INTEGER,
    staffed_beds            INTEGER,
    patient_admissions      INTEGER,
    patient_days            INTEGER,
    net_patient_revenue     NUMERIC(18,2),
    days_ar                 NUMERIC(8,2),  -- days in accounts receivable

    -- Extraction metadata
    extraction_model        TEXT,           -- which AI model extracted this
    extraction_confidence   TEXT,           -- high, medium, low
    extracted_at            TIMESTAMP DEFAULT NOW()
);
```

---

### doc_download_queue

Durable download queue. Survives process restarts.

```sql
CREATE TABLE doc_download_queue (
    id              SERIAL PRIMARY KEY,
    doc_url         TEXT NOT NULL UNIQUE,
    issue_id        INTEGER,
    borrower_id     INTEGER,
    doc_type_hint   TEXT,
    discovered_at   TIMESTAMP DEFAULT NOW(),
    status          TEXT DEFAULT 'pending',  -- pending, downloading, downloaded, failed
    attempts        INTEGER DEFAULT 0,
    last_attempt    TIMESTAMP,
    last_error      TEXT,
    priority        INTEGER DEFAULT 5,       -- 1=highest, 10=lowest
    downloaded_at   TIMESTAMP,
    local_path      TEXT
);
```

---

## Indexes (Performance Critical)

```sql
-- Most frequent queries
CREATE INDEX idx_documents_borrower_date ON documents(borrower_id, posted_date DESC);
CREATE INDEX idx_documents_type_date ON documents(doc_type, posted_date DESC);
CREATE INDEX idx_events_borrower_date ON events(borrower_id, event_date DESC);
CREATE INDEX idx_events_type ON events(event_type);
CREATE INDEX idx_cusips_issue ON cusips(issue_id);
CREATE INDEX idx_bond_issues_borrower ON bond_issues(borrower_id);
CREATE INDEX idx_queue_status_priority ON doc_download_queue(status, priority);
CREATE INDEX idx_metrics_borrower_year ON extracted_metrics(borrower_id, fiscal_year);
```

---

## Key Query Patterns

```sql
-- All documents for a borrower in the last 90 days
SELECT d.* FROM documents d
JOIN bond_issues bi ON d.issue_id = bi.issue_id
WHERE bi.borrower_id = :borrower_id
  AND d.posted_date >= NOW() - INTERVAL '90 days'
ORDER BY d.posted_date DESC;

-- Late filers (expected filing date passed, no financial statement)
SELECT b.borrower_name,
       b.fiscal_year_end,
       (DATE_TRUNC('year', NOW()) + FYE_OFFSET + INTERVAL '180 days') AS expected_date,
       MAX(d.doc_date) AS last_filed
FROM borrowers b
LEFT JOIN bond_issues bi ON bi.borrower_id = b.borrower_id
LEFT JOIN documents d ON d.issue_id = bi.issue_id AND d.doc_type = 'financial_statement'
WHERE b.on_watchlist = true
GROUP BY b.borrower_id
HAVING expected_date < NOW() AND (last_filed IS NULL OR last_filed < expected_date - INTERVAL '1 year');

-- Borrower event timeline
SELECT event_date, event_type, severity, summary
FROM events
WHERE borrower_id = :borrower_id
ORDER BY event_date DESC;

-- Highest distress score borrowers
SELECT b.borrower_name, b.distress_score, b.distress_status, b.sector
FROM borrowers b
WHERE b.on_watchlist = true
ORDER BY b.distress_score DESC;
```
