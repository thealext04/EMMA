# EMMA Municipal Distress Monitoring System

A borrower-centric municipal credit intelligence platform that detects early signs of financial distress by monitoring disclosures on [EMMA (Electronic Municipal Market Access)](https://emma.msrb.org), the SEC-designated repository for municipal securities.

---

## What This Is

Most EMMA tools are bond metadata scrapers. This system is different — it is a **continuous credit monitoring engine** structured around borrowers, not CUSIPs.

Core capabilities:

- Monitor a curated watchlist of municipal borrowers
- Detect late financial disclosures (a leading distress indicator)
- Scan EMMA market-wide for covenant violations, rating events, and restructurings
- Extract structured financial metrics from PDF filings using AI
- Maintain borrower-level event timelines
- Generate distress risk scores

---

## Why Borrower-Centric Matters

Municipal bonds use conduit financing. The issuer (e.g., Dormitory Authority of NY) is not the credit risk — the **borrower** (e.g., Manhattan College) is. Most tools stop at the issuer or CUSIP level. This system maps everything back to the borrowing entity.

```
Borrower (credit risk)
  └── Issuer (conduit)
        └── Bond Issue / Series
              └── CUSIPs
                    └── Disclosure Documents
```

---

## Project Phases

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | EMMA Scraping & Discovery Engine | In Progress |
| 2 | Database & Borrower-Centric Data Model | Planned |
| 3 | Late Disclosure Detection | Planned |
| 4 | AI Document Parsing Pipeline | Planned |
| 5 | Market-Wide Distress Detection | Planned |
| 6 | Distress Scoring & Reporting | Planned |

See [docs/PHASES.md](docs/PHASES.md) for full phase breakdown.

---

## Documentation

| Document | Description |
|----------|-------------|
| [docs/PROJECT_OVERVIEW.md](docs/PROJECT_OVERVIEW.md) | Full project context and goals |
| [docs/PHASES.md](docs/PHASES.md) | Phase-by-phase build plan |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System architecture and pipeline design |
| [docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md) | Database tables and relationships |
| [docs/SCRAPING_STRATEGY.md](docs/SCRAPING_STRATEGY.md) | EMMA scraping approach and rate limits |
| [docs/EMMA_ENDPOINTS.md](docs/EMMA_ENDPOINTS.md) | EMMA API endpoints reference |
| [docs/AI_PARSING.md](docs/AI_PARSING.md) | AI document classification and extraction |

---

## Repository Structure

```
EMMA/
├── README.md
├── CLAUDE.md                  # Instructions for AI agents
├── docs/
│   ├── PROJECT_OVERVIEW.md
│   ├── PHASES.md
│   ├── ARCHITECTURE.md
│   ├── DATABASE_SCHEMA.md
│   ├── SCRAPING_STRATEGY.md
│   ├── EMMA_ENDPOINTS.md
│   └── AI_PARSING.md
├── src/
│   ├── scraper/               # Phase 1: Discovery & scraping
│   ├── db/                    # Phase 2: Database models
│   ├── monitor/               # Phase 3: Disclosure monitoring
│   ├── parser/                # Phase 4: AI document parsing
│   └── distress/              # Phase 5-6: Detection & scoring
├── data/
│   └── raw_documents/         # Downloaded PDFs (gitignored)
└── scripts/                   # Utility scripts
```

---

## Key Concepts

**Continuing Disclosures** — Documents filed by borrowers: audited financials, operating reports, budget filings. Filed annually, typically within 180 days of fiscal year end.

**Material Event Notices** — High-signal distress indicators: covenant violations, rating changes, payment defaults, bankruptcy filings.

**Late Filing Detection** — Failure to file within the 180-day window is itself a distress signal, often preceding covenant violations or liquidity crises.

---

## Data Sources

All data comes from EMMA (emma.msrb.org), operated by the Municipal Securities Rulemaking Board (MSRB). EMMA is publicly accessible and contains:

- ~1.2M municipal bond issues
- ~50k active issuers
- ~500–2,000 new disclosures per day

This system monitors a focused subset — a watchlist of 50–200 borrowers with 300–1,000 tracked issues.
