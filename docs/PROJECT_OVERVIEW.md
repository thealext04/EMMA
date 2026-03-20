# Project Overview — EMMA Municipal Distress Monitoring System

## Purpose

This system is a municipal credit intelligence platform designed to detect early signs of financial distress among municipal borrowers. It uses data from EMMA (Electronic Municipal Market Access), the SEC-designated official repository for municipal securities disclosures, operated by the MSRB (Municipal Securities Rulemaking Board).

The goal is not to build another bond data scraper. The goal is to build a **continuous, borrower-centric distress monitoring engine** capable of identifying deteriorating credits before rating agencies act.

---

## What EMMA Contains

EMMA is the authoritative source for three types of municipal data:

### 1. Bond Metadata (Static)
- CUSIP identifiers
- Coupon rates and maturities
- Par amounts
- Issuer names
- Issue dates and series names

### 2. Continuing Disclosure Filings (Dynamic)
Documents uploaded by issuers/borrowers on an ongoing basis:
- Audited financial statements (annual)
- Operating and management reports
- Budget filings
- Enrollment reports (education sector)
- Patient volume reports (healthcare sector)

These are typically 80–200 page PDFs requiring AI extraction to be useful.

### 3. Material Event Notices (High Signal)
Mandatory disclosures of significant events:
- Covenant violations
- Rating changes (upgrades and downgrades)
- Payment defaults
- Bankruptcy filings
- Forbearance agreements
- Debt restructurings
- Liquidity facility terminations

These are the **highest-value distress signals** in the system.

---

## How Municipal Finance Works (Why Borrower-Centric Matters)

Municipal bonds frequently use **conduit financing**:

```
Borrower:        Manhattan College          ← credit risk
Conduit Issuer:  Dormitory Authority of NY  ← legal issuer
Bond Series:     Series 2019 Revenue Bonds
CUSIPs:          one per maturity date
```

The **issuer** is a financing conduit. The **borrower** bears the credit risk and is responsible for repayment.

Most EMMA tools track data at the issuer or CUSIP level. This fundamentally misidentifies where the credit risk lives. **This system is organized around borrowers.**

---

## Four Monitoring Modules

### Module 1 — Watchlist Monitoring

A curated list of borrowers is continuously monitored. For each borrower, the system tracks:

- New continuing disclosure filings
- Audited financial statements
- Material event notices
- Rating changes
- Debt restructuring activity

Events are assembled into a borrower timeline:

```
Manhattan College
  2026-03-12  FY2025 financial statements filed
  2026-05-11  Moody's downgrade: Baa3 → Ba1
  2026-06-02  Covenant waiver notice filed
```

### Module 2 — Late Disclosure Detection

Municipal borrowers must file annual financial statements within **180 days of fiscal year end**. Failure to file is a mandatory disclosure event — and a strong early distress indicator.

Typical delinquency pattern:
1. Late filing (silent warning)
2. Auditor issues going concern opinion
3. Covenant violation notice filed
4. Rating downgrade
5. Forbearance or restructuring

The system computes expected filing dates for all watchlist borrowers and flags delinquencies in real time.

Example output:
```
Borrower:         Manhattan College
Fiscal Year End:  June 30, 2025
Expected Filing:  December 27, 2025
Actual Filing:    Not yet filed
Days Late:        79
```

### Module 3 — Market-Wide Distress Detection

The system scans all new EMMA filings (500–2,000/day) for distress signals:

- Covenant violations
- Forbearance agreements
- Going concern opinions
- Rating downgrades
- Liquidity facility terminations
- Debt restructurings
- Failure-to-file notices

When a new signal is detected, the borrower is surfaced for watchlist consideration.

### Module 4 — Watchlist Spreadsheet Export (Phase 7)

A structured Excel output that auto-populates from extracted metrics, replacing the
manually maintained credit tracking spreadsheet. One row per borrower, columns spanning
FY2022–FY2025 for key financial metrics, with computed YoY change columns.

Key fields auto-populated: revenue, contributions/gifts, enrollment, operating expenses,
operating profit/loss, margins, total cash & investments, coverage ratios, DSCR, credit
rating, distress status, EMMA link, and last-updated timestamp.

The export is scheduled to refresh automatically. Analyst-curated fields (BDO rating,
key notes, liquidity covenant terms) are maintained in the database and merged at export time.

---

## Competitive Differentiation

Most existing EMMA tools (e.g., Apify `emma-municipal-bonds-scraper`) collect bond metadata: coupons, maturities, yields, ratings at issuance. They are **bond listing scrapers**.

This system is a **municipal distress intelligence platform**:

| Capability | Bond Scraper | This System |
|---|---|---|
| Bond metadata | Yes | Yes |
| Continuing disclosure monitoring | No | Yes |
| Late filing detection | No | Yes |
| AI financial extraction from PDFs | No | Yes |
| Borrower-level event timelines | No | Yes |
| Distress risk scoring | No | Yes |
| Market-wide signal detection | No | Yes |
| Watchlist management | No | Yes |
| Automated spreadsheet reporting | No | Yes (Phase 7) |

---

## Target Scale

The system is designed to monitor a focused universe, not crawl EMMA exhaustively:

| Metric | Target Range |
|--------|-------------|
| Watchlist borrowers | 50–200 |
| Monitored bond issues | 300–1,000 |
| New documents processed/day | 10–50 |
| Documents requiring AI extraction | 2–10/day |

Market-wide scans cover all new filings but only trigger AI extraction for classified high-value documents.

---

## Long-Term Vision

The platform becomes a **Municipal Distress Early Warning System** capable of:

- Detecting deteriorating credits 6–18 months before rating agency action
- Extracting 30+ financial metrics from PDFs automatically
- Generating executive-level credit reports on demand
- Alerting on new distress discoveries in near real-time
- Maintaining historical borrower financial profiles over time
