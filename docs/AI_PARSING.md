# AI Document Parsing

## Why AI Is Required

Municipal financial disclosures are PDF documents, typically 80–200 pages. They contain the most important credit information — revenue trends, cash positions, debt coverage, enrollment — buried in narrative text, footnotes, and financial tables.

Traditional scrapers cannot reliably extract this. AI document parsing is required.

---

## Cost Control: Classify Before Extracting

The single most important cost optimization: **classify every document before sending it to AI extraction.**

Most documents on EMMA are not worth AI processing:
- Bond issuance documents (offering statements, POS) — legal boilerplate
- Trustee notices — administrative
- Auditor independence letters — routine
- Supplement filings — usually minor updates

Only a few document types warrant full AI extraction.

**Estimated classification distribution for a watchlist of 100 borrowers:**

| Document Type | % of Filings | AI Extract? |
|---------------|-------------|-------------|
| Audited financial statement | 15% | Yes — full |
| Event notice | 10% | Yes — event type + summary |
| Operating/management report | 20% | Yes — operating metrics |
| Budget | 5% | Partial — deficit signals only |
| Bond issuance / official statement | 30% | No |
| Rating notice | 5% | Yes — brief |
| Other / administrative | 15% | No |

**This reduces AI calls by ~70% vs processing everything.**

---

## Document Classification

### Step 1 — Metadata Classification (Free)

EMMA provides a document type label with each filing. Use this as the first pass:

```python
EMMA_TYPE_MAP = {
    "Annual Financial Report":          "financial_statement",
    "Audited Financial Statements":     "financial_statement",
    "Operating Data":                   "operating_report",
    "Event Notice":                     "event_notice",
    "Failure to File Notice":           "event_notice",
    "Rating Change":                    "rating_notice",
    "Official Statement":               "bond_issuance",
    "Preliminary Official Statement":   "bond_issuance",
    "Budget":                           "budget",
    # ... extend as needed
}
```

This alone handles ~80% of classification correctly.

### Step 2 — Title/Filename Heuristic

Apply keyword matching to the document title:

```python
TITLE_KEYWORDS = {
    "financial_statement": [
        "audited", "financial statement", "annual report",
        "audit report", "comprehensive annual"
    ],
    "event_notice": [
        "covenant", "default", "rating", "waiver", "forbearance",
        "bankruptcy", "restructur", "material event"
    ],
    "operating_report": [
        "operating", "management", "enrollment", "patient", "occupancy"
    ],
}
```

### Step 3 — First-Page AI Classification (Cheap)

For ambiguous documents, extract the first 2 pages of text and classify with a lightweight prompt:

```
Classify this municipal bond disclosure document.
First 2 pages: {text}

Respond with one of:
- financial_statement
- event_notice
- operating_report
- budget
- rating_notice
- bond_issuance
- other

Respond with only the category name.
```

Uses a small/fast model. Cost: ~$0.001 per document.

---

## PDF Text Extraction

```python
import pdfplumber

def extract_text(pdf_path: str, max_pages: int = None) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        pages = pdf.pages[:max_pages] if max_pages else pdf.pages
        text = "\n\n".join(
            page.extract_text() or "" for page in pages
        )
    return text
```

For scanned PDFs (image-based), fall back to OCR:

```python
import pytesseract
from pdf2image import convert_from_path

def extract_text_ocr(pdf_path: str) -> str:
    images = convert_from_path(pdf_path, dpi=200)
    return "\n\n".join(
        pytesseract.image_to_string(img) for img in images
    )
```

Detection heuristic: if `pdfplumber` returns <100 characters for a page that clearly has content, it's a scanned PDF.

---

## AI Extraction Prompts

### Financial Statement Extraction

```
You are extracting financial data from a municipal bond issuer's audited financial statement.

Document text:
{full_document_text}

Extract the following metrics for the most recent fiscal year. If a value is not found, return null.

Return as JSON:
{
  "fiscal_year_end": "YYYY-MM-DD",
  "total_revenue": null,
  "operating_revenue": null,
  "net_income": null,
  "operating_income": null,
  "ebitda": null,
  "days_cash_on_hand": null,
  "cash_and_investments": null,
  "unrestricted_net_assets": null,
  "total_long_term_debt": null,
  "annual_debt_service": null,
  "dscr": null,
  "going_concern_opinion": false,
  "going_concern_text": null,
  "notes": "any relevant observations"
}

All dollar values in thousands (000s omitted).
```

### Higher Education Supplement

Append to financial statement prompt when sector = `higher_ed`:

```
Also extract:
{
  "total_enrollment": null,
  "fte_enrollment": null,
  "tuition_revenue": null,
  "tuition_discount_rate": null,
  "endowment_value": null
}
```

### Healthcare Supplement

Append when sector = `healthcare`:

```
Also extract:
{
  "licensed_beds": null,
  "staffed_beds": null,
  "patient_admissions": null,
  "patient_days": null,
  "net_patient_revenue": null,
  "days_ar": null
}
```

### Event Notice Extraction

```
You are analyzing a material event notice for a municipal bond.

Document text:
{document_text}

Extract:
{
  "event_type": "covenant_violation | payment_default | rating_change | bankruptcy | forbearance | debt_restructuring | going_concern | other",
  "event_date": "YYYY-MM-DD or null",
  "borrower_name": null,
  "issuer_name": null,
  "severity": "low | medium | high | critical",
  "summary": "2-3 sentence plain English summary of what happened",
  "key_passage": "most relevant verbatim quote from the document"
}
```

---

## Going Concern Detection (High Priority)

A going concern opinion is a critical distress signal. It must be detected with very high recall — false negatives are worse than false positives.

Keyword pre-filter (fast, before AI):

```python
GOING_CONCERN_KEYWORDS = [
    "going concern",
    "substantial doubt",
    "ability to continue as a going concern",
    "going-concern",
]

def has_going_concern_risk(text: str) -> bool:
    text_lower = text.lower()
    return any(kw in text_lower for kw in GOING_CONCERN_KEYWORDS)
```

If keywords are found, run AI to confirm and extract context.

---

## Structured Output Validation

Always validate AI output before storing:

```python
from pydantic import BaseModel, validator
from typing import Optional
from datetime import date

class FinancialMetrics(BaseModel):
    fiscal_year_end: Optional[date]
    total_revenue: Optional[float]
    net_income: Optional[float]
    days_cash_on_hand: Optional[float]
    dscr: Optional[float]
    going_concern_opinion: bool = False

    @validator('dscr')
    def dscr_reasonable(cls, v):
        if v is not None and (v < 0 or v > 50):
            raise ValueError(f"DSCR {v} outside plausible range")
        return v

    @validator('days_cash_on_hand')
    def cash_days_reasonable(cls, v):
        if v is not None and v > 3650:  # >10 years is implausible
            raise ValueError(f"Days cash {v} implausible")
        return v
```

Store both the raw AI response and the validated structured output.

---

## Model Selection

| Task | Recommended Model | Reason |
|------|------------------|--------|
| Document classification (first-page) | claude-haiku-4-5 | Fast, cheap, binary output |
| Financial statement extraction | claude-sonnet-4-6 | Balance of accuracy and cost |
| Event notice extraction | claude-sonnet-4-6 | Good at structured extraction |
| Complex/ambiguous documents | claude-opus-4-6 | Best accuracy for edge cases |

Start with `claude-sonnet-4-6` for extraction. Upgrade to Opus only for documents where Sonnet confidence is flagged as low.

---

## Pipeline Flow

```
New document in queue (status = downloaded)
        │
        ▼
Metadata classification (EMMA type label)
        │
        ├── bond_issuance → skip (status = skipped)
        ├── other admin → skip
        └── financial_statement / event_notice / operating_report
                │
                ▼
        Extract PDF text (pdfplumber)
                │
                ├── <100 chars/page → OCR fallback
                └── Text OK
                        │
                        ▼
                Keyword pre-filters
                (going concern, covenant, default)
                        │
                        ▼
                AI extraction prompt
                (sector-specific)
                        │
                        ▼
                Output validation (Pydantic)
                        │
                        ├── Validation error → log, flag for review
                        └── Valid
                                │
                                ▼
                        Write to extracted_metrics
                        Generate events if signals found
                        Update document status = extracted
```

---

## Cost Estimates

Assumptions: 100-borrower watchlist, ~10 AI-worthy documents/day

| Task | Volume/day | Cost/call | Daily cost |
|------|-----------|-----------|-----------|
| Classification (haiku) | 50 docs | $0.001 | $0.05 |
| Financial statement extraction (sonnet) | 3 docs | $0.05 | $0.15 |
| Event notice extraction (sonnet) | 5 docs | $0.03 | $0.15 |
| Operating report (sonnet) | 2 docs | $0.04 | $0.08 |
| **Total** | | | **~$0.43/day** |

Monthly cost estimate: **~$13/month** for a 100-borrower watchlist.

Scale to 500 borrowers: ~$65/month. Still very low.
