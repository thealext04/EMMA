"""
extractor.py — AI extraction of structured financial data from PDF text.

Handles three document types:
  - financial_statement: Full P&L, balance sheet, DSCR, going concern
  - event_notice:        Material event classification and summary
  - operating_report:    Operating metrics (enrollment, revenue, occupancy)

Sector-specific supplements:
  - higher_ed:   enrollment, tuition, endowment
  - healthcare:  beds, admissions, patient revenue

Going concern detection:
  Fast keyword pre-filter before any AI call. High recall is critical —
  false negatives (missing a going concern) are worse than false positives.

Pydantic validation:
  All AI output is validated before storage. Out-of-range values are flagged
  and the raw JSON is always preserved alongside validated data.

Usage:
    from src.parser.extractor import extract_financial_statement, extract_event_notice
    from src.config import settings
    import anthropic

    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    metrics, sector_metrics, raw_json = extract_financial_statement(
        text, sector="higher_ed", borrower_name="Rider University", client=client
    )
"""

import json
import logging
from datetime import date
from typing import Optional, Union

from pydantic import BaseModel, field_validator, model_validator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Going concern detection
# ---------------------------------------------------------------------------

GOING_CONCERN_KEYWORDS = [
    "going concern",
    "substantial doubt",
    "ability to continue as a going concern",
    "going-concern",
    "raise substantial doubt",
    "raised substantial doubt",
]


def has_going_concern_risk(text: str) -> bool:
    """
    Fast keyword pre-filter for going concern language.
    Run this BEFORE AI extraction to decide if going concern check is needed.
    High recall — false negatives are worse than false positives here.
    """
    text_lower = text.lower()
    return any(kw in text_lower for kw in GOING_CONCERN_KEYWORDS)


# ---------------------------------------------------------------------------
# Pydantic output models
# ---------------------------------------------------------------------------

class FinancialMetrics(BaseModel):
    """
    Core financial metrics extracted from a financial disclosure document.
    All dollar values are in thousands (as presented in the document).

    period_type:   "annual" | "interim" | "unknown"
    period_months: months covered (12=annual, 6=semi-annual, 3=quarterly)
    period_label:  human-readable period description for display
    """
    period_type: str = "unknown"          # annual | interim | unknown
    period_months: Optional[int] = None   # 12, 6, 3, etc.
    period_label: Optional[str] = None    # e.g. "FY2025 Annual", "6-month ended Dec 31 2025"
    fiscal_year_end: Optional[date] = None
    total_revenue: Optional[float] = None
    operating_revenue: Optional[float] = None
    net_income: Optional[float] = None
    operating_income: Optional[float] = None
    ebitda: Optional[float] = None
    days_cash_on_hand: Optional[float] = None
    cash_and_investments: Optional[float] = None
    unrestricted_net_assets: Optional[float] = None
    total_long_term_debt: Optional[float] = None
    annual_debt_service: Optional[float] = None
    dscr: Optional[float] = None
    going_concern_opinion: bool = False
    going_concern_text: Optional[str] = None
    notes: Optional[str] = None

    @field_validator("dscr")
    @classmethod
    def dscr_reasonable(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and (v < 0 or v > 50):
            logger.warning("DSCR value %s outside plausible range (0–50) — setting None", v)
            return None
        return v

    @field_validator("days_cash_on_hand")
    @classmethod
    def cash_days_reasonable(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and v > 3650:  # > 10 years is implausible
            logger.warning("days_cash_on_hand %s implausible (>3650) — setting None", v)
            return None
        return v

    @field_validator("total_revenue", "operating_revenue", "net_income",
                     "operating_income", "ebitda", "cash_and_investments",
                     "unrestricted_net_assets", "total_long_term_debt",
                     "annual_debt_service")
    @classmethod
    def dollar_reasonable(cls, v: Optional[float]) -> Optional[float]:
        # Reject values that are clearly wrong orders of magnitude
        # Municipal borrowers: revenue typically $1M–$10B range (in thousands: 1,000–10,000,000)
        if v is not None and abs(v) > 100_000_000:
            logger.warning("Dollar value %s seems implausibly large — setting None", v)
            return None
        return v


class HigherEdMetrics(BaseModel):
    """Sector-specific metrics for higher education borrowers."""
    total_enrollment: Optional[int] = None
    fte_enrollment: Optional[float] = None
    tuition_revenue: Optional[float] = None          # in thousands
    tuition_discount_rate: Optional[float] = None    # 0.0–1.0 (e.g. 0.45 = 45%)
    endowment_value: Optional[float] = None          # in thousands

    @field_validator("tuition_discount_rate")
    @classmethod
    def discount_rate_range(cls, v: Optional[float]) -> Optional[float]:
        if v is not None:
            # Accept either decimal (0.45) or percentage (45.0) — normalize to decimal
            if v > 1.0:
                v = v / 100.0
            if not (0.0 <= v <= 1.0):
                logger.warning("tuition_discount_rate %s out of range — setting None", v)
                return None
        return v

    @field_validator("total_enrollment", "fte_enrollment")
    @classmethod
    def enrollment_reasonable(cls, v):
        if v is not None and (v < 0 or v > 500_000):
            logger.warning("Enrollment %s implausible — setting None", v)
            return None
        return v


class HealthcareMetrics(BaseModel):
    """Sector-specific metrics for healthcare borrowers."""
    licensed_beds: Optional[int] = None
    staffed_beds: Optional[int] = None
    patient_admissions: Optional[int] = None
    patient_days: Optional[int] = None
    net_patient_revenue: Optional[float] = None      # in thousands
    days_ar: Optional[float] = None                  # days in accounts receivable

    @field_validator("days_ar")
    @classmethod
    def days_ar_reasonable(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and (v < 0 or v > 365):
            logger.warning("days_ar %s out of range — setting None", v)
            return None
        return v


class EventNoticeResult(BaseModel):
    """Structured extraction from a material event notice."""
    event_type: str  # covenant_violation | payment_default | rating_change | bankruptcy | ...
    event_date: Optional[date] = None
    borrower_name: Optional[str] = None
    issuer_name: Optional[str] = None
    severity: str = "medium"  # low | medium | high | critical
    summary: str
    key_passage: Optional[str] = None

    @model_validator(mode="after")
    def normalize_severity(self) -> "EventNoticeResult":
        valid = {"low", "medium", "high", "critical"}
        if self.severity not in valid:
            self.severity = "medium"
        return self

    @model_validator(mode="after")
    def normalize_event_type(self) -> "EventNoticeResult":
        valid = {
            "covenant_violation", "payment_default", "rating_change",
            "bankruptcy", "forbearance", "debt_restructuring",
            "going_concern", "liquidity_facility_termination", "other",
        }
        if self.event_type not in valid:
            self.event_type = "other"
        return self


# ---------------------------------------------------------------------------
# Prompt templates
# ---------------------------------------------------------------------------

_FS_BASE_PROMPT = """\
You are extracting financial data from a municipal bond issuer's financial disclosure document.

Borrower: {borrower_name}

Document text:
{document_text}

FIRST, determine the reporting period:
- "period_type": "annual" if this covers a full fiscal year (12 months), "interim" if it covers less than 12 months (e.g. 6-month, quarterly), or "unknown" if unclear.
- "period_months": the number of months this document covers (12, 6, 3, etc.). Return null if unknown.
- "fiscal_year_end": the end date of the period covered (YYYY-MM-DD), or null if not found.
- "period_label": a short human-readable description, e.g. "FY2025 Annual", "6-month ended Dec 31 2025", "Q2 FY2025"

IMPORTANT RULES FOR FLOW METRICS (revenue, income, EBITDA):
- If period_type is "annual": extract all metrics normally.
- If period_type is "interim": set total_revenue, operating_revenue, net_income, operating_income, ebitda, tuition_revenue, and net_patient_revenue to null. DO NOT annualize or extrapolate.
- Balance sheet / snapshot metrics (cash, debt, net assets, enrollment, endowment, beds, DSCR, days cash) are point-in-time and should ALWAYS be extracted regardless of period type.

All dollar values in THOUSANDS (000s omitted) as presented in the document.
If a value is not found or not clearly stated, return null.

Return ONLY valid JSON matching this exact schema — no markdown, no explanation:
{{
  "period_type": "annual or interim or unknown",
  "period_months": null,
  "period_label": "e.g. FY2025 Annual or 6-month ended Dec 31 2025",
  "fiscal_year_end": "YYYY-MM-DD or null",
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
  "notes": "any important observations or caveats — always note if this is an interim filing and what period it covers"
}}"""

_HIGHER_ED_SUPPLEMENT = """
Also extract these higher education metrics into the SAME JSON object:
  "total_enrollment": null,
  "fte_enrollment": null,
  "tuition_revenue": null,
  "tuition_discount_rate": null,
  "endowment_value": null
"""

_HEALTHCARE_SUPPLEMENT = """
Also extract these healthcare metrics into the SAME JSON object:
  "licensed_beds": null,
  "staffed_beds": null,
  "patient_admissions": null,
  "patient_days": null,
  "net_patient_revenue": null,
  "days_ar": null
"""

_EVENT_NOTICE_PROMPT = """\
You are analyzing a material event notice for a municipal bond.

Document text:
{document_text}

Extract the following and return ONLY valid JSON — no markdown, no explanation:
{{
  "event_type": "covenant_violation | payment_default | rating_change | bankruptcy | forbearance | debt_restructuring | going_concern | liquidity_facility_termination | other",
  "event_date": "YYYY-MM-DD or null",
  "borrower_name": null,
  "issuer_name": null,
  "severity": "low | medium | high | critical",
  "summary": "2-3 sentence plain English summary of what happened",
  "key_passage": "most relevant verbatim quote from the document (max 500 chars)"
}}"""

_OPERATING_REPORT_PROMPT = """\
You are extracting key operating metrics from a municipal bond borrower's management or operating report.

Borrower: {borrower_name}

Document text:
{document_text}

Extract the most recent period's key financial and operating metrics.
Return ONLY valid JSON — no markdown, no explanation:
{{
  "fiscal_year_end": "YYYY-MM-DD or null",
  "total_revenue": null,
  "operating_income": null,
  "days_cash_on_hand": null,
  "notes": "key takeaways from the report"
}}"""


# ---------------------------------------------------------------------------
# Extraction functions
# ---------------------------------------------------------------------------

def _parse_json_response(raw: str, model_class) -> tuple:
    """
    Parse and validate a JSON string against a Pydantic model.
    Returns (validated_model, raw_json_str).
    """
    # Strip any accidental markdown fences
    text = raw.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()

    data = json.loads(text)
    validated = model_class(**{k: v for k, v in data.items() if k in model_class.model_fields})
    return validated, raw


def extract_financial_statement(
    text: str,
    sector: str,
    borrower_name: str,
    client,
) -> tuple[FinancialMetrics, Optional[Union[HigherEdMetrics, HealthcareMetrics]], str]:
    """
    Extract structured financial metrics from an audited financial statement.

    Args:
        text:          Full document text (from pdfplumber / OCR).
        sector:        Borrower sector — affects which supplement is appended to prompt.
        borrower_name: Used in the prompt for context.
        client:        anthropic.Anthropic() instance.

    Returns:
        (FinancialMetrics, sector_metrics_or_None, raw_json_str)
    """
    from src.config import settings  # noqa: PLC0415

    # Build prompt with sector supplement
    prompt = _FS_BASE_PROMPT.format(
        borrower_name=borrower_name,
        document_text=text[:80_000],  # ~60k tokens — well within context window
    )
    if sector == "higher_ed":
        prompt = prompt.rstrip() + "\n" + _HIGHER_ED_SUPPLEMENT
    elif sector == "healthcare":
        prompt = prompt.rstrip() + "\n" + _HEALTHCARE_SUPPLEMENT

    logger.info(
        "Calling %s for financial statement extraction (%d chars of text)",
        settings.extraction_model, len(text),
    )

    response = client.messages.create(
        model=settings.extraction_model,
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )
    raw_json = response.content[0].text

    # Parse base metrics
    data = json.loads(_clean_json(raw_json))

    # Validate base metrics
    base_fields = set(FinancialMetrics.model_fields.keys())
    base_metrics = FinancialMetrics(**{k: v for k, v in data.items() if k in base_fields})

    # Validate sector metrics
    sector_metrics: Optional[Union[HigherEdMetrics, HealthcareMetrics]] = None
    if sector == "higher_ed":
        he_fields = set(HigherEdMetrics.model_fields.keys())
        sector_metrics = HigherEdMetrics(**{k: v for k, v in data.items() if k in he_fields})
    elif sector == "healthcare":
        hc_fields = set(HealthcareMetrics.model_fields.keys())
        sector_metrics = HealthcareMetrics(**{k: v for k, v in data.items() if k in hc_fields})

    return base_metrics, sector_metrics, raw_json


def extract_event_notice(
    text: str,
    client,
) -> tuple[EventNoticeResult, str]:
    """
    Extract structured data from a material event notice.

    Returns:
        (EventNoticeResult, raw_json_str)
    """
    from src.config import settings  # noqa: PLC0415

    prompt = _EVENT_NOTICE_PROMPT.format(
        document_text=text[:40_000],
    )

    logger.info("Calling %s for event notice extraction", settings.extraction_model)

    response = client.messages.create(
        model=settings.extraction_model,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    raw_json = response.content[0].text

    data = json.loads(_clean_json(raw_json))
    result = EventNoticeResult(**{k: v for k, v in data.items() if k in EventNoticeResult.model_fields})

    return result, raw_json


def extract_operating_report(
    text: str,
    borrower_name: str,
    client,
) -> tuple[dict, str]:
    """
    Extract key metrics from a management / operating report.
    Returns lighter output than a full financial statement.

    Returns:
        (metrics_dict, raw_json_str)
    """
    from src.config import settings  # noqa: PLC0415

    prompt = _OPERATING_REPORT_PROMPT.format(
        borrower_name=borrower_name,
        document_text=text[:40_000],
    )

    logger.info("Calling %s for operating report extraction", settings.extraction_model)

    response = client.messages.create(
        model=settings.extraction_model,
        max_tokens=512,
        messages=[{"role": "user", "content": prompt}],
    )
    raw_json = response.content[0].text

    data = json.loads(_clean_json(raw_json))
    return data, raw_json


def _clean_json(raw: str) -> str:
    """Strip markdown code fences from an AI JSON response."""
    text = raw.strip()
    if text.startswith("```"):
        parts = text.split("```")
        # parts[1] is the content between fences
        if len(parts) >= 2:
            text = parts[1]
            if text.startswith("json"):
                text = text[4:]
    return text.strip()
