"""
repositories/metrics.py — Data-access layer for the extracted_metrics table.

Deduplication strategy
-----------------------
EMMA frequently posts multiple documents for the same filing date — e.g. the
main audited financials AND a compliance certificate filed the same day.  Both
are tagged financial_statement and both cover the same fiscal year.

To avoid duplicate rows, upsert() uses a two-pass lookup:

  1. Check by doc_id (exact re-extraction of the same PDF)
  2. For annual docs: check by (borrower_id, period_end_date, period_type='annual')
     — if a row already exists for that fiscal year, MERGE by filling nulls only.
     Existing non-null values are never overwritten by a later, less complete doc.

This produces one canonical row per borrower per fiscal year that accumulates
the best available data across all PDFs filed for that period.  The
source_doc_ids column records every document that contributed to the row.

Interim docs (period_type='interim') are stored one row per doc_id as usual
because snapshot metrics (cash, debt, enrollment) are genuinely different
point-in-time observations.
"""

import json
import logging
from datetime import datetime
from typing import Optional, Union

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import ExtractedMetrics

logger = logging.getLogger(__name__)


def _safe_json_loads(s: str) -> dict:
    """Parse a JSON string into a dict; return empty dict on any error."""
    try:
        result = json.loads(s)
        return result if isinstance(result, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


class MetricsRepository:
    """CRUD operations for the extracted_metrics table."""

    def __init__(self, session: Session) -> None:
        self.session = session

    # ------------------------------------------------------------------
    # Upsert
    # ------------------------------------------------------------------

    def upsert(
        self,
        doc_id: int,
        borrower_id: int,
        metrics,                          # FinancialMetrics from extractor.py
        sector_metrics=None,              # HigherEdMetrics | HealthcareMetrics | None
        extraction_model: str = "",
        extraction_confidence: str = "medium",
        raw_json: str = "",
        citations_json: str = "",         # JSON object: {field_name: "verbatim passage"}
    ) -> tuple[ExtractedMetrics, bool]:
        """
        Insert or merge an extracted_metrics record.

        Lookup priority:
          1. Exact doc_id match — re-extraction of the same PDF overwrites fully.
          2. Annual docs only: match on (borrower_id, period_end_date, 'annual').
             If a row for this fiscal year already exists, FILL NULLS only —
             never overwrite a populated field with a null from a thinner doc.
          3. No match — create new row.

        source_doc_ids accumulates every doc_id that contributed data.

        Returns:
            (ExtractedMetrics, created) where created=True if a new row was inserted.
        """
        from sqlalchemy import and_  # noqa: PLC0415

        # Pass 1: exact doc_id (re-extraction)
        record = self.session.execute(
            select(ExtractedMetrics).where(ExtractedMetrics.doc_id == doc_id)
        ).scalar_one_or_none()
        created = False
        merge_mode = False  # True = fill-nulls only, False = full overwrite

        if record:
            logger.debug("Re-extracting existing record for doc_id=%d", doc_id)

        # Pass 2: annual dedup — find existing row for same borrower + fiscal year
        elif (
            metrics.period_type == "annual"
            and metrics.fiscal_year_end is not None
        ):
            record = self.session.execute(
                select(ExtractedMetrics).where(
                    and_(
                        ExtractedMetrics.borrower_id == borrower_id,
                        ExtractedMetrics.period_end_date == metrics.fiscal_year_end,
                        ExtractedMetrics.period_type == "annual",
                    )
                )
            ).scalar_one_or_none()
            if record:
                merge_mode = True
                logger.debug(
                    "Merging doc_id=%d into existing annual record "
                    "(borrower=%d FY=%s) — filling nulls only",
                    doc_id, borrower_id, metrics.fiscal_year_end,
                )

        # Pass 3: create new row
        if record is None:
            record = ExtractedMetrics(doc_id=doc_id, borrower_id=borrower_id)
            self.session.add(record)
            created = True
            logger.debug("Creating new metrics record for doc_id=%d", doc_id)

        # Track all source documents that contributed to this row
        existing_ids = set(
            (record.source_doc_ids or "").split(",")
        ) - {""}
        existing_ids.add(str(doc_id))
        record.source_doc_ids = ",".join(sorted(existing_ids, key=int))

        def _set(field: str, value) -> None:
            """
            Set a field on the record.
            In merge_mode: only write if the current value is None (fill nulls only).
            In normal mode: always write (full overwrite on re-extraction).
            """
            if merge_mode and getattr(record, field) is not None:
                return  # existing non-null value wins
            if value is not None:
                setattr(record, field, value)

        # --- Period context (always overwrite — period doesn't change) ---
        record.period_type   = metrics.period_type
        record.period_months = metrics.period_months

        # --- Core financial metrics ---
        if metrics.fiscal_year_end:
            record.period_end_date = metrics.fiscal_year_end
            record.fiscal_year = metrics.fiscal_year_end.year

        # Flow metrics — Claude nulls these for interim filings.
        _set("total_revenue",           metrics.total_revenue)
        _set("operating_revenue",       metrics.operating_revenue)
        _set("net_income",              metrics.net_income)
        _set("operating_income",        metrics.operating_income)
        _set("ebitda",                  metrics.ebitda)

        # Snapshot metrics — point-in-time, valid for any period type.
        _set("days_cash_on_hand",       metrics.days_cash_on_hand)
        _set("cash_and_investments",    metrics.cash_and_investments)
        _set("unrestricted_net_assets", metrics.unrestricted_net_assets)
        _set("total_long_term_debt",    metrics.total_long_term_debt)
        _set("annual_debt_service",     metrics.annual_debt_service)
        _set("dscr",                    metrics.dscr)

        # MVP additions — Phase 5.1 (cross-sector)
        # credit_rating: store latest rating found; in merge_mode existing wins
        _set("credit_rating",         metrics.credit_rating)
        # Flow metrics — Claude nulls these for interim filings
        _set("operating_expenses",    metrics.operating_expenses)
        _set("interest_expense",      metrics.interest_expense)
        # Boolean flags: convert Pydantic default False → None (DB NULL = not extracted)
        # so merge_mode doesn't overwrite a True from a prior doc with a False from a thinner one
        _set("technical_default",     metrics.technical_default or None)
        _set("forbearance_agreement", metrics.forbearance_agreement or None)
        _set("forbearance_text",      metrics.forbearance_text)

        # --- Sector-specific metrics ---
        if sector_metrics is not None:
            from src.parser.extractor import HigherEdMetrics, HealthcareMetrics  # noqa: PLC0415

            if isinstance(sector_metrics, HigherEdMetrics):
                _set("total_enrollment",      sector_metrics.total_enrollment)
                _set("fte_enrollment",        sector_metrics.fte_enrollment)
                _set("tuition_revenue",       sector_metrics.tuition_revenue)
                _set("tuition_discount_rate", sector_metrics.tuition_discount_rate)
                _set("endowment_value",       sector_metrics.endowment_value)
                _set("gift_revenue",          sector_metrics.gift_revenue)

            elif isinstance(sector_metrics, HealthcareMetrics):
                _set("licensed_beds",       sector_metrics.licensed_beds)
                _set("staffed_beds",        sector_metrics.staffed_beds)
                _set("patient_admissions",  sector_metrics.patient_admissions)
                _set("patient_days",        sector_metrics.patient_days)
                _set("net_patient_revenue", sector_metrics.net_patient_revenue)   # fixed: was bypassing _set()
                _set("days_ar",             sector_metrics.days_ar)               # fixed: was bypassing _set()
                _set("municipal_debt",      sector_metrics.municipal_debt)

        # --- Extraction metadata ---
        record.extraction_model      = extraction_model
        record.extraction_confidence = extraction_confidence
        record.extracted_at          = datetime.utcnow()

        # Store raw JSON if the column exists (added in Phase 4 migration)
        if hasattr(record, "raw_json"):
            record.raw_json = raw_json

        # Store / merge citations (field → verbatim passage from source PDF)
        if hasattr(record, "citations_json") and citations_json:
            if merge_mode and record.citations_json:
                # Merge: existing citations win; incoming fills any missing fields
                existing_citations = _safe_json_loads(record.citations_json)
                incoming_citations = _safe_json_loads(citations_json)
                merged = {**incoming_citations, **existing_citations}  # existing takes priority
                record.citations_json = json.dumps(merged)
            else:
                record.citations_json = citations_json

        return record, created

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def list_for_borrower(
        self,
        borrower_id: int,
        limit: int = 50,
    ) -> list[ExtractedMetrics]:
        """Return extracted metrics for a borrower, most recent first."""
        stmt = (
            select(ExtractedMetrics)
            .where(ExtractedMetrics.borrower_id == borrower_id)
            .order_by(ExtractedMetrics.period_end_date.desc().nulls_last())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars())

    def latest_for_borrower(
        self,
        borrower_id: int,
        annual_only: bool = True,
    ) -> Optional[ExtractedMetrics]:
        """
        Return the most recent metrics record for a borrower.

        Args:
            annual_only: If True (default), only returns records where
                         period_type='annual' or period_months=12.
                         Interim/quarterly filings are excluded so that
                         revenue and income figures are always comparable.
                         Set to False to include interim records.
        """
        stmt = select(ExtractedMetrics).where(
            ExtractedMetrics.borrower_id == borrower_id
        )
        if annual_only:
            from sqlalchemy import or_  # noqa: PLC0415
            stmt = stmt.where(
                or_(
                    ExtractedMetrics.period_type == "annual",
                    ExtractedMetrics.period_months == 12,
                )
            )
        stmt = stmt.order_by(ExtractedMetrics.period_end_date.desc().nulls_last()).limit(1)
        return self.session.execute(stmt).scalar_one_or_none()

    def get_for_doc(self, doc_id: int) -> Optional[ExtractedMetrics]:
        """Return the metrics record for a specific document, if extracted."""
        return self.session.execute(
            select(ExtractedMetrics).where(ExtractedMetrics.doc_id == doc_id)
        ).scalar_one_or_none()
