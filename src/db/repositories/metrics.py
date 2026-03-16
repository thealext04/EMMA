"""
repositories/metrics.py — Data-access layer for the extracted_metrics table.

Stores structured financial data extracted by the Phase 4 AI pipeline.
One row per document (idempotent on doc_id — safe to re-run extraction).

Usage:
    from src.db.repositories.metrics import MetricsRepository
    from src.parser.extractor import FinancialMetrics, HigherEdMetrics

    repo = MetricsRepository(session)
    record, created = repo.upsert(
        doc_id=42,
        borrower_id=5,
        metrics=financial_metrics,
        sector_metrics=higher_ed_metrics,
        extraction_model="claude-sonnet-4-6",
        extraction_confidence="high",
        raw_json='{"fiscal_year_end": "2025-06-30", ...}',
    )
    session.commit()
"""

import logging
from datetime import datetime
from typing import Optional, Union

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import ExtractedMetrics

logger = logging.getLogger(__name__)


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
    ) -> tuple[ExtractedMetrics, bool]:
        """
        Insert or update an extracted_metrics record for a document.

        Idempotent on doc_id — if the document has already been extracted,
        the existing record is updated with the new values (enables re-extraction
        as AI models improve).

        Args:
            doc_id:                 Primary key of the source Document.
            borrower_id:            Primary key of the borrower.
            metrics:                FinancialMetrics Pydantic model from extractor.py.
            sector_metrics:         HigherEdMetrics or HealthcareMetrics (optional).
            extraction_model:       Model name used (e.g. "claude-sonnet-4-6").
            extraction_confidence:  "high" | "medium" | "low"
            raw_json:               Raw AI response JSON string (for audit/reprocessing).

        Returns:
            (ExtractedMetrics, created) where created=True if a new row was inserted.
        """
        existing = self.session.execute(
            select(ExtractedMetrics).where(ExtractedMetrics.doc_id == doc_id)
        ).scalar_one_or_none()

        if existing:
            record = existing
            created = False
            logger.debug("Updating existing metrics record for doc_id=%d", doc_id)
        else:
            record = ExtractedMetrics(doc_id=doc_id, borrower_id=borrower_id)
            self.session.add(record)
            created = True
            logger.debug("Creating new metrics record for doc_id=%d", doc_id)

        # --- Period context ---
        record.period_type   = metrics.period_type    # 'annual' | 'interim' | 'unknown'
        record.period_months = metrics.period_months  # 12, 6, 3, etc.

        # --- Core financial metrics ---
        if metrics.fiscal_year_end:
            record.period_end_date = metrics.fiscal_year_end
            record.fiscal_year = metrics.fiscal_year_end.year

        # Flow metrics (revenue, income) — only meaningful for full-year periods.
        # Claude nulls these out for interim filings per the extraction prompt.
        record.total_revenue           = metrics.total_revenue
        record.operating_revenue       = metrics.operating_revenue
        record.net_income              = metrics.net_income
        record.operating_income        = metrics.operating_income
        record.ebitda                  = metrics.ebitda

        # Snapshot / point-in-time metrics — valid for any period type.
        record.days_cash_on_hand       = metrics.days_cash_on_hand
        record.cash_and_investments    = metrics.cash_and_investments
        record.unrestricted_net_assets = metrics.unrestricted_net_assets
        record.total_long_term_debt    = metrics.total_long_term_debt
        record.annual_debt_service     = metrics.annual_debt_service
        record.dscr                    = metrics.dscr

        # --- Sector-specific metrics ---
        if sector_metrics is not None:
            # Import here to avoid circular at module level
            from src.parser.extractor import HigherEdMetrics, HealthcareMetrics  # noqa: PLC0415

            if isinstance(sector_metrics, HigherEdMetrics):
                record.total_enrollment      = sector_metrics.total_enrollment
                record.fte_enrollment        = sector_metrics.fte_enrollment
                record.tuition_revenue       = sector_metrics.tuition_revenue
                record.tuition_discount_rate = sector_metrics.tuition_discount_rate
                record.endowment_value       = sector_metrics.endowment_value

            elif isinstance(sector_metrics, HealthcareMetrics):
                record.licensed_beds      = sector_metrics.licensed_beds
                record.staffed_beds       = sector_metrics.staffed_beds
                record.patient_admissions = sector_metrics.patient_admissions
                record.patient_days       = sector_metrics.patient_days
                record.net_patient_revenue = sector_metrics.net_patient_revenue
                record.days_ar            = sector_metrics.days_ar

        # --- Extraction metadata ---
        record.extraction_model      = extraction_model
        record.extraction_confidence = extraction_confidence
        record.extracted_at          = datetime.utcnow()

        # Store raw JSON if the column exists (added in Phase 4 migration)
        if hasattr(record, "raw_json"):
            record.raw_json = raw_json

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
