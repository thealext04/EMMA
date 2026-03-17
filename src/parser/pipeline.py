"""
pipeline.py — Phase 4 AI extraction pipeline orchestrator.

For each pending document:
  1. Check if doc type warrants extraction (skip bond_issuance, other)
  2. Fetch PDF from EMMA URL (streaming — no local storage required)
  3. Extract text via pdfplumber (OCR fallback for scanned PDFs)
  4. Keyword pre-filter for going concern language
  5. Route to the appropriate AI extraction function by doc_type
  6. Validate output with Pydantic
  7. Write metrics to extracted_metrics table
  8. Generate distress events if signals found (going concern, DSCR breach)
  9. Mark document extraction_status = extracted / failed / skipped

Usage:
    from src.parser.pipeline import ExtractionPipeline
    from src.config import settings
    import anthropic

    pipeline = ExtractionPipeline(db_session, http_session, anthropic_client)
    summary = pipeline.run(limit=10, doc_type_filter="financial_statement")
    print(summary)
"""

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.db.models import Borrower, Document, Event
from src.db.repositories.metrics import MetricsRepository
from src.parser.classifier import should_extract
from src.parser.extractor import (
    EventNoticeResult,
    FinancialMetrics,
    extract_event_notice,
    extract_financial_statement,
    extract_operating_report,
    has_going_concern_risk,
    pre_scan_event_notice,
)
from src.parser.pdf_extractor import extract_from_url, extract_from_path

logger = logging.getLogger(__name__)

# Pause between documents to avoid overwhelming the API (not EMMA — AI calls)
INTER_DOCUMENT_DELAY = 0.5  # seconds


# ---------------------------------------------------------------------------
# Result summary
# ---------------------------------------------------------------------------

@dataclass
class PipelineRun:
    """Summary of one pipeline.run() call."""
    started_at: datetime = field(default_factory=datetime.utcnow)
    processed: int = 0
    extracted: int = 0
    skipped: int = 0
    failed: int = 0
    going_concern_found: int = 0
    dscr_breach_found: int = 0

    def __str__(self) -> str:
        elapsed = (datetime.utcnow() - self.started_at).seconds
        return (
            f"Pipeline run complete in {elapsed}s — "
            f"processed={self.processed} "
            f"extracted={self.extracted} "
            f"skipped={self.skipped} "
            f"failed={self.failed} "
            f"going_concern={self.going_concern_found} "
            f"dscr_breach={self.dscr_breach_found}"
        )


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

class ExtractionPipeline:
    """
    Orchestrates Phase 4 AI extraction for pending documents.

    Thread-safety: not thread-safe — use one instance per thread/process.
    The DB session and Anthropic client are not shared.
    """

    def __init__(
        self,
        db_session: Session,
        http_session: requests.Session,
        anthropic_client,               # anthropic.Anthropic instance
    ) -> None:
        self.db = db_session
        self.http = http_session
        self.ai = anthropic_client
        self.metrics_repo = MetricsRepository(db_session)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        limit: int = 50,
        doc_type_filter: Optional[str] = None,
        borrower_id: Optional[int] = None,
        dry_run: bool = False,
    ) -> PipelineRun:
        """
        Process pending documents through the full extraction pipeline.

        Args:
            limit:           Max documents to process in this run.
            doc_type_filter: Only process this doc_type (e.g. "financial_statement").
            borrower_id:     Only process documents for this borrower.
            dry_run:         Fetch + extract text but do NOT write to DB or generate events.

        Returns:
            PipelineRun summary dataclass.
        """
        summary = PipelineRun()

        docs = self._get_pending_docs(limit, doc_type_filter, borrower_id)
        if not docs:
            logger.info("No pending documents to process.")
            return summary

        logger.info(
            "ExtractionPipeline: processing %d documents (dry_run=%s)", len(docs), dry_run
        )

        for doc in docs:
            summary.processed += 1
            try:
                result = self._process_one(doc, dry_run=dry_run)
                if result == "skipped":
                    summary.skipped += 1
                elif result == "extracted":
                    summary.extracted += 1
                    # Check flags on the doc's metrics after writing
                    m = self.metrics_repo.get_for_doc(doc.doc_id) if not dry_run else None
                    if m and m.dscr is not None and m.dscr < 1.0:
                        summary.dscr_breach_found += 1
                elif result == "going_concern":
                    summary.extracted += 1
                    summary.going_concern_found += 1
                elif result == "failed":
                    summary.failed += 1
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Unexpected error processing doc_id=%d: %s", doc.doc_id, exc, exc_info=True
                )
                if not dry_run:
                    doc.extraction_status = "failed"
                    self.db.commit()
                summary.failed += 1

            if summary.processed < len(docs):
                time.sleep(INTER_DOCUMENT_DELAY)

        logger.info(str(summary))
        return summary

    # ------------------------------------------------------------------
    # Internal processing
    # ------------------------------------------------------------------

    def _process_one(self, doc: Document, dry_run: bool = False) -> str:
        """
        Process a single document through the pipeline.

        Returns one of: "extracted", "going_concern", "skipped", "failed"
        """
        borrower = self._get_borrower(doc.borrower_id)
        borrower_name = borrower.borrower_name if borrower else "Unknown"
        sector = borrower.sector if borrower else "other"

        logger.info(
            "Processing [%d] %s — %s (%s)",
            doc.doc_id, doc.doc_type, borrower_name, doc.posted_date or "no date"
        )

        # --- Step 1: Check if doc type warrants extraction ---
        if not should_extract(doc.doc_type):
            logger.debug("Skipping doc_id=%d (type=%s)", doc.doc_id, doc.doc_type)
            if not dry_run:
                doc.extraction_status = "skipped"
                self.db.commit()
            return "skipped"

        # --- Step 2: Fetch and extract PDF text ---
        try:
            if doc.local_path and __import__("os").path.exists(doc.local_path):
                text, method, page_count = extract_from_path(doc.local_path)
            elif doc.doc_url:
                text, method, page_count = extract_from_url(self.http, doc.doc_url)
            else:
                logger.error("doc_id=%d has no URL or local_path — skipping", doc.doc_id)
                if not dry_run:
                    doc.extraction_status = "failed"
                    self.db.commit()
                return "failed"
        except Exception as exc:
            logger.error("PDF fetch/extract failed for doc_id=%d: %s", doc.doc_id, exc)
            if not dry_run:
                doc.extraction_status = "failed"
                self.db.commit()
            return "failed"

        logger.debug(
            "Extracted %d chars via %s (%d pages) for doc_id=%d",
            len(text), method, page_count, doc.doc_id,
        )

        if not dry_run:
            doc.page_count = page_count

        if not text.strip():
            logger.warning("doc_id=%d yielded empty text — marking failed", doc.doc_id)
            if not dry_run:
                doc.extraction_status = "failed"
                self.db.commit()
            return "failed"

        # --- Step 3: Keyword pre-scan ---
        going_concern_flagged = has_going_concern_risk(text)
        if going_concern_flagged:
            logger.warning("Going concern language detected in doc_id=%d", doc.doc_id)

        if dry_run:
            # Report what we found without writing anything
            logger.info(
                "[DRY RUN] doc_id=%d — %d chars, method=%s, going_concern=%s",
                doc.doc_id, len(text), method, going_concern_flagged,
            )
            return "extracted"

        # --- Step 4: AI extraction by doc type ---
        try:
            result_status = self._run_ai_extraction(
                doc=doc,
                text=text,
                borrower_name=borrower_name,
                sector=sector,
                going_concern_flagged=going_concern_flagged,
            )
        except Exception as exc:
            logger.error("AI extraction failed for doc_id=%d: %s", doc.doc_id, exc, exc_info=True)
            doc.extraction_status = "failed"
            self.db.commit()
            return "failed"

        # --- Step 5: Mark document as extracted ---
        doc.extraction_status = "extracted"
        doc.extracted_at = datetime.utcnow()
        self.db.commit()

        # --- Step 6: Refresh distress score for this borrower ---
        try:
            from src.distress.scoring import update_borrower_score  # noqa: PLC0415
            score, status = update_borrower_score(doc.borrower_id, self.db)
            self.db.commit()
            logger.debug(
                "Distress score refreshed: borrower=%d score=%d status=%s",
                doc.borrower_id, score, status,
            )
        except Exception as exc:
            logger.warning(
                "Could not refresh distress score for borrower=%d: %s",
                doc.borrower_id, exc,
            )

        return "going_concern" if going_concern_flagged else "extracted"

    def _run_ai_extraction(
        self,
        doc: Document,
        text: str,
        borrower_name: str,
        sector: str,
        going_concern_flagged: bool,
    ) -> str:
        """Route to the correct AI extraction function and write results."""
        from src.config import settings  # noqa: PLC0415

        model = settings.extraction_model

        if doc.doc_type in ("financial_statement", "budget"):
            metrics, sector_metrics, raw_json = extract_financial_statement(
                text=text,
                sector=sector,
                borrower_name=borrower_name,
                client=self.ai,
            )

            # Write metrics to DB (with field-level citations for provenance)
            self.metrics_repo.upsert(
                doc_id=doc.doc_id,
                borrower_id=doc.borrower_id,
                metrics=metrics,
                sector_metrics=sector_metrics,
                extraction_model=model,
                extraction_confidence="medium",
                raw_json=raw_json,
                citations_json=_extract_citations_json(raw_json),
            )

            # Update doc's fiscal_year from extracted date
            if metrics.fiscal_year_end:
                doc.fiscal_year = metrics.fiscal_year_end.year

            # Generate distress events if signals found
            if metrics.going_concern_opinion or going_concern_flagged:
                self._write_going_concern_event(doc, metrics)

            if metrics.dscr is not None and metrics.dscr < 1.0:
                self._write_dscr_breach_event(doc, metrics)

        elif doc.doc_type == "event_notice":
            # Fast keyword pre-scan before AI call — surfaces critical signals immediately
            prescan_severity, prescan_keywords = pre_scan_event_notice(text)
            if prescan_severity in ("critical", "high"):
                logger.warning(
                    "EVENT NOTICE pre-scan [%s] keywords=%s doc_id=%d borrower=%d",
                    prescan_severity.upper(), prescan_keywords, doc.doc_id, doc.borrower_id,
                )
            elif prescan_severity == "medium":
                logger.info(
                    "Event notice pre-scan [medium] keywords=%s doc_id=%d",
                    prescan_keywords, doc.doc_id,
                )

            result, raw_json = extract_event_notice(text=text, client=self.ai)

            # Elevate severity to pre-scan level if AI returned something lower
            # (AI can be conservative; keyword matches are hard evidence)
            _severity_rank = {"low": 0, "medium": 1, "high": 2, "critical": 3}
            if _severity_rank.get(prescan_severity, 0) > _severity_rank.get(result.severity, 1):
                logger.info(
                    "Elevating event notice severity %s → %s (keyword pre-scan)",
                    result.severity, prescan_severity,
                )
                result.severity = prescan_severity

            # Write as a lightweight metrics record (just the event date + notes)
            placeholder = FinancialMetrics(
                fiscal_year_end=result.event_date,
                notes=f"Event notice: {result.summary}",
            )
            self.metrics_repo.upsert(
                doc_id=doc.doc_id,
                borrower_id=doc.borrower_id,
                metrics=placeholder,
                sector_metrics=None,
                extraction_model=model,
                extraction_confidence="high",
                raw_json=raw_json,
                citations_json="",  # event notices carry source in key_passage, not citations
            )

            # Write an event record for the material event
            self._write_event_notice_event(doc, result)

        elif doc.doc_type in ("operating_report", "rating_notice"):
            op_data, raw_json = extract_operating_report(
                text=text,
                borrower_name=borrower_name,
                client=self.ai,
            )

            # Map operating report fields to FinancialMetrics
            metrics = FinancialMetrics(
                fiscal_year_end=_parse_date(op_data.get("fiscal_year_end")),
                total_revenue=op_data.get("total_revenue"),
                operating_income=op_data.get("operating_income"),
                days_cash_on_hand=op_data.get("days_cash_on_hand"),
                notes=op_data.get("notes"),
            )
            self.metrics_repo.upsert(
                doc_id=doc.doc_id,
                borrower_id=doc.borrower_id,
                metrics=metrics,
                sector_metrics=None,
                extraction_model=model,
                extraction_confidence="medium",
                raw_json=raw_json,
                citations_json=_extract_citations_json(raw_json),
            )

        return "extracted"

    # ------------------------------------------------------------------
    # Event writers
    # ------------------------------------------------------------------

    def _write_going_concern_event(self, doc: Document, metrics: FinancialMetrics) -> None:
        """Write a going_concern Event if one doesn't already exist for this doc."""
        event_date = metrics.fiscal_year_end or date.today()
        # Idempotent check
        existing = self.db.execute(
            select(Event).where(
                Event.borrower_id == doc.borrower_id,
                Event.event_type == "going_concern",
                Event.doc_id == doc.doc_id,
            )
        ).scalar_one_or_none()

        if not existing:
            event = Event(
                borrower_id=doc.borrower_id,
                doc_id=doc.doc_id,
                event_type="going_concern",
                event_date=event_date,
                detected_date=date.today(),
                severity="critical",
                summary=(
                    metrics.going_concern_text
                    or "Going concern opinion noted in audited financial statements."
                ),
                confirmed=False,
            )
            self.db.add(event)
            logger.warning(
                "GOING CONCERN EVENT written for borrower_id=%d doc_id=%d",
                doc.borrower_id, doc.doc_id,
            )

    def _write_dscr_breach_event(self, doc: Document, metrics: FinancialMetrics) -> None:
        """Write a dscr_breach Event if one doesn't already exist for this doc."""
        event_date = metrics.fiscal_year_end or date.today()
        dscr = metrics.dscr

        existing = self.db.execute(
            select(Event).where(
                Event.borrower_id == doc.borrower_id,
                Event.event_type == "dscr_breach",
                Event.doc_id == doc.doc_id,
            )
        ).scalar_one_or_none()

        if not existing:
            severity = "critical" if dscr < 0.8 else "high"
            event = Event(
                borrower_id=doc.borrower_id,
                doc_id=doc.doc_id,
                event_type="dscr_breach",
                event_date=event_date,
                detected_date=date.today(),
                severity=severity,
                summary=f"Debt service coverage ratio of {dscr:.2f}x (below 1.0x threshold).",
                confirmed=False,
            )
            self.db.add(event)
            logger.warning(
                "DSCR BREACH EVENT written for borrower_id=%d doc_id=%d dscr=%.2f",
                doc.borrower_id, doc.doc_id, dscr,
            )

    def _write_event_notice_event(self, doc: Document, result: EventNoticeResult) -> None:
        """Write an Event record from an extracted event notice."""
        event_date = result.event_date or date.today()

        existing = self.db.execute(
            select(Event).where(
                Event.borrower_id == doc.borrower_id,
                Event.event_type == result.event_type,
                Event.doc_id == doc.doc_id,
            )
        ).scalar_one_or_none()

        if not existing:
            event = Event(
                borrower_id=doc.borrower_id,
                doc_id=doc.doc_id,
                event_type=result.event_type,
                event_date=event_date,
                detected_date=date.today(),
                severity=result.severity,
                summary=result.summary,
                raw_text=result.key_passage,
                confirmed=False,
            )
            self.db.add(event)
            logger.info(
                "Event notice written: type=%s severity=%s for borrower_id=%d",
                result.event_type, result.severity, doc.borrower_id,
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_pending_docs(
        self,
        limit: int,
        doc_type_filter: Optional[str],
        borrower_id: Optional[int],
    ) -> list[Document]:
        """Query pending documents, applying filters."""
        stmt = (
            select(Document)
            .where(Document.extraction_status == "pending")
            .order_by(Document.posted_date.desc().nulls_last())
            .limit(limit)
        )
        if doc_type_filter:
            stmt = stmt.where(Document.doc_type == doc_type_filter)
        if borrower_id:
            stmt = stmt.where(Document.borrower_id == borrower_id)

        return list(self.db.execute(stmt).scalars())

    def _get_borrower(self, borrower_id: int) -> Optional[Borrower]:
        """Fetch borrower by ID."""
        return self.db.execute(
            select(Borrower).where(Borrower.borrower_id == borrower_id)
        ).scalar_one_or_none()


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _parse_date(value) -> Optional[date]:
    """Parse a date from a string like 'YYYY-MM-DD', or return None."""
    if not value:
        return None
    try:
        from datetime import date as date_cls  # noqa: PLC0415
        if isinstance(value, date_cls):
            return value
        return date_cls.fromisoformat(str(value)[:10])
    except (ValueError, TypeError):
        return None


def _extract_citations_json(raw_json: str) -> str:
    """
    Pull the 'citations' dict out of a raw AI JSON response and return it
    as a compact JSON string.  Returns "" if no citations key is present.

    The citations dict maps metric field names to verbatim passages from the
    source PDF, enabling field-level provenance: any number in extracted_metrics
    can be traced back to the exact sentence or table row Claude read.

    Handles both plain JSON and responses wrapped in markdown code fences.
    """
    from src.parser.extractor import _clean_json  # noqa: PLC0415
    try:
        data = json.loads(_clean_json(raw_json))
        citations = data.get("citations")
        if citations and isinstance(citations, dict):
            # Keep only string values; drop nulls and non-strings
            clean = {k: v for k, v in citations.items() if isinstance(v, str) and v.strip()}
            if clean:
                return json.dumps(clean, ensure_ascii=False)
    except (json.JSONDecodeError, AttributeError, TypeError):
        pass
    return ""
