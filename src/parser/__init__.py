"""
parser/ — AI document parsing pipeline (Phase 4).

Public entry point:
    from src.parser.pipeline import ExtractionPipeline

Pipeline flow:
    pending document
        → classify doc_type (keyword/metadata)
        → fetch PDF from EMMA URL (streaming — no local storage required)
        → extract text (pdfplumber, OCR fallback)
        → AI extraction (Claude Sonnet — sector-specific prompts)
        → Pydantic validation
        → write to extracted_metrics
        → generate distress events (going concern, DSCR breach)
        → mark extraction_status = extracted
"""

from src.parser.pipeline import ExtractionPipeline  # noqa: F401
