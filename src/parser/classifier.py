"""
classifier.py — Document type classification for the Phase 4 parsing pipeline.

Three-level classification strategy (cheapest → most expensive):
  1. Metadata/keyword match (free) — reuses existing classify_doc_type() from document.py
  2. First-page AI classification (cheap) — Haiku, only for ambiguous "other" docs
  3. Human review flag — for anything AI is uncertain about

Doc types that warrant full AI extraction:
    financial_statement, event_notice, operating_report, budget, rating_notice

Doc types that are skipped:
    bond_issuance, other

Usage:
    from src.parser.classifier import should_extract, classify_document
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Doc types that warrant full AI extraction (high value)
EXTRACT_TYPES = frozenset({
    "financial_statement",
    "event_notice",
    "operating_report",
    "budget",
    "rating_notice",
})

# Doc types explicitly skipped (legal boilerplate / admin)
SKIP_TYPES = frozenset({
    "bond_issuance",
    "other",
})

# Haiku prompt for first-page classification
_CLASSIFICATION_PROMPT = """\
You are classifying a municipal bond disclosure document.

Read the following text from the first pages of the document and classify it.

Document text:
{text}

Respond with EXACTLY ONE of these labels — nothing else:
financial_statement
event_notice
operating_report
budget
rating_notice
bond_issuance
other

Your response must be a single line containing only the label."""


def should_extract(doc_type: str) -> bool:
    """
    Return True if this document type warrants AI extraction.

    financial_statement, event_notice, operating_report, budget, rating_notice → True
    bond_issuance, other → False
    """
    return doc_type in EXTRACT_TYPES


def classify_from_metadata(title: str, emma_type_label: str = "") -> str:
    """
    Classify a document using its EMMA metadata (title + type label).

    Delegates to the existing keyword classifier in document.py — do not
    duplicate logic here.

    Returns a canonical doc_type string.
    """
    # Reuse the existing keyword classifier — it already handles all known EMMA label patterns
    from src.db.repositories.document import classify_doc_type  # noqa: PLC0415
    return classify_doc_type(title, emma_type_label)


def classify_with_ai(first_page_text: str, client) -> str:
    """
    Use Claude Haiku to classify an ambiguous document from its first-page text.

    This is the fallback when metadata classification returns "other" but the
    document might actually be worth extracting.

    Args:
        first_page_text: Text from the first 1–2 pages of the document.
        client:          anthropic.Anthropic() client instance.

    Returns:
        Canonical doc_type string (one of the 7 valid types).
    """
    from src.config import settings  # noqa: PLC0415

    prompt = _CLASSIFICATION_PROMPT.format(text=first_page_text[:4000])  # cap tokens

    try:
        response = client.messages.create(
            model=settings.classification_model,
            max_tokens=20,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip().lower()

        # Validate the response is one of our known types
        valid_types = EXTRACT_TYPES | SKIP_TYPES
        if raw in valid_types:
            logger.debug("AI classified document as: %s", raw)
            return raw
        else:
            logger.warning("AI returned unexpected doc type %r — defaulting to 'other'", raw)
            return "other"

    except Exception as exc:  # noqa: BLE001
        logger.error("AI classification failed: %s — defaulting to 'other'", exc)
        return "other"


def classify_document(
    title: str,
    emma_type_label: str = "",
    first_page_text: str = "",
    client=None,
) -> tuple[str, str]:
    """
    Classify a document using the cheapest method that gives a confident result.

    Strategy:
        1. Keyword/metadata match (free) → return if not "other"
        2. AI first-page classification (cheap, only if text + client provided)
        3. Return "other"

    Args:
        title:            Document title from EMMA.
        emma_type_label:  EMMA's own document category label.
        first_page_text:  First 1–2 pages of extracted text (optional).
        client:           anthropic.Anthropic() instance (optional — for AI fallback).

    Returns:
        (doc_type, method) where method is "metadata" or "ai"
    """
    # Step 1: free keyword match
    doc_type = classify_from_metadata(title, emma_type_label)
    if doc_type != "other":
        return doc_type, "metadata"

    # Step 2: AI fallback (only if we have text and a client)
    if first_page_text and client:
        doc_type = classify_with_ai(first_page_text, client)
        return doc_type, "ai"

    return "other", "metadata"
