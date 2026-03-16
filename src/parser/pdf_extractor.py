"""
pdf_extractor.py — PDF text extraction with OCR fallback.

Supports two modes:
  1. From a URL (streaming — no local storage required)
  2. From a local file path (when PDFs have been downloaded)

Text-based PDFs: pdfplumber (fast, accurate)
Scanned/image PDFs: pytesseract OCR (slower, higher cost)

Detection: if average characters per page < 100, treat as scanned.

Usage:
    from src.parser.pdf_extractor import extract_from_url, extract_from_path

    text, method, page_count = extract_from_url(session, "https://emma.msrb.org/...")
    text, method, page_count = extract_from_path("/path/to/file.pdf")
"""

import io
import logging
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# If avg characters per page falls below this threshold, assume scanned PDF
SCANNED_THRESHOLD_CHARS_PER_PAGE = 100

# Maximum pages to send to AI (full document — financial statements are 80–200 pages)
DEFAULT_MAX_PAGES: Optional[int] = None  # None = all pages

# For first-pass classification only: just read the first 2 pages
CLASSIFICATION_MAX_PAGES = 2


def _extract_from_bytes(pdf_bytes: bytes, max_pages: Optional[int] = None) -> tuple[str, int]:
    """
    Extract text from raw PDF bytes using pdfplumber.

    Returns:
        (text, page_count)
    """
    import pdfplumber  # lazy import — not required for non-parsing runs

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        pages = pdf.pages[:max_pages] if max_pages else pdf.pages
        page_count = len(pdf.pages)
        text = "\n\n".join(
            (page.extract_text() or "") for page in pages
        )

    return text, page_count


def _ocr_from_bytes(pdf_bytes: bytes, dpi: int = 200) -> str:
    """
    Extract text from a scanned PDF via OCR.
    Converts each page to an image then runs pytesseract.

    Returns:
        text (all pages joined)
    """
    try:
        import pytesseract  # lazy import
        from pdf2image import convert_from_bytes  # lazy import
    except ImportError as exc:
        raise RuntimeError(
            "OCR fallback requires pytesseract and pdf2image. "
            "Install with: pip install pytesseract pdf2image"
        ) from exc

    logger.info("Running OCR on scanned PDF (%d bytes)", len(pdf_bytes))
    images = convert_from_bytes(pdf_bytes, dpi=dpi)
    pages_text = [pytesseract.image_to_string(img) for img in images]
    return "\n\n".join(pages_text)


def is_scanned(text: str, page_count: int) -> bool:
    """
    Return True if the PDF appears to be a scanned (image-based) document.

    Heuristic: if the average characters extracted per page is below the
    threshold, pdfplumber likely couldn't read the text layer.
    """
    if page_count == 0:
        return False
    avg_chars = len(text) / page_count
    return avg_chars < SCANNED_THRESHOLD_CHARS_PER_PAGE


def extract_from_url(
    session: requests.Session,
    url: str,
    max_pages: Optional[int] = DEFAULT_MAX_PAGES,
    ocr_fallback: bool = True,
) -> tuple[str, str, int]:
    """
    Fetch a PDF from a URL and extract its text.

    The PDF is loaded into memory (BytesIO) — never written to disk.

    Args:
        session:      requests.Session with browser-like headers (EMMAsession).
        url:          Direct PDF URL (e.g. https://emma.msrb.org/P123-P456.pdf).
        max_pages:    Limit pages extracted (None = all). Use 2 for classification.
        ocr_fallback: If True, retry with OCR if text is sparse (scanned PDF).

    Returns:
        (text, method, page_count)
        method is "pdfplumber" or "ocr"

    Raises:
        requests.HTTPError: on 4xx/5xx response
        RuntimeError: if PDF content cannot be read
    """
    logger.info("Fetching PDF: %s", url)
    response = session.get(url, timeout=30, stream=False)
    response.raise_for_status()

    content_type = response.headers.get("Content-Type", "")
    if "text/html" in content_type and b"<html" in response.content[:200]:
        raise RuntimeError(
            f"Expected PDF but got HTML — session may have expired. URL: {url}"
        )

    pdf_bytes = response.content
    if not pdf_bytes:
        raise RuntimeError(f"Empty response for URL: {url}")

    logger.debug("PDF fetched: %.1f KB", len(pdf_bytes) / 1024)

    text, page_count = _extract_from_bytes(pdf_bytes, max_pages=max_pages)

    if ocr_fallback and is_scanned(text, page_count):
        logger.info(
            "PDF appears scanned (avg %.0f chars/page) — switching to OCR",
            len(text) / max(page_count, 1),
        )
        text = _ocr_from_bytes(pdf_bytes)
        return text, "ocr", page_count

    return text, "pdfplumber", page_count


def extract_from_path(
    local_path: str,
    max_pages: Optional[int] = DEFAULT_MAX_PAGES,
    ocr_fallback: bool = True,
) -> tuple[str, str, int]:
    """
    Extract text from a locally-stored PDF file.

    Args:
        local_path:   Absolute path to the PDF file on disk.
        max_pages:    Limit pages extracted (None = all).
        ocr_fallback: If True, retry with OCR if text is sparse.

    Returns:
        (text, method, page_count)
        method is "pdfplumber" or "ocr"
    """
    with open(local_path, "rb") as f:
        pdf_bytes = f.read()

    text, page_count = _extract_from_bytes(pdf_bytes, max_pages=max_pages)

    if ocr_fallback and is_scanned(text, page_count):
        logger.info(
            "Local PDF appears scanned — switching to OCR: %s", local_path
        )
        text = _ocr_from_bytes(pdf_bytes)
        return text, "ocr", page_count

    return text, "pdfplumber", page_count
