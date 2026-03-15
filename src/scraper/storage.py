"""
storage.py — Raw document file storage for downloaded EMMA PDFs.

Directory structure:
    data/raw_documents/{YYYY}/{MM}/{borrower-slug}/{date}_{type}_{doc_id}.pdf

Example:
    data/raw_documents/2026/03/manhattan-college/20260312_financial-statement_abc123.pdf

Rules:
  - Always store raw before parsing
  - Never overwrite an existing file (safe to call twice)
  - Slugify borrower names to be filesystem-safe
  - Return the full absolute path after storing

Usage:
    from src.scraper.storage import DocumentStorage
    store = DocumentStorage()
    path = store.save(pdf_bytes, borrower_name="Manhattan College",
                      doc_date="2026-03-12", doc_type="Financial Statement", doc_id="abc123")
"""

import logging
import os
import re
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)

# Base storage directory relative to project root
DEFAULT_STORAGE_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "raw_documents"
)


def slugify(text: str) -> str:
    """
    Convert a string to a filesystem-safe slug.
    E.g., "Manhattan College" → "manhattan-college"
         "Dormitory Auth. of NY" → "dormitory-auth-of-ny"
    """
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)        # Remove non-word chars except hyphen
    text = re.sub(r"[\s_]+", "-", text)          # Spaces/underscores → hyphen
    text = re.sub(r"-{2,}", "-", text)            # Collapse multiple hyphens
    return text[:80]                              # Truncate to prevent path length issues


class DocumentStorage:
    """
    Manages the raw document file tree.
    """

    def __init__(self, base_dir: str = DEFAULT_STORAGE_DIR) -> None:
        self.base_dir = os.path.abspath(base_dir)
        os.makedirs(self.base_dir, exist_ok=True)

    def get_path(
        self,
        borrower_name: str,
        doc_date: Optional[str],    # ISO date string: YYYY-MM-DD
        doc_type: str,
        doc_id: str,
    ) -> str:
        """
        Compute the expected storage path for a document (without saving it).

        Args:
            borrower_name:  Borrower name (will be slugified).
            doc_date:       Document date as ISO string, or None.
            doc_type:       Document type string (e.g., "Financial Statement").
            doc_id:         EMMA document ID or unique identifier.

        Returns:
            Absolute file path (str).
        """
        if doc_date:
            try:
                d = date.fromisoformat(doc_date)
                year = d.strftime("%Y")
                month = d.strftime("%m")
                date_prefix = d.strftime("%Y%m%d")
            except ValueError:
                year, month, date_prefix = "0000", "00", "00000000"
        else:
            year, month, date_prefix = "0000", "00", "00000000"

        borrower_slug = slugify(borrower_name) if borrower_name else "unknown"
        type_slug = slugify(doc_type)
        filename = f"{date_prefix}_{type_slug}_{doc_id}.pdf"

        return os.path.join(self.base_dir, year, month, borrower_slug, filename)

    def save(
        self,
        content: bytes,
        borrower_name: str,
        doc_date: Optional[str],
        doc_type: str,
        doc_id: str,
    ) -> str:
        """
        Save raw PDF bytes to the document tree.

        Never overwrites existing files (idempotent — safe to call twice).

        Returns:
            Absolute path to the saved file.

        Raises:
            OSError: If the file cannot be written.
        """
        path = self.get_path(borrower_name, doc_date, doc_type, doc_id)
        dir_path = os.path.dirname(path)

        # Idempotency check
        if os.path.exists(path):
            logger.debug("Document already stored, skipping: %s", path)
            return path

        os.makedirs(dir_path, exist_ok=True)

        with open(path, "wb") as f:
            f.write(content)

        size_kb = len(content) / 1024
        logger.info(
            "Stored: %s (%.1f KB) → %s",
            doc_id,
            size_kb,
            path,
        )
        return path

    def exists(
        self,
        borrower_name: str,
        doc_date: Optional[str],
        doc_type: str,
        doc_id: str,
    ) -> bool:
        """Return True if the document is already stored locally."""
        path = self.get_path(borrower_name, doc_date, doc_type, doc_id)
        return os.path.exists(path)

    def get_stats(self) -> dict:
        """Count files and total size in the raw document store."""
        total_files = 0
        total_bytes = 0
        for root, _, files in os.walk(self.base_dir):
            for fname in files:
                if fname.endswith(".pdf"):
                    total_files += 1
                    total_bytes += os.path.getsize(os.path.join(root, fname))
        return {
            "total_documents": total_files,
            "total_size_mb": round(total_bytes / (1024 * 1024), 2),
            "base_dir": self.base_dir,
        }
