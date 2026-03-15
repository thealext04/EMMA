"""
document_fetcher.py — Queue-based PDF downloader.

Pulls items from DocumentQueue and downloads them at a controlled rate.
Stores raw PDFs via DocumentStorage before any processing.

Key rules:
  - 1 request every 2–3 seconds (never faster)
  - Max 3 concurrent workers
  - Never re-download a document that already exists on disk
  - Store raw bytes first; parsing happens separately (Phase 4)

Usage:
    # Single-threaded (development / testing)
    fetcher = DocumentFetcher(session, queue, storage)
    fetcher.run(max_items=50)

    # Multi-threaded (production)
    fetcher = DocumentFetcher(session, queue, storage, workers=3)
    fetcher.run_threaded(max_items=200)
"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests

from src.scraper.document_queue import DocumentQueue
from src.scraper.models import QueueItem, RunMetrics
from src.scraper.retry import fetch_with_retry
from src.scraper.storage import DocumentStorage

logger = logging.getLogger(__name__)

MAX_WORKERS = 3
DEFAULT_BATCH_SIZE = 50


class DocumentFetcher:
    """
    Downloads queued documents and stores raw PDFs.

    Thread-safe — multiple workers can call _download_one() concurrently
    because the queue uses its own lock and storage is append-only.
    """

    def __init__(
        self,
        session: requests.Session,
        queue: DocumentQueue,
        storage: DocumentStorage,
        workers: int = 1,
    ) -> None:
        if workers > MAX_WORKERS:
            raise ValueError(f"Max workers is {MAX_WORKERS}, got {workers}")

        self.session = session
        self.queue = queue
        self.storage = storage
        self.workers = workers
        self._metrics = RunMetrics(run_date="")

    # ------------------------------------------------------------------
    # Public run methods
    # ------------------------------------------------------------------

    def run(self, max_items: int = DEFAULT_BATCH_SIZE) -> RunMetrics:
        """
        Single-threaded download run. Good for development and testing.

        Args:
            max_items: Maximum number of items to download in this run.

        Returns:
            RunMetrics with counts for this run.
        """
        from datetime import datetime
        self._metrics = RunMetrics(run_date=datetime.utcnow().isoformat())

        # Reset any items stuck in 'downloading' from a previous crash
        self.queue.reset_stuck_downloading()

        items = self.queue.get_pending(limit=max_items)
        if not items:
            logger.info("DocumentFetcher: queue is empty — nothing to download")
            return self._metrics

        logger.info("DocumentFetcher: starting single-threaded run — %d items", len(items))

        for item in items:
            self._download_one(item)
            self._metrics.documents_downloaded += 1 if item.status == "downloaded" else 0

        logger.info(
            "DocumentFetcher run complete — downloaded: %d  failed: %d",
            self._metrics.documents_downloaded,
            self._metrics.download_failures,
        )
        return self._metrics

    def run_threaded(self, max_items: int = DEFAULT_BATCH_SIZE) -> RunMetrics:
        """
        Multi-threaded download run using a thread pool.

        Args:
            max_items: Maximum number of items to download in this run.

        Returns:
            RunMetrics with counts for this run.
        """
        from datetime import datetime
        self._metrics = RunMetrics(run_date=datetime.utcnow().isoformat())

        self.queue.reset_stuck_downloading()
        items = self.queue.get_pending(limit=max_items)

        if not items:
            logger.info("DocumentFetcher: queue is empty — nothing to download")
            return self._metrics

        logger.info(
            "DocumentFetcher: starting threaded run — %d items, %d workers",
            len(items),
            self.workers,
        )

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {executor.submit(self._download_one, item): item for item in items}
            for future in as_completed(futures):
                item = futures[future]
                try:
                    success = future.result()
                    if success:
                        self._metrics.documents_downloaded += 1
                    else:
                        self._metrics.download_failures += 1
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "Unexpected error downloading %s: %s", item.doc_id, exc
                    )
                    self._metrics.download_failures += 1

        logger.info(
            "DocumentFetcher threaded run complete — downloaded: %d  failed: %d",
            self._metrics.documents_downloaded,
            self._metrics.download_failures,
        )
        return self._metrics

    # ------------------------------------------------------------------
    # Core download logic
    # ------------------------------------------------------------------

    def _download_one(self, item: QueueItem) -> bool:
        """
        Download one document and store it.

        Returns True on success, False on failure.
        Updates queue status accordingly.
        """
        # Skip if already stored on disk
        if self.storage.exists(
            borrower_name=item.borrower_name,
            doc_date=item.doc_date,
            doc_type=item.doc_type,
            doc_id=item.doc_id,
        ):
            logger.debug("Document already on disk, skipping: %s", item.doc_id)
            expected_path = self.storage.get_path(
                item.borrower_name, item.doc_date, item.doc_type, item.doc_id
            )
            self.queue.mark_downloaded(item.doc_id, local_path=expected_path)
            return True

        # Mark as downloading (increments attempt count)
        self.queue.mark_downloading(item.doc_id)

        logger.info(
            "Downloading: %s | %s | %s",
            item.doc_id,
            item.doc_type,
            item.borrower_name,
        )

        try:
            response = fetch_with_retry(
                self.session,
                item.doc_url,
                is_download=True,
                stream=True,
            )

            # Verify we got a PDF (or at least some binary content)
            content_type = response.headers.get("Content-Type", "")
            if "text/html" in content_type and b"<html" in response.content[:200]:
                raise ValueError(
                    f"Expected PDF but got HTML — session may have expired. "
                    f"URL: {item.doc_url}"
                )

            # Read content
            pdf_bytes = response.content
            if not pdf_bytes:
                raise ValueError(f"Empty response body for {item.doc_url}")

            size_kb = len(pdf_bytes) / 1024
            logger.debug("Downloaded %.1f KB for %s", size_kb, item.doc_id)

            # Store raw PDF
            local_path = self.storage.save(
                content=pdf_bytes,
                borrower_name=item.borrower_name,
                doc_date=item.doc_date,
                doc_type=item.doc_type,
                doc_id=item.doc_id,
            )

            self.queue.mark_downloaded(item.doc_id, local_path=local_path)
            return True

        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            error = f"HTTP {status}: {item.doc_url}"
            self.queue.mark_failed(item.doc_id, error=error)
            logger.error("Download failed HTTP %s: %s", status, item.doc_id)
            return False

        except requests.RequestException as exc:
            error = f"Network error: {type(exc).__name__}: {exc}"
            self.queue.mark_failed(item.doc_id, error=error)
            logger.error("Download network error for %s: %s", item.doc_id, exc)
            return False

        except (ValueError, OSError) as exc:
            error = f"{type(exc).__name__}: {exc}"
            self.queue.mark_failed(item.doc_id, error=error)
            logger.error("Download processing error for %s: %s", item.doc_id, exc)
            return False
