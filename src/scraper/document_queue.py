"""
document_queue.py — File-based download queue for EMMA disclosure documents.

The queue is stored as a JSON file at data/queue/queue.json.
It persists between runs and is safe to resume after crashes.

Status lifecycle:
    pending → downloading → downloaded
                         ↘ failed (retried up to max_attempts)

Phase 2 will migrate this queue into the PostgreSQL database.
For Phase 1, the JSON file approach is sufficient and requires no DB setup.

Usage:
    queue = DocumentQueue()
    queue.add(doc, borrower_name="Manhattan College", priority=3)

    for item in queue.get_pending(limit=10):
        queue.mark_downloading(item.doc_id)
        # ... download ...
        queue.mark_downloaded(item.doc_id, local_path="/path/to/file.pdf")
"""

import json
import logging
import os
import threading
from dataclasses import asdict
from datetime import datetime
from typing import Optional

from src.scraper.models import DisclosureDocument, QueueItem

logger = logging.getLogger(__name__)

DEFAULT_QUEUE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "queue", "queue.json"
)

MAX_ATTEMPTS = 3


class DocumentQueue:
    """
    Thread-safe persistent download queue.

    All operations are atomic within a single process.
    For multi-process use, the JSON file approach requires an external lock;
    Phase 2 replaces this with PostgreSQL advisory locks.
    """

    def __init__(self, queue_file: str = DEFAULT_QUEUE_FILE) -> None:
        self.queue_file = os.path.abspath(queue_file)
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(self.queue_file), exist_ok=True)
        self._items: dict[str, QueueItem] = {}
        self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(
        self,
        doc: DisclosureDocument,
        borrower_name: str,
        priority: int = 5,
    ) -> bool:
        """
        Add a document to the queue.

        Idempotent — if the doc_id is already in the queue (any status),
        the call is a no-op and returns False.

        Args:
            doc:           DisclosureDocument to queue.
            borrower_name: Borrower name for storage path computation.
            priority:      Download priority (1 = highest, 10 = lowest).

        Returns:
            True if added, False if already existed.
        """
        with self._lock:
            if doc.doc_id in self._items:
                existing = self._items[doc.doc_id]
                logger.debug(
                    "Queue: doc %s already present (status=%s)", doc.doc_id, existing.status
                )
                return False

            item = QueueItem(
                doc_id=doc.doc_id,
                doc_url=doc.doc_url,
                issue_id=doc.issue_id,
                borrower_name=borrower_name,
                doc_type=doc.doc_type,
                doc_date=doc.doc_date.isoformat() if doc.doc_date else None,
                discovered_at=datetime.utcnow().isoformat(),
                status="pending",
                priority=priority,
                attempts=0,
            )
            self._items[doc.doc_id] = item
            self._save()
            logger.info(
                "Queue: added %s (%s) for %s [priority=%d]",
                doc.doc_id,
                doc.doc_type,
                borrower_name,
                priority,
            )
            return True

    def add_batch(
        self,
        docs: list[DisclosureDocument],
        borrower_name: str,
        priority: int = 5,
    ) -> int:
        """Add multiple documents at once. Returns count of newly added items."""
        added = sum(
            1 for doc in docs if self.add(doc, borrower_name=borrower_name, priority=priority)
        )
        logger.info("Queue: batch add — %d new / %d total", added, len(docs))
        return added

    def get_pending(self, limit: int = 50) -> list[QueueItem]:
        """
        Return up to `limit` items ready to download, ordered by priority (asc).

        "Ready" means status == "pending" OR
        status == "failed" with attempts < MAX_ATTEMPTS.
        """
        with self._lock:
            ready = [
                item for item in self._items.values()
                if item.status == "pending" or (
                    item.status == "failed" and item.attempts < MAX_ATTEMPTS
                )
            ]
            ready.sort(key=lambda i: (i.priority, i.discovered_at))
            return ready[:limit]

    def mark_downloading(self, doc_id: str) -> None:
        """Mark an item as currently downloading."""
        with self._lock:
            item = self._items.get(doc_id)
            if item:
                item.status = "downloading"
                item.attempts += 1
                self._save()

    def mark_downloaded(self, doc_id: str, local_path: str) -> None:
        """Mark an item as successfully downloaded."""
        with self._lock:
            item = self._items.get(doc_id)
            if item:
                item.status = "downloaded"
                item.local_path = local_path
                item.error = None
                self._save()
                logger.info("Queue: downloaded %s → %s", doc_id, local_path)

    def mark_failed(self, doc_id: str, error: str) -> None:
        """
        Mark an item as failed. It will be retried on the next run
        until attempts >= MAX_ATTEMPTS, after which it stays failed permanently.
        """
        with self._lock:
            item = self._items.get(doc_id)
            if item:
                item.status = "failed"
                item.error = error[:500]  # Truncate long errors
                self._save()
                logger.warning(
                    "Queue: failed %s (attempt %d/%d) — %s",
                    doc_id,
                    item.attempts,
                    MAX_ATTEMPTS,
                    error[:100],
                )

    def get_stats(self) -> dict:
        """Return queue statistics."""
        with self._lock:
            counts: dict[str, int] = {}
            for item in self._items.values():
                counts[item.status] = counts.get(item.status, 0) + 1

            retryable = sum(
                1 for i in self._items.values()
                if i.status == "failed" and i.attempts < MAX_ATTEMPTS
            )

            return {
                "total": len(self._items),
                "pending": counts.get("pending", 0),
                "downloading": counts.get("downloading", 0),
                "downloaded": counts.get("downloaded", 0),
                "failed": counts.get("failed", 0),
                "retryable_failed": retryable,
                "queue_file": self.queue_file,
            }

    def reset_stuck_downloading(self) -> int:
        """
        Reset items stuck in 'downloading' state back to 'pending'.
        Call this on startup to recover from crash mid-download.
        """
        with self._lock:
            reset_count = 0
            for item in self._items.values():
                if item.status == "downloading":
                    item.status = "pending"
                    reset_count += 1
            if reset_count:
                self._save()
                logger.info(
                    "Queue: reset %d stuck 'downloading' items to 'pending'", reset_count
                )
            return reset_count

    def get_item(self, doc_id: str) -> Optional[QueueItem]:
        """Return a specific queue item by doc_id."""
        with self._lock:
            return self._items.get(doc_id)

    def remove_old_downloaded(self, keep_days: int = 30) -> int:
        """
        Remove successfully downloaded items older than keep_days.
        Keeps the queue file from growing unbounded.
        Returns count removed.
        """
        from datetime import timedelta
        cutoff = datetime.utcnow() - timedelta(days=keep_days)
        removed = 0

        with self._lock:
            to_remove = [
                doc_id for doc_id, item in self._items.items()
                if item.status == "downloaded"
                and datetime.fromisoformat(item.discovered_at) < cutoff
            ]
            for doc_id in to_remove:
                del self._items[doc_id]
                removed += 1
            if removed:
                self._save()
                logger.info("Queue: removed %d old downloaded entries", removed)
        return removed

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Load queue from disk. Initializes empty if file doesn't exist."""
        if not os.path.exists(self.queue_file):
            logger.debug("Queue file not found — starting fresh: %s", self.queue_file)
            return

        try:
            with open(self.queue_file, "r", encoding="utf-8") as f:
                raw = json.load(f)
            self._items = {
                doc_id: QueueItem(**item_data)
                for doc_id, item_data in raw.items()
            }
            logger.info(
                "Queue loaded: %d items from %s", len(self._items), self.queue_file
            )
        except (json.JSONDecodeError, TypeError, KeyError) as exc:
            logger.error(
                "Queue file corrupted (%s) — starting fresh: %s",
                exc,
                self.queue_file,
            )
            # Backup the corrupted file
            backup = self.queue_file + ".corrupt"
            if os.path.exists(self.queue_file):
                os.rename(self.queue_file, backup)
            self._items = {}

    def _save(self) -> None:
        """Persist queue to disk. Called inside the lock."""
        try:
            tmp_path = self.queue_file + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(
                    {doc_id: asdict(item) for doc_id, item in self._items.items()},
                    f,
                    indent=2,
                    default=str,
                )
            # Atomic rename to avoid partial writes
            os.replace(tmp_path, self.queue_file)
        except OSError as exc:
            logger.error("Queue save failed: %s", exc)
