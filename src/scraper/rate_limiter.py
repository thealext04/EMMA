"""
rate_limiter.py — Thread-safe rate limiting for EMMA requests.

Two separate rate limits:
  - Discovery layer  : 1 request / second   (JSON/HTML endpoints)
  - Download layer   : 1 request / 2.5 sec  (PDF downloads)

Usage:
    limiter = RateLimiter()
    limiter.wait()              # discovery
    limiter.wait(is_download=True)  # PDF download
"""

import logging
import threading
import time

logger = logging.getLogger(__name__)

# Conservative limits — well below EMMA's observed thresholds
DISCOVERY_DELAY_SEC: float = 1.0
DOWNLOAD_DELAY_SEC: float = 2.5


class RateLimiter:
    """
    Enforces per-layer minimum delays between requests.
    Thread-safe via a single lock shared across both layers.
    """

    def __init__(
        self,
        discovery_delay: float = DISCOVERY_DELAY_SEC,
        download_delay: float = DOWNLOAD_DELAY_SEC,
    ) -> None:
        self.discovery_delay = discovery_delay
        self.download_delay = download_delay

        self._lock = threading.Lock()
        self._last_discovery: float = 0.0
        self._last_download: float = 0.0

    def wait(self, is_download: bool = False) -> float:
        """
        Sleep if necessary to respect the rate limit.

        Returns:
            float: Actual seconds slept (0.0 if no sleep was needed).
        """
        with self._lock:
            now = time.monotonic()

            if is_download:
                elapsed = now - self._last_download
                required = self.download_delay
                last_attr = "_last_download"
            else:
                elapsed = now - self._last_discovery
                required = self.discovery_delay
                last_attr = "_last_discovery"

            sleep_for = max(0.0, required - elapsed)

            if sleep_for > 0:
                logger.debug(
                    "Rate limiter: sleeping %.2fs (%s)",
                    sleep_for,
                    "download" if is_download else "discovery",
                )
                time.sleep(sleep_for)

            setattr(self, last_attr, time.monotonic())
            return sleep_for

    def reset(self) -> None:
        """Reset all timers (e.g., after a long pause in scraping)."""
        with self._lock:
            self._last_discovery = 0.0
            self._last_download = 0.0


# Module-level singleton — share one limiter across all scraper modules
_default_limiter = RateLimiter()


def wait(is_download: bool = False) -> float:
    """Convenience wrapper for the default rate limiter."""
    return _default_limiter.wait(is_download=is_download)
