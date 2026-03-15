"""
retry.py — Exponential backoff retry logic for EMMA HTTP requests.

Strategy:
  - Up to 3 attempts per request
  - Backoff schedule: 5s → 30s → 120s
  - 429 (rate limited): double the backoff
  - 503 / 502: standard backoff
  - Timeout / ConnectionError: standard backoff
  - 404 / 410: do not retry (document gone)

Usage:
    from src.scraper.retry import fetch_with_retry
    response = fetch_with_retry(session, url)
    response = fetch_with_retry(session, url, params={"key": "val"}, is_download=True)
"""

import logging
import time
from typing import Any, Optional

import requests

from src.scraper import rate_limiter

logger = logging.getLogger(__name__)

# Seconds to wait before each retry attempt (index = attempt number, 0-based)
BACKOFF_SCHEDULE: list[float] = [5.0, 30.0, 120.0]

# HTTP status codes that should NOT be retried
NO_RETRY_STATUSES: set[int] = {400, 401, 403, 404, 410}


def fetch_with_retry(
    session: requests.Session,
    url: str,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    is_download: bool = False,
    max_attempts: int = 3,
    stream: bool = False,
) -> requests.Response:
    """
    GET a URL with retry and exponential backoff.

    Args:
        session:       Active requests.Session.
        url:           Full URL to fetch.
        params:        Optional query parameters.
        headers:       Optional per-request header overrides.
        is_download:   If True, uses slower download rate limit (PDF fetching).
        max_attempts:  Maximum number of total attempts (default 3).
        stream:        If True, streams response body (for large PDF downloads).

    Returns:
        requests.Response on success.

    Raises:
        requests.HTTPError:     After max_attempts on non-retryable HTTP errors.
        requests.RequestException: After max_attempts on network-level failures.
    """
    last_exc: Optional[Exception] = None

    for attempt in range(max_attempts):
        # Enforce rate limit before every attempt
        rate_limiter.wait(is_download=is_download)

        try:
            resp = session.get(
                url,
                params=params,
                headers=headers,
                timeout=20,
                stream=stream,
            )

            # --- Success ---
            if resp.status_code == 200:
                logger.debug("GET %s → 200 (attempt %d)", url, attempt + 1)
                return resp

            # --- Non-retryable errors ---
            if resp.status_code in NO_RETRY_STATUSES:
                logger.warning(
                    "GET %s → %d (non-retryable) — aborting",
                    url,
                    resp.status_code,
                )
                resp.raise_for_status()

            # --- Rate limited (429) ---
            if resp.status_code == 429:
                backoff = BACKOFF_SCHEDULE[min(attempt, len(BACKOFF_SCHEDULE) - 1)] * 2
                logger.warning(
                    "GET %s → 429 — rate limited. Sleeping %.0fs before retry %d/%d",
                    url,
                    backoff,
                    attempt + 1,
                    max_attempts,
                )
                time.sleep(backoff)
                last_exc = requests.HTTPError(response=resp)
                continue

            # --- Server errors (5xx) ---
            if resp.status_code >= 500:
                backoff = BACKOFF_SCHEDULE[min(attempt, len(BACKOFF_SCHEDULE) - 1)]
                logger.warning(
                    "GET %s → %d — server error. Sleeping %.0fs before retry %d/%d",
                    url,
                    resp.status_code,
                    backoff,
                    attempt + 1,
                    max_attempts,
                )
                time.sleep(backoff)
                last_exc = requests.HTTPError(response=resp)
                continue

            # --- Any other non-200 ---
            resp.raise_for_status()

        except (requests.Timeout, requests.ConnectionError) as exc:
            backoff = BACKOFF_SCHEDULE[min(attempt, len(BACKOFF_SCHEDULE) - 1)]
            logger.warning(
                "GET %s → network error (%s). Sleeping %.0fs before retry %d/%d",
                url,
                type(exc).__name__,
                backoff,
                attempt + 1,
                max_attempts,
            )
            time.sleep(backoff)
            last_exc = exc

    # All attempts exhausted
    logger.error("GET %s — failed after %d attempts", url, max_attempts)
    if last_exc:
        raise last_exc
    raise requests.RequestException(f"Failed after {max_attempts} attempts: {url}")
