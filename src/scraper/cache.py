"""
cache.py — File-based HTTP response caching for EMMA discovery requests.

Stores responses as JSON in data/.cache/ with per-page-type TTLs.
Only caches discovery responses (JSON/HTML), never PDFs.

TTLs:
  - Search results         :  6 hours
  - Continuing disclosure  : 24 hours
  - Issue details          : 30 days
  - Security/CUSIP details : 30 days
  - Trade data             :  1 hour

Usage:
    cache = FileCache()
    content = cache.get(url)              # None if missing/expired
    cache.set(url, response_text, ttl_hours=24)

    # Or use the higher-level helper:
    content = cached_get(session, url, ttl_hours=24)
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Default cache directory — relative to project root
DEFAULT_CACHE_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", ".cache"
)

# TTL constants (hours) — use these when calling cached_get()
TTL_SEARCH = 6
TTL_CONTINUING_DISCLOSURE = 24
TTL_ISSUE_DETAILS = 30 * 24        # 30 days in hours
TTL_SECURITY_DETAILS = 30 * 24
TTL_TRADE_DATA = 1


class FileCache:
    """
    Simple file-based cache backed by the local filesystem.
    Each cache entry is a JSON file containing the content and expiry timestamp.
    """

    def __init__(self, cache_dir: str = DEFAULT_CACHE_DIR) -> None:
        self.cache_dir = os.path.abspath(cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)
        logger.debug("Cache directory: %s", self.cache_dir)

    def _cache_path(self, key: str) -> str:
        hashed = hashlib.sha256(key.encode()).hexdigest()
        return os.path.join(self.cache_dir, hashed + ".json")

    def get(self, key: str) -> Optional[str]:
        """
        Return cached content for key, or None if missing or expired.
        """
        path = self._cache_path(key)
        if not os.path.exists(path):
            return None

        try:
            with open(path, "r", encoding="utf-8") as f:
                entry = json.load(f)

            expires = datetime.fromisoformat(entry["expires"])
            if datetime.now() > expires:
                logger.debug("Cache expired for key: %s", key[:60])
                os.remove(path)
                return None

            logger.debug("Cache HIT for key: %s", key[:60])
            return entry["content"]

        except (json.JSONDecodeError, KeyError, OSError) as exc:
            logger.warning("Cache read error (%s) for key: %s", exc, key[:60])
            return None

    def set(self, key: str, content: str, ttl_hours: float) -> None:
        """
        Store content under key with a TTL.
        """
        path = self._cache_path(key)
        expires = (datetime.now() + timedelta(hours=ttl_hours)).isoformat()

        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"content": content, "expires": expires, "key": key[:120]}, f)
            logger.debug("Cache SET: key=%s ttl=%.1fh", key[:60], ttl_hours)
        except OSError as exc:
            logger.warning("Cache write error (%s) for key: %s", exc, key[:60])

    def invalidate(self, key: str) -> bool:
        """Remove a cache entry. Returns True if it existed."""
        path = self._cache_path(key)
        if os.path.exists(path):
            os.remove(path)
            logger.debug("Cache INVALIDATED: %s", key[:60])
            return True
        return False

    def clear_expired(self) -> int:
        """Remove all expired cache entries. Returns count deleted."""
        deleted = 0
        now = datetime.now()
        for fname in os.listdir(self.cache_dir):
            if not fname.endswith(".json"):
                continue
            path = os.path.join(self.cache_dir, fname)
            try:
                with open(path, "r", encoding="utf-8") as f:
                    entry = json.load(f)
                if datetime.fromisoformat(entry["expires"]) < now:
                    os.remove(path)
                    deleted += 1
            except (json.JSONDecodeError, KeyError, OSError):
                pass  # Leave corrupt files alone
        logger.info("Cleared %d expired cache entries", deleted)
        return deleted


# Module-level singleton
_default_cache = FileCache()


def cached_get(
    session: requests.Session,
    url: str,
    ttl_hours: float = TTL_CONTINUING_DISCLOSURE,
    params: Optional[dict] = None,
    bypass: bool = False,
) -> str:
    """
    GET a URL and cache the response text.

    Args:
        session:    Active requests.Session.
        url:        URL to fetch.
        ttl_hours:  How long to cache the response.
        params:     Query parameters (included in cache key).
        bypass:     If True, skip cache lookup and always re-fetch.

    Returns:
        Response text (HTML or JSON as string).
    """
    # Build a stable cache key from URL + params
    cache_key = url
    if params:
        sorted_params = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        cache_key = f"{url}?{sorted_params}"

    if not bypass:
        cached = _default_cache.get(cache_key)
        if cached is not None:
            return cached

    # Cache miss — fetch from EMMA
    resp = session.get(url, params=params, timeout=20)
    resp.raise_for_status()
    content = resp.text

    _default_cache.set(cache_key, content, ttl_hours=ttl_hours)
    return content
