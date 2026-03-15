"""
session.py — EMMA session manager.

Creates and maintains a requests.Session that behaves like a real Chrome browser.
All scraper modules should obtain their session through this module.

Key behaviors:
- Realistic Chrome User-Agent and Accept headers
- Warm-up GET to emma.msrb.org to pick up cookies naturally
- Automatic re-initialization on HTTP 403 (session expired)
- No hardcoded session cookies
"""

import logging
import time
from typing import Optional

import requests

logger = logging.getLogger(__name__)

EMMA_BASE = "https://emma.msrb.org"
WARMUP_URL = EMMA_BASE + "/"

# Chrome 124 on macOS — realistic and stable
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Referer": EMMA_BASE + "/",
}

# Headers for JSON API endpoints
JSON_HEADERS = {
    **HEADERS,
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


class EMMAsession:
    """
    Wraps requests.Session with EMMA-specific behavior.

    Usage:
        mgr = EMMAsession()
        session = mgr.get_session()
        resp = session.get(url, ...)
    """

    def __init__(self, timeout: int = 20):
        self.timeout = timeout
        self._session: Optional[requests.Session] = None
        self._init_count = 0

    def get_session(self) -> requests.Session:
        """Return the active session, initializing if necessary."""
        if self._session is None:
            self._session = self._create_session()
        return self._session

    def reinitialize(self) -> requests.Session:
        """
        Force a fresh session. Call this after a 403 or unexpected redirect.
        Waits 5 seconds before re-initializing to avoid hammering on error.
        """
        logger.warning("Reinitializing EMMA session (attempt %d)", self._init_count + 1)
        time.sleep(5)
        self._session = self._create_session()
        return self._session

    def _create_session(self) -> requests.Session:
        """
        Create a new requests.Session, set headers, and warm it up
        by visiting the EMMA homepage to collect cookies naturally.
        """
        self._init_count += 1
        session = requests.Session()
        session.headers.update(HEADERS)

        try:
            logger.debug("Warming up EMMA session (visit homepage)")
            resp = session.get(WARMUP_URL, timeout=self.timeout)
            resp.raise_for_status()
            logger.info(
                "Session initialized — cookies: %s",
                list(session.cookies.keys()),
            )
        except requests.RequestException as exc:
            logger.warning("Session warm-up failed: %s — proceeding anyway", exc)

        # Required: set the Disclaimer6 cookie to indicate acceptance of MSRB's
        # public Terms of Use (https://www.msrb.org/terms-of-use). Without this
        # cookie, EMMA redirects all requests to the Terms of Use page instead of
        # returning real data. This is the equivalent of clicking "I Agree" on the
        # disclaimer modal that EMMA presents to first-time visitors.
        session.cookies.set("Disclaimer6", "msrborg", domain="emma.msrb.org")
        logger.debug("Set Disclaimer6=msrborg cookie on emma.msrb.org")

        return session

    def set_json_headers(self) -> None:
        """Switch session headers to JSON/XHR mode for API endpoints."""
        if self._session:
            self._session.headers.update(JSON_HEADERS)

    def set_html_headers(self) -> None:
        """Switch session headers back to HTML/browser mode."""
        if self._session:
            self._session.headers.update(HEADERS)

    @property
    def cookies(self) -> dict:
        """Return current session cookies as a plain dict."""
        if self._session is None:
            return {}
        return dict(self._session.cookies)
