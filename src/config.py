"""
config.py — Centralized configuration for the EMMA monitoring system.

All settings are read from environment variables (with defaults).
Use a .env file in the project root for local configuration — it is
loaded automatically when this module is imported.

Usage:
    from src.config import settings

    print(settings.storage_dir)     # path to raw document store
    print(settings.anthropic_key)   # Claude API key
    settings.assert_storage_ready() # raises if drive not mounted
"""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Project root (two levels up from this file: src/config.py → project root)
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv() -> None:
    """
    Load .env from the project root if python-dotenv is installed.
    Silently skips if the package is not available or file doesn't exist.
    """
    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        return
    try:
        from dotenv import load_dotenv  # type: ignore[import]
        load_dotenv(env_file)
        logger.debug("Loaded .env from %s", env_file)
    except ImportError:
        # python-dotenv not installed — fall back to OS environment only
        pass


_load_dotenv()


# ---------------------------------------------------------------------------
# Settings dataclass
# ---------------------------------------------------------------------------

@dataclass
class Settings:
    """
    All runtime configuration in one place.

    Environment variable names are documented per field.
    All variables can be set in a .env file at the project root.
    """

    # ------------------------------------------------------------------
    # Document storage
    # ------------------------------------------------------------------

    storage_dir: str = field(default_factory=lambda: os.environ.get(
        "EMMA_STORAGE_DIR",
        str(PROJECT_ROOT / "data" / "raw_documents"),
    ))
    """
    Where raw PDFs are stored.

    Env var: EMMA_STORAGE_DIR

    Local (default):
        data/raw_documents/   (inside the project directory)

    External drive (macOS example):
        EMMA_STORAGE_DIR=/Volumes/EmmaArchive/raw_documents

    External drive (Linux/USB example):
        EMMA_STORAGE_DIR=/media/alex/EmmaArchive/raw_documents

    The system will warn at startup if this path is not accessible.
    """

    # ------------------------------------------------------------------
    # Database
    # ------------------------------------------------------------------

    database_url: str = field(default_factory=lambda: os.environ.get(
        "EMMA_DATABASE_URL",
        f"sqlite:///{PROJECT_ROOT / 'data' / 'emma.db'}",
    ))
    """
    SQLAlchemy connection string.

    Env var: EMMA_DATABASE_URL

    SQLite (default, development):
        sqlite:////absolute/path/to/emma.db

    PostgreSQL (production):
        EMMA_DATABASE_URL=postgresql://user:pass@localhost:5432/emma
    """

    # ------------------------------------------------------------------
    # AI / Claude API
    # ------------------------------------------------------------------

    anthropic_api_key: str = field(default_factory=lambda: os.environ.get(
        "ANTHROPIC_API_KEY", ""
    ))
    """
    Claude API key from console.anthropic.com.

    Env var: ANTHROPIC_API_KEY
    Required for Phase 4 (AI extraction). Not needed for scraping-only runs.
    """

    extraction_model: str = field(default_factory=lambda: os.environ.get(
        "EMMA_EXTRACTION_MODEL", "claude-sonnet-4-6"
    ))
    """
    Claude model used for financial statement and event notice extraction.
    Env var: EMMA_EXTRACTION_MODEL
    Default: claude-sonnet-4-6
    """

    classification_model: str = field(default_factory=lambda: os.environ.get(
        "EMMA_CLASSIFICATION_MODEL", "claude-haiku-4-5"
    ))
    """
    Claude model used for first-page document classification (cheap calls).
    Env var: EMMA_CLASSIFICATION_MODEL
    Default: claude-haiku-4-5
    """

    # ------------------------------------------------------------------
    # Scraper rate limits
    # ------------------------------------------------------------------

    discovery_delay_sec: float = field(default_factory=lambda: float(os.environ.get(
        "EMMA_DISCOVERY_DELAY", "1.0"
    )))
    """Seconds between discovery (JSON/HTML) requests. Default: 1.0"""

    download_delay_sec: float = field(default_factory=lambda: float(os.environ.get(
        "EMMA_DOWNLOAD_DELAY", "2.5"
    )))
    """Seconds between PDF download requests. Default: 2.5"""

    max_workers: int = field(default_factory=lambda: int(os.environ.get(
        "EMMA_MAX_WORKERS", "3"
    )))
    """Maximum parallel download workers. Default: 3 (project max)."""

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------

    cache_dir: str = field(default_factory=lambda: os.environ.get(
        "EMMA_CACHE_DIR",
        str(PROJECT_ROOT / "data" / ".cache"),
    ))
    """Directory for HTTP response cache files."""

    # ------------------------------------------------------------------
    # Validation helpers
    # ------------------------------------------------------------------

    def storage_is_ready(self) -> bool:
        """
        Return True if the storage directory exists and is writable.

        For external drives: the drive must be mounted (macOS: /Volumes/<name>)
        before the storage_dir path will exist.
        """
        path = Path(self.storage_dir)
        return path.exists() and os.access(path, os.W_OK)

    def assert_storage_ready(self) -> None:
        """
        Raise RuntimeError if the storage directory is not accessible.

        Call this at the start of any command that downloads or reads PDFs.
        Gives a clear error message explaining how to configure an external drive.
        """
        if not self.storage_is_ready():
            path = Path(self.storage_dir)
            if not path.exists():
                msg = (
                    f"Storage directory not found: {self.storage_dir}\n\n"
                    "If you are using an external drive:\n"
                    "  1. Connect and mount the drive\n"
                    "  2. Verify the mount path (macOS: /Volumes/<DriveName>)\n"
                    "  3. Set EMMA_STORAGE_DIR in your .env file:\n"
                    "       EMMA_STORAGE_DIR=/Volumes/EmmaArchive/raw_documents\n"
                    "\n"
                    "If you are using local storage, create the directory:\n"
                    f"       mkdir -p {self.storage_dir}"
                )
            else:
                msg = (
                    f"Storage directory exists but is not writable: {self.storage_dir}\n"
                    "Check permissions: chmod u+w <path>"
                )
            raise RuntimeError(msg)

    def assert_api_key(self) -> None:
        """Raise RuntimeError if ANTHROPIC_API_KEY is not set."""
        if not self.anthropic_api_key:
            raise RuntimeError(
                "ANTHROPIC_API_KEY is not set.\n"
                "Add it to your .env file:\n"
                "  ANTHROPIC_API_KEY=sk-ant-..."
            )

    def summary(self) -> str:
        """Return a human-readable config summary (safe to log — masks API key)."""
        key_preview = (
            f"{self.anthropic_api_key[:8]}..." if self.anthropic_api_key else "NOT SET"
        )
        storage_status = "✓ ready" if self.storage_is_ready() else "✗ NOT ACCESSIBLE"
        return (
            f"EMMA Configuration\n"
            f"  storage_dir:          {self.storage_dir} [{storage_status}]\n"
            f"  database_url:         {self.database_url}\n"
            f"  extraction_model:     {self.extraction_model}\n"
            f"  classification_model: {self.classification_model}\n"
            f"  anthropic_api_key:    {key_preview}\n"
            f"  discovery_delay:      {self.discovery_delay_sec}s\n"
            f"  download_delay:       {self.download_delay_sec}s\n"
            f"  max_workers:          {self.max_workers}\n"
        )


# ---------------------------------------------------------------------------
# Module-level singleton — import this everywhere
# ---------------------------------------------------------------------------

settings = Settings()
