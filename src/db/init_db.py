"""
init_db.py — Create all database tables and indexes.

Safe to run multiple times (uses CREATE TABLE IF NOT EXISTS via SQLAlchemy's
checkfirst behavior).  Does NOT drop existing tables.

Usage:
    python -m src.db.init_db

Or from code:
    from src.db.init_db import init_db
    init_db()
"""

import logging
from pathlib import Path

from src.db.engine import DATABASE_URL, engine
from src.db.models import Base

logger = logging.getLogger(__name__)


def init_db() -> None:
    """
    Create all tables and indexes defined in models.py.

    For SQLite: also ensures the data/ directory exists.
    For PostgreSQL: connects using DATABASE_URL and creates schema.

    Safe to call multiple times — existing tables and indexes are not modified.
    """
    # Ensure data/ directory exists for SQLite
    if DATABASE_URL.startswith("sqlite"):
        db_path_str = DATABASE_URL.replace("sqlite:///", "")
        db_path = Path(db_path_str)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("SQLite database path: %s", db_path.resolve())

    logger.info("Creating database schema (if not exists)...")
    Base.metadata.create_all(bind=engine, checkfirst=True)
    logger.info("Database schema ready.")

    # Log all table names that were created/verified
    table_names = sorted(Base.metadata.tables.keys())
    logger.debug("Tables: %s", ", ".join(table_names))


if __name__ == "__main__":
    import sys
    from src.scraper.logger import configure_logging

    configure_logging(level="INFO", json_output=False)
    logger = logging.getLogger(__name__)

    print("Initializing EMMA database...")
    print(f"Target: {DATABASE_URL.split('@')[-1]}")  # hide credentials in URL

    try:
        init_db()
        print("Database initialized successfully.")
        print(f"Tables created: {', '.join(sorted(Base.metadata.tables.keys()))}")
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
