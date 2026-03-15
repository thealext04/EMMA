"""
engine.py — SQLAlchemy engine and session factory.

Database selection:
    SQLite  (default, dev)  — set by omitting DATABASE_URL or setting it to
                              "sqlite:///data/emma.db"
    PostgreSQL (production) — set DATABASE_URL=postgresql://user:pass@host/dbname

Usage:
    from src.db.engine import Session

    with Session() as session:
        session.add(...)
        session.commit()

Environment variables:
    DATABASE_URL    Full SQLAlchemy connection string.
                    Defaults to sqlite:///data/emma.db (relative to project root).
    DB_ECHO         Set to "1" to enable SQL query logging (development only).
"""

import logging
import os
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Resolve connection string
# ---------------------------------------------------------------------------

_DEFAULT_SQLITE_PATH = Path(__file__).resolve().parents[2] / "data" / "emma.db"
_DEFAULT_URL = f"sqlite:///{_DEFAULT_SQLITE_PATH}"

DATABASE_URL: str = os.environ.get("DATABASE_URL", _DEFAULT_URL)
_ECHO: bool = os.environ.get("DB_ECHO", "0") == "1"

# ---------------------------------------------------------------------------
# Create engine
# ---------------------------------------------------------------------------

_connect_args: dict = {}
if DATABASE_URL.startswith("sqlite"):
    # Enable WAL mode for SQLite — allows concurrent reads during writes.
    # Also enforce foreign keys (SQLite disables them by default).
    _connect_args = {"check_same_thread": False}

engine = create_engine(
    DATABASE_URL,
    echo=_ECHO,
    connect_args=_connect_args,
    # Pool settings — SQLite uses StaticPool by default; Postgres uses QueuePool.
    pool_pre_ping=True,   # Verify connections are alive before using them.
)


# Enable foreign key enforcement for SQLite connections.
if DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()


logger.info("Database engine created: %s", DATABASE_URL.split("@")[-1])  # hide credentials

# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

Session = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,   # Allow reading attributes after commit without re-query.
)
