"""
src/db — Database layer for the EMMA Municipal Distress Monitoring System.

Exports the three things callers need most:

    from src.db import Session, init_db, Base

Modules:
    engine.py       — SQLAlchemy engine + session factory (SQLite dev / Postgres prod)
    models.py       — ORM models for all tables
    init_db.py      — Creates tables and indexes; safe to call repeatedly
    repositories/   — Data-access objects per entity type
"""

from src.db.engine import Session, engine  # noqa: F401
from src.db.models import Base              # noqa: F401
from src.db.init_db import init_db          # noqa: F401
