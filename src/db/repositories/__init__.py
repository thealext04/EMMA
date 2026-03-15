"""
repositories/ — Data-access objects for each entity type.

Each repository wraps a SQLAlchemy Session and provides typed CRUD
operations, keeping SQL out of business logic and CLI commands.

Available repositories:
    BorrowerRepository  — borrowers table
    BondIssueRepository — bond_issues table (Phase 2)
"""

from src.db.repositories.borrower import BorrowerRepository    # noqa: F401
from src.db.repositories.bond_issue import BondIssueRepository  # noqa: F401
