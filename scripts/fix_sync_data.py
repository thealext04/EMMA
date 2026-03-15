"""
scripts/fix_sync_data.py — One-time data-quality fix script.

Applies three corrections to an already-populated database:

1. Schema migration — adds `former_names` column to borrowers table
   (safe to run on an existing DB; skips if column already exists).

2. Document reclassification — re-applies the corrected classify_doc_type()
   to every row in the documents table.
   Fixes:
     - "Financial Operating Filing" was misclassified as rating_notice
       (because "oper-ating" contains "rating" as a substring).
       Now correctly classified as financial_statement.
     - "Event Filing" was classified as "other".
       Now correctly classified as event_notice.

3. False-positive bond issue cleanup — deletes bond issues (and their
   documents) that were pulled in as false positives due to geographic
   or word-overlap collisions:
     - University of San Francisco: San Francisco Redevelopment bonds
     - Azusa Pacific University: Pacific University / Fresno Pacific / Univ of the Pacific
     - Hawaii Pacific University: Hawaii Pacific Health hospital system bonds

4. Former-name aliases — sets former_names on Augsburg University and
   Utica University so the next sync also searches their pre-rename names
   ("Augsburg College" and "Utica College" respectively).

Run from the project root:
    python scripts/fix_sync_data.py
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text, select
from src.db.engine import Session, engine, DATABASE_URL
from src.db.models import Borrower, BondIssue, Document
from src.db.repositories.document import reclassify_all_documents

logging.basicConfig(level=logging.WARNING)

# ---------------------------------------------------------------------------
# False-positive emma_issue_ids to remove, keyed by borrower_name fragment
# ---------------------------------------------------------------------------

FALSE_POSITIVE_ISSUES: dict[str, list[str]] = {
    # University of San Francisco — SF Redevelopment city bonds
    "University of San Francisco": [
        "EP344960",   # SF Redevelopment 2011A Tax Allocation
        "EA332849",   # SF Redevelopment 2009 Series F
        "EP328542",   # SF Redevelopment Taxable
        "EP327979",   # SF Redevelopment 2009 Series B
        "AF3479B88ED706C21822F05903035968",  # SF Planning & Urban Research
    ],
    # Azusa Pacific University — different "Pacific" universities
    "Azusa Pacific University": [
        "P2424358",                           # University of the Pacific 2023
        "29A8B447C2D01B7DA271DA405C0268E2",   # Pacific University 2004
        "MS25747",                            # Pacific University 2004 (duplicate)
        "MS53374",                            # Univ of the Pacific 2000
        "MS53593",                            # Fresno Pacific Univ Series A
        "829EC93378A11AB4A9B3D542FC38C8E3",   # Fresno Pacific Univ Series A (duplicate)
    ],
    # Hawaii Pacific University — Hawaii Pacific Health hospital system
    "Hawaii Pacific University": [
        "8DCF61A07E13E4A9C9867A398C0BBB24",   # Hawaii Pacific Health 2023C
        "6A286BD703A6F69ADBDD4F8200950605",   # Hawaii Pacific Health Refunding B
        "B7E3BA80142D5F0A561E1D545E939FD0",   # Hawaii Pacific Health Refunding A
        "EA348731",                           # Hawaii Pacific Health 2012 Series A
        "EA348732",                           # Hawaii Pacific Health 2012 Series B
        "E991F17DCC86A6E74352DA9E432E320F",   # Hawaii Pacific Health 2009
        "MS105943",                           # Hawaii Pacific Health Group A-1
        "MS34137",                            # Hawaii Pacific Health Group A-2
        "MS204353",                           # Hawaii Pacific Health / Kapiolani 2004
        "538C7053204DBB22EEE69DD28D63B9AE",   # Hawaii Pacific Health (variable)
    ],
}

# Former names for institutions that renamed — pipe-separated, stored in DB
FORMER_NAMES: dict[str, str] = {
    "Augsburg University": "Augsburg College",
    "Utica University":    "Utica College",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _column_exists(conn, table: str, column: str) -> bool:
    """Check whether a column exists in a SQLite table."""
    result = conn.execute(text(f"PRAGMA table_info({table})"))
    return any(row[1] == column for row in result)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> None:
    print(f"\nDatabase: {DATABASE_URL.split('@')[-1]}")
    print("=" * 60)

    with engine.connect() as conn:
        # ------------------------------------------------------------------
        # Step 1: Schema migration — add former_names if missing
        # ------------------------------------------------------------------
        print("\n[1/4] Schema migration: borrowers.former_names column")
        if _column_exists(conn, "borrowers", "former_names"):
            print("      Column already exists — skipping.")
        else:
            conn.execute(text("ALTER TABLE borrowers ADD COLUMN former_names TEXT"))
            conn.commit()
            print("      Column added.")

    with Session() as session:
        # ------------------------------------------------------------------
        # Step 2: Reclassify all documents with the corrected classifier
        # ------------------------------------------------------------------
        print("\n[2/4] Reclassifying documents...")
        updated, distribution = reclassify_all_documents(session)
        session.commit()
        print(f"      Updated {updated} document(s).")
        print("      Final distribution:")
        for dtype, count in sorted(distribution.items(), key=lambda x: -x[1]):
            print(f"        {dtype:<25} {count:>5}")

        # ------------------------------------------------------------------
        # Step 3: Remove false-positive bond issues
        # ------------------------------------------------------------------
        print("\n[3/4] Removing false-positive bond issues...")
        total_issues_removed = 0
        total_docs_removed = 0

        for borrower_fragment, bad_issue_ids in FALSE_POSITIVE_ISSUES.items():
            # Find the borrower
            borrower = session.execute(
                select(Borrower).where(
                    Borrower.borrower_name.ilike(f"%{borrower_fragment}%")
                )
            ).scalar_one_or_none()

            if not borrower:
                print(f"      WARNING: borrower '{borrower_fragment}' not found — skipping.")
                continue

            print(f"\n      {borrower.borrower_name}:")
            for emma_id in bad_issue_ids:
                issue = session.execute(
                    select(BondIssue).where(BondIssue.emma_issue_id == emma_id)
                ).scalar_one_or_none()

                if not issue:
                    print(f"        {emma_id:<45} not in DB — skipping")
                    continue

                # Delete associated documents first
                docs = session.execute(
                    select(Document).where(Document.issue_id == issue.issue_id)
                ).scalars().all()
                for d in docs:
                    session.delete(d)
                    total_docs_removed += 1

                print(
                    f"        {emma_id:<45} REMOVED  "
                    f"({len(docs)} docs, \"{(issue.series_name or '')[:40]}\")"
                )
                session.delete(issue)
                total_issues_removed += 1

        session.commit()
        print(
            f"\n      Removed {total_issues_removed} false-positive issue(s) "
            f"and {total_docs_removed} associated document(s)."
        )

        # ------------------------------------------------------------------
        # Step 4: Set former_names on renamed institutions
        # ------------------------------------------------------------------
        print("\n[4/4] Setting former_names aliases...")
        for current_name, former_name in FORMER_NAMES.items():
            borrower = session.execute(
                select(Borrower).where(Borrower.borrower_name == current_name)
            ).scalar_one_or_none()

            if not borrower:
                print(f"      WARNING: '{current_name}' not found — skipping.")
                continue

            borrower.former_names = former_name
            print(f"      {current_name:<45} ← former: \"{former_name}\"")

        session.commit()

    print("\n" + "=" * 60)
    print("Fix complete. Recommended next steps:")
    print("  # Re-sync the three affected borrowers with --clean:")
    print("  python -m src.scraper.cli borrower sync 3  --clean --no-cache  # USF")
    print("  python -m src.scraper.cli borrower sync 9  --clean --no-cache  # Azusa Pacific")
    print("  python -m src.scraper.cli borrower sync 18 --clean --no-cache  # Hawaii Pacific")
    print("  # Sync the two formerly-zero borrowers (now have aliases):")
    print("  python -m src.scraper.cli borrower sync 23 --no-cache           # Augsburg")
    print("  python -m src.scraper.cli borrower sync 25 --no-cache           # Utica")


if __name__ == "__main__":
    run()
