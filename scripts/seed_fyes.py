#!/usr/bin/env python3
"""
scripts/seed_fyes.py — Populate fiscal year ends for all tracked borrowers.

Default: higher_ed sector → "06-30" (June 30).
Known exceptions are listed in OVERRIDES below.

Run this after seeding borrowers to ensure all Phase 3 late-filing
detection has the FYE data it needs.  Safe to run multiple times —
only updates borrowers where fiscal_year_end is currently NULL.

Usage:
    cd /Users/alexthompson/Documents/EMMA
    source .venv/bin/activate
    python scripts/seed_fyes.py           # dry run (show what would change)
    python scripts/seed_fyes.py --apply   # write changes to the database
"""

import argparse
import sys
from pathlib import Path

# Allow running from the repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.engine import Session
from src.db.repositories.borrower import BorrowerRepository

# -----------------------------------------------------------------------
# Known FYE exceptions (borrower_name → "MM-DD")
# Update this dict as you learn the actual FYE for each borrower.
# Any borrower NOT listed here will use DEFAULT_HIGHER_ED_FYE.
# -----------------------------------------------------------------------
OVERRIDES: dict[str, str] = {
    # For-profit; fiscal year ends December 31
    "Grand Canyon University": "12-31",

    # Add corrections here as needed, e.g.:
    # "University of Tulsa": "05-31",
}

DEFAULT_HIGHER_ED_FYE = "06-30"


def main(apply: bool = False) -> None:
    header = "DRY RUN — " if not apply else ""
    print(f"\n{header}Fiscal Year End Seeder")
    print("=" * 60)

    with Session() as session:
        repo = BorrowerRepository(session)
        borrowers = repo.list_all()

        updated = []
        already_set = []
        skipped_non_ed = []

        for b in borrowers:
            # Determine target FYE
            if b.borrower_name in OVERRIDES:
                target_fye = OVERRIDES[b.borrower_name]
            elif b.sector == "higher_ed":
                target_fye = DEFAULT_HIGHER_ED_FYE
            else:
                skipped_non_ed.append(b)
                continue

            if b.fiscal_year_end is None:
                if apply:
                    b.fiscal_year_end = target_fye
                updated.append((b, target_fye, "SET"))
            elif b.fiscal_year_end != target_fye:
                # Already has a different FYE — flag for review but don't overwrite
                already_set.append((b, b.fiscal_year_end, target_fye))
            else:
                already_set.append((b, b.fiscal_year_end, None))

        if apply:
            session.commit()

    # ---- Report ----
    if updated:
        action = "Set" if apply else "Would set"
        print(f"\n  {action} FYE for {len(updated)} borrower(s):")
        for b, fye, _ in updated:
            print(f"    #{b.borrower_id:>3}  {b.borrower_name:<45}  → {fye}")

    if already_set:
        print(f"\n  Already configured ({len(already_set)} borrower(s)):")
        for b, current_fye, suggested in already_set:
            mismatch = f"  ⚠  suggested={suggested}" if suggested else ""
            print(f"    #{b.borrower_id:>3}  {b.borrower_name:<45}  {current_fye}{mismatch}")

    if skipped_non_ed:
        print(f"\n  Skipped (no default for sector) — {len(skipped_non_ed)} borrower(s):")
        for b in skipped_non_ed:
            print(f"    #{b.borrower_id:>3}  {b.borrower_name:<45}  sector={b.sector}")

    print()
    if not apply:
        print("  → Run with --apply to write changes to the database.")
    else:
        print(f"  → Done. {len(updated)} FYE(s) updated.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed fiscal year ends for tracked borrowers")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes to the database (default: dry run)",
    )
    args = parser.parse_args()
    main(apply=args.apply)
