"""
scripts/seed_borrowers.py — Seed the borrowers watchlist with the initial 30 institutions.

Run from the project root:
    python scripts/seed_borrowers.py

Safe to run multiple times — uses get_or_create() so existing rows are skipped.

Notes:
    - All sector: higher_ed
    - FYE 06-30 is the standard for private non-profit universities.
      Grand Canyon University uses 12-31 (legacy from its public-company era).
      FYEs marked with (est.) should be confirmed against actual bond covenants.
    - Two name corrections applied vs. the source list:
        "William Jessep" → "William Jessup University"
        "Witworth"       → "Whitworth University"
    - Anderson University: two exist (SC and IN). Entered as SC — update state
      to IN if you are tracking the Indiana institution.
    - Thomas M. Cooley: now formally "Western Michigan University Thomas M. Cooley
      Law School". Entered under the name most likely to appear in EMMA bond docs.
"""

import sys
import logging
from pathlib import Path

# Ensure project root is on the path when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db.engine import Session
from src.db.init_db import init_db
from src.db.repositories.borrower import BorrowerRepository

logging.basicConfig(level=logging.WARNING)  # suppress INFO noise during seeding

# ---------------------------------------------------------------------------
# Borrower definitions
# ---------------------------------------------------------------------------
# Keys: borrower_name, state, city, fiscal_year_end, watchlist_notes
# All sector = "higher_ed"
# ---------------------------------------------------------------------------

BORROWERS = [
    {
        "borrower_name":  "Grand Canyon University",
        "state":          "AZ",
        "city":           "Phoenix",
        "fiscal_year_end": "12-31",   # December — legacy of public-company era (LOPE)
        "watchlist_notes": "Large for-profit converted to nonprofit 2018; enrollment trends, accreditation risk",
    },
    {
        "borrower_name":  "University of San Francisco",
        "state":          "CA",
        "city":           "San Francisco",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Jesuit; Bay Area cost pressure, enrollment trends",
    },
    {
        "borrower_name":  "Simmons University",
        "state":          "MA",
        "city":           "Boston",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Women's college; Boston market competition",
    },
    {
        "borrower_name":  "University of Hartford",
        "state":          "CT",
        "city":           "West Hartford",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Connecticut private; enrollment pressure, program restructuring",
    },
    {
        "borrower_name":  "Harrisburg University of Science and Technology",
        "state":          "PA",
        "city":           "Harrisburg",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "STEM-focused; relatively young institution, limited endowment",
    },
    {
        "borrower_name":  "Art Center College of Design",
        "state":          "CA",
        "city":           "Pasadena",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Specialty arts institution; tuition-dependent, niche enrollment",
    },
    {
        "borrower_name":  "La Salle University",
        "state":          "PA",
        "city":           "Philadelphia",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Lasallian Catholic; Philadelphia private market pressure",
    },
    {
        "borrower_name":  "Azusa Pacific University",
        "state":          "CA",
        "city":           "Azusa",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Christian; Southern California competitive market, financial stress history",
    },
    {
        "borrower_name":  "Rider University",
        "state":          "NJ",
        "city":           "Lawrenceville",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "New Jersey private; enrollment decline, financial covenant scrutiny",
    },
    {
        "borrower_name":  "Rosalind Franklin University of Medicine and Science",
        "state":          "IL",
        "city":           "North Chicago",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Health sciences; graduate/professional focus, clinical partnership risk",
    },
    {
        "borrower_name":  "Saint Mary's College of California",
        "state":          "CA",
        "city":           "Moraga",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Lasallian Catholic; Bay Area cost structure, small endowment",
    },
    {
        "borrower_name":  "Webster University",
        "state":          "MO",
        "city":           "Webster Groves",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Multi-campus global model; campus consolidation underway",
    },
    {
        "borrower_name":  "William Jessup University",
        "state":          "CA",
        "city":           "Rocklin",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Christian; small enrollment base, California market",
    },
    {
        "borrower_name":  "Whitworth University",
        "state":          "WA",
        "city":           "Spokane",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Presbyterian; Pacific Northwest, modest endowment",
    },
    {
        "borrower_name":  "Saint Leo University",
        "state":          "FL",
        "city":           "Saint Leo",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Catholic; heavy online enrollment, adult learner focus",
    },
    {
        "borrower_name":  "Manhattan College",
        "state":          "NY",
        "city":           "Riverdale",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Lasallian; New York City tuition pressure, small endowment",
    },
    {
        "borrower_name":  "Seattle Pacific University",
        "state":          "WA",
        "city":           "Seattle",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Free Methodist; governance disputes, enrollment softness",
    },
    {
        "borrower_name":  "Hawaii Pacific University",
        "state":          "HI",
        "city":           "Honolulu",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Independent; island market, international student concentration",
    },
    {
        "borrower_name":  "University of Tulsa",
        "state":          "OK",
        "city":           "Tulsa",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Independent research; endowment exposure to energy sector, restructuring",
    },
    {
        "borrower_name":  "Columbia College Chicago",
        "state":          "IL",
        "city":           "Chicago",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Arts and media; enrollment decline, significant financial stress",
    },
    {
        "borrower_name":  "Guilford College",
        "state":          "NC",
        "city":           "Greensboro",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Quaker; small liberal arts, tuition-dependent, enrollment pressure",
    },
    {
        "borrower_name":  "Regis University",
        "state":          "CO",
        "city":           "Denver",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Jesuit; Colorado market, program mix shift toward professional/online",
    },
    {
        "borrower_name":  "Augsburg University",
        "state":          "MN",
        "city":           "Minneapolis",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Lutheran; urban campus, modest endowment, adult learner focus",
        "former_names":   ["Augsburg College"],   # renamed 2017; older EMMA bonds use former name
    },
    {
        "borrower_name":  "Vaughn College of Aeronautics and Technology",
        "state":          "NY",
        "city":           "East Elmhurst",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Specialty aviation/engineering; small enrollment, single-sector concentration",
    },
    {
        "borrower_name":  "Utica University",
        "state":          "NY",
        "city":           "Utica",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Independent; upstate New York demographic headwinds",
        "former_names":   ["Utica College"],   # renamed 2022; older EMMA bonds use former name
    },
    {
        "borrower_name":  "Anderson University",
        "state":          "IN",
        "city":           "Anderson",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Church of God (Anderson, IN); Free Methodist-affiliated liberal arts",
    },
    {
        "borrower_name":  "Hartwick College",
        "state":          "NY",
        "city":           "Oneonta",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Liberal arts; rural upstate NY, enrollment decline, balance sheet pressure",
    },
    {
        "borrower_name":  "Lake Erie College",
        "state":          "OH",
        "city":           "Painesville",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Small liberal arts; Ohio private market, very small enrollment base",
    },
    {
        "borrower_name":  "Truett McConnell University",
        "state":          "GA",
        "city":           "Cleveland",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Southern Baptist; small institution, Georgia mountains",
    },
    {
        "borrower_name":  "Thomas M. Cooley Law School",
        "state":          "MI",
        "city":           "Lansing",
        "fiscal_year_end": "06-30",
        "watchlist_notes": "Now Western Michigan Univ. Thomas M. Cooley Law School; enrollment decline, accreditation scrutiny",
    },
]


# ---------------------------------------------------------------------------
# Seed
# ---------------------------------------------------------------------------

def seed() -> None:
    print("Initializing database...")
    init_db()

    with Session() as session:
        repo = BorrowerRepository(session)

        added = 0
        skipped = 0

        print(f"\nSeeding {len(BORROWERS)} higher education borrowers...\n")
        print(f"  {'#':>3}  {'Name':<50}  {'State':>5}  {'FYE':>5}  Result")
        print("  " + "-" * 80)

        for b in BORROWERS:
            try:
                borrower, created = repo.get_or_create(
                    borrower_name=b["borrower_name"],
                    sector="higher_ed",
                    state=b["state"],
                    city=b["city"],
                    fiscal_year_end=b["fiscal_year_end"],
                    watchlist_notes=b.get("watchlist_notes"),
                    former_names=b.get("former_names"),
                )
                session.flush()
                result = "ADDED" if created else "exists"
                if created:
                    added += 1
                else:
                    skipped += 1
                print(
                    f"  {borrower.borrower_id:>3}  {b['borrower_name']:<50}  "
                    f"{b['state']:>5}  {b['fiscal_year_end']:>5}  {result}"
                )
            except Exception as exc:
                print(f"  ERR  {b['borrower_name']}: {exc}")

        session.commit()

    print(f"\nDone: {added} added, {skipped} already in database.")
    print("\nNext steps:")
    print("  python -m src.scraper.cli borrower list")
    print("  python -m src.scraper.cli borrower sync <id>")


if __name__ == "__main__":
    seed()
