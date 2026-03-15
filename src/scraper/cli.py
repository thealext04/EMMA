"""
cli.py — Command-line interface for the EMMA Phase 1 scraper.

Commands:
    search      Search for bond issues by borrower/issuer name
    discover    Discover new disclosure documents for a bond issue
    download    Process the download queue (fetch queued PDFs)
    events      Fetch recent material event notices
    queue       Show download queue status
    stats       Show storage and queue statistics

Usage:
    python -m src.scraper.cli search "Manhattan College"
    python -m src.scraper.cli search "Rider University" --state NJ
    python -m src.scraper.cli discover --issue-id ABC123DE
    python -m src.scraper.cli discover --cusip 04781GAB7
    python -m src.scraper.cli download --workers 2 --limit 100
    python -m src.scraper.cli events --days 7 --high-signal-only
    python -m src.scraper.cli queue
    python -m src.scraper.cli stats
"""

import argparse
import json
import sys
from datetime import datetime
from typing import Optional

from src.scraper.logger import configure_logging, get_logger

logger = get_logger(__name__)


def main(argv: Optional[list[str]] = None) -> int:
    """Entry point. Returns exit code (0 = success, 1 = error)."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    configure_logging(
        level=args.log_level,
        log_file=args.log_file,
        json_output=not args.plain_logs,
    )

    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    try:
        return args.func(args)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 0
    except Exception as exc:  # noqa: BLE001
        logger.error("Unexpected error: %s", exc, exc_info=True)
        return 1


# ---------------------------------------------------------------------------
# Command implementations
# ---------------------------------------------------------------------------

def cmd_search(args: argparse.Namespace) -> int:
    """Search for bond issues by borrower name with match scoring and maturity filtering."""
    from src.scraper.session import EMMAsession
    from src.scraper.borrower_search import find_issues_for_borrower

    state_note = f" (state={args.state})" if args.state else ""
    matured_note = " [including matured]" if args.include_matured else ""
    print(
        f"\nSearching EMMA for borrower: '{args.query}'"
        f"{state_note}{matured_note}"
    )
    print(f"Min confidence: {args.min_confidence:.0%}  |  Exclude matured: {not args.include_matured}")
    print("-" * 60)

    mgr = EMMAsession()
    session = mgr.get_session()

    results = find_issues_for_borrower(
        session,
        borrower_name=args.query,
        state=args.state,
        min_confidence=args.min_confidence,
        exclude_matured=not args.include_matured,
        use_cache=not args.no_cache,
    )

    if not results:
        print("No results found.")
        return 0

    for r in results:
        confidence_bar = "█" * int(r.match_confidence * 10) + "░" * (10 - int(r.match_confidence * 10))
        matured_flag = "  ⚠ MATURED?" if r.potentially_matured else ""
        print(
            f"  [{r.issue_id}]\n"
            f"     Issuer     : {r.issuer_name}\n"
            f"     Series     : {r.issue_name}\n"
            f"     State      : {r.state or 'N/A'}  |  Dated: {r.issue_date or 'N/A'}\n"
            f"     Confidence : {confidence_bar} {r.match_confidence:.0%}{matured_flag}\n"
            f"     URL        : {r.emma_url}\n"
        )

    print(f"Total: {len(results)} issue(s) found")
    return 0


def cmd_discover(args: argparse.Namespace) -> int:
    """Discover new disclosure documents for a bond issue."""
    from src.scraper.session import EMMAsession
    from src.scraper.issue_details import fetch_issue_details, fetch_cusip_to_issue
    from src.scraper.continuing_disclosure import fetch_disclosure_documents, get_latest_posted_date
    from src.scraper.document_queue import DocumentQueue

    mgr = EMMAsession()
    session = mgr.get_session()
    queue = DocumentQueue()

    # Resolve issue_id from CUSIP if needed
    issue_id = args.issue_id
    borrower_name = "Unknown Borrower"

    if args.cusip and not issue_id:
        print(f"Resolving CUSIP {args.cusip} → issue ID...")
        detail = fetch_cusip_to_issue(session, args.cusip)
        if not detail:
            print(f"ERROR: Could not resolve CUSIP {args.cusip}")
            return 1
        issue_id = detail.issue_id
        borrower_name = detail.borrower_name or detail.issuer_name
        print(f"  → Issue ID: {issue_id}  |  Borrower: {borrower_name}")

    if not issue_id:
        print("ERROR: Provide --issue-id or --cusip")
        return 1

    # Fetch issue details for borrower name
    if borrower_name == "Unknown Borrower":
        detail = fetch_issue_details(session, issue_id, use_cache=not args.no_cache)
        if detail:
            borrower_name = detail.borrower_name or detail.issuer_name

    # Parse last_seen_date if provided
    last_seen: Optional[datetime] = None
    if args.since:
        try:
            last_seen = datetime.fromisoformat(args.since)
        except ValueError:
            print(f"ERROR: Invalid --since date format. Use YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS")
            return 1

    print(f"\nFetching disclosures for issue: {issue_id}")
    print(f"Borrower: {borrower_name}")
    if last_seen:
        print(f"Only documents newer than: {last_seen.date()}")
    print("-" * 60)

    docs = fetch_disclosure_documents(
        session,
        issue_id=issue_id,
        last_seen_date=last_seen,
        use_cache=not args.no_cache,
    )

    if not docs:
        print("No new documents found.")
        return 0

    added = queue.add_batch(docs, borrower_name=borrower_name)

    for doc in docs:
        posted = doc.posted_date.strftime("%Y-%m-%d") if doc.posted_date else "N/A"
        print(
            f"  [{doc.doc_id[:12]}] {doc.doc_type}\n"
            f"     Title  : {doc.title[:70]}\n"
            f"     Posted : {posted}  |  URL: {doc.doc_url}\n"
        )

    print(f"Found {len(docs)} document(s) — {added} new added to download queue")
    _print_queue_stats(queue)
    return 0


def cmd_download(args: argparse.Namespace) -> int:
    """Process the download queue."""
    from src.scraper.session import EMMAsession
    from src.scraper.document_queue import DocumentQueue
    from src.scraper.document_fetcher import DocumentFetcher
    from src.scraper.storage import DocumentStorage

    mgr = EMMAsession()
    session = mgr.get_session()
    queue = DocumentQueue()
    storage = DocumentStorage()

    stats = queue.get_stats()
    pending = stats["pending"] + stats["retryable_failed"]

    if pending == 0:
        print("Queue is empty — nothing to download.")
        _print_queue_stats(queue)
        return 0

    workers = min(args.workers, 3)
    limit = args.limit

    print(f"\nDownloading up to {limit} documents with {workers} worker(s)...")
    print("-" * 60)

    fetcher = DocumentFetcher(session, queue, storage, workers=workers)

    if workers > 1:
        metrics = fetcher.run_threaded(max_items=limit)
    else:
        metrics = fetcher.run(max_items=limit)

    print(f"\nRun complete:")
    print(f"  Downloaded : {metrics.documents_downloaded}")
    print(f"  Failed     : {metrics.download_failures}")
    _print_queue_stats(queue)

    storage_stats = storage.get_stats()
    print(f"\nStorage: {storage_stats['total_documents']} documents / {storage_stats['total_size_mb']} MB")
    return 0


def cmd_events(args: argparse.Namespace) -> int:
    """Fetch recent material event notices."""
    from src.scraper.session import EMMAsession
    from src.scraper.event_notices import fetch_event_notices, filter_high_signal

    mgr = EMMAsession()
    session = mgr.get_session()

    print(f"\nFetching event notices (last {args.days} days)...")
    print("-" * 60)

    notices = fetch_event_notices(
        session,
        days_back=args.days,
        state=args.state,
        use_cache=not args.no_cache,
    )

    if args.high_signal_only:
        notices = filter_high_signal(notices)

    if not notices:
        print("No event notices found.")
        return 0

    for n in notices:
        posted = n.posted_date.strftime("%Y-%m-%d") if n.posted_date else "N/A"
        signal = " ⚠️  HIGH SIGNAL" if n.is_high_signal else ""
        print(
            f"  [{n.notice_id[:12]}] {n.event_type}{signal}\n"
            f"     Issuer : {n.issuer_name}\n"
            f"     Title  : {n.title[:70]}\n"
            f"     Posted : {posted}  |  State: {n.state or 'N/A'}\n"
        )

    high = sum(1 for n in notices if n.is_high_signal)
    print(f"Total: {len(notices)} notice(s) — {high} high-signal")
    return 0


def cmd_queue(args: argparse.Namespace) -> int:
    """Show download queue status."""
    from src.scraper.document_queue import DocumentQueue
    queue = DocumentQueue()
    _print_queue_stats(queue, verbose=True)
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    """Show storage and queue statistics."""
    from src.scraper.document_queue import DocumentQueue
    from src.scraper.storage import DocumentStorage

    queue = DocumentQueue()
    storage = DocumentStorage()

    print("\n=== EMMA Scraper Statistics ===\n")

    print("DOWNLOAD QUEUE")
    _print_queue_stats(queue, verbose=True)

    print("\nDOCUMENT STORAGE")
    s = storage.get_stats()
    print(f"  Total documents : {s['total_documents']}")
    print(f"  Total size      : {s['total_size_mb']} MB")
    print(f"  Storage root    : {s['base_dir']}")

    return 0


# ---------------------------------------------------------------------------
# Database commands
# ---------------------------------------------------------------------------

def cmd_initdb(args: argparse.Namespace) -> int:
    """Create all database tables. Safe to run multiple times."""
    from src.db.init_db import init_db
    from src.db.engine import DATABASE_URL

    print(f"\nInitializing database: {DATABASE_URL.split('@')[-1]}")
    try:
        init_db()
        print("Database ready.")
    except Exception as exc:
        print(f"ERROR: {exc}")
        return 1
    return 0


def cmd_borrower_add(args: argparse.Namespace) -> int:
    """Add a borrower to the watchlist database."""
    from src.db.engine import Session
    from src.db.repositories.borrower import BorrowerRepository

    with Session() as session:
        repo = BorrowerRepository(session)
        try:
            borrower, created = repo.get_or_create(
                borrower_name=args.name,
                sector=args.sector,
                state=args.state,
                city=args.city,
                fiscal_year_end=args.fye,
                watchlist_notes=args.notes,
            )
            session.commit()
        except ValueError as exc:
            print(f"ERROR: {exc}")
            return 1

        if created:
            print(f"\nAdded borrower #{borrower.borrower_id}:")
        else:
            print(f"\nBorrower already exists (#{borrower.borrower_id}):")

        _print_borrower(borrower)
    return 0


def cmd_borrower_list(args: argparse.Namespace) -> int:
    """List tracked borrowers."""
    from src.db.engine import Session
    from src.db.repositories.borrower import BorrowerRepository

    with Session() as session:
        repo = BorrowerRepository(session)

        if args.show_all:
            borrowers = repo.list_all()
            header = "All borrowers"
        else:
            borrowers = repo.list_watchlist(sector=args.sector, state=args.state)
            header = "Watchlist"

        if not borrowers:
            print("No borrowers found.")
            return 0

        filters = []
        if args.sector:
            filters.append(f"sector={args.sector}")
        if args.state:
            filters.append(f"state={args.state}")
        filter_str = f"  [{', '.join(filters)}]" if filters else ""

        print(f"\n{header}{filter_str} — {len(borrowers)} borrower(s)\n")
        print(f"  {'ID':>4}  {'Name':<40}  {'Sector':<20}  {'State':>5}  {'FYE':>5}  Status")
        print("  " + "-" * 95)

        for b in borrowers:
            score_str = f"[{b.distress_score:>3}]" if b.distress_score is not None else "     "
            print(
                f"  {b.borrower_id:>4}  {b.borrower_name:<40.40}  {(b.sector or ''):.<20}  "
                f"{(b.state or ''):>5}  {(b.fiscal_year_end or ''):>5}  "
                f"{score_str} {b.distress_status or ''}"
            )

    return 0


def cmd_borrower_show(args: argparse.Namespace) -> int:
    """Show detail for one borrower."""
    from src.db.engine import Session
    from src.db.repositories.borrower import BorrowerRepository
    from src.db.repositories.bond_issue import BondIssueRepository

    with Session() as session:
        repo = BorrowerRepository(session)
        borrower = repo.get(args.borrower_id)

        if not borrower:
            print(f"Borrower #{args.borrower_id} not found.")
            return 1

        _print_borrower(borrower)

        issue_repo = BondIssueRepository(session)
        issues = issue_repo.list_for_borrower(args.borrower_id)

        if issues:
            print(f"\n  Bond Issues ({len(issues)}):")
            for i in issues:
                print(
                    f"    [{i.emma_issue_id}]  {i.series_name or 'N/A'}"
                    f"  |  {i.state or ''}  |  dated {i.issue_date or 'N/A'}"
                )
        else:
            print("\n  No bond issues linked yet.")

    return 0


def cmd_borrower_sync(args: argparse.Namespace) -> int:
    """
    Discover bond issues and disclosure documents for a borrower and store
    them in the database.

    This is the core Phase 2 integration: Phase 1 scraping → Phase 2 DB.
    Documents are stored as URL + metadata only — no PDFs are downloaded.
    """
    from src.db.engine import Session
    from src.db.repositories.borrower import BorrowerRepository
    from src.db.repositories.bond_issue import BondIssueRepository
    from src.db.repositories.document import DocumentRepository, classify_doc_type
    from src.scraper.session import EMMAsession
    from src.scraper.borrower_search import find_issues_for_borrower
    from src.scraper.continuing_disclosure import fetch_disclosure_documents

    with Session() as session:
        borrower_repo = BorrowerRepository(session)
        borrower = borrower_repo.get(args.borrower_id)
        if not borrower:
            print(f"Borrower #{args.borrower_id} not found. Run 'borrower add' first.")
            return 1

        print(f"\nSyncing: {borrower.borrower_name} (#{borrower.borrower_id})")
        print("-" * 60)

        emma = EMMAsession()
        emma_session = emma.get_session()

        # --- Step 1: Discover bond issues via EMMA Advanced Search ---
        print(f"Searching EMMA for bond issues...")
        issues_found = find_issues_for_borrower(
            emma_session,
            borrower_name=borrower.borrower_name,
            state=borrower.state,
            min_confidence=args.min_confidence,
            exclude_matured=not args.include_matured,
        )

        if not issues_found:
            print("  No active bond issues found on EMMA.")
            return 0

        print(f"  Found {len(issues_found)} bond issue(s) on EMMA.\n")

        issue_repo = BondIssueRepository(session)
        doc_repo = DocumentRepository(session)

        total_issues_added = 0
        total_docs_added = 0
        total_docs_skipped = 0

        for emma_issue in issues_found:
            # --- Step 2: Upsert bond issue into database ---
            db_issue, issue_created = issue_repo.upsert_from_emma(
                borrower_id=borrower.borrower_id,
                emma_issue_id=emma_issue.issue_id,
                series_name=emma_issue.issue_name,
                issue_date=emma_issue.issue_date,
                state=emma_issue.state,
            )
            session.flush()   # assign db_issue.issue_id before inserting documents

            status = "NEW" if issue_created else "exists"
            print(
                f"  [{status}] {emma_issue.issue_id}  "
                f"{emma_issue.issue_name[:55]}"
            )

            if issue_created:
                total_issues_added += 1

            # --- Step 3: Fetch disclosure documents for this issue ---
            try:
                disclosure_docs = fetch_disclosure_documents(
                    emma_session,
                    issue_id=emma_issue.issue_id,
                    use_cache=not args.no_cache,
                )
            except Exception as exc:
                logger.warning(
                    "Failed to fetch disclosures for %s: %s", emma_issue.issue_id, exc
                )
                disclosure_docs = []

            # --- Step 4: Store document URLs in database ---
            issue_added = 0
            for doc in disclosure_docs:
                doc_type = classify_doc_type(doc.title, doc.doc_type)

                # Build a stable unique ID from the URL if EMMA didn't provide one
                emma_doc_id = doc.doc_id or doc.doc_url.split("/")[-1].split("?")[0]

                posted = (
                    doc.posted_date.date()
                    if doc.posted_date and hasattr(doc.posted_date, "date")
                    else doc.posted_date
                )

                _, created = doc_repo.upsert(
                    issue_id=db_issue.issue_id,
                    borrower_id=borrower.borrower_id,
                    emma_doc_id=emma_doc_id,
                    doc_type=doc_type,
                    doc_url=doc.doc_url,
                    title=doc.title,
                    doc_date=doc.doc_date,
                    posted_date=posted,
                )
                if created:
                    total_docs_added += 1
                    issue_added += 1
                else:
                    total_docs_skipped += 1

            if disclosure_docs:
                print(f"         {len(disclosure_docs)} documents found  ({issue_added} new)")

        session.commit()

    print(f"\nSync complete:")
    print(f"  Bond issues  : {total_issues_added} new / {len(issues_found) - total_issues_added} already known")
    print(f"  Documents    : {total_docs_added} new URLs stored / {total_docs_skipped} already known")
    print(f"\nNo PDFs downloaded — run 'borrower show {args.borrower_id}' to review.")
    return 0


def cmd_borrower_update(args: argparse.Namespace) -> int:
    """Update editable fields on an existing borrower record."""
    from src.db.engine import Session
    from src.db.repositories.borrower import BorrowerRepository, VALID_SECTORS, VALID_DISTRESS_STATUSES

    with Session() as session:
        repo = BorrowerRepository(session)
        borrower = repo.get(args.borrower_id)

        if not borrower:
            print(f"Borrower #{args.borrower_id} not found.")
            return 1

        changed: list[str] = []

        if args.name is not None:
            borrower.borrower_name = args.name.strip()
            changed.append("name")
        if args.sector is not None:
            if args.sector not in VALID_SECTORS:
                print(f"ERROR: Invalid sector '{args.sector}'. Valid: {sorted(VALID_SECTORS)}")
                return 1
            borrower.sector = args.sector
            changed.append("sector")
        if args.state is not None:
            borrower.state = args.state.upper()
            changed.append("state")
        if args.city is not None:
            borrower.city = args.city
            changed.append("city")
        if args.fye is not None:
            borrower.fiscal_year_end = args.fye
            changed.append("fiscal_year_end")
        if args.notes is not None:
            borrower.watchlist_notes = args.notes
            changed.append("watchlist_notes")
        if args.status is not None:
            if args.status not in VALID_DISTRESS_STATUSES:
                print(f"ERROR: Invalid status '{args.status}'. Valid: {sorted(VALID_DISTRESS_STATUSES)}")
                return 1
            borrower.distress_status = args.status
            changed.append("distress_status")

        if not changed:
            print("Nothing to update — no fields specified.")
            return 0

        session.commit()
        print(f"\nUpdated borrower #{borrower.borrower_id} ({', '.join(changed)}):")
        _print_borrower(borrower)

    return 0


def _print_borrower(b) -> None:
    """Pretty-print a single Borrower record."""
    print(f"  ID             : {b.borrower_id}")
    print(f"  Name           : {b.borrower_name}")
    print(f"  Sector         : {b.sector or 'N/A'}")
    print(f"  State          : {b.state or 'N/A'}")
    print(f"  City           : {b.city or 'N/A'}")
    print(f"  Fiscal Year End: {b.fiscal_year_end or 'N/A'}")
    print(f"  Distress Status: {b.distress_status or 'N/A'}")
    print(f"  Distress Score : {b.distress_score if b.distress_score is not None else 'N/A'}")
    print(f"  On Watchlist   : {b.on_watchlist}")
    print(f"  Watchlist Since: {b.watchlist_since or 'N/A'}")
    if b.watchlist_notes:
        print(f"  Notes          : {b.watchlist_notes}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _print_queue_stats(queue, verbose: bool = False) -> None:
    stats = queue.get_stats()
    print(f"\nQueue status:")
    print(f"  Pending    : {stats['pending']}")
    print(f"  Downloaded : {stats['downloaded']}")
    print(f"  Failed     : {stats['failed']} ({stats['retryable_failed']} retryable)")
    print(f"  Total      : {stats['total']}")
    if verbose:
        print(f"  Queue file : {stats['queue_file']}")


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="emma-scraper",
        description="EMMA Municipal Distress Monitoring — Phase 1 Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Global options
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Write logs to this file in addition to stderr",
    )
    parser.add_argument(
        "--plain-logs",
        action="store_true",
        help="Use plain text logs instead of JSON",
    )

    subparsers = parser.add_subparsers(title="commands", metavar="COMMAND")

    # --- search ---
    p_search = subparsers.add_parser(
        "search",
        help="Search for bond issues by borrower name (with match scoring and maturity filter)",
    )
    p_search.add_argument("query", help="Borrower name to search (e.g. 'Rider University')")
    p_search.add_argument("--state", default=None, help="Filter by two-letter state code")
    p_search.add_argument(
        "--min-confidence",
        type=float,
        default=0.6,
        metavar="FLOAT",
        help=(
            "Minimum match confidence 0.0–1.0 (default: 0.6). "
            "Lower = more permissive, higher = stricter. "
            "Use 1.0 to require all borrower name tokens to match exactly."
        ),
    )
    p_search.add_argument(
        "--include-matured",
        action="store_true",
        help=(
            "Include bonds that the age heuristic flags as potentially "
            "matured or fully called (excluded by default)."
        ),
    )
    p_search.add_argument("--no-cache", action="store_true", help="Bypass response cache")
    p_search.set_defaults(func=cmd_search)

    # --- discover ---
    p_discover = subparsers.add_parser(
        "discover", help="Discover new disclosure documents for a bond issue"
    )
    p_discover.add_argument("--issue-id", default=None, help="EMMA issue ID")
    p_discover.add_argument("--cusip", default=None, help="CUSIP (resolved to issue ID)")
    p_discover.add_argument(
        "--since",
        default=None,
        help="Only return documents newer than this date (YYYY-MM-DD)",
    )
    p_discover.add_argument("--no-cache", action="store_true", help="Bypass response cache")
    p_discover.set_defaults(func=cmd_discover)

    # --- download ---
    p_download = subparsers.add_parser("download", help="Process the download queue")
    p_download.add_argument(
        "--workers", type=int, default=1, help="Number of download workers (max 3)"
    )
    p_download.add_argument(
        "--limit", type=int, default=50, help="Max documents to download in this run"
    )
    p_download.set_defaults(func=cmd_download)

    # --- events ---
    p_events = subparsers.add_parser("events", help="Fetch recent material event notices")
    p_events.add_argument(
        "--days", type=int, default=7, help="How many days back to search (default: 7)"
    )
    p_events.add_argument("--state", default=None, help="Filter by state code")
    p_events.add_argument(
        "--high-signal-only",
        action="store_true",
        help="Show only high-signal distress notices",
    )
    p_events.add_argument("--no-cache", action="store_true", help="Bypass response cache")
    p_events.set_defaults(func=cmd_events)

    # --- queue ---
    p_queue = subparsers.add_parser("queue", help="Show download queue status")
    p_queue.set_defaults(func=cmd_queue)

    # --- stats ---
    p_stats = subparsers.add_parser("stats", help="Show storage and queue statistics")
    p_stats.set_defaults(func=cmd_stats)

    # --- db init ---
    p_initdb = subparsers.add_parser("initdb", help="Create database tables (run once)")
    p_initdb.set_defaults(func=cmd_initdb)

    # --- borrower ---
    p_borrower = subparsers.add_parser("borrower", help="Manage tracked borrowers")
    borrower_sub = p_borrower.add_subparsers(title="actions", metavar="ACTION")

    # borrower add
    p_badd = borrower_sub.add_parser("add", help="Add a borrower to the watchlist")
    p_badd.add_argument("name", help="Full legal borrower name")
    p_badd.add_argument(
        "--sector",
        required=True,
        choices=["higher_ed", "healthcare", "general_government", "housing",
                 "utility", "transportation", "other"],
        help="Borrower sector",
    )
    p_badd.add_argument("--state", default=None, help="Two-letter state code")
    p_badd.add_argument("--city",  default=None, help="City")
    p_badd.add_argument("--fye",   default=None,
                        metavar="MM-DD", help="Fiscal year end date (e.g. 06-30)")
    p_badd.add_argument("--notes", default=None, help="Watchlist notes")
    p_badd.set_defaults(func=cmd_borrower_add)

    # borrower list
    p_blist = borrower_sub.add_parser("list", help="List tracked borrowers")
    p_blist.add_argument("--sector", default=None, help="Filter by sector")
    p_blist.add_argument("--state",  default=None, help="Filter by state")
    p_blist.add_argument("--all",    action="store_true", dest="show_all",
                         help="Include borrowers not on watchlist")
    p_blist.set_defaults(func=cmd_borrower_list)

    # borrower show
    p_bshow = borrower_sub.add_parser("show", help="Show detail for one borrower")
    p_bshow.add_argument("borrower_id", type=int, help="Borrower ID")
    p_bshow.set_defaults(func=cmd_borrower_show)

    # borrower update
    p_bupdate = borrower_sub.add_parser("update", help="Update fields on a borrower record")
    p_bupdate.add_argument("borrower_id", type=int, help="Borrower ID")
    p_bupdate.add_argument("--name",   default=None, help="New borrower name")
    p_bupdate.add_argument("--sector", default=None, help="New sector")
    p_bupdate.add_argument("--state",  default=None, help="Two-letter state code")
    p_bupdate.add_argument("--city",   default=None, help="City")
    p_bupdate.add_argument("--fye",    default=None, metavar="MM-DD", help="Fiscal year end")
    p_bupdate.add_argument("--notes",  default=None, help="Watchlist notes (replaces existing)")
    p_bupdate.add_argument("--status", default=None,
                           choices=["monitor", "watch", "distressed", "resolved"],
                           help="Distress status")
    p_bupdate.set_defaults(func=cmd_borrower_update)

    # borrower sync
    p_bsync = borrower_sub.add_parser(
        "sync",
        help="Discover bond issues + documents on EMMA and store to database (no downloads)",
    )
    p_bsync.add_argument("borrower_id", type=int, help="Borrower ID")
    p_bsync.add_argument(
        "--min-confidence",
        type=float,
        default=0.6,
        metavar="FLOAT",
        help="Minimum name-match confidence (default: 0.6)",
    )
    p_bsync.add_argument(
        "--include-matured",
        action="store_true",
        help="Include matured/called bond issues (excluded by default)",
    )
    p_bsync.add_argument(
        "--no-cache",
        action="store_true",
        help="Bypass response cache for fresh EMMA data",
    )
    p_bsync.set_defaults(func=cmd_borrower_sync)

    return parser


if __name__ == "__main__":
    sys.exit(main())
