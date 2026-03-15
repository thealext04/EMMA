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
    """Search for bond issues by issuer/borrower name."""
    from src.scraper.session import EMMAsession
    from src.scraper.issue_search import search_all_pages

    print(f"\nSearching EMMA for: '{args.query}'" + (f" (state={args.state})" if args.state else ""))
    print("-" * 60)

    mgr = EMMAsession()
    session = mgr.get_session()

    results = search_all_pages(
        session,
        search_text=args.query,
        state=args.state,
        use_cache=not args.no_cache,
    )

    if not results:
        print("No results found.")
        return 0

    for r in results:
        par = f"${r.par_amount:,.0f}" if r.par_amount else "N/A"
        print(
            f"  [{r.issue_id}] {r.issuer_name}\n"
            f"     Series : {r.issue_name}\n"
            f"     State  : {r.state or 'N/A'}  |  Type: {r.bond_type or 'N/A'}  |  Par: {par}\n"
            f"     URL    : {r.emma_url}\n"
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
    p_search = subparsers.add_parser("search", help="Search for bond issues by name")
    p_search.add_argument("query", help="Issuer or borrower name to search")
    p_search.add_argument("--state", default=None, help="Filter by two-letter state code")
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

    return parser


if __name__ == "__main__":
    sys.exit(main())
