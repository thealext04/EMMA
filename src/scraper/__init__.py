"""
src/scraper — EMMA Phase 1 Scraping & Discovery Engine

Modules:
    models              — Dataclass models for all structured data
    session             — EMMA session manager (browser-like headers, cookie handling)
    rate_limiter        — Discovery vs download rate limits
    cache               — File-based HTTP response cache
    retry               — Exponential backoff retry logic
    storage             — Raw PDF document storage
    logger              — Structured JSON logger
    issue_search        — Issue Search API client (/api/Search/Issue)
    issue_details       — Issue Details fetcher (/IssueView/Details/{id})
    continuing_disclosure — Disclosure document list parser
    document_queue      — File-based download queue
    document_fetcher    — Queue-based PDF downloader
    event_notices       — Material Event Notice fetcher
    cli                 — Command-line interface

Quick start:
    from src.scraper.session import EMMAsession
    from src.scraper.issue_search import search_all_pages
    from src.scraper.continuing_disclosure import fetch_disclosure_documents
    from src.scraper.document_queue import DocumentQueue
    from src.scraper.document_fetcher import DocumentFetcher
    from src.scraper.storage import DocumentStorage

    mgr = EMMAsession()
    session = mgr.get_session()
    results = search_all_pages(session, search_text="Manhattan College")
"""
