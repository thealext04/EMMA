"""
models.py — SQLAlchemy ORM models for all database tables.

Hierarchy:
    borrowers
      └── bond_issues (via borrower_id)
            ├── cusips       (via issue_id)
            └── documents    (via issue_id)
                  └── extracted_metrics (via doc_id)

    events              (via borrower_id, optional doc_id)
    doc_download_queue  (via issue_id, borrower_id — nullable FKs)

Valid enum-like values are documented per column via comments.
Validation is intentionally light here — the repository layer enforces business rules.
"""

from datetime import date, datetime

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# borrowers
# ---------------------------------------------------------------------------

class Borrower(Base):
    """
    The primary credit entity.  All other tables reference back to this one.

    sector values:
        higher_ed, healthcare, general_government, housing,
        utility, transportation, other

    distress_status values:
        monitor     — normal surveillance (default)
        watch       — elevated concern
        distressed  — active distress signals present
        resolved    — distress resolved or bonds matured/defeased
    """
    __tablename__ = "borrowers"

    borrower_id     = Column(Integer, primary_key=True, autoincrement=True)
    borrower_name   = Column(Text, nullable=False)
    sector          = Column(Text)                          # higher_ed | healthcare | ...
    state           = Column(String(2))
    city            = Column(Text)
    distress_status = Column(Text, default="monitor")
    distress_score  = Column(Integer)                       # 0–100
    fiscal_year_end = Column(Text)                          # MM-DD, e.g. "06-30"
    on_watchlist    = Column(Boolean, default=True)         # all inserted borrowers are tracked
    watchlist_since = Column(Date)
    watchlist_notes = Column(Text)
    created_at      = Column(DateTime, default=func.now())
    updated_at      = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    bond_issues = relationship("BondIssue", back_populates="borrower", lazy="select")
    events      = relationship("Event",     back_populates="borrower", lazy="select")
    documents   = relationship("Document",  back_populates="borrower", lazy="select")

    def __repr__(self) -> str:
        return f"<Borrower {self.borrower_id}: {self.borrower_name!r} [{self.sector}]>"


# ---------------------------------------------------------------------------
# issuers
# ---------------------------------------------------------------------------

class Issuer(Base):
    """
    Conduit issuer — the legal entity that sells the bonds.
    Separate from the borrower (the credit risk).

    issuer_type values:
        state_authority, county, city, housing_authority, school_district, other
    """
    __tablename__ = "issuers"

    issuer_id      = Column(Integer, primary_key=True, autoincrement=True)
    issuer_name    = Column(Text, nullable=False)
    issuer_type    = Column(Text)
    state          = Column(String(2))
    emma_issuer_id = Column(Text, unique=True)              # EMMA's internal issuer ID
    created_at     = Column(DateTime, default=func.now())

    # Relationships
    bond_issues = relationship("BondIssue", back_populates="issuer", lazy="select")

    def __repr__(self) -> str:
        return f"<Issuer {self.issuer_id}: {self.issuer_name!r}>"


# ---------------------------------------------------------------------------
# bond_issues
# ---------------------------------------------------------------------------

class BondIssue(Base):
    """
    One bond series (e.g. "Series 2019A Revenue Bonds").
    A borrower can have many bond issues across many conduit issuers.

    bond_type values:    revenue, go (general obligation), conduit, special_assessment, other
    tax_status values:   tax_exempt, taxable, amt
    """
    __tablename__ = "bond_issues"

    issue_id                  = Column(Integer, primary_key=True, autoincrement=True)
    borrower_id               = Column(Integer, ForeignKey("borrowers.borrower_id"), nullable=False)
    issuer_id                 = Column(Integer, ForeignKey("issuers.issuer_id"))
    emma_issue_id             = Column(Text, unique=True)   # EMMA's internal issue ID
    series_name               = Column(Text)                # e.g. "Series 2019A"
    par_amount                = Column(Numeric(18, 2))
    issue_date                = Column(Date)
    sale_date                 = Column(Date)
    bond_type                 = Column(Text)
    tax_status                = Column(Text)
    state                     = Column(String(2))
    continuing_disclosure_url = Column(Text)
    last_disclosure_check     = Column(DateTime)
    last_seen_doc_date        = Column(Date)                # incremental update cursor
    created_at                = Column(DateTime, default=func.now())
    updated_at                = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    borrower  = relationship("Borrower",  back_populates="bond_issues")
    issuer    = relationship("Issuer",    back_populates="bond_issues")
    cusips    = relationship("Cusip",     back_populates="bond_issue", lazy="select")
    documents = relationship("Document",  back_populates="bond_issue", lazy="select")

    def __repr__(self) -> str:
        return f"<BondIssue {self.issue_id}: {self.emma_issue_id!r} — {self.series_name!r}>"


# ---------------------------------------------------------------------------
# cusips
# ---------------------------------------------------------------------------

class Cusip(Base):
    """
    One CUSIP = one maturity date within a bond issue.
    A bond issue typically has 20–30 CUSIPs (one per annual maturity).
    """
    __tablename__ = "cusips"

    cusip_id         = Column(Integer, primary_key=True, autoincrement=True)
    cusip            = Column(String(9), unique=True, nullable=False)
    issue_id         = Column(Integer, ForeignKey("bond_issues.issue_id"))
    maturity_date    = Column(Date)
    coupon_rate      = Column(Numeric(6, 4))                # e.g. 0.0450 = 4.50%
    par_amount       = Column(Numeric(18, 2))
    rating_sp        = Column(Text)
    rating_moodys    = Column(Text)
    rating_fitch     = Column(Text)
    callable         = Column(Boolean)
    call_date        = Column(Date)
    emma_security_id = Column(Text)
    created_at       = Column(DateTime, default=func.now())
    updated_at       = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    bond_issue = relationship("BondIssue", back_populates="cusips")

    def __repr__(self) -> str:
        return f"<Cusip {self.cusip} maturity={self.maturity_date}>"


# ---------------------------------------------------------------------------
# documents
# ---------------------------------------------------------------------------

class Document(Base):
    """
    Every disclosure document filed on EMMA for a tracked bond issue.

    doc_type values:
        financial_statement — audited annual financials
        event_notice        — material event notice
        operating_report    — management / operating report
        budget              — annual budget filing
        rating_notice       — rating change notification
        bond_issuance       — official statement or POS
        other               — unclassified

    extraction_status values:
        pending, extracted, failed, skipped
    """
    __tablename__ = "documents"

    doc_id            = Column(Integer, primary_key=True, autoincrement=True)
    issue_id          = Column(Integer, ForeignKey("bond_issues.issue_id"))
    borrower_id       = Column(Integer, ForeignKey("borrowers.borrower_id"))
    emma_doc_id       = Column(Text, unique=True)
    doc_type          = Column(Text, nullable=False)
    title             = Column(Text)                        # document title from EMMA
    doc_date          = Column(Date)                        # date of the document
    posted_date       = Column(Date)                        # date posted to EMMA
    fiscal_year       = Column(Integer)                     # fiscal year covered
    doc_url           = Column(Text)
    local_path        = Column(Text)
    file_size_bytes   = Column(Integer)
    page_count        = Column(Integer)
    extraction_status = Column(Text, default="pending")
    extracted_at      = Column(DateTime)
    created_at        = Column(DateTime, default=func.now())

    # Relationships
    bond_issue         = relationship("BondIssue",        back_populates="documents")
    borrower           = relationship("Borrower",         back_populates="documents")
    events             = relationship("Event",            back_populates="document",  lazy="select")
    extracted_metrics  = relationship("ExtractedMetrics", back_populates="document",  lazy="select")

    def __repr__(self) -> str:
        return f"<Document {self.doc_id}: {self.doc_type!r} posted={self.posted_date}>"


# ---------------------------------------------------------------------------
# events
# ---------------------------------------------------------------------------

class Event(Base):
    """
    Distress signals and notable events at the borrower level.
    Drives event timelines and distress scoring.

    event_type values:
        late_filing, going_concern, covenant_violation, covenant_waiver,
        rating_downgrade, rating_upgrade, rating_withdrawal,
        payment_default, forbearance, debt_restructuring, bankruptcy,
        liquidity_facility_termination, financial_statement_filed, dscr_breach

    severity values:
        low, medium, high, critical
    """
    __tablename__ = "events"

    event_id      = Column(Integer, primary_key=True, autoincrement=True)
    borrower_id   = Column(Integer, ForeignKey("borrowers.borrower_id"), nullable=False)
    doc_id        = Column(Integer, ForeignKey("documents.doc_id"))     # source doc, if any
    event_type    = Column(Text, nullable=False)
    event_date    = Column(Date, nullable=False)
    detected_date = Column(Date, default=date.today)
    severity      = Column(Text)
    summary       = Column(Text)
    raw_text      = Column(Text)                            # extracted passage from doc
    confirmed     = Column(Boolean, default=False)
    created_at    = Column(DateTime, default=func.now())

    # Relationships
    borrower = relationship("Borrower", back_populates="events")
    document = relationship("Document", back_populates="events")

    def __repr__(self) -> str:
        return f"<Event {self.event_id}: {self.event_type!r} [{self.severity}] {self.event_date}>"


# ---------------------------------------------------------------------------
# extracted_metrics
# ---------------------------------------------------------------------------

class ExtractedMetrics(Base):
    """
    Structured financial data extracted from documents by AI (Phase 4).
    One row per document per fiscal year.

    extraction_confidence values: high, medium, low
    """
    __tablename__ = "extracted_metrics"

    metric_id       = Column(Integer, primary_key=True, autoincrement=True)
    doc_id          = Column(Integer, ForeignKey("documents.doc_id"))
    borrower_id     = Column(Integer, ForeignKey("borrowers.borrower_id"))
    fiscal_year     = Column(Integer)
    period_end_date = Column(Date)

    # Income / Revenue
    total_revenue           = Column(Numeric(18, 2))
    operating_revenue       = Column(Numeric(18, 2))
    net_income              = Column(Numeric(18, 2))
    operating_income        = Column(Numeric(18, 2))
    ebitda                  = Column(Numeric(18, 2))

    # Liquidity
    days_cash_on_hand       = Column(Numeric(8, 2))
    cash_and_investments    = Column(Numeric(18, 2))
    unrestricted_net_assets = Column(Numeric(18, 2))

    # Debt
    total_long_term_debt    = Column(Numeric(18, 2))
    annual_debt_service     = Column(Numeric(18, 2))
    dscr                    = Column(Numeric(8, 4))

    # Higher Ed Specific
    total_enrollment        = Column(Integer)
    fte_enrollment          = Column(Numeric(10, 2))
    tuition_revenue         = Column(Numeric(18, 2))
    tuition_discount_rate   = Column(Numeric(6, 4))
    endowment_value         = Column(Numeric(18, 2))

    # Healthcare Specific
    licensed_beds           = Column(Integer)
    staffed_beds            = Column(Integer)
    patient_admissions      = Column(Integer)
    patient_days            = Column(Integer)
    net_patient_revenue     = Column(Numeric(18, 2))
    days_ar                 = Column(Numeric(8, 2))

    # Extraction metadata
    extraction_model        = Column(Text)
    extraction_confidence   = Column(Text)
    extracted_at            = Column(DateTime, default=func.now())

    # Relationships
    document = relationship("Document", back_populates="extracted_metrics")

    def __repr__(self) -> str:
        return (
            f"<ExtractedMetrics {self.metric_id}: borrower={self.borrower_id} "
            f"FY={self.fiscal_year}>"
        )


# ---------------------------------------------------------------------------
# doc_download_queue
# ---------------------------------------------------------------------------

class DocDownloadQueue(Base):
    """
    Durable download queue stored in the database (replaces the Phase 1 JSON queue).
    Survives process restarts. Idempotent on doc_url.

    status values: pending, downloading, downloaded, failed
    priority:      1 (highest) → 10 (lowest)
    """
    __tablename__ = "doc_download_queue"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    doc_url       = Column(Text, unique=True, nullable=False)
    issue_id      = Column(Integer)                         # nullable — may not be resolved yet
    borrower_id   = Column(Integer)
    doc_type_hint = Column(Text)
    discovered_at = Column(DateTime, default=func.now())
    status        = Column(Text, default="pending")
    attempts      = Column(Integer, default=0)
    last_attempt  = Column(DateTime)
    last_error    = Column(Text)
    priority      = Column(Integer, default=5)
    downloaded_at = Column(DateTime)
    local_path    = Column(Text)

    def __repr__(self) -> str:
        return f"<DocDownloadQueue {self.id}: {self.status!r} {self.doc_url[:60]}>"


# ---------------------------------------------------------------------------
# Indexes (defined here so create_all() picks them up automatically)
# ---------------------------------------------------------------------------

Index("idx_documents_borrower_date",  Document.borrower_id, Document.posted_date)
Index("idx_documents_type_date",      Document.doc_type,    Document.posted_date)
Index("idx_events_borrower_date",     Event.borrower_id,    Event.event_date)
Index("idx_events_type",              Event.event_type)
Index("idx_cusips_issue",             Cusip.issue_id)
Index("idx_bond_issues_borrower",     BondIssue.borrower_id)
Index("idx_queue_status_priority",    DocDownloadQueue.status, DocDownloadQueue.priority)
Index("idx_metrics_borrower_year",    ExtractedMetrics.borrower_id, ExtractedMetrics.fiscal_year)
Index("idx_borrowers_watchlist",      Borrower.on_watchlist, Borrower.distress_score)
Index("idx_borrowers_sector_state",   Borrower.sector, Borrower.state)
