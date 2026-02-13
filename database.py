"""
SQLAlchemy models and init.
Database is the glue — agents read/write here, never talk to each other.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import JSON, Boolean, CheckConstraint, Column, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint, create_engine
from sqlalchemy.orm import DeclarativeBase, relationship, sessionmaker

from config import DATABASE_URL


class Base(DeclarativeBase):
    pass


class Client(Base):
    """Client profile — from onboarding. Single source of truth."""

    __tablename__ = "clients"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), unique=True, nullable=False, index=True)
    business_name = Column(String(255), nullable=False)
    website_url = Column(Text)
    google_business_profile_url = Column(String(500))
    phone_number = Column(String(50))
    services_offered = Column(JSON, default=list)  # list[str]
    cities_served = Column(JSON, default=list)
    zip_codes_served = Column(JSON, default=list)
    ideal_customer_types = Column(JSON, default=list)
    brand_tone = Column(String(50), default="friendly")
    differentiators = Column(JSON, default=list)
    client_vertical = Column(String(50), default="junk_removal")  # junk_removal | plumbing | hvac | ...
    avg_page_quality_score = Column(Float)  # calculated: avg of client's page quality scores
    seasonality_notes = Column(Text)
    asset_links = Column(JSON, default=dict)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ResearchLog(Base):
    """
    Raw research entries from Researcher agent.
    Human can review/override before Strategist runs.
    """

    __tablename__ = "research_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), nullable=False, index=True)
    competitor_name = Column(String(255), nullable=False)
    source_type = Column(String(50), nullable=False)  # 'website' | 'reviews'
    raw_text = Column(Text, nullable=False)
    extracted_services = Column(JSON, default=list)  # list[str]
    pricing_mentions = Column(JSON, default=list)  # list[str]
    complaints = Column(JSON, default=list)  # list[str]
    missed_opportunities = Column(JSON, default=list)  # list[str]
    extracted_profile = Column(JSON)  # Full JSON from Ollama extraction (trust_signals, content_signals, etc.)
    website_quality_score = Column(Integer)  # 0-100, from WebsiteQualityScorer (primary page or avg)
    competitor_comparison_score = Column(Float)  # avg page_quality_score across all sampled pages (0-100)
    confidence_score = Column(Integer, default=0)  # 0-100
    city = Column(String(100))  # research context
    created_at = Column(DateTime, default=datetime.utcnow)


class MarketSnapshot(Base):
    """
    Aggregated research — built from research_logs after human review.
    Strategist reads this; never ResearchLog directly unless configured.
    """

    __tablename__ = "market_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), nullable=False, index=True)
    snapshot_id = Column(String(100), unique=True, nullable=False, index=True)
    city = Column(String(100), nullable=False)
    primary_service = Column(String(100), default="junk removal")
    secondary_services = Column(JSON, default=list)
    queries_executed = Column(JSON, default=list)
    result_sets = Column(JSON, default=list)
    strong_competitors = Column(JSON, default=list)
    weak_competitors = Column(JSON, default=list)
    common_messaging_themes = Column(JSON, default=list)
    content_gaps = Column(JSON, default=list)
    snapshot_date = Column(String(20))
    created_at = Column(DateTime, default=datetime.utcnow)


class Opportunity(Base):
    """
    Money page — actionable opportunities per client.
    Status: OPEN (available) or USED (acted on).
    """

    __tablename__ = "opportunities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), nullable=False, index=True)
    service = Column(String(255), nullable=False)
    geo = Column(String(100), nullable=False)
    opportunity_score = Column(Integer, default=0)
    reason = Column(Text)
    why_recommended = Column(JSON)  # {"confidence":"...","geo":"...","competition":"...","novelty":"...","timing":"..."}
    roi_projection = Column(JSON)  # {"monthly_searches":N,"estimated_leads":{...},"estimated_revenue":{...},"assumptions":[...]}
    seasonality = Column(JSON)  # {"current_season":"spring","match":true,"boost_applied":0.15}
    competition_level = Column(String(50))  # low | medium | high
    recommended_action = Column(Text)
    status = Column(String(20), default="OPEN")  # OPEN | USED
    created_at = Column(DateTime, default=datetime.utcnow)


class OpportunityScore(Base):
    """Topic scoring — from Strategist agent."""

    __tablename__ = "opportunity_scores"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), nullable=False, index=True)
    snapshot_id = Column(String(100), nullable=False, index=True)
    current_season = Column(String(50), nullable=False)
    result_id = Column(String(150), unique=True, index=True)
    tier_1_topics = Column(JSON, default=list)
    tier_2_topics = Column(JSON, default=list)
    tier_3_topics = Column(JSON, default=list)
    excluded_topics = Column(JSON, default=list)
    created_at = Column(DateTime, default=datetime.utcnow)


class KeywordIntelligence(Base):
    """
    Keyword intelligence store — extracted from competitor sites, blogs, GBP.
    Schema: id, keyword, keyword_type (seo|geo|service_city), source_url, company_name,
    city, state, frequency, confidence_score, first_seen, last_seen.
    """

    __tablename__ = "keyword_intelligence"
    __table_args__ = (
        UniqueConstraint("keyword", "region", name="uq_keyword_region"),
        CheckConstraint(
            "keyword_type IS NULL OR keyword_type IN ('seo','geo','service_city','service','service_geo','modifier','long_tail','brand')",
            name="ck_keyword_type",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    keyword = Column(Text, nullable=False, index=True)
    keyword_type = Column(String(50))  # seo | geo | service_city (or legacy: service | service_geo | ...)
    source_url = Column(Text)
    company_name = Column(String(255))
    city = Column(String(100))
    state = Column(String(50))
    frequency = Column(Integer, default=1)
    confidence_score = Column(Float, default=0.5)  # 0.0–1.0 for new rows; legacy uses 0–100 int
    keyword_confidence_score = Column(Float, default=0.5)  # 0.0–1.0 canonical from weighted formula
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    # Supporting columns for weighted confidence (2.1)
    avg_source_quality = Column(Float, default=0.0)
    top_competitor_count = Column(Integer, default=0)
    keyword_type_weight = Column(Float, default=0.5)
    last_confidence_update = Column(DateTime)
    in_title_h1_count = Column(Integer, default=0)  # Sources where keyword appeared in title/H1
    # Legacy columns (kept for backward compatibility with store_keywords, opportunity_scorer, etc.)
    geo_phrase = Column(String(100))
    region = Column(String(100), index=True)  # required for uq_keyword_region; use city+state for new schema
    source = Column(String(50))
    client_id = Column(String(100), ForeignKey("clients.client_id"), index=True)


class KeywordIntel(Base):
    """
    Keyword intelligence — simplified schema.
    keyword, confidence_score, intent, city, service, source (competitor|client|generated).
    """

    __tablename__ = "keyword_intel"
    __table_args__ = (
        CheckConstraint(
            "source IN ('competitor','client','generated')",
            name="ck_keyword_intel_source",
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    keyword = Column(Text, nullable=False, index=True)
    confidence_score = Column(Integer, default=0)
    intent = Column(Text)
    city = Column(Text)
    service = Column(Text)
    source = Column(String(50), nullable=False)  # competitor | client | generated
    created_at = Column(DateTime, default=datetime.utcnow)


class ContentStrategy(Base):
    """Action plan — clear instructions the client can act on. strategy_type: action | page."""

    __tablename__ = "content_strategies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), nullable=False, index=True)
    topic = Column(String(255), nullable=False)
    recommended_actions = Column(JSON, default=list)  # list[str]
    priority_score = Column(Integer, default=0)
    strategy_type = Column(String(20), default="action")  # action | page
    created_at = Column(DateTime, default=datetime.utcnow)


class StrategistUpsellFlag(Base):
    """Upsell flags from Strategist — e.g. quality gap, missing geo coverage."""

    __tablename__ = "strategist_upsell_flags"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), nullable=False, index=True)
    flag = Column(String(100), nullable=False)  # quality_gap | missing_geo | competitor_ahead | etc.
    reason = Column(Text)
    priority = Column(Integer, default=0)  # 1–5, higher = more urgent
    created_at = Column(DateTime, default=datetime.utcnow)


class ContentDraft(Base):
    """Drafted content — from Strategist agent."""

    __tablename__ = "content_drafts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), nullable=False, index=True)
    topic = Column(String(255), nullable=False)
    content_type = Column(String(50), nullable=False)  # social, google_business, blog
    platform = Column(String(50), nullable=False)
    title = Column(String(500))
    body = Column(Text, nullable=False)
    body_refined = Column(Text)  # After differentiation
    word_count = Column(Integer, default=0)
    change_notes = Column(JSON, default=list)
    extracted_keywords = Column(JSON, default=list)  # list[str] — primary keywords from Strategist
    extracted_geo_phrases = Column(JSON, default=list)  # list[str] — city + service from Strategist
    status = Column(String(30), default="draft")  # draft, approved, published
    created_at = Column(DateTime, default=datetime.utcnow)


class ContentLog(Base):
    """Published content log — for Publishing agent (later)."""

    __tablename__ = "content_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), nullable=False, index=True)
    platform = Column(String(50), nullable=False)
    topic = Column(String(255), nullable=False)
    published_date = Column(String(20), nullable=False)
    content_type = Column(String(50))
    url = Column(String(500))
    created_at = Column(DateTime, default=datetime.utcnow)


class GeoPhraseIntelligence(Base):
    """
    Geo phrase intelligence — normalized service+location phrases.
    Tracks frequency, source quality, and confidence per phrase.
    """

    __tablename__ = "geo_phrase_intelligence"

    id = Column(Integer, primary_key=True, autoincrement=True)
    phrase = Column(Text, nullable=False, index=True)
    service = Column(Text)
    city = Column(Text)
    state = Column(Text)
    frequency = Column(Integer, default=1)
    avg_source_quality = Column(Float, default=0)
    confidence_score = Column(Float, default=0.5)
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class GeoPhrase(Base):
    """
    Geo phrases — service+location combinations with confidence.
    source_urls: list of URLs where this phrase was observed.
    """

    __tablename__ = "geo_phrases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(Text)
    state = Column(Text)
    service = Column(Text)
    geo_phrase = Column(Text)
    confidence_score = Column(Float, default=0.5)
    source_urls = Column(JSON, default=list)  # list[str] — JSONB in Postgres
    created_at = Column(DateTime, default=datetime.utcnow)


class GeoPageOutline(Base):
    """
    Geo page outlines — recommended page structure for service+location landing pages.
    section_outline: list of section titles/content plan. internal_links: suggested anchor targets.
    client_id: optional, scopes outline to a client. competitor_comparison_score: how this page compares to competitors.
    page_status: DRAFT | PUBLISHED | etc.
    """

    __tablename__ = "geo_page_outlines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), index=True)
    city = Column(Text)
    state = Column(Text)
    service = Column(Text)
    geo_phrase = Column(Text)
    page_title = Column(Text)
    meta_description = Column(Text)
    h1 = Column(Text)
    section_outline = Column(JSON, default=list)  # list[dict] — JSONB in Postgres
    generated_sections = Column(JSON, default=list)  # list[dict] — full {heading, body} from full-page generator
    internal_links = Column(JSON, default=list)  # list[str] — JSONB in Postgres
    confidence_score = Column(Float, default=0.5)
    competitor_comparison_score = Column(Float)
    page_status = Column(Text, default="DRAFT")
    created_at = Column(DateTime, default=datetime.utcnow)


class CompetitorWebsite(Base):
    """
    One row per competitor website (by domain per client).
    site_score = average(page_scores) from CompetitorPageScore.
    """

    __tablename__ = "competitor_websites"
    __table_args__ = (UniqueConstraint("client_id", "domain", name="uq_competitor_website_domain"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), nullable=False, index=True)
    domain = Column(Text, nullable=False, index=True)
    competitor_name = Column(Text)
    base_url = Column(Text)
    site_score = Column(Float)  # avg(page_scores) — computed from CompetitorPageScore
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CompetitorPageScore(Base):
    """Page-level scores — stored internally. site_score = AVG(page_score) per competitor."""

    __tablename__ = "competitor_page_scores"
    __table_args__ = (UniqueConstraint("competitor_website_id", "page_url", name="uq_page_score"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    competitor_website_id = Column(Integer, ForeignKey("competitor_websites.id"), nullable=False, index=True)
    page_url = Column(Text, nullable=False)
    page_score = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class CompetitorGeoCoverage(Base):
    """
    Competitor geo coverage — per-competitor tracking of service+location pages.
    ranking_position: organic rank or null. page_exists: whether a dedicated page exists.
    page_quality_score: 0–1 or 0–100 quality score.
    page_url, page_title, page_h1: extracted when page exists.
    """

    __tablename__ = "competitor_geo_coverage"

    id = Column(Integer, primary_key=True, autoincrement=True)
    competitor_name = Column(Text)
    website = Column(Text)
    city = Column(Text)
    state = Column(Text)
    service = Column(Text)
    ranking_position = Column(Integer)
    page_exists = Column(Boolean, default=False)
    page_quality_score = Column(Float)
    page_url = Column(Text)
    page_title = Column(Text)
    page_h1 = Column(Text)


class GeoCoverageDensity(Base):
    """
    Aggregated competitor geo coverage — City × Service density and avg quality.
    competitor_count: # competitors with page_exists for this (city, state, service).
    avg_quality_score: average page_quality_score of those pages (0–100).
    """

    __tablename__ = "geo_coverage_density"
    __table_args__ = (UniqueConstraint("city", "state", "service", name="uq_geo_coverage_density"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(Text, nullable=False, index=True)
    state = Column(Text, index=True)
    service = Column(Text, nullable=False, index=True)
    competitor_count = Column(Integer, default=0)  # with page_exists
    avg_quality_score = Column(Float)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class BacklinkOpportunity(Base):
    """
    Backlink opportunities — potential link sources (directories, local sites, etc.).
    linked_competitors: list of competitor names/domains that have links from this source.
    Scoped by client_id: competitors are from this client's research.
    """

    __tablename__ = "backlink_opportunities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), index=True)
    domain = Column(Text)
    source_type = Column(Text)  # directory | local_site | resource | etc.
    city = Column(Text)
    state = Column(Text)
    linked_competitors = Column(JSON, default=list)  # list[str] — JSONB in Postgres
    confidence_score = Column(Float)


class ClientRoadmap(Base):
    """
    Client roadmap — prioritized tasks for the client (content, technical, backlinks, etc.).
    priority: lower = higher priority (1 = top).
    task_type: content | technical | backlink | geo_page | etc.
    plan_period: 30 | 60 | 90 (days). status: PENDING | COMPLETED. is_locked: must-do vs optional.
    """

    __tablename__ = "client_roadmap"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), index=True)
    priority = Column(Integer)
    task_type = Column(Text)
    title = Column(Text)
    description = Column(Text)
    expected_impact = Column(Text)
    confidence_score = Column(Float)
    plan_period = Column(Integer)  # 30 | 60 | 90 days
    status = Column(String(20), default="PENDING")  # PENDING | COMPLETED
    is_locked = Column(Boolean, default=True)  # True = must-do, False = optional


class SalesProposal(Base):
    """
    Sales proposals — generated from opportunities, with summary, impact, and document.
    status: DRAFT | SENT | ACCEPTED | DECLINED
    """

    __tablename__ = "sales_proposals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), nullable=False, index=True)
    proposal_date = Column(DateTime, default=datetime.utcnow)
    summary = Column(Text)
    opportunity_list = Column(JSON, default=list)  # JSONB in Postgres
    estimated_impact = Column(JSON, default=dict)  # JSONB in Postgres
    generated_document = Column(Text)
    status = Column(Text, default="DRAFT")


class ClientPortalToken(Base):
    """
    Magic-link tokens for client portal access.
    token: URL-safe secret. expires_at: when token becomes invalid.
    """

    __tablename__ = "client_portal_tokens"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), nullable=False, index=True)
    token = Column(String(128), nullable=False, unique=True, index=True)
    expires_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class PdfExport(Base):
    """
    PDF exports — tracks exported files for content or proposals.
    export_type: CONTENT | PROPOSAL
    status: READY | FAILED
    """

    __tablename__ = "pdf_exports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), nullable=False, index=True)
    export_type = Column(Text, nullable=False)  # 'CONTENT' | 'PROPOSAL'
    pdf_file_path = Column(Text)
    status = Column(Text, nullable=False, default="READY")  # 'READY' | 'FAILED'
    created_at = Column(DateTime, default=datetime.utcnow)


class WebsiteGapProposalOutcome(Base):
    """
    Logged website gap proposal outcomes — for correlating gap types → deal size,
    gap severity → close rate. Feeds future proposal improvements.
    outcome: pending | accepted | won | lost
    deal_size: actual $ when outcome=won
    """

    __tablename__ = "website_gap_proposal_outcomes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), nullable=False, index=True)
    gap_types = Column(JSON, nullable=False)  # ["technical_seo", "geo_coverage", ...]
    gap_severities = Column(JSON, nullable=False)  # {"technical_seo": "major", "geo_coverage": "critical"}
    proposed_total_low = Column(Float)   # from investment range
    proposed_total_high = Column(Float)
    outcome = Column(String(30), nullable=False, default="pending")  # pending | accepted | won | lost
    deal_size = Column(Float)   # actual $ when won
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Website(Base):
    """
    One row per domain — client or competitor.
    Links to Client (client_id) for client sites, or ResearchLog (research_log_id) for competitors.
    Stores extracted_profile and quality for gap analysis.
    """

    __tablename__ = "websites"

    id = Column(Integer, primary_key=True, autoincrement=True)
    domain = Column(Text, nullable=False, index=True)
    base_url = Column(Text)
    extracted_profile = Column(JSON)  # Full profile from Ollama (trust_signals, content_signals, etc.)
    quality_score = Column(Float)  # 0–100
    client_id = Column(String(100), ForeignKey("clients.client_id"), index=True)  # If client site
    research_log_id = Column(Integer, ForeignKey("research_logs.id"), index=True)  # If competitor
    created_at = Column(DateTime, default=datetime.utcnow)


class ContentPerformance(Base):
    """Performance metrics for content drafts — impressions, clicks, calls, direction requests."""

    __tablename__ = "content_performance"

    id = Column(Integer, primary_key=True, autoincrement=True)
    content_id = Column(Integer, ForeignKey("content_drafts.id"), nullable=False, index=True)
    impressions = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    calls = Column(Integer, default=0)
    direction_requests = Column(Integer, default=0)
    recorded_at = Column(DateTime, default=datetime.utcnow)


class KeywordPerformance(Base):
    """Performance metrics for keywords — impressions, clicks, calls, direction_requests, confidence."""

    __tablename__ = "keyword_performance"

    id = Column(Integer, primary_key=True, autoincrement=True)
    keyword = Column(Text, nullable=False, index=True)
    geo_phrase = Column(Text, nullable=True)
    impressions = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    calls = Column(Integer, default=0)
    direction_requests = Column(Integer, default=0)
    confidence_score = Column(Float)
    confidence_declining = Column(Integer, default=0)  # 1 = flagged (decayed due to no new data 30+ days)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# Engine and session
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db():
    """Create all tables. Migrate existing tables if needed."""
    Base.metadata.create_all(bind=engine)
    # Add confidence_score if missing (migration for existing keyword_intelligence)
    from sqlalchemy import text
    with engine.connect() as conn:
        try:
            r = conn.execute(text(
                "SELECT COUNT(*) FROM pragma_table_info('keyword_intelligence') WHERE name='confidence_score'"
            )).scalar()
            if r == 0:
                conn.execute(text(
                    "ALTER TABLE keyword_intelligence ADD COLUMN confidence_score INTEGER DEFAULT 0"
                ))
                conn.commit()
        except Exception:
            conn.rollback()
        try:
            r = conn.execute(text(
                "SELECT COUNT(*) FROM pragma_table_info('keyword_intelligence') WHERE name='geo_phrase'"
            )).scalar()
            if r == 0:
                conn.execute(text(
                    "ALTER TABLE keyword_intelligence ADD COLUMN geo_phrase VARCHAR(100)"
                ))
                conn.commit()
        except Exception:
            conn.rollback()
        try:
            r = conn.execute(text(
                "SELECT COUNT(*) FROM pragma_table_info('opportunities') WHERE name='why_recommended'"
            )).scalar()
            if r == 0:
                conn.execute(text(
                    "ALTER TABLE opportunities ADD COLUMN why_recommended TEXT"
                ))
                conn.commit()
        except Exception:
            conn.rollback()
        try:
            r = conn.execute(text(
                "SELECT COUNT(*) FROM pragma_table_info('opportunities') WHERE name='roi_projection'"
            )).scalar()
            if r == 0:
                conn.execute(text(
                    "ALTER TABLE opportunities ADD COLUMN roi_projection TEXT"
                ))
                conn.commit()
        except Exception:
            conn.rollback()
        try:
            r = conn.execute(text(
                "SELECT COUNT(*) FROM pragma_table_info('opportunities') WHERE name='seasonality'"
            )).scalar()
            if r == 0:
                conn.execute(text(
                    "ALTER TABLE opportunities ADD COLUMN seasonality TEXT"
                ))
                conn.commit()
        except Exception:
            conn.rollback()
        try:
            r = conn.execute(text(
                "SELECT COUNT(*) FROM pragma_table_info('clients') WHERE name='client_vertical'"
            )).scalar()
            if r == 0:
                conn.execute(text(
                    "ALTER TABLE clients ADD COLUMN client_vertical VARCHAR(50) DEFAULT 'junk_removal'"
                ))
                conn.commit()
        except Exception:
            conn.rollback()
        for col, sql_type in [
            ("website_url", "TEXT"),
            ("avg_page_quality_score", "REAL"),
        ]:
            try:
                r = conn.execute(text(
                    f"SELECT COUNT(*) FROM pragma_table_info('clients') WHERE name='{col}'"
                )).scalar()
                if r == 0:
                    conn.execute(text(
                        f"ALTER TABLE clients ADD COLUMN {col} {sql_type}"
                    ))
                    conn.commit()
            except Exception:
                conn.rollback()
        try:
            r = conn.execute(text(
                "SELECT COUNT(*) FROM pragma_table_info('research_logs') WHERE name='extracted_profile'"
            )).scalar()
            if r == 0:
                conn.execute(text(
                    "ALTER TABLE research_logs ADD COLUMN extracted_profile TEXT"
                ))
                conn.commit()
        except Exception:
            conn.rollback()
        try:
            r = conn.execute(text(
                "SELECT COUNT(*) FROM pragma_table_info('research_logs') WHERE name='website_quality_score'"
            )).scalar()
            if r == 0:
                conn.execute(text(
                    "ALTER TABLE research_logs ADD COLUMN website_quality_score INTEGER"
                ))
                conn.commit()
        except Exception:
            conn.rollback()
        try:
            r = conn.execute(text(
                "SELECT COUNT(*) FROM pragma_table_info('research_logs') WHERE name='competitor_comparison_score'"
            )).scalar()
            if r == 0:
                conn.execute(text(
                    "ALTER TABLE research_logs ADD COLUMN competitor_comparison_score REAL"
                ))
                conn.commit()
        except Exception:
            conn.rollback()
        try:
            r = conn.execute(text(
                "SELECT COUNT(*) FROM pragma_table_info('content_strategies') WHERE name='strategy_type'"
            )).scalar()
            if r == 0:
                conn.execute(text(
                    "ALTER TABLE content_strategies ADD COLUMN strategy_type VARCHAR(20) DEFAULT 'action'"
                ))
                conn.commit()
        except Exception:
            conn.rollback()
        for col, sql_type in [
            ("source_url", "TEXT"),
            ("company_name", "VARCHAR(255)"),
            ("city", "VARCHAR(100)"),
            ("state", "VARCHAR(50)"),
            ("avg_source_quality", "REAL DEFAULT 0"),
            ("top_competitor_count", "INTEGER DEFAULT 0"),
            ("keyword_type_weight", "REAL DEFAULT 0.5"),
            ("last_confidence_update", "TIMESTAMP"),
            ("keyword_confidence_score", "REAL DEFAULT 0.5"),
            ("in_title_h1_count", "INTEGER DEFAULT 0"),
        ]:
            try:
                r = conn.execute(text(
                    f"SELECT COUNT(*) FROM pragma_table_info('keyword_intelligence') WHERE name='{col}'"
                )).scalar()
                if r == 0:
                    conn.execute(text(
                        f"ALTER TABLE keyword_intelligence ADD COLUMN {col} {sql_type}"
                    ))
                    conn.commit()
            except Exception:
                conn.rollback()
        # competitor_geo_coverage: add page_url, page_title, page_h1 if missing
        try:
            tables = conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='competitor_geo_coverage'"
            )).fetchall()
            if tables:
                for col, sql_type in [("page_url", "TEXT"), ("page_title", "TEXT"), ("page_h1", "TEXT")]:
                    try:
                        r = conn.execute(text(
                            f"SELECT COUNT(*) FROM pragma_table_info('competitor_geo_coverage') WHERE name='{col}'"
                        )).scalar()
                        if r == 0:
                            conn.execute(text(
                                f"ALTER TABLE competitor_geo_coverage ADD COLUMN {col} {sql_type}"
                            ))
                            conn.commit()
                    except Exception:
                        conn.rollback()
        except Exception:
            conn.rollback()

        # backlink_opportunities: add client_id if missing
        try:
            tables = conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='backlink_opportunities'"
            )).fetchall()
            if tables:
                r = conn.execute(text(
                    "SELECT COUNT(*) FROM pragma_table_info('backlink_opportunities') WHERE name='client_id'"
                )).scalar()
                if r == 0:
                    conn.execute(text("ALTER TABLE backlink_opportunities ADD COLUMN client_id VARCHAR(100)"))
                    conn.commit()
        except Exception:
            conn.rollback()

        # geo_page_outlines: add client_id, competitor_comparison_score, page_status if missing
        try:
            tables = conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='geo_page_outlines'"
            )).fetchall()
            if tables:
                for col, sql_type in [
                    ("client_id", "VARCHAR(100)"),
                    ("competitor_comparison_score", "REAL"),
                    ("page_status", "TEXT DEFAULT 'DRAFT'"),
                    ("generated_sections", "TEXT"),
                ]:
                    try:
                        r = conn.execute(text(
                            f"SELECT COUNT(*) FROM pragma_table_info('geo_page_outlines') WHERE name='{col}'"
                        )).scalar()
                        if r == 0:
                            conn.execute(text(
                                f"ALTER TABLE geo_page_outlines ADD COLUMN {col} {sql_type}"
                            ))
                            conn.commit()
                    except Exception:
                        conn.rollback()
        except Exception:
            conn.rollback()

        # content_drafts: add extracted_keywords, extracted_geo_phrases if missing
        for col in ("extracted_keywords", "extracted_geo_phrases"):
            try:
                r = conn.execute(text(
                    f"SELECT COUNT(*) FROM pragma_table_info('content_drafts') WHERE name='{col}'"
                )).scalar()
                if r == 0:
                    conn.execute(text(f"ALTER TABLE content_drafts ADD COLUMN {col} TEXT"))
                    conn.commit()
            except Exception:
                conn.rollback()

        # keyword_performance: add calls, direction_requests, confidence_declining if missing
        for col, col_type in [("calls", "INTEGER DEFAULT 0"), ("direction_requests", "INTEGER DEFAULT 0"), ("confidence_declining", "INTEGER DEFAULT 0")]:
            try:
                r = conn.execute(text(
                    f"SELECT COUNT(*) FROM pragma_table_info('keyword_performance') WHERE name='{col}'"
                )).scalar()
                if r == 0:
                    conn.execute(text(f"ALTER TABLE keyword_performance ADD COLUMN {col} {col_type}"))
                    conn.commit()
            except Exception:
                conn.rollback()

        # client_roadmap: add plan_period, status, is_locked if missing
        for col, col_type in [
            ("plan_period", "INTEGER"),
            ("status", "VARCHAR(20) DEFAULT 'PENDING'"),
            ("is_locked", "BOOLEAN DEFAULT 1"),
        ]:
            try:
                r = conn.execute(text(
                    f"SELECT COUNT(*) FROM pragma_table_info('client_roadmap') WHERE name='{col}'"
                )).scalar()
                if r == 0:
                    conn.execute(text(f"ALTER TABLE client_roadmap ADD COLUMN {col} {col_type}"))
                    conn.commit()
            except Exception:
                conn.rollback()

        # Backfill keyword_type_weight from keyword_type (service_city=1.0, seo=0.7, geo=0.4)
        try:
            conn.execute(text("""
                UPDATE keyword_intelligence SET keyword_type_weight = CASE
                    WHEN LOWER(TRIM(COALESCE(keyword_type, ''))) IN ('service_city', 'service_geo') THEN 1.0
                    WHEN LOWER(TRIM(COALESCE(keyword_type, ''))) = 'seo' THEN 0.7
                    WHEN LOWER(TRIM(COALESCE(keyword_type, ''))) = 'geo' THEN 0.4
                    ELSE 0.5
                END
                WHERE keyword_type IS NOT NULL AND TRIM(keyword_type) != ''
            """))
            conn.commit()
        except Exception:
            conn.rollback()


def get_db():
    """Dependency for DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
