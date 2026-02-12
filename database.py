"""
SQLAlchemy models and init.
Database is the glue — agents read/write here, never talk to each other.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import JSON, Column, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, create_engine
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
    website_url = Column(String(500))
    google_business_profile_url = Column(String(500))
    phone_number = Column(String(50))
    services_offered = Column(JSON, default=list)  # list[str]
    cities_served = Column(JSON, default=list)
    zip_codes_served = Column(JSON, default=list)
    ideal_customer_types = Column(JSON, default=list)
    brand_tone = Column(String(50), default="friendly")
    differentiators = Column(JSON, default=list)
    client_vertical = Column(String(50), default="junk_removal")  # junk_removal | plumbing | hvac | ...
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
    Tracks keyword, type, region, source, frequency, confidence_score, first/last seen.
    """

    __tablename__ = "keyword_intelligence"
    __table_args__ = (UniqueConstraint("keyword", "region", name="uq_keyword_region"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    keyword = Column(String(255), nullable=False, index=True)
    keyword_type = Column(String(50))  # service | service_geo | geo | modifier | long_tail | brand
    geo_phrase = Column(String(100))  # normalized geo e.g. "milwaukee wi" when service+geo
    region = Column(String(100), nullable=False, index=True)
    source = Column(String(50), nullable=False)  # competitor | client | platform
    client_id = Column(String(100), ForeignKey("clients.client_id"), index=True)
    frequency = Column(Integer, default=1)
    confidence_score = Column(Integer, default=0)  # 0-100: ranks keywords for high-confidence vs experimental
    first_seen = Column(DateTime, default=datetime.utcnow)
    last_seen = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ContentStrategy(Base):
    """Action plan — clear instructions the client can act on."""

    __tablename__ = "content_strategies"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(100), ForeignKey("clients.client_id"), nullable=False, index=True)
    topic = Column(String(255), nullable=False)
    recommended_actions = Column(JSON, default=list)  # list[str]
    priority_score = Column(Integer, default=0)
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


def get_db():
    """Dependency for DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
