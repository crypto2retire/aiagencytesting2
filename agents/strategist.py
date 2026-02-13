"""
Strategist — prioritized actions, recommended pages, upsell flags.

Consumes: keyword_intelligence, geo clusters, website quality scores, opportunity scores.
Outputs only: prioritized actions, recommended pages, upsell flags.
Never browses the web. No LLM drafting.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional

from config import MIN_CONFIDENCE_FOR_STRATEGIST, STRATEGIST_LOG
from sqlalchemy import func, or_

from sqlalchemy.orm import Session

from database import (
    Client,
    CompetitorWebsite,
    ContentStrategy,
    KeywordIntelligence,
    KeywordIntel,
    KeywordPerformance,
    MarketSnapshot,
    ResearchLog,
    SessionLocal,
    StrategistUpsellFlag,
)
from verticals import get_vertical_config, is_excluded_from_content

from .opportunity_scorer import score_opportunities
from geo_coverage_aggregator import get_geo_coverage_density

from geo_phrase_extractor import extract_geo_phrases_from_profile, cluster_geo_phrases_by_city
from geo_phrase_confidence import get_keyword_confidence_for_phrase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(STRATEGIST_LOG), logging.StreamHandler()],
)
log = logging.getLogger(__name__)


def _is_semantically_same(topic_a: str, topic_b: str) -> bool:
    """True if topics are effectively the same."""
    a = (topic_a or "").strip().lower()
    b = (topic_b or "").strip().lower()
    if a == b:
        return True
    return a in b or b in a


def _get_regions_for_client(db: Session, client: Client, client_id: str) -> List[str]:
    """Regions = cities_served + snapshot cities."""
    regions = [c.strip() for c in (client.cities_served or []) if c and str(c).strip()]
    for s in db.query(MarketSnapshot).filter(MarketSnapshot.client_id == client_id).all():
        if s.city and s.city.strip() and s.city.strip() not in regions:
            regions.append(s.city.strip())
    return regions


def _load_keyword_intelligence(db: Session, client_id: str, regions: List[str]) -> str:
    """
    Load keyword intelligence (KeywordIntelligence + KeywordIntel) as summary text.
    """
    if not regions:
        q = db.query(KeywordIntelligence).filter(KeywordIntelligence.client_id == client_id)
    else:
        q = db.query(KeywordIntelligence).filter(
            or_(
                KeywordIntelligence.client_id == client_id,
                KeywordIntelligence.region.in_(regions),
            )
        )
    rows = (
        q.order_by(
            func.coalesce(KeywordIntelligence.confidence_score, 0).desc(),
            KeywordIntelligence.frequency.desc(),
        )
        .limit(30)
        .all()
    )
    kw_intel = []
    for r in rows:
        kw = (r.keyword or "").strip()
        conf = float(r.confidence_score or 0)
        if conf > 1:
            conf /= 100.0
        kw_intel.append(f"  - {kw} (conf={conf:.0%})")
    if kw_intel:
        return "Keyword intelligence:\n" + "\n".join(kw_intel[:20])

    # Fallback: KeywordIntel by city match
    if regions:
        region_lower = [r.strip().lower() for r in regions if r][:10]
        ki_rows = (
            db.query(KeywordIntel)
            .filter(
                KeywordIntel.city.isnot(None),
                KeywordIntel.city != "",
                func.lower(KeywordIntel.city).in_(region_lower),
            )
            .order_by(KeywordIntel.confidence_score.desc())
            .limit(15)
            .all()
        )
        if ki_rows:
            lines = [f"  - {r.keyword} (city={r.city}, conf={r.confidence_score})" for r in ki_rows]
            return "Keyword intel (by region):\n" + "\n".join(lines)
    return "No keyword data yet."


def _load_geo_clusters(db: Session, regions: List[str], vertical: str) -> List[dict]:
    """
    Load geo clusters from GeoCoverageDensity.
    Returns (city, state, service) with competitor_count and avg_quality_score.
    Low competitor_count = opportunity.
    """
    clusters = get_geo_coverage_density(db=db)
    if not clusters:
        return []
    region_set = {r.strip().lower() for r in regions if r}
    out = []
    for c in clusters:
        city = (c.get("city") or "").strip().lower()
        if city and city not in region_set:
            continue
        service = (c.get("service") or "").strip().lower()
        if is_excluded_from_content(service, vertical):
            continue
        out.append({
            "city": c.get("city"),
            "state": c.get("state"),
            "service": c.get("service"),
            "competitor_count": c.get("competitor_count", 0),
            "avg_quality_score": c.get("avg_quality_score"),
        })
    return out


def _load_website_quality(db: Session, client_id: str) -> dict:
    """
    Load client and competitor website quality scores.
    Returns {client_score, competitors: [(name, site_score), ...]}.
    """
    client = db.query(Client).filter(Client.client_id == client_id).first()
    client_score = float(client.avg_page_quality_score) if getattr(client, "avg_page_quality_score", None) is not None else None
    competitors = []
    for cw in db.query(CompetitorWebsite).filter(CompetitorWebsite.client_id == client_id).all():
        sc = cw.site_score
        if sc is not None:
            competitors.append((cw.competitor_name or cw.domain, float(sc)))
    return {"client_score": client_score, "competitors": competitors}


def _get_landing_page_recommendations(
    db: Session,
    client_id: str,
    regions: List[str],
    geo_clusters: List[dict],
    vertical: str = "junk_removal",
    limit: int = 15,
) -> List[dict]:
    """
    Recommend service-city landing pages from:
    - Geo clusters (GeoCoverageDensity): low competitor_count = opportunity
    - Research-based geo phrase clusters (fallback)
    """
    recommendations = []

    # From geo clusters: (city, service) with low competitor_count
    if geo_clusters:
        for c in geo_clusters:
            if c.get("competitor_count", 0) > 3:
                continue
            service = (c.get("service") or "").strip()
            city = (c.get("city") or "").strip()
            if not service or not city:
                continue
            topic = f"{service} in {city}"
            kw_conf = get_keyword_confidence_for_phrase(db, service, city=city)
            recommendations.append({
                "service": service,
                "city": city,
                "topic": topic,
                "keyword_confidence": kw_conf,
                "is_high_confidence_missing": kw_conf >= 0.65,
                "competition_level": "low" if c.get("competitor_count", 0) < 2 else "medium",
            })

    # Fallback: research-based clusters
    if not recommendations and regions:
        known_cities = [c.strip().lower() for c in regions if c and str(c).strip()]
        logs = db.query(ResearchLog).filter(ResearchLog.client_id == client_id).all()
        all_phrases = []
        for rl in logs:
            profile = rl.extracted_profile or {}
            if profile:
                pairs = extract_geo_phrases_from_profile(profile, known_cities)
                all_phrases.extend(pairs)
        city_clusters = cluster_geo_phrases_by_city(all_phrases, vertical=vertical)
        if city_clusters:
            city_competition = {c: len(cl.service_counts) for c, cl in city_clusters.items()}
            median_services = sorted(city_competition.values())[len(city_competition) // 2] if city_competition else 0
            for city, cluster in city_clusters.items():
                comp_level = "low" if city_competition.get(city, 0) <= median_services else "medium"
                for service in cluster.missing_services:
                    if is_excluded_from_content(service, vertical):
                        continue
                    kw_conf = get_keyword_confidence_for_phrase(db, service, city=city)
                    topic = f"{service} in {city}" if city else service
                    recommendations.append({
                        "service": service,
                        "city": city,
                        "topic": topic,
                        "keyword_confidence": kw_conf,
                        "is_high_confidence_missing": kw_conf >= 0.65,
                        "competition_level": comp_level,
                    })

    def _sort_key(r):
        hc = 1 if r.get("is_high_confidence_missing") else 0
        low_comp = 1 if r.get("competition_level") == "low" else 0
        return (-hc, -low_comp, -r.get("keyword_confidence", 0))

    recommendations.sort(key=_sort_key)
    return recommendations[:limit]


def _compute_upsell_flags(
    db: Session,
    client_id: str,
    quality: dict,
    geo_clusters: List[dict],
    recommendations: List[dict],
) -> List[dict]:
    """
    Compute upsell flags from quality gaps, missing geo coverage, etc.
    """
    flags = []
    client_score = quality.get("client_score")
    competitors = quality.get("competitors", [])

    # Quality gap: competitors ahead of client
    if client_score is not None and competitors:
        avg_comp = sum(c[1] for c in competitors) / len(competitors)
        if avg_comp > client_score + 10:
            flags.append({
                "flag": "competitor_ahead",
                "reason": f"Competitors avg {avg_comp:.0f}/100 vs your {client_score:.0f}/100. Consider improving site quality.",
                "priority": 4,
            })

    # Weak competitors: client ahead — easier wins
    if client_score is not None and competitors:
        weak = [c for c in competitors if c[1] < client_score - 10]
        if weak:
            flags.append({
                "flag": "weak_competitors",
                "reason": f"Outranking {len(weak)} weak competitor(s). Prioritize content to capture their traffic.",
                "priority": 3,
            })

    # Missing geo coverage
    if recommendations:
        high_conf = [r for r in recommendations if r.get("is_high_confidence_missing")]
        if high_conf:
            flags.append({
                "flag": "missing_geo_pages",
                "reason": f"{len(high_conf)} high-confidence service-city pages missing. Create landing pages for quick wins.",
                "priority": 5,
            })

    # Low competitor density in served regions
    if geo_clusters:
        low_density = [c for c in geo_clusters if c.get("competitor_count", 0) < 2]
        if low_density and not flags:
            flags.append({
                "flag": "low_competition_markets",
                "reason": f"{len(low_density)} city-service combos with low competition. Expand geo pages.",
                "priority": 2,
            })

    return flags


def _save_strategy(
    db: Session,
    client_id: str,
    city: str,
    opportunities: List[dict],
    landing_page_recs: Optional[List[dict]] = None,
    upsell_flags: Optional[List[dict]] = None,
    vertical: str = "junk_removal",
) -> None:
    """Save prioritized actions, recommended pages, upsell flags."""
    db.query(ContentStrategy).filter(ContentStrategy.client_id == client_id).delete()
    db.query(StrategistUpsellFlag).filter(StrategistUpsellFlag.client_id == client_id).delete()

    seen_topics = set()

    # Prioritized actions (from opportunities)
    for opp in (opportunities or []):
        if opp.get("duplicate", False):
            continue
        topic = opp.get("service", "")
        if is_excluded_from_content(topic, vertical):
            continue
        if any(_is_semantically_same(topic, s) for s in seen_topics):
            continue
        seen_topics.add((topic or "").strip().lower())
        score = opp.get("score", 50)
        actions = [
            f"Google Business Profile post targeting '{topic} in {city}'" if city else f"Google Business Profile post for '{topic}'",
            f"SEO blog page for '{topic}'",
            "Before/after photo post on Facebook",
        ]
        db.add(ContentStrategy(
            client_id=client_id,
            topic=topic,
            recommended_actions=actions,
            priority_score=score,
            strategy_type="action",
        ))

    # Recommended pages (service-city landing pages)
    for rec in (landing_page_recs or [])[:10]:
        topic = rec.get("topic", "")
        if not topic or any(_is_semantically_same(topic, s) for s in seen_topics):
            continue
        seen_topics.add(topic.strip().lower())
        svc = rec.get("service", "")
        cty = rec.get("city", "")
        score = 70 if rec.get("is_high_confidence_missing") else 50
        if rec.get("competition_level") == "low":
            score += 15
        actions = [
            f"Create service-city landing page: '{svc}' in {cty}",
            f"Target local SEO for '{svc} {cty}'",
            "Include location-specific content and testimonials",
        ]
        if rec.get("is_high_confidence_missing"):
            actions.insert(0, "HIGH CONFIDENCE: Strong keyword data — prioritize this page")
        db.add(ContentStrategy(
            client_id=client_id,
            topic=topic,
            recommended_actions=actions,
            priority_score=score,
            strategy_type="page",
        ))

    # Upsell flags
    for f in (upsell_flags or []):
        db.add(StrategistUpsellFlag(
            client_id=client_id,
            flag=f.get("flag", ""),
            reason=f.get("reason", ""),
            priority=f.get("priority", 0),
        ))

    db.commit()


def _weight_by_performance(opportunities: List[dict], top_performing: set) -> List[dict]:
    """Sort opportunities so those matching top-performing keywords appear first."""
    if not top_performing:
        return opportunities

    def _matches(opp: dict) -> bool:
        s = (opp.get("service") or "").strip().lower()
        g = (opp.get("geo") or "").strip().lower()
        combo = f"{g} {s}".strip() if g else s
        return s in top_performing or combo in top_performing or any(
            s in t or t in s or combo in t or t in combo for t in top_performing
        )

    return sorted(opportunities, key=lambda o: (0 if _matches(o) else 1, -(o.get("score", 0))))


def _get_top_performing_keywords(db: Session, limit: int = 20) -> set:
    """Pull top-performing keywords from keyword_performance."""
    from database import KeywordPerformance
    rows = (
        db.query(KeywordPerformance)
        .filter(KeywordPerformance.confidence_score.isnot(None))
        .order_by(KeywordPerformance.confidence_score.desc())
        .limit(limit)
        .all()
    )
    terms = set()
    for r in rows:
        if r.keyword and str(r.keyword).strip():
            terms.add(str(r.keyword).strip().lower())
        if r.geo_phrase and str(r.geo_phrase).strip():
            terms.add(str(r.geo_phrase).strip().lower())
    return terms


def generate_strategy(client_id: str) -> dict:
    """
    Consume: keyword_intelligence, geo clusters, website quality scores, opportunity scores.
    Output only: prioritized actions, recommended pages, upsell flags.
    Returns {action_count, page_count, upsell_count}.
    """
    log.info(f"Strategist starting for client={client_id}")
    db = SessionLocal()
    try:
        client = db.query(Client).filter(func.lower(Client.client_id) == client_id.lower()).first()
        if not client:
            log.error("Client not found")
            return {}

        client_id = client.client_id
        vertical = (client.client_vertical or "junk_removal").strip().lower()
        city = (client.cities_served or [""])[0] if client.cities_served else ""

        # 1. Load inputs
        regions = _get_regions_for_client(db, client, client_id)
        keyword_summary = _load_keyword_intelligence(db, client_id, regions)
        geo_clusters = _load_geo_clusters(db, regions, vertical)
        quality = _load_website_quality(db, client_id)

        # Opportunity scores (from opportunity scorer — uses keyword_intel, research, etc.)
        opportunities = score_opportunities(client_id)
        if not opportunities:
            opportunities = [{"service": "general service", "score": 50, "duplicate": False}]
        top_performing = _get_top_performing_keywords(db)
        opportunities = _weight_by_performance(opportunities, top_performing)

        # 2. Recommended pages (from geo clusters + keyword intel)
        landing_page_recs = _get_landing_page_recommendations(
            db, client_id, regions, geo_clusters, vertical=vertical, limit=15
        )

        # 3. Upsell flags
        upsell_flags = _compute_upsell_flags(db, client_id, quality, geo_clusters, landing_page_recs)

        # 4. Save outputs
        _save_strategy(
            db, client_id, city,
            opportunities[:10],
            landing_page_recs=landing_page_recs,
            upsell_flags=upsell_flags,
            vertical=vertical,
        )

        action_count = len([o for o in opportunities[:10] if not o.get("duplicate") and not is_excluded_from_content(o.get("service", ""), vertical)])
        page_count = min(10, len(landing_page_recs))
        upsell_count = len(upsell_flags)

        log.info(f"Strategist done. actions={action_count}, pages={page_count}, upsells={upsell_count}")
        return {"action_count": action_count, "page_count": page_count, "upsell_count": upsell_count}
    except Exception as e:
        db.rollback()
        log.exception(str(e))
        raise
    finally:
        db.close()


def run_strategist(client_id: str) -> dict:
    """Alias for generate_strategy."""
    return generate_strategy(client_id)
