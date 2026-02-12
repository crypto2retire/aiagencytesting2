"""
Opportunity Scoring Agent — easy wins ranking.

Turns raw competitor research into prioritized, low-effort / high-impact content
opportunities. Answers: "What should this business post FIRST to get traction fastest?"

Scoring: confidence + geo + novelty + competitor gap. Transparent for client trust.
Formula: (confidence*0.5) + (geo_bonus*0.3) + (novelty_bonus*0.2) → 0-100
"""

import datetime
import logging
from typing import List, Optional

from sqlalchemy import or_

from database import Client, KeywordIntelligence, MarketSnapshot, Opportunity, OpportunityScore, ResearchLog, SessionLocal
from keyword_history import get_decay_factor
from roi_projection import compute_roi_projection
from seasonality import check_seasonality
from verticals import get_average_job_value, get_opportunity_services

from config import OPPORTUNITY_LOG

N_RECENT_RUNS = 2  # Duplication guard: same service+geo cannot be recommended in last N runs
MIN_UNIQUE_RESULTS = 3  # Never return fewer than 3 unless data insufficient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(OPPORTUNITY_LOG), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

INTENT_KEYWORDS = [
    "removal", "pickup", "haul", "hauling", "cleanout", "dispose",
]



def _get_keyword_confidence(db, client_id: str, service: str, geo: str) -> float:
    """
    0-1: max effective confidence of keywords matching this service.
    Applies decay: frequently repeated keywords lose novelty; unseen 30+ days regain it.
    """
    if not service:
        return 0.5
    kws = db.query(KeywordIntelligence).filter(
        or_(KeywordIntelligence.client_id == client_id, KeywordIntelligence.region == geo),
        KeywordIntelligence.keyword.like(f"%{service}%"),
    ).all()
    if not kws:
        return 0.5  # default when no keyword data
    best = 0.0
    for k in kws:
        raw = (k.confidence_score or 0) / 100.0
        decay = get_decay_factor(k.keyword or "")
        effective = raw * decay
        best = max(best, effective)
    return min(best, 1.0)


def _generate_why_recommended(
    confidence: float,
    has_geo: bool,
    competitor_mentions: int,
    is_novel: bool,
    client_seasonality: Optional[str] = None,
) -> dict:
    """
    Deterministic, human-readable explainer. No LLM.
    Plain English, client-auditable, references scoring factors.
    """
    if confidence >= 0.75:
        conf_text = "Strong search intent and consistent competitor usage"
    elif confidence >= 0.5:
        conf_text = "Moderate search intent supported by keyword data"
    else:
        conf_text = "Emerging interest; less keyword data but low competition"

    if has_geo:
        geo_text = "Strong local relevance (city + service)"
    else:
        geo_text = "Service matches your market; consider adding city for local boost"

    if competitor_mentions < 2:
        comp_text = "Low content saturation among top competitors"
    elif competitor_mentions < 4:
        comp_text = "Moderate competitor coverage; room to differentiate"
    else:
        comp_text = "Competitors mention it often; requires stronger angle to stand out"

    if is_novel:
        nov_text = "Not previously recommended for this market"
    else:
        nov_text = "Previously surfaced; may still have value if not yet acted on"

    if client_seasonality and client_seasonality.strip():
        time_text = f"Seasonal note: {client_seasonality.strip()[:80]}"
    else:
        time_text = "Aligned with current search demand"

    return {
        "confidence": conf_text,
        "geo": geo_text,
        "competition": comp_text,
        "novelty": nov_text,
        "timing": time_text,
    }


def _apply_seasonality_to_timing(time_text: str, seasonality: dict, has_client_note: bool = False) -> str:
    """Enrich timing text when seasonal match detected. Keeps client note if present."""
    if has_client_note:
        return time_text
    if seasonality and seasonality.get("match") and seasonality.get("current_season"):
        season = seasonality["current_season"]
        boost = seasonality.get("boost_applied", 0)
        return f"Seasonal boost: {season} demand pattern (+{int(boost*100)}% score)"
    return time_text or "Aligned with current search demand"


def _is_recently_recommended(db, client_id: str, service: str, geo: str) -> bool:
    """True if same service+geo was recommended in the last N runs."""
    runs = (
        db.query(OpportunityScore)
        .filter(OpportunityScore.client_id == client_id)
        .order_by(OpportunityScore.created_at.desc())
        .limit(N_RECENT_RUNS + 1)
        .all()
    )
    if len(runs) <= 1:
        return False
    cutoff = runs[N_RECENT_RUNS - 1].created_at  # Nth most recent run
    existing = db.query(Opportunity).filter(
        Opportunity.client_id == client_id,
        Opportunity.service == service,
        Opportunity.geo == geo,
        Opportunity.created_at >= cutoff,
    ).first()
    return existing is not None


def score_opportunities(client_id: str) -> List[dict]:
    """
    Analyze research logs + keywords to produce ranked opportunities.
    Returns list of {service, competitor_mentions, score, ...}.
    Uses: confidence (keywords), geo_bonus, novelty, duplication guard.
    """
    log.info(f"Opportunity scorer starting for client_id={client_id}")
    db = SessionLocal()
    try:
        logs = (
            db.query(ResearchLog)
            .filter(ResearchLog.client_id == client_id)
            .all()
        )

        if not logs:
            log.warning("No research logs to score")
            return []

        latest = db.query(MarketSnapshot).filter(MarketSnapshot.client_id == client_id).order_by(MarketSnapshot.created_at.desc()).first()
        geo = (latest.city or "").strip() if latest else ""
        geo_bonus = 1.0 if geo else 0.3
        has_geo = bool(geo and geo.strip())
        client = db.query(Client).filter(Client.client_id == client_id).first()
        seasonality = (client.seasonality_notes or "").strip() if client else ""
        vertical = (client.client_vertical or "junk_removal").strip().lower() if client else "junk_removal"
        opportunity_services = get_opportunity_services(vertical)

        # Build text blob from research
        text_parts = []
        for rl in logs:
            for s in rl.extracted_services or []:
                text_parts.append(str(s).lower())
            for m in rl.missed_opportunities or []:
                text_parts.append(str(m).lower())
            text_parts.append((rl.raw_text or "")[:1000].lower())
        text_blob = " ".join(text_parts)

        service_frequency = {svc: text_blob.count(svc) for svc in opportunity_services}

        opportunities = []
        for service in opportunity_services:
            competitor_count = service_frequency.get(service, 0)

            # Duplicate guard: same service+geo in last N runs → near zero
            if _is_recently_recommended(db, client_id, service, geo):
                why = _generate_why_recommended(0.5, has_geo, competitor_count, is_novel=False, client_seasonality=seasonality)
                seas = check_seasonality(service, industry=vertical)
                why["timing"] = _apply_seasonality_to_timing(why["timing"], seas, has_client_note=bool(seasonality))
                opportunities.append({
                    "service": service,
                    "competitor_mentions": competitor_count,
                    "score": 1,
                    "duplicate": True,
                    "why_recommended": why,
                    "seasonality": seas,
                })
                continue

            # Confidence from keyword_intelligence (0-1)
            confidence = _get_keyword_confidence(db, client_id, service, geo)

            # Novelty: not recently recommended
            novelty_bonus = 1.0

            # Base formula: (confidence*0.5) + (geo_bonus*0.3) + (novelty*0.2)
            raw_score = (confidence * 0.5) + (geo_bonus * 0.3) + (novelty_bonus * 0.2)
            score = int(round(raw_score * 100))

            # Legacy: light penalty for high competitor mentions
            score = max(0, score - competitor_count * 5)
            if any(k in service for k in INTENT_KEYWORDS):
                score = min(100, score + 5)

            # Seasonality: boost if service aligns with current season (no filter)
            seas = check_seasonality(service, industry=vertical)
            if seas.get("match"):
                score = min(100, int(round(score * (1 + seas.get("boost_applied", 0)))))

            why = _generate_why_recommended(confidence, has_geo, competitor_count, is_novel=True, client_seasonality=seasonality)
            why["timing"] = _apply_seasonality_to_timing(why["timing"], seas, has_client_note=bool(seasonality))

            opportunities.append({
                "service": service,
                "competitor_mentions": competitor_count,
                "score": max(score, 1),
                "why_recommended": why,
                "seasonality": seas,
            })

        opportunities.sort(key=lambda x: x["score"], reverse=True)
        unique_by_score = [o for o in opportunities if not o.get("duplicate", False)]
        if len(unique_by_score) < MIN_UNIQUE_RESULTS:
            log.warning(f"Only {len(unique_by_score)} unique opportunities (data insufficient for {MIN_UNIQUE_RESULTS})")
        save_opportunities(db, client_id, opportunities, geo, vertical)
        log.info(f"Saved {len(opportunities)} ranked opportunities")
        return opportunities
    finally:
        db.close()


def save_opportunities(db, client_id: str, opportunities: List[dict], geo: str = "", vertical: str = "junk_removal") -> None:
    """
    Save ranked opportunities. Global duplication guard:
    - Same service+geo cannot be recommended twice within last N runs
    - Skip duplicates, select next highest scoring unique
    - Never return fewer than MIN_UNIQUE_RESULTS unless data insufficient
    """
    if not opportunities:
        return

    # Walk sorted list (score desc), skip duplicates, build unique surfacable
    unique_sorted = [o for o in opportunities if not o.get("duplicate", False)]
    surfacable = [o for o in unique_sorted if o["score"] >= 40]
    if len(surfacable) < MIN_UNIQUE_RESULTS:
        for o in unique_sorted:
            if o in surfacable or o["score"] < 20:
                continue
            surfacable.append(o)
            if len(surfacable) >= MIN_UNIQUE_RESULTS:
                break
    surfacable = surfacable[:10]

    tier_1 = [o["service"] for o in surfacable[:3] if o["score"] >= 70]
    tier_2 = [o["service"] for o in surfacable[3:6] if o["score"] >= 40]
    tier_3 = [o["service"] for o in surfacable[6:]]

    latest = db.query(MarketSnapshot).filter(MarketSnapshot.client_id == client_id).order_by(MarketSnapshot.created_at.desc()).first()
    snapshot_id = latest.snapshot_id if latest else f"{client_id}-scored"
    geo = geo or (latest.city if latest else "")

    opp = OpportunityScore(
        client_id=client_id,
        snapshot_id=snapshot_id,
        current_season=datetime.datetime.utcnow().strftime("%B %Y"),
        result_id=f"{client_id}-opp-{datetime.datetime.utcnow().strftime('%Y%m%d%H%M')}",
        tier_1_topics=tier_1 or ([surfacable[0]["service"]] if surfacable else [opportunities[0]["service"]] if opportunities else [get_opportunity_services(vertical)[0] if get_opportunity_services(vertical) else "general service"]),
        tier_2_topics=tier_2,
        tier_3_topics=tier_3,
    )
    db.add(opp)
    db.commit()

    for o in surfacable[:5]:
        comp_level = "low" if o.get("competitor_mentions", 0) < 2 else ("medium" if o.get("competitor_mentions", 0) < 4 else "high")
        reason = f"Competitors mention it rarely ({o.get('competitor_mentions', 0)}x), high intent in reviews"
        why = o.get("why_recommended") or {}
        seas = o.get("seasonality") or {}
        roi = compute_roi_projection(
            opportunity_score=o.get("score", 0),
            has_geo=bool(geo and str(geo).strip()),
            service=o.get("service", ""),
            avg_job_value=get_average_job_value(vertical),
        )
        action = "Google Business Post"
        existing = db.query(Opportunity).filter(
            Opportunity.client_id == client_id,
            Opportunity.service == o["service"],
            Opportunity.geo == geo,
        ).first()
        if not existing:
            db.add(Opportunity(
                client_id=client_id,
                service=o["service"],
                geo=geo,
                opportunity_score=o["score"],
                reason=reason,
                why_recommended=why,
                roi_projection=roi,
                seasonality=seas,
                competition_level=comp_level,
                recommended_action=action,
                status="OPEN",
            ))
    db.commit()
