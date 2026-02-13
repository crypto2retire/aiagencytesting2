"""
Roadmap Generator — generates client_roadmap items from:
- Missing geo pages (GeoPhrase without GeoPageOutline)
- Weak competitor coverage (low website_quality_score)
- Backlink gaps (BacklinkOpportunity)
- Low website quality / missed opportunities (from research)
Sort by confidence_score desc.
"""

from database import (
    BacklinkOpportunity,
    Client,
    ClientRoadmap,
    GeoPageOutline,
    GeoPhrase,
    ResearchLog,
    SessionLocal,
)
from geo_phrase_extractor import cluster_geo_phrases_by_city, extract_geo_phrases_from_profile
from geo_phrase_confidence import calculate_geo_phrase_confidence, get_keyword_confidence_for_phrase
from verticals import is_excluded_from_content


def _cap_conf(v: float) -> float:
    """Normalize confidence to 0–1 (handles 0–100 or 0–1 input)."""
    v = float(v)
    if v > 1:
        v = v / 100.0
    return max(0.0, min(1.0, v))


def generate_roadmap(client_id: str, db=None) -> int:
    """
    Generate client_roadmap items from missing geo pages, weak competitors, backlinks, website quality.
    Clears existing items and repopulates. Returns count added.
    """
    sess = db or SessionLocal()
    try:
        client = sess.query(Client).filter(Client.client_id == client_id).first()
        if not client:
            return 0

        regions = [c.strip() for c in (client.cities_served or []) if c and str(c).strip()]
        vertical = (client.client_vertical or "junk_removal").strip().lower()

        # Clear existing roadmap for this client
        sess.query(ClientRoadmap).filter(ClientRoadmap.client_id == client_id).delete()

        items = []

        # 1. Missing geo pages — GeoPhrase (conf > 0.6) without GeoPageOutline, in client cities
        known_cities = {c.lower() for c in regions} if regions else set()
        phrases = sess.query(GeoPhrase).filter(
            GeoPhrase.confidence_score > 0.6,
            GeoPhrase.city.isnot(None),
            GeoPhrase.service.isnot(None),
        ).all()

        for p in phrases:
            if not known_cities:
                continue
            city_raw = (p.city or "").strip()
            city_lower = city_raw.lower()
            if city_lower not in known_cities and not any(city_lower in (r or "").lower() for r in regions):
                continue
            if is_excluded_from_content(p.service or "", vertical):
                continue
            existing = sess.query(GeoPageOutline).filter(
                GeoPageOutline.city.ilike(city_raw),
                GeoPageOutline.service == (p.service or ""),
            ).first()
            if existing:
                continue
            conf = _cap_conf(p.confidence_score or 0.5)
            items.append({
                "task_type": "geo_page",
                "title": f"Create {p.service or 'service'} page for {p.city or ''}",
                "description": f"High-confidence geo phrase '{p.geo_phrase or ''}' has no outline yet. Create landing page.",
                "expected_impact": "Capture local search intent in underserved area.",
                "confidence_score": conf,
            })

        # 2. Missing geo from city clusters (research-log based)
        logs = sess.query(ResearchLog).filter(ResearchLog.client_id == client_id).all()
        all_phrases = []
        for rl in logs:
            profile = rl.extracted_profile or {}
            if profile:
                all_phrases.extend(extract_geo_phrases_from_profile(profile, [c.lower() for c in regions]))
        city_clusters = cluster_geo_phrases_by_city(all_phrases, vertical=vertical)
        for city, cluster in city_clusters.items():
            for service in (cluster.missing_services or [])[:5]:
                if is_excluded_from_content(service, vertical):
                    continue
                kw_conf = get_keyword_confidence_for_phrase(sess, service, city=city)
                geo_conf = calculate_geo_phrase_confidence(
                    frequency=0, avg_source_quality=0, keyword_confidence=kw_conf
                )
                items.append({
                    "task_type": "geo_page",
                    "title": f"Create {service} page for {city}",
                    "description": f"Competitors cover other services in {city} but not {service}. Create landing page.",
                    "expected_impact": "Fill geo gap; capture demand competitors miss.",
                    "confidence_score": _cap_conf(geo_conf),
                })

        # 3. Weak competitor coverage — outrank low-quality competitors
        def _q(rl):
            return getattr(rl, "competitor_comparison_score", None) or rl.website_quality_score or 100
        weak = [rl for rl in logs if _q(rl) < 60]
        if weak:
            weak.sort(key=lambda r: _q(r))
            avg_weak = sum(_q(rl) for rl in weak) / len(weak)
            names = [rl.competitor_name for rl in weak[:5] if rl.competitor_name]
            items.append({
                "task_type": "weak_competitor",
                "title": "Outrank weak competitors",
                "description": f"{len(weak)} competitors have low site quality (avg {avg_weak:.0f}/100). {', '.join(names[:3])}{'...' if len(names) > 3 else ''}.",
                "expected_impact": "Easier to outrank with better content and UX.",
                "confidence_score": 0.65,
            })

        # 4. Backlink gaps
        bl_opps = sess.query(BacklinkOpportunity).filter(
            BacklinkOpportunity.client_id == client_id,
        ).order_by(BacklinkOpportunity.confidence_score.desc()).limit(10).all()

        for bl in bl_opps:
            comps = (bl.linked_competitors or [])[:3]
            comp_str = ", ".join(comps) if comps else "competitors"
            items.append({
                "task_type": "backlink",
                "title": f"Get listed on {bl.domain or 'directory'}",
                "description": f"{comp_str} are listed; client is not. {bl.source_type or 'source'}.",
                "expected_impact": "Build local authority and referral traffic.",
                "confidence_score": _cap_conf(float(bl.confidence_score or 0.5)),
            })

        # 5. Low website quality / missed opportunities (from research gaps)
        all_missed = []
        for rl in logs:
            all_missed.extend(rl.missed_opportunities or [])
        if all_missed:
            missed_unique = list(dict.fromkeys(str(m) for m in all_missed if m))[:5]
            items.append({
                "task_type": "website_quality",
                "title": "Address common competitor gaps",
                "description": f"Competitors miss: {'; '.join(missed_unique[:3])}{'...' if len(missed_unique) > 3 else ''}.",
                "expected_impact": "Differentiate with licensing, guarantees, stronger CTAs.",
                "confidence_score": 0.55,
            })

        # Dedupe by title, sort by confidence desc
        seen_titles = set()
        deduped = []
        for it in items:
            key = (it["task_type"], it["title"][:80])
            if key in seen_titles:
                continue
            seen_titles.add(key)
            deduped.append(it)
        deduped.sort(key=lambda x: -(x["confidence_score"] or 0))

        for i, it in enumerate(deduped[:30]):
            conf = it.get("confidence_score") or 0.5
            plan = 30 if i < 10 else (60 if i < 20 else 90)
            locked = conf >= 0.6
            sess.add(ClientRoadmap(
                client_id=client_id,
                priority=i + 1,
                task_type=it["task_type"],
                title=it["title"],
                description=it["description"],
                expected_impact=it["expected_impact"],
                confidence_score=it["confidence_score"],
                plan_period=plan,
                status="PENDING",
                is_locked=locked,
            ))

        if db is None:
            sess.commit()
        return len(deduped[:30])
    except Exception:
        if db is None:
            sess.rollback()
        raise
    finally:
        if db is None:
            sess.close()


def get_roadmap(client_id: str, db=None, limit: int = 30) -> list:
    """Get roadmap items for client, sorted by confidence_score desc."""
    sess = db or SessionLocal()
    try:
        rows = sess.query(ClientRoadmap).filter(
            ClientRoadmap.client_id == client_id,
        ).order_by(ClientRoadmap.confidence_score.desc()).limit(limit).all()
        return [
            {
                "id": r.id,
                "priority": r.priority,
                "task_type": r.task_type,
                "title": r.title,
                "description": r.description,
                "expected_impact": r.expected_impact,
                "confidence_score": float(r.confidence_score) if r.confidence_score is not None else 0,
            }
            for r in rows
        ]
    finally:
        if db is None:
            sess.close()
