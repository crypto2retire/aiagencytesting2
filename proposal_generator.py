"""
Automated Proposal Generator — turn opportunities into client-ready proposals.
Uses existing data only. No hallucinations. Clear ROI framing. Trust-building explanations.
"""

from pathlib import Path
from typing import Any, Callable, List, Optional


def get_proposal_path(client_id: str) -> Path:
    """Output path: proposals/{{client_id}}_proposal.md"""
    from config import PROPOSALS_DIR
    safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in str(client_id or "")) or "client"
    return PROPOSALS_DIR / f"{safe_id}_proposal.md"


def save_proposal(client_id: str, markdown: str) -> Path:
    """Save proposal to proposals/{{client_id}}_proposal.md. Returns path."""
    path = get_proposal_path(client_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(markdown, encoding="utf-8")
    return path


def _parse_json(val: Any) -> dict:
    """Parse JSON from DB (may be dict or JSON string)."""
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            import json
            return json.loads(val) if val.strip() else {}
        except Exception:
            return {}
    return {}


def _why_summary(why: dict) -> str:
    """Build human-readable summary from why_recommended."""
    if not why:
        return "Strong local intent and underserved market."
    parts = []
    for k in ("confidence", "geo", "competition", "novelty", "timing"):
        v = why.get(k)
        if v and isinstance(v, str):
            parts.append(v)
    return " ".join(parts) if parts else "Strong local intent and underserved market."


def _competition_gap(why: dict, competition_level: Optional[str] = None) -> str:
    """Why competitors are missing this — from competition field or level."""
    comp = why.get("competition") if why else None
    if comp and isinstance(comp, str):
        return comp
    level = (competition_level or "").lower()
    if level == "low":
        return "Low content saturation among top competitors; few are actively targeting this."
    if level == "medium":
        return "Moderate competitor coverage; room to differentiate with clearer messaging."
    return "Competitors mention it; a focused local angle can capture intent."


def generate_proposal(
    business_name: str,
    opportunities: List[Any],
    top_n: int = 3,
    enrich_roi: Optional[Callable[[Any], dict]] = None,
) -> str:
    """
    Generate client-ready proposal markdown.
    Uses top ranked opportunities, why_recommended, ROI projections.
    No LLM — template + data only.
    """
    opps = [o for o in opportunities if o][:top_n]
    if not opps:
        return f"""# Local Growth Opportunities for {business_name}

## Overview
We analyzed your local market and competitors to identify high-impact, low-competition growth opportunities.

*No opportunities available yet. Run the Strategist to identify opportunities, then generate this proposal again.*

---

## Next Steps
Run market research and opportunity scoring to populate recommendations.
"""

    sections = []
    sections.append(f"# Local Growth Opportunities for {business_name}\n")
    sections.append("## Overview")
    sections.append("We analyzed your local market and competitors to identify high-impact, low-competition growth opportunities.\n")
    sections.append("---\n")
    sections.append("## Top Opportunities\n")

    for o in opps:
        service = getattr(o, "service", "") or ""
        geo = getattr(o, "geo", "") or ""
        title = f"{service}" + (f" — {geo}" if geo else "")

        why_raw = getattr(o, "why_recommended", None)
        why = _parse_json(why_raw)
        summary = _why_summary(why)

        roi_raw = getattr(o, "roi_projection", None)
        roi = _parse_json(roi_raw)
        if not roi and enrich_roi:
            roi = enrich_roi(o) or {}
        leads = roi.get("estimated_leads", {}).get("expected", 0) or 0
        revenue = roi.get("estimated_revenue", {}).get("expected", 0) or 0

        comp_level = getattr(o, "competition_level", None)
        gap = _competition_gap(why, comp_level)

        block = f"""### {title}
**Why this matters**
{summary}

**Estimated Impact**
- Monthly Leads: {leads}
- Estimated Revenue: ${revenue:,}

**Why competitors are missing this**
{gap}

---
"""
        sections.append(block)

    sections.append("## Next Steps\n")
    sections.append("We recommend launching content and local optimization for these services first to capture demand quickly and build authority.")

    return "\n".join(sections)


def generate_ai_sales_proposal(client_id: str, db, status: str = "DRAFT") -> Optional[dict]:
    """
    Gather client data, call Ollama generate_sales_proposal, return result or None.
    Saves to SalesProposal table if successful.
    status: 'DRAFT' | 'READY'
    """
    from database import Client, GeoPhrase, KeywordIntelligence, ResearchLog, ClientRoadmap, GeoPageOutline, SalesProposal
    from agents.ollama_client import generate_sales_proposal
    from sqlalchemy import func, or_

    client = db.query(Client).filter(func.lower(Client.client_id) == client_id.lower()).first()
    if not client:
        return None

    cities = (client.cities_served or [])[:5]
    regions = [c.lower() for c in cities if c]

    # Geo phrases: GeoPhrase or KeywordIntelligence with geo_phrase
    geo_phrases = []
    for row in db.query(GeoPhrase).filter(GeoPhrase.geo_phrase.isnot(None)).order_by(GeoPhrase.confidence_score.desc().nullslast()).limit(15).all():
        geo_phrases.append({"geo_phrase": row.geo_phrase, "confidence_score": row.confidence_score})
    if not geo_phrases:
        kw_geo = db.query(KeywordIntelligence).filter(
            KeywordIntelligence.geo_phrase.isnot(None),
            or_(KeywordIntelligence.client_id.is_(None), KeywordIntelligence.client_id == client_id),
        ).order_by(KeywordIntelligence.keyword_confidence_score.desc().nullslast()).limit(10).all()
        for k in kw_geo:
            geo_phrases.append({"geo_phrase": k.geo_phrase, "confidence_score": k.keyword_confidence_score or 0})

    # Keywords: top by keyword_confidence_score
    keywords = []
    for k in db.query(KeywordIntelligence).filter(
        or_(KeywordIntelligence.client_id.is_(None), KeywordIntelligence.client_id == client_id),
    ).order_by(KeywordIntelligence.keyword_confidence_score.desc().nullslast()).limit(15).all():
        keywords.append({"keyword": k.keyword, "confidence_score": k.keyword_confidence_score or 0})

    # Competitor coverage & quality
    logs = db.query(ResearchLog).filter(ResearchLog.client_id == client_id).order_by(ResearchLog.created_at.desc()).limit(20).all()
    comp_lines = []
    client_avg = getattr(client, "avg_page_quality_score", None)
    for rl in logs:
        q = getattr(rl, "competitor_comparison_score", None) or rl.website_quality_score or 0
        name = (rl.extracted_profile or {}).get("company_name") or rl.competitor_name
        comp_lines.append(f"  • {name}: {q}/100")
    competitor_coverage = "\n".join(comp_lines) if comp_lines else "  (none)"

    # Content roadmap: ClientRoadmap + GeoPageOutline
    roadmap_parts = []
    for r in db.query(ClientRoadmap).filter(ClientRoadmap.client_id == client_id).order_by(ClientRoadmap.priority.asc().nullslast()).limit(10).all():
        roadmap_parts.append(f"  • [{r.priority}] {r.title or r.description or ''}")
    for o in db.query(GeoPageOutline).filter(GeoPageOutline.client_id == client_id).order_by(GeoPageOutline.confidence_score.desc().nullslast()).limit(10).all():
        roadmap_parts.append(f"  • {o.geo_phrase or o.service or ''} — {o.page_status or 'DRAFT'} (conf: {o.confidence_score or 0:.0%})")
    content_roadmap = "\n".join(roadmap_parts) if roadmap_parts else "  (none)"

    try:
        result = generate_sales_proposal(
            client_name=client.business_name or "",
            city=", ".join(cities[:3]) if cities else "",
            niche=getattr(client, "client_vertical", "") or "",
            website=client.website_url or "",
            geo_phrases=geo_phrases,
            keywords=keywords,
            competitor_coverage=competitor_coverage,
            content_roadmap=content_roadmap,
        )
    except Exception:
        return None
    if not result:
        return None

    # Save to SalesProposal
    prop = SalesProposal(
        client_id=client_id,
        summary=result.get("summary") or "",
        opportunity_list=result.get("opportunity_list") or [],
        estimated_impact=result.get("estimated_impact") or {},
        generated_document=result.get("generated_document") or "",
        status=status,
    )
    db.add(prop)
    db.commit()
    return result
