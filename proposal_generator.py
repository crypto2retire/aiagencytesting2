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
