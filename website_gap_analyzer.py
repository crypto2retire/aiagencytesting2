"""
Website Gap Analyzer — compares client website vs competitor averages.
Outputs structured gap analysis JSON in six required categories.

Requires websites table populated (e.g. via populate_websites_from_research).
"""

from typing import Any, Dict, List, Optional

from website_quality_scorer import score_website_quality


def _safe_list(obj: Any) -> list:
    if obj is None:
        return []
    return list(obj) if isinstance(obj, (list, tuple)) else []


def _extract_category_scores(profile: Optional[dict]) -> Dict[str, float]:
    """Extract per-category scores from profile via website_quality_scorer logic."""
    if not profile or not isinstance(profile, dict):
        return {
            "technical_seo": 0.0,
            "content_depth": 0.0,
            "geo_coverage": 0.0,
            "keyword_overlap": 0.0,
            "trust_signals": 0.0,
            "conversion_elements": 0.0,
        }
    ws = score_website_quality(profile)
    # Map WebsiteQualityScore fields to our categories
    # geo_coverage ≈ local_signals; keyword_overlap from seo_keywords count
    seo_kw = len(_safe_list(profile.get("seo_keywords")))
    sc_phrases = len(_safe_list(profile.get("service_city_phrases")))
    kw_score = min(20.0, (seo_kw + sc_phrases) * 2) if (seo_kw or sc_phrases) else 0.0
    trust = profile.get("trust_signals") or {}
    trust_score = sum(3 for k in ("reviews_mentioned", "licenses_or_insurance") if trust.get(k)) + sum(2 for k in ("years_in_business", "guarantees") if trust.get(k))
    trust_score = min(20.0, trust_score)
    return {
        "technical_seo": ws.technical_seo,
        "content_depth": ws.content_depth,
        "geo_coverage": ws.local_signals,
        "keyword_overlap": kw_score,
        "trust_signals": trust_score,
        "conversion_elements": ws.conversion_elements,
    }


# Severity thresholds (pts)
GAP_MINOR = (5, 10)   # 5–10 pts
GAP_MAJOR = (10, 20)  # 10–20 pts
GAP_CRITICAL = 20     # 20+ pts


def _gap_severity(gap_delta: float) -> Optional[str]:
    """
    Flag severity when gap_delta exceeds threshold.
    gap_delta = |client_score - competitor_avg| (magnitude of gap).
    """
    d = abs(gap_delta)
    if d >= GAP_CRITICAL:
        return "critical"
    if d >= GAP_MAJOR[0]:
        return "major"
    if d >= GAP_MINOR[0]:
        return "minor"
    return None


def _build_gap_category(category: str, client_val: float, comp_avg: float, scale_100: bool = True) -> dict:
    """
    Build per-category gap analysis.
    gap = client_score - competitor_avg (negative when client behind).
    Scores scaled to 0-100 by default for readability.
    """
    if scale_100:
        client_score = round(client_val * 5, 0)  # 0-20 -> 0-100
        competitor_avg = round(comp_avg * 5, 0)
    else:
        client_score = round(client_val, 1)
        competitor_avg = round(comp_avg, 1)
    gap = int(client_score - competitor_avg)
    gap_delta = abs(gap)
    severity = _gap_severity(gap_delta)
    return {
        "category": category,
        "client_score": int(client_score),
        "competitor_avg": int(competitor_avg),
        "gap": gap,
        "severity": severity or "none",
        "flagged": severity is not None,
    }


class WebsiteGapAnalyzer:
    """
    Compares client website vs competitor averages.
    Inputs: client_website_id, competitor_website_ids[]
    Outputs: structured gap analysis JSON.
    """

    def __init__(self, db):
        self.db = db

    def analyze(
        self,
        client_website_id: int,
        competitor_website_ids: List[int],
    ) -> Dict[str, Any]:
        """
        Compare client website vs competitor averages.
        Returns gap analysis with required categories.
        """
        from database import Website

        client_site = self.db.query(Website).filter(Website.id == client_website_id).first()
        if not client_site:
            return {
                "error": "client_website_not_found",
                "technical_seo": {},
                "content_depth": {},
                "geo_coverage": {},
                "keyword_overlap": {},
                "trust_signals": {},
                "conversion_elements": {},
            }

        comp_sites = (
            self.db.query(Website).filter(Website.id.in_(competitor_website_ids)).all()
            if competitor_website_ids
            else []
        )
        client_scores = _extract_category_scores(client_site.extracted_profile)

        comp_scores_list = []
        for c in comp_sites:
            comp_scores_list.append(_extract_category_scores(c.extracted_profile))

        comp_avgs = {}
        for cat in client_scores:
            vals = [s[cat] for s in comp_scores_list if cat in s]
            comp_avgs[cat] = sum(vals) / len(vals) if vals else 0.0

        result = {
            "client_website_id": client_website_id,
            "client_domain": client_site.domain or "",
            "competitor_count": len(comp_sites),
            "technical_seo": _build_gap_category("technical_seo", client_scores["technical_seo"], comp_avgs.get("technical_seo", 0)),
            "content_depth": _build_gap_category("content_depth", client_scores["content_depth"], comp_avgs.get("content_depth", 0)),
            "geo_coverage": _build_gap_category("geo_coverage", client_scores["geo_coverage"], comp_avgs.get("geo_coverage", 0)),
            "keyword_overlap": _build_gap_category("keyword_overlap", client_scores["keyword_overlap"], comp_avgs.get("keyword_overlap", 0)),
            "trust_signals": _build_gap_category("trust_signals", client_scores["trust_signals"], comp_avgs.get("trust_signals", 0)),
            "conversion_elements": _build_gap_category("conversion_elements", client_scores["conversion_elements"], comp_avgs.get("conversion_elements", 0)),
        }
        return result


def populate_websites_from_research(db, client_id: str) -> List[int]:
    """
    Create Website rows from ResearchLog + Client for a client.
    Returns list of created website IDs (client + competitors).
    Call before using WebsiteGapAnalyzer if websites table is empty.
    """
    from database import Client, ResearchLog, Website
    from urllib.parse import urlparse

    ids = []
    client = db.query(Client).filter(Client.client_id == client_id).first()
    if client and client.website_url:
        domain = urlparse(client.website_url).netloc or client.website_url
        existing = db.query(Website).filter(Website.client_id == client_id, Website.domain == domain).first()
        if not existing:
            w = Website(
                domain=domain,
                base_url=client.website_url,
                extracted_profile=None,
                quality_score=client.avg_page_quality_score,
                client_id=client_id,
            )
            db.add(w)
            db.flush()
            ids.append(w.id)
    for rl in db.query(ResearchLog).filter(ResearchLog.client_id == client_id).all():
        profile = rl.extracted_profile or {}
        url = (profile.get("website_url") or "").strip()
        if not url:
            continue
        domain = urlparse(url).netloc or url
        existing = db.query(Website).filter(Website.research_log_id == rl.id).first()
        if not existing:
            q = getattr(rl, "competitor_comparison_score", None) or rl.website_quality_score
            w = Website(
                domain=domain,
                base_url=url,
                extracted_profile=profile,
                quality_score=float(q) if q is not None else None,
                research_log_id=rl.id,
            )
            db.add(w)
            db.flush()
            ids.append(w.id)
    db.commit()
    return ids
