"""
Website Quality Scorer — scores competitor sites from extracted_profile data.
Total: 0–100 (page_quality_score) across five categories (0–20 each):
- Page speed signals (structure, content length)
- Content depth
- Local signals (address, city mentions)
- Conversion elements (CTA, phone, forms)
- Technical SEO basics (meta, headings, trust)
"""

import json
from dataclasses import dataclass
from typing import Any, Optional, Tuple

from config import OLLAMA_MODEL, OLLAMA_TIMEOUT
from llm import run_ollama
from prompts.scoring import get_prompt as get_scoring_prompt


@dataclass
class WebsiteQualityScore:
    """
    Breakdown and total website quality score (page_quality_score 0–100).
    Five categories × 20 pts each.
    Legacy fields (seo_structure, conversion_readiness, technical_trust) kept for backward compat.
    """

    page_speed_signals: float = 0.0   # 0–20
    content_depth: float = 0.0        # 0–20
    local_signals: float = 0.0         # 0–20
    conversion_elements: float = 0.0   # 0–20
    technical_seo: float = 0.0        # 0–20
    total: float = 0.0                # 0–100 (page_quality_score)
    seo_structure: float = 0.0        # legacy
    conversion_readiness: float = 0.0  # legacy
    technical_trust: float = 0.0      # legacy

    @property
    def page_quality_score(self) -> float:
        """Stored as page_quality_score in competitor_geo_coverage etc."""
        return self.total

    def to_dict(self) -> dict:
        return {
            "page_speed_signals": self.page_speed_signals,
            "content_depth": self.content_depth,
            "local_signals": self.local_signals,
            "conversion_elements": self.conversion_elements,
            "technical_seo": self.technical_seo,
            "total": self.total,
            "page_quality_score": self.total,
        }


def _safe_list(obj: Any) -> list:
    """Return list from obj, or empty list."""
    if obj is None:
        return []
    return list(obj) if isinstance(obj, (list, tuple)) else []


def _score_page_speed_signals(profile: dict) -> float:
    """
    Page speed / UX signals (0–20): inferred from content structure.
    No real metrics; proxies: clean structure, service pages, lightweight content.
    """
    content = profile.get("content_signals") or {}
    primary = _safe_list(profile.get("primary_services"))
    secondary = _safe_list(profile.get("secondary_services"))
    score = 0.0
    # Good structure (multiple pages, sections) suggests organized site
    pages = content.get("service_pages_count_estimate")
    if isinstance(pages, (int, float)) and pages is not None:
        if pages >= 3:
            score += 8
        elif pages >= 1:
            score += 4
    # Service breadth = substantive content (not thin page)
    n_svc = len([s for s in primary + secondary if s and isinstance(s, str)])
    if n_svc >= 4:
        score += 6
    elif n_svc >= 2:
        score += 3
    # Clear heading structure from title_keywords (proxy for H1/H2)
    titles = _safe_list(content.get("title_keywords"))
    if len(titles) >= 2:
        score += 6
    elif len(titles) >= 1:
        score += 3
    # New extraction field if present
    tech = profile.get("technical_signals") or {}
    if tech.get("heading_structure_clear"):
        score += 4
    return min(score, 20.0)


def _score_content_depth(profile: dict) -> float:
    """
    Content Depth (0–20): service pages, blog, location pages, service breadth.
    """
    primary = _safe_list(profile.get("primary_services"))
    secondary = _safe_list(profile.get("secondary_services"))
    content = profile.get("content_signals") or {}

    score = 0.0
    n_primary = len([s for s in primary if s and isinstance(s, str)])
    n_secondary = len([s for s in secondary if s and isinstance(s, str)])
    if n_primary >= 3 or (n_primary + n_secondary) >= 5:
        score += 6
    elif n_primary >= 1:
        score += 3

    pages = content.get("service_pages_count_estimate")
    if isinstance(pages, (int, float)) and pages is not None:
        if pages >= 5:
            score += 5
        elif pages >= 2:
            score += 3
        elif pages >= 1:
            score += 2

    if content.get("blog_present"):
        score += 4
    if content.get("location_pages_present"):
        score += 4

    return min(score, 20.0)


def _score_local_signals(profile: dict) -> float:
    """
    Local signals (0–20): address, city mentions, geo targeting.
    """
    local = profile.get("local_signals") or {}
    service_city = _safe_list(profile.get("service_city_phrases"))
    geo = _safe_list(profile.get("geo_keywords"))
    score = 0.0
    if local.get("address_mentioned"):
        score += 7
    if local.get("phone_mentioned"):
        score += 6
    city_count = local.get("city_mentions_count")
    if isinstance(city_count, (int, float)) and city_count >= 3:
        score += 7
    elif isinstance(city_count, (int, float)) and city_count >= 1:
        score += 4
    else:
        n_sc = len([p for p in service_city if p and isinstance(p, str)])
        n_geo = len([g for g in geo if g and isinstance(g, str)])
        if n_sc >= 2 or n_geo >= 2:
            score += 5
        elif n_sc >= 1 or n_geo >= 1:
            score += 3
    return min(score, 20.0)


def _score_conversion_elements(profile: dict) -> float:
    """
    Conversion elements (0–20): CTA, phone, forms.
    """
    ctas = _safe_list(profile.get("calls_to_action"))
    conv = profile.get("conversion_signals") or {}
    score = 0.0
    n_cta = len([c for c in ctas if c and isinstance(c, str)])
    if n_cta >= 4:
        score += 10
    elif n_cta >= 2:
        score += 6
    elif n_cta >= 1:
        score += 3
    if conv.get("phone_in_cta"):
        score += 5
    elif any(re.search(r"call|phone|tel", (c or "").lower()) for c in ctas):
        score += 5
    if conv.get("form_or_quote_mentioned"):
        score += 5
    elif any(re.search(r"quote|estimate|form|contact", (c or "").lower()) for c in ctas):
        score += 3
    return min(score, 20.0)


def _score_technical_seo(profile: dict) -> float:
    """
    Technical SEO basics (0–20): meta, headings, trust signals.
    """
    trust = profile.get("trust_signals") or {}
    tech = profile.get("technical_signals") or {}
    content = profile.get("content_signals") or {}
    score = 0.0
    if tech.get("has_meta_description"):
        score += 5
    if tech.get("heading_structure_clear"):
        score += 4
    elif _safe_list(content.get("title_keywords")):
        score += 2
    if trust.get("reviews_mentioned"):
        score += 3
    if trust.get("years_in_business"):
        score += 2
    if trust.get("licenses_or_insurance"):
        score += 3
    if trust.get("guarantees"):
        score += 2
    return min(score, 20.0)


def score_website_quality_ollama(extracted_profile: Optional[dict]) -> Tuple[Optional[WebsiteQualityScore], Optional[str]]:
    """
    Score website using Ollama LLM with the scoring prompt.
    Returns (WebsiteQualityScore, notes) or (None, error_message) on failure.
    """
    if not extracted_profile or not isinstance(extracted_profile, dict):
        return None, "No extracted profile"

    profile_str = json.dumps(extracted_profile, indent=2)
    prompt = get_scoring_prompt(page_data=profile_str)

    try:
        data = run_ollama(prompt, model=OLLAMA_MODEL)
    except (json.JSONDecodeError, Exception):
        return None, "Ollama scoring failed or returned invalid JSON"
    if not isinstance(data, dict):
        return None, "Ollama scoring failed or returned invalid JSON"

    # New schema: overall_score, seo_score, content_score, local_seo_score, conversion_score, trust_score (1–10)
    # Map to internal 0–20 per category, 0–100 total
    def _scale(s: float) -> float:
        return max(0, min(20, float(s or 0) * 2))

    seo = _scale(data.get("seo_score"))
    content = _scale(data.get("content_score"))
    local = _scale(data.get("local_seo_score"))
    conversion = _scale(data.get("conversion_score"))
    trust = _scale(data.get("trust_score"))

    page_speed = seo  # structure, headings from SEO rubric
    technical = trust  # trust signals
    total = float(data.get("overall_score", 0) or 0) * 10
    if total <= 0:
        total = page_speed + content + local + conversion + technical
    total = max(0, min(100, total))

    issues = data.get("issues") or []
    opportunities = data.get("opportunities") or []
    notes = None
    if isinstance(issues, list) and issues:
        notes = "Issues: " + "; ".join(str(i) for i in issues[:5])
    if isinstance(opportunities, list) and opportunities:
        opp_str = "Opportunities: " + "; ".join(str(o) for o in opportunities[:5])
        notes = f"{notes}. {opp_str}" if notes else opp_str

    score = WebsiteQualityScore(
        page_speed_signals=page_speed,
        content_depth=content,
        local_signals=local,
        conversion_elements=conversion,
        technical_seo=technical,
        total=total,
        seo_structure=seo + technical * 0.5,
        conversion_readiness=conversion,
        technical_trust=technical,
    )
    return score, notes


def score_website_quality(extracted_profile: Optional[dict]) -> WebsiteQualityScore:
    """
    Score a website's quality from extracted_profile (Ollama JSON extraction).
    Returns breakdown and total page_quality_score (0–100).
    Five factors: page speed, content depth, local signals, conversion elements, technical SEO.
    """
    if not extracted_profile or not isinstance(extracted_profile, dict):
        return WebsiteQualityScore(total=0.0)

    page_speed = _score_page_speed_signals(extracted_profile)
    content = _score_content_depth(extracted_profile)
    local = _score_local_signals(extracted_profile)
    conversion = _score_conversion_elements(extracted_profile)
    technical = _score_technical_seo(extracted_profile)

    total = page_speed + content + local + conversion + technical

    return WebsiteQualityScore(
        page_speed_signals=page_speed,
        content_depth=content,
        local_signals=local,
        conversion_elements=conversion,
        technical_seo=technical,
        total=min(100.0, total),
        seo_structure=technical + local * 0.5,  # legacy approx
        conversion_readiness=conversion,
        technical_trust=technical,
    )
