"""
Proposal Mapper — converts gap analysis into proposal-ready content.
Maps each flagged category to problem_statement, business_impact, recommended_solution, effort, price_range.
Uses learned correlations (gap type → deal size, severity → close rate) when db provided.
"""

from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

# Per-category templates: (problem_tpl, impact_tpl, solution_tpl, effort, price_min, price_max)
# {severity: multiplier for impact % and price}
CATEGORY_MAPPINGS = {
    "technical_seo": {
        "problem": "Client website lacks meta descriptions, clear heading structure, and technical SEO basics competitors have",
        "impact_tpl": "Losing ~{pct}% of organic visibility due to weaker technical foundation",
        "solution": "Implement meta tags, heading hierarchy, and schema markup",
        "effort": "Low",
        "price_range": (500, 1500),
    },
    "content_depth": {
        "problem": "Client has fewer service pages and less content depth than competitors",
        "impact_tpl": "Missing ~{pct}% of long-tail search demand",
        "solution": "Create dedicated service pages and expand content pillars",
        "effort": "Medium",
        "price_range": (1500, 4000),
    },
    "geo_coverage": {
        "problem": "Client website only targets {client_cities} cities while competitors target {comp_cities}+",
        "impact_tpl": "Missing ~{pct}% of local search traffic",
        "solution": "Create geo-specific service pages",
        "effort": "Medium",
        "price_range": (1500, 3000),
    },
    "keyword_overlap": {
        "problem": "Client misses key search terms competitors rank for",
        "impact_tpl": "Underperforming on ~{pct}% of high-intent keywords",
        "solution": "Audit and integrate missing keywords into content",
        "effort": "Medium",
        "price_range": (1000, 2500),
    },
    "trust_signals": {
        "problem": "Client lacks reviews, licenses, and trust elements competitors display",
        "impact_tpl": "Lower conversion rate—~{pct}% of visitors bounce without trust cues",
        "solution": "Add trust badges, reviews section, and credential mentions",
        "effort": "Low",
        "price_range": (500, 1200),
    },
    "conversion_elements": {
        "problem": "Client has weaker CTAs, phone visibility, and conversion paths than competitors",
        "impact_tpl": "Missing ~{pct}% of lead conversion opportunities",
        "solution": "Optimize CTAs, sticky phone/contact, and form placement",
        "effort": "Low",
        "price_range": (600, 1500),
    },
}

SEVERITY_MULTIPLIERS = {"minor": 0.5, "major": 0.75, "critical": 1.0}


class ProposalMapper:
    """
    Converts gap analysis into proposal-ready items.
    Input: gap analysis dict from WebsiteGapAnalyzer.analyze()
    Output: list of {problem_statement, business_impact, recommended_solution, estimated_effort, suggested_price_range}
    """

    def map_gaps_to_proposals(
        self,
        gap_analysis: Dict[str, Any],
        db: Optional[Session] = None,
    ) -> List[Dict[str, Any]]:
        """
        Convert flagged gap categories into proposal items.
        Only processes categories with flagged=True and severity in (minor, major, critical).
        When db provided, uses learned gap_type→deal_size and severity→close_rate to improve prices/ordering.
        """
        learned_prices = {}
        severity_order = {}
        if db:
            try:
                from proposal_outcomes import get_learned_price_adjustments, get_severity_close_rates
                learned_prices = get_learned_price_adjustments(db)
                close_rates = get_severity_close_rates(db)
                # Higher close rate = higher priority (sort first)
                for sev, cr in close_rates.items():
                    severity_order[sev] = cr.get("close_rate", 0)
            except Exception:
                pass

        results = []
        categories = [
            "technical_seo", "content_depth", "geo_coverage",
            "keyword_overlap", "trust_signals", "conversion_elements",
        ]
        for cat in categories:
            data = gap_analysis.get(cat) or {}
            if not isinstance(data, dict) or not data.get("flagged"):
                continue
            severity = (data.get("severity") or "").lower()
            if severity not in SEVERITY_MULTIPLIERS:
                continue
            mapping = CATEGORY_MAPPINGS.get(cat, {})
            if not mapping:
                continue
            mult = SEVERITY_MULTIPLIERS[severity]
            pct = min(90, int(abs(data.get("gap", 0)) * mult))
            if cat in learned_prices:
                low, high = learned_prices[cat]
                price_low = int(low * mult)
                price_high = int(high * mult)
            else:
                low, high = mapping.get("price_range", (500, 1500))
                price_low = int(low * mult)
                price_high = int(high * mult)
            problem = mapping.get("problem", "")
            impact = (mapping.get("impact_tpl") or "").replace("{pct}", str(pct))
            # Geo: derive city counts from scores (0-100 scale)
            if "{client_cities}" in problem or "{comp_cities}" in problem:
                cs = data.get("client_score", 0) or 0
                ca = data.get("competitor_avg", 0) or 0
                client_cities = max(1, min(5, cs // 15))
                comp_cities = max(6, min(15, ca // 10))
                problem = problem.replace("{client_cities}", str(client_cities))
                problem = problem.replace("{comp_cities}", str(comp_cities))
            results.append({
                "category": cat,
                "problem_statement": problem,
                "business_impact": impact,
                "recommended_solution": mapping.get("solution", ""),
                "estimated_effort": mapping.get("effort", "Medium"),
                "suggested_price_range": f"${price_low:,}–${price_high:,}",
                "severity": severity,
                "gap": data.get("gap"),
            })
        # Sort by severity close rate (higher first) when we have learned data
        if severity_order:
            results.sort(
                key=lambda r: -(severity_order.get((r.get("severity") or "").lower(), 0)),
            )
        return results
