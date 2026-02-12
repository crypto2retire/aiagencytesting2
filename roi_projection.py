"""
ROI Projection Model — translate SEO opportunities into business outcomes.
Conservative assumptions. No external APIs. No guarantees.
Investor-grade framing: low / expected / high ranges.
"""

from typing import Any, Dict, Optional

# Junk removal industry defaults (configurable)
DEFAULT_AVG_JOB_VALUE = 350
CONVERSION_LOW = 0.003  # 0.3% combined (rank capture × lead conversion)
CONVERSION_EXPECTED = 0.013  # 1.3%
CONVERSION_HIGH = 0.03  # 3%

# Assumptions per user spec: rank capture 3–10%, conversion 1–5%
ASSUMPTIONS = [
    "Local service intent",
    "Mid-tier ranking achievable",
    "Industry-average conversion rates",
]


def estimate_monthly_searches(
    opportunity_score: int,
    has_geo: bool,
    service: str = "",
) -> int:
    """
    Heuristic monthly search volume. No external APIs.
    Based on: score (demand signal), geo (local intent boost).
    Conservative: 150–2000 range.
    """
    base = 80 + (opportunity_score or 0) * 10
    if has_geo:
        base = int(base * 1.15)
    return max(150, min(2000, base))


def compute_roi_projection(
    opportunity_score: int = 0,
    has_geo: bool = True,
    service: str = "",
    avg_job_value: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Deterministic ROI projection. No LLM, no APIs.
    Returns low / expected / high ranges for leads and revenue.
    """
    job_val = int(avg_job_value) if avg_job_value is not None else DEFAULT_AVG_JOB_VALUE
    monthly = estimate_monthly_searches(opportunity_score, has_geo, service)

    leads_low = max(0, int(round(monthly * CONVERSION_LOW)))
    leads_expected = max(0, int(round(monthly * CONVERSION_EXPECTED)))
    leads_high = max(0, int(round(monthly * CONVERSION_HIGH)))

    revenue_low = leads_low * job_val
    revenue_expected = leads_expected * job_val
    revenue_high = leads_high * job_val

    return {
        "monthly_searches": monthly,
        "estimated_leads": {
            "low": leads_low,
            "expected": leads_expected,
            "high": leads_high,
        },
        "estimated_revenue": {
            "low": revenue_low,
            "expected": revenue_expected,
            "high": revenue_high,
        },
        "assumptions": ASSUMPTIONS.copy(),
    }
