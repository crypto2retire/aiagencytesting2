"""
Seasonality Detection — observe patterns, do NOT predict the future.
Maps keywords to seasonal clusters. Boosts confidence when service aligns with current season.
Out-of-season opportunities are NOT filtered; they simply don't get the boost.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

RULES_PATH = Path(__file__).resolve().parent / "config" / "seasonality_rules.json"
SEASONAL_BOOST = 0.15  # 15% score boost when service matches current season

# Month (1-12) → season
MONTH_TO_SEASON = {
    1: "winter", 2: "winter", 3: "spring",
    4: "spring", 5: "spring", 6: "summer",
    7: "summer", 8: "summer", 9: "fall",
    10: "fall", 11: "fall", 12: "winter",
}


def _load_rules(industry: str = "junk_removal") -> Dict[str, List[str]]:
    """Load seasonal clusters per industry."""
    try:
        if RULES_PATH.exists():
            data = json.loads(RULES_PATH.read_text())
            return data.get(industry, data.get("junk_removal", {}))
    except Exception:
        pass
    return {}


def get_current_season() -> str:
    """Return current season: spring, summer, fall, winter."""
    return MONTH_TO_SEASON.get(datetime.utcnow().month, "spring")


def _service_matches_season(service: str, seasonal_keywords: List[str]) -> bool:
    """
    True if service matches any keyword in the seasonal cluster.
    Match: service contains keyword OR keyword contains service (normalized, lowercase).
    """
    if not service or not seasonal_keywords:
        return False
    s = (service or "").strip().lower()
    if not s:
        return False
    for kw in seasonal_keywords:
        k = (kw or "").strip().lower()
        if not k:
            continue
        if k in s or s in k:
            return True
    return False


def check_seasonality(
    service: str,
    industry: str = "junk_removal",
) -> dict:
    """
    Detect if service aligns with current season.
    Returns:
        {
            "current_season": "spring",
            "match": true,
            "boost_applied": 0.15  # or 0 if no match
        }
    """
    current = get_current_season()
    rules = _load_rules(industry)
    seasonal_keywords = rules.get(current, [])

    match = _service_matches_season(service, seasonal_keywords)
    boost = SEASONAL_BOOST if match else 0.0

    return {
        "current_season": current,
        "match": match,
        "boost_applied": boost,
    }
