"""
Vertical Config — industry-parameterized, rule-driven, config-based.
One system → multiple home services. No hard-coded vertical logic.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

VERTICALS_PATH = Path(__file__).resolve().parent / "config" / "verticals.json"
CONFIG_DIR = Path(__file__).resolve().parent / "config"
DEFAULT_VERTICAL = "junk_removal"

_CACHE: Optional[Dict[str, dict]] = None


def _load_verticals() -> Dict[str, dict]:
    """Load vertical config. Cached."""
    global _CACHE
    if _CACHE is not None:
        return _CACHE
    try:
        if VERTICALS_PATH.exists():
            _CACHE = json.loads(VERTICALS_PATH.read_text())
            return _CACHE
    except Exception:
        pass
    _CACHE = {}
    return _CACHE


def get_vertical_config(vertical: Optional[str] = None) -> dict:
    """
    Get config for a vertical. Falls back to default if missing.
    Returns: {average_job_value, core_services, niche, negative_keywords, opportunity_services}
    """
    v = (vertical or "").strip().lower() or DEFAULT_VERTICAL
    data = _load_verticals()
    cfg = data.get(v, data.get(DEFAULT_VERTICAL, {}))
    return dict(cfg)


def get_opportunity_services(vertical: Optional[str] = None) -> List[str]:
    """Services to score for opportunities. Excludes services no content should be created for."""
    cfg = get_vertical_config(vertical)
    svc = cfg.get("opportunity_services", cfg.get("core_services", []))
    excluded = {s.lower().strip() for s in cfg.get("excluded_from_content", []) if s}
    lst = list(svc) if svc else ["general service"]
    return [s for s in lst if (s or "").lower().strip() not in excluded]


def is_excluded_from_content(service: str, vertical: Optional[str] = None) -> bool:
    """True if this service should not have content created for it."""
    cfg = get_vertical_config(vertical)
    excluded = {s.lower().strip() for s in cfg.get("excluded_from_content", []) if s}
    return (service or "").lower().strip() in excluded


def get_niche(vertical: Optional[str] = None) -> str:
    """Search niche for Tavily/research. From vertical config."""
    cfg = get_vertical_config(vertical)
    return cfg.get("niche", "Junk Removal")


def get_average_job_value(vertical: Optional[str] = None) -> int:
    """Average job value for ROI projection."""
    cfg = get_vertical_config(vertical)
    return int(cfg.get("average_job_value", 350))


def get_negative_keywords_path(vertical: Optional[str] = None) -> Path:
    """Path to negative keywords file for this vertical."""
    cfg = get_vertical_config(vertical)
    fname = cfg.get("negative_keywords", "negative_keywords.json")
    return CONFIG_DIR / fname


def list_verticals() -> List[str]:
    """Available vertical keys."""
    return list(_load_verticals().keys())
