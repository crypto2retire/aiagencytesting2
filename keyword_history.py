"""
Keyword Decay Tracking — file-based history for investor-grade data moat.

Tracks first_seen, last_seen, usage_count, avg_confidence in data/keyword_history.json.
Updated on every research run. No database required.

Decay logic:
- Frequently repeated keywords lose novelty value
- Keywords not seen for 30+ days regain novelty
- High-confidence + low-frequency keywords score highest
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

HISTORY_PATH = Path(__file__).resolve().parent / "data" / "keyword_history.json"
STALE_DAYS = 30


def _ensure_data_dir():
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)


def load_history() -> Dict[str, dict]:
    """Load keyword history from JSON. Returns {} if missing or invalid."""
    _ensure_data_dir()
    try:
        if HISTORY_PATH.exists():
            return json.loads(HISTORY_PATH.read_text())
    except Exception:
        pass
    return {}


def save_history(data: Dict[str, dict]) -> None:
    """Persist keyword history to JSON."""
    _ensure_data_dir()
    HISTORY_PATH.write_text(json.dumps(data, indent=2))


def update_keyword(keyword: str, confidence: float, region: Optional[str] = None) -> None:
    """
    Update history for a keyword. Call on every research run when keyword is stored.
    confidence: 0.0-1.0
    """
    data = load_history()
    key = keyword.lower().strip()
    if not key:
        return

    today = datetime.utcnow().strftime("%Y-%m-%d")
    now = datetime.utcnow()

    if key not in data:
        data[key] = {
            "first_seen": today,
            "last_seen": today,
            "usage_count": 1,
            "avg_confidence": round(confidence, 2),
        }
    else:
        rec = data[key]
        count = rec.get("usage_count", 0) + 1
        old_avg = rec.get("avg_confidence", 0.5)
        new_avg = round((old_avg * (count - 1) + confidence) / count, 2)
        data[key] = {
            "first_seen": rec.get("first_seen", today),
            "last_seen": today,
            "usage_count": count,
            "avg_confidence": new_avg,
        }
    save_history(data)


def get_decay_factor(keyword: str) -> float:
    """
    Decay factor 0.0-1.0 for scoring.
    - High confidence + low frequency + recently stale (30+ days) → 1.0 (full value)
    - Frequently repeated → lower (lose novelty)
    - Not seen for 30+ days → regain novelty (boost)
    """
    data = load_history()
    key = keyword.lower().strip()
    if key not in data:
        return 1.0  # New keyword, full novelty

    rec = data[key]
    count = rec.get("usage_count", 0)
    last_seen = rec.get("last_seen", "")
    avg_conf = rec.get("avg_confidence", 0.5)

    try:
        last_dt = datetime.strptime(last_seen, "%Y-%m-%d")
    except Exception:
        last_dt = datetime.utcnow()
    days_since = (datetime.utcnow() - last_dt).days

    # Frequently repeated → lose novelty (1.0 → 0.3 as count grows)
    if count >= 10:
        freq_decay = 0.3
    elif count >= 5:
        freq_decay = 0.5
    elif count >= 3:
        freq_decay = 0.7
    else:
        freq_decay = 1.0

    # Not seen for 30+ days → regain novelty (boost up to 1.2)
    if days_since >= STALE_DAYS:
        novelty_boost = min(1.2, 0.8 + (days_since / 100))
    else:
        novelty_boost = 1.0

    return min(1.0, freq_decay * novelty_boost)
