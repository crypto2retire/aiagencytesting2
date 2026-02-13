"""
Post-processing filter and confidence scoring for keywords.
Even good LLM output needs validation before saving to DB.
Scores rank keywords: high-confidence money terms vs experimental phrases.
Geo phrase detection: normalize city+state to {service} {city} {state_abbrev}.

Industry-specific negative keywords: junk terms never enter scoring, learning, or recommendations.

Keyword Confidence (weighted factors, 0.0–1.0):
- frequency_weight = 0.30   (frequency across competitors)
- source_quality_weight = 0.25 (website quality of sources)
- keyword_type_weight = 0.20 (service_city > seo > geo)
- competitor_strength_weight = 0.15 (presence in top competitors)
- recency_weight = 0.10     (still relevant?)
"""

import json
import math
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional, Set

# Weighted confidence factors (design doc 1.2)
# Keyword confidence: frequency, title/H1 presence, geo relevance, low competition
FREQUENCY_WEIGHT = 0.30
TITLE_H1_WEIGHT = 0.25
GEO_RELEVANCE_WEIGHT = 0.25
LOW_COMPETITION_WEIGHT = 0.20
# Legacy (for backward compat in compute_keyword_confidence_weighted)
SOURCE_QUALITY_WEIGHT = 0.25
KEYWORD_TYPE_WEIGHT = 0.20
COMPETITOR_STRENGTH_WEIGHT = 0.15
RECENCY_WEIGHT = 0.10

from services_taxonomy import SERVICE_NOUNS, SERVICE_VERBS, EXCLUDED_TERMS

# Configurable negative keywords — path from verticals config
_NEGATIVE_CACHE: dict = {}  # vertical -> Set[str]


def _load_negative_keywords(vertical: str = "junk_removal") -> Set[str]:
    """Load negative keywords for vertical. Cached per vertical."""
    global _NEGATIVE_CACHE
    if vertical in _NEGATIVE_CACHE:
        return _NEGATIVE_CACHE[vertical]
    try:
        from verticals import get_negative_keywords_path
        path = get_negative_keywords_path(vertical)
        if path.exists():
            data = json.loads(path.read_text())
            terms = data.get(vertical, data.get("junk_removal", list(data.values())[0] if data else []))
            result = {str(t).lower().strip() for t in (terms or []) if t}
        else:
            result = set()
    except Exception:
        result = set()
    _NEGATIVE_CACHE[vertical] = result
    return result


def is_negative_keyword(keyword: str, vertical: str = "junk_removal") -> bool:
    """
    True if keyword contains or closely matches a negative term.
    Enforcement: remove immediately; do not score, store, or reference downstream.
    """
    if not keyword or not isinstance(keyword, str):
        return False
    k = keyword.lower().strip()
    if not k:
        return False
    neg = _load_negative_keywords(vertical)
    return any(term in k for term in neg)

# State name → abbreviation (no external APIs)
STATE_ABBREV = {
    "alabama": "al", "alaska": "ak", "arizona": "az", "arkansas": "ar",
    "california": "ca", "colorado": "co", "connecticut": "ct",
    "delaware": "de", "florida": "fl", "georgia": "ga", "hawaii": "hi",
    "idaho": "id", "illinois": "il", "indiana": "in", "iowa": "ia",
    "kansas": "ks", "kentucky": "ky", "louisiana": "la", "maine": "me",
    "maryland": "md", "massachusetts": "ma", "michigan": "mi",
    "minnesota": "mn", "mississippi": "ms", "missouri": "mo",
    "montana": "mt", "nebraska": "ne", "nevada": "nv", "new hampshire": "nh",
    "new jersey": "nj", "new mexico": "nm", "new york": "ny",
    "north carolina": "nc", "north dakota": "nd", "ohio": "oh",
    "oklahoma": "ok", "oregon": "or", "pennsylvania": "pa",
    "rhode island": "ri", "south carolina": "sc", "south dakota": "sd",
    "tennessee": "tn", "texas": "tx", "utah": "ut", "vermont": "vt",
    "virginia": "va", "washington": "wa", "west virginia": "wv",
    "wisconsin": "wi", "wyoming": "wy", "district of columbia": "dc",
}
STATE_ABBREV_INV = {v: v for v in STATE_ABBREV.values()}


def detect_and_normalize_geo_keyword(keyword: str) -> dict:
    """
    Detect city+state in keyword, normalize to {service} {city} {state_abbrev}.
    Returns: {service, geo, normalized_keyword, is_geo_phrase, confidence}
    """
    if not keyword or not isinstance(keyword, str):
        return {"service": "", "geo": "", "normalized_keyword": keyword or "", "is_geo_phrase": False, "confidence": 0.0}
    k = keyword.lower().strip()
    words = k.split()

    geo_part = ""
    service_part = k
    is_geo = False
    confidence = 0.75  # service-only default

    # Pattern: "... in City State" or "... City, State" or "... City State"
    # Try to find state (abbrev or full name)
    for i, w in enumerate(words):
        w_clean = w.strip(".,")
        if len(w_clean) == 2 and w_clean in STATE_ABBREV_INV:
            # Found state abbrev
            city_words = words[max(0, i - 1) : i] if i > 0 else []
            state_abbrev = w_clean
            # Find service (everything before city, or before "in")
            before = words[: max(0, i - 1)]
            if "in" in before:
                idx = before.index("in")
                service_part = " ".join(before[:idx])
            else:
                service_part = " ".join(before) if before else " ".join(words)
            geo_part = " ".join(city_words + [state_abbrev]).strip()
            if geo_part:
                is_geo = True
                confidence = 0.94
                break
        elif w_clean in STATE_ABBREV:
            state_abbrev = STATE_ABBREV[w_clean]
            city_words = words[max(0, i - 1) : i] if i > 0 else []
            before = words[: max(0, i - 1)]
            if "in" in before:
                idx = before.index("in")
                service_part = " ".join(before[:idx])
            else:
                service_part = " ".join(before) if before else " ".join(words)
            geo_part = " ".join(city_words + [state_abbrev]).strip()
            if geo_part:
                is_geo = True
                confidence = 0.94
                break

    # Normalize: remove "in", commas; lowercase
    if is_geo and service_part and geo_part:
        normalized = f"{service_part} {geo_part}".strip()
        normalized = re.sub(r"\s+", " ", normalized)
        return {
            "service": service_part.strip(),
            "geo": geo_part.strip(),
            "normalized_keyword": normalized,
            "is_geo_phrase": True,
            "confidence": confidence,
        }

    # Already in "service city st" format (e.g. "junk removal milwaukee wi")
    if re.search(r"\b(wi|az|tx|ca|fl|il|oh|mi|mn|co|nv|or|wa|ny|pa)\b", k):
        parts = k.split()
        for i, w in enumerate(parts):
            if len(w) == 2 and w in STATE_ABBREV_INV and i >= 1:
                geo_part = " ".join(parts[max(0, i - 1) : i + 1])
                service_part = " ".join(parts[: max(0, i - 1)])
                if service_part and (any(n in service_part for n in SERVICE_NOUNS) or any(v in service_part for v in SERVICE_VERBS)):
                    return {
                        "service": service_part.strip(),
                        "geo": geo_part.strip(),
                        "normalized_keyword": k,
                        "is_geo_phrase": True,
                        "confidence": 0.94,
                    }
                break

    return {
        "service": k,
        "geo": "",
        "normalized_keyword": k,
        "is_geo_phrase": False,
        "confidence": 0.75,
    }


def score_keyword(
    keyword: str,
    geo_terms: Optional[Iterable[str]] = None,
) -> int:
    """
    Confidence score 0-100. Enables ranking:
    - High-confidence money keywords
    - Experimental / emerging phrases
    - Unused opportunities
    """
    if not keyword or not isinstance(keyword, str):
        return 0
    k = keyword.lower().strip()
    if not k:
        return 0

    score = 0

    if any(noun in k for noun in SERVICE_NOUNS):
        score += 40

    if any(verb in k for verb in SERVICE_VERBS):
        score += 30

    if geo_terms:
        geo_lower = [g.lower().strip() for g in geo_terms if g and str(g).strip()]
        if any(geo in k for geo in geo_lower):
            score += 20

    if len(k.split()) >= 3:
        score += 10

    return min(score, 100)


def _normalize_frequency(frequency: int, max_frequency: int = 20) -> float:
    """Normalize frequency 0–1. Cap at max_frequency for scale."""
    if frequency <= 0:
        return 0.0
    if max_frequency <= 0:
        return 1.0
    return min(1.0, frequency / max_frequency)


def _get_attr(row: Any, name: str, default: Any = None) -> Any:
    """Get attribute from row (object or dict)."""
    if hasattr(row, name):
        return getattr(row, name)
    if isinstance(row, dict):
        return row.get(name, default)
    return default


def calculate_keyword_confidence(keyword_row: Any) -> float:
    """
    Compute keyword confidence 0.0–1.0 using:
    - Frequency across competitors
    - Presence in titles/H1s
    - Geo relevance (city/service match)
    - Low competition indicators

    confidence =
      frequency_score * 0.30 +
      title_h1_score * 0.25 +
      geo_relevance * 0.25 +
      low_competition * 0.20
    """
    freq = int(_get_attr(keyword_row, "frequency", 0) or 0)
    in_title = int(_get_attr(keyword_row, "in_title_h1_count", 0) or 0)
    type_weight = float(_get_attr(keyword_row, "keyword_type_weight", 0.5) or 0.5)
    top_count = int(_get_attr(keyword_row, "top_competitor_count", 0) or 0)
    avg_qual_raw = float(_get_attr(keyword_row, "avg_source_quality", 0) or 0)

    # 1. Frequency across competitors — log scale 0–1
    FREQ_MAX = 50
    frequency_score = math.log10(1 + max(0, freq)) / math.log10(1 + FREQ_MAX)
    frequency_score = min(1.0, frequency_score)

    # 2. Presence in titles/H1s — in_title_h1_count sources have keyword in title
    #    Normalize: 1+ sources = some boost, 3+ = strong signal
    TITLE_MAX = 5
    title_h1_score = min(1.0, in_title / TITLE_MAX) if in_title else 0.0

    # 3. Geo relevance — service_city > seo > geo (city/service match)
    geo_relevance = max(0.0, min(1.0, type_weight))

    # 4. Low competition — fewer competitors with this keyword = easier to rank
    #    top_competitor_count low = low competition = opportunity
    #    Also: low avg source quality of competitors = weak sites = easier to outrank
    comp_presence = min(1.0, top_count / 5.0) if top_count else 0.0
    low_competition = 1.0 - comp_presence
    # Bonus: if competitors have low site quality (avg < 50), it's easier
    avg_qual = avg_qual_raw / 100.0 if avg_qual_raw > 1 else avg_qual_raw
    if avg_qual > 0 and avg_qual < 0.5:
        low_competition = min(1.0, low_competition + 0.2)  # weak competitors = opportunity

    confidence = (
        frequency_score * FREQUENCY_WEIGHT +
        title_h1_score * TITLE_H1_WEIGHT +
        geo_relevance * GEO_RELEVANCE_WEIGHT +
        low_competition * LOW_COMPETITION_WEIGHT
    )
    return max(0.0, min(1.0, confidence))


def get_keyword_type_weight(keyword_type: Optional[str]) -> float:
    """service_city=1.0, seo=0.7, geo=0.4. Returns 0–1."""
    t = (keyword_type or "").strip().lower()
    if t in ("service_city", "service_geo"):
        return 1.0
    if t == "seo":
        return 0.7
    if t == "geo":
        return 0.4
    if t in ("service", "modifier", "long_tail"):
        return 0.6
    return 0.5


def _recency_score(last_seen: Optional[object]) -> float:
    """Still relevant? Recent = higher. Returns 0–1."""
    if last_seen is None:
        return 1.0
    if isinstance(last_seen, datetime):
        days = (datetime.utcnow() - last_seen).days
    else:
        try:
            dt = datetime.strptime(str(last_seen)[:10], "%Y-%m-%d")
            days = (datetime.utcnow() - dt).days
        except Exception:
            return 0.7
    if days < 7:
        return 1.0
    if days < 30:
        return 0.85
    if days < 90:
        return 0.6
    return 0.3


def compute_keyword_confidence_weighted(
    *,
    frequency: int = 0,
    max_frequency: int = 20,
    source_quality: Optional[float] = None,
    keyword_type: Optional[str] = None,
    competitor_strength: Optional[float] = None,
    recency_factor: Optional[float] = None,
    last_seen: Optional[object] = None,
) -> float:
    """
    Weighted confidence 0.0–1.0 using collected signals.
    Missing factors use neutral 0.5. Final score is clamped.
    """
    freq_norm = _normalize_frequency(frequency, max_frequency)
    type_score = get_keyword_type_weight(keyword_type)
    quality = (source_quality / 100.0) if source_quality is not None else 0.5
    strength = competitor_strength if competitor_strength is not None else 0.5
    recency = recency_factor if recency_factor is not None else _recency_score(last_seen)

    score = (
        FREQUENCY_WEIGHT * freq_norm
        + SOURCE_QUALITY_WEIGHT * quality
        + KEYWORD_TYPE_WEIGHT * type_score
        + COMPETITOR_STRENGTH_WEIGHT * strength
        + RECENCY_WEIGHT * recency
    )
    return max(0.0, min(1.0, score))


def score_keyword_confidence(
    keyword: str,
    geo_terms: Optional[Iterable[str]] = None,
    vertical: str = "junk_removal",
) -> float:
    """
    Confidence 0.0–1.0 based on service intent strength.
    Rubric:
    - Core service + city/state: 0.85–1.0
    - Core service only: 0.65–0.84
    - Ambiguous but related: 0.40–0.64
    - Generic or weak intent: below 0.40

    Negative keywords: do not score — return 0.0 immediately.
    """
    if not keyword or not isinstance(keyword, str):
        return 0.0
    k = keyword.lower().strip()
    if not k:
        return 0.0

    if is_negative_keyword(k, vertical=vertical):
        return 0.0

    has_service_noun = any(noun in k for noun in SERVICE_NOUNS)
    has_service_verb = any(verb in k for verb in SERVICE_VERBS)
    has_service = has_service_noun or has_service_verb

    has_geo = False
    if geo_terms:
        geo_lower = [g.lower().strip() for g in geo_terms if g and str(g).strip()]
        has_geo = any(geo in k for geo in geo_lower)
    # Also detect state abbreviations (wi, az, tx, ca, etc.)
    if re.search(r"\b(wi|az|tx|ca|fl|il|oh|mi|mn|co|nv|or|wa)\b", k):
        has_geo = True

    if any(term in k for term in EXCLUDED_TERMS):
        return 0.35  # generic / weak intent

    if has_service and has_geo:
        return 0.92  # core service + city/state
    if has_service:
        return 0.75  # core service only
    # Ambiguous but related (e.g. passes filter with weak match)
    return 0.52


def is_valid_keyword(keyword: str, vertical: str = "junk_removal") -> bool:
    """Pass only if keyword has service intent, no excluded terms, and no negative terms."""
    if not keyword or not isinstance(keyword, str):
        return False
    k = keyword.lower().strip()
    if not k:
        return False

    if is_negative_keyword(k, vertical=vertical):
        return False

    if any(term in k for term in EXCLUDED_TERMS):
        return False

    has_service_noun = any(noun in k for noun in SERVICE_NOUNS)
    has_service_verb = any(verb in k for verb in SERVICE_VERBS)

    return has_service_noun or has_service_verb
