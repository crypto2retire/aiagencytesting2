"""
Geo Phrase Confidence — normalized 0–1 score for service+city phrases.

Factors:
- Frequency (log scale)
- Avg source website quality (0–100 → 0–1)
- Keyword confidence of base terms (from KeywordIntelligence)
- City population weight (optional, for later)
"""

import math
from typing import Any, Optional


# Weights for confidence factors (sum = 1.0)
FREQUENCY_WEIGHT = 0.35
SOURCE_QUALITY_WEIGHT = 0.30
KEYWORD_CONFIDENCE_WEIGHT = 0.25
CITY_POPULATION_WEIGHT = 0.10  # Optional; use 1.0 when not provided

FREQ_MAX = 50  # frequency at which frequency_score = 1.0


def _get_attr(obj: Any, name: str, default: Any = None) -> Any:
    """Get attribute from object or dict."""
    if obj is None:
        return default
    if hasattr(obj, name):
        return getattr(obj, name, default)
    if isinstance(obj, dict):
        return obj.get(name, default)
    return default


def calculate_geo_phrase_confidence(
    phrase_row: Any = None,
    *,
    frequency: Optional[int] = None,
    avg_source_quality: Optional[float] = None,
    keyword_confidence: Optional[float] = None,
    city_population_weight: Optional[float] = None,
) -> float:
    """
    Compute geo phrase confidence 0.0–1.0.

    confidence =
      frequency_score * 0.35 +
      avg_source_quality * 0.30 +
      keyword_confidence * 0.25 +
      city_population_weight * 0.10

    Args:
        phrase_row: GeoPhraseIntelligence-like row (optional). If provided,
            frequency and avg_source_quality are read from it.
        frequency: Override or supply when no row. Default 0.
        avg_source_quality: 0–100 or 0–1. Override or supply. Default 0.
        keyword_confidence: Avg confidence of base terms from KeywordIntelligence.
            0–1. Default 0.5 when not provided.
        city_population_weight: 0–1. Optional for later. Default 1.0 (no penalty).

    Returns:
        Confidence clamped to [0, 1].
    """
    if phrase_row is not None:
        freq = int(_get_attr(phrase_row, "frequency", 0) or 0)
        avg_qual_raw = float(_get_attr(phrase_row, "avg_source_quality", 0) or 0)
    else:
        freq = int(frequency if frequency is not None else 0)
        avg_qual_raw = float(avg_source_quality if avg_source_quality is not None else 0)

    kw_conf = keyword_confidence if keyword_confidence is not None else 0.5
    city_weight = city_population_weight if city_population_weight is not None else 1.0

    # frequency_score: log scale 0–1
    frequency_score = math.log10(1 + max(0, freq)) / math.log10(1 + FREQ_MAX)
    frequency_score = min(1.0, frequency_score)

    # avg_source_quality: 0–100 → 0–1
    if avg_qual_raw > 1:
        source_quality = avg_qual_raw / 100.0
    else:
        source_quality = avg_qual_raw
    source_quality = max(0.0, min(1.0, source_quality))

    # keyword_confidence: already 0–1
    kw_conf = max(0.0, min(1.0, kw_conf))

    # city_population_weight: 0–1 (default 1.0 when omitted)
    city_weight = max(0.0, min(1.0, city_weight))

    confidence = (
        frequency_score * FREQUENCY_WEIGHT
        + source_quality * SOURCE_QUALITY_WEIGHT
        + kw_conf * KEYWORD_CONFIDENCE_WEIGHT
        + city_weight * CITY_POPULATION_WEIGHT
    )
    return max(0.0, min(1.0, confidence))


def get_keyword_confidence_for_phrase(
    db,
    service: str,
    city: Optional[str] = None,
    region: Optional[str] = None,
) -> float:
    """
    Fetch avg confidence of KeywordIntelligence rows matching this phrase's base terms.
    Use when calculating geo phrase confidence and keyword data is available.

    Args:
        db: SQLAlchemy session
        service: Service part of phrase (e.g. "junk removal")
        city: City part (optional)
        region: Region/geo for lookup (optional, used if city not set)

    Returns:
        Average confidence 0–1 of matching keywords, or 0.5 if none found.
    """
    if not db or not service:
        return 0.5

    try:
        from sqlalchemy import or_
        from database import KeywordIntelligence

        geo = (city or region or "").strip().lower()
        service_lower = (service or "").strip().lower()
        if not service_lower:
            return 0.5

        q = db.query(KeywordIntelligence).filter(
            KeywordIntelligence.keyword.ilike(f"%{service_lower}%")
        )
        if geo:
            q = q.filter(
                or_(
                    KeywordIntelligence.region.ilike(f"%{geo}%"),
                    KeywordIntelligence.geo_phrase.ilike(f"%{geo}%"),
                )
            )

        rows = q.limit(20).all()
        if not rows:
            return 0.5

        total = 0.0
        count = 0
        for r in rows:
            raw = float(r.confidence_score or 0)
            if raw > 1:
                raw /= 100.0
            total += max(0.0, min(1.0, raw))
            count += 1

        return total / count if count else 0.5
    except Exception:
        return 0.5
