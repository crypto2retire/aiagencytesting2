"""
Performance Ingestor — manual ingestion of content performance metrics.
Saves to content_performance and increments keyword_performance totals.
Guards: impressions < 20 ignored; confidence decay 10% per 30 days; declining flags.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from sqlalchemy import func

from config import PERFORMANCE_LOG
from database import ContentDraft, ContentPerformance, KeywordPerformance, SessionLocal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(PERFORMANCE_LOG), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

MIN_IMPRESSIONS_FOR_INGEST = 20
DECAY_INTERVAL_DAYS = 30
DECAY_RATE = 0.9  # 10% decay per 30 days


def ingest_manual_performance(
    content_id: int,
    impressions: int = 0,
    clicks: int = 0,
    calls: int = 0,
    directions: int = 0,
) -> bool:
    """
    Ingest performance data for a content draft.
    - Inserts a row into content_performance
    - Increments keyword_performance totals for associated extracted_keywords & extracted_geo_phrases
    Returns True on success, False if content_id not found or impressions < 20.
    """
    if impressions < MIN_IMPRESSIONS_FOR_INGEST:
        log.info("ingest_ignored content_id=%s impressions=%s (below threshold %s)", content_id, impressions, MIN_IMPRESSIONS_FOR_INGEST)
        return False

    db = SessionLocal()
    try:
        draft = db.query(ContentDraft).filter(ContentDraft.id == content_id).first()
        if not draft:
            log.warning("ingest_skipped content_id=%s reason=content_not_found", content_id)
            return False

        # Apply decay to stale keywords before ingesting
        decayed = apply_confidence_decay(db)
        if decayed:
            log.info("decay_applied count=%s keywords=%s", len(decayed), [d[0] for d in decayed[:5]])

        # 1. Save to content_performance
        row = ContentPerformance(
            content_id=content_id,
            impressions=impressions,
            clicks=clicks,
            calls=calls,
            direction_requests=directions,
            recorded_at=datetime.utcnow(),
        )
        db.add(row)
        db.commit()
        log.info("content_performance_saved content_id=%s impressions=%s clicks=%s calls=%s directions=%s", content_id, impressions, clicks, calls, directions)

        # 2. Get associated keywords and geo phrases
        keywords = _as_string_list(draft.extracted_keywords)
        geo_phrases = _as_string_list(draft.extracted_geo_phrases)

        # 3. Increment keyword_performance for each keyword (geo_phrase=None)
        now = datetime.utcnow()
        for kw in keywords:
            if not kw or not str(kw).strip():
                continue
            _upsert_increment_keyword(db, str(kw).strip(), None, impressions, clicks, calls, directions, now)

        # 4. Increment keyword_performance for each geo phrase (keyword=phrase, geo_phrase=phrase)
        for phrase in geo_phrases:
            if not phrase or not str(phrase).strip():
                continue
            p = str(phrase).strip()
            _upsert_increment_keyword(db, p, p, impressions, clicks, calls, directions, now)

        db.commit()
        log.info("keyword_performance_updated content_id=%s keywords=%s geo_phrases=%s", content_id, len(keywords), len(geo_phrases))
        return True
    except Exception as e:
        db.rollback()
        log.exception("ingest_failed content_id=%s error=%s", content_id, e)
        raise
    finally:
        db.close()


def apply_confidence_decay(db) -> List[Tuple[str, float, float]]:
    """
    Apply 10% decay per 30 days to keywords with no new data.
    Returns list of (keyword, old_score, new_score) for flagged keywords.
    Sets confidence_declining=1 when decay is applied.
    """
    now = datetime.utcnow()
    cutoff = now - timedelta(days=DECAY_INTERVAL_DAYS)
    rows = db.query(KeywordPerformance).filter(
        KeywordPerformance.last_updated < cutoff,
        KeywordPerformance.confidence_score.isnot(None),
    ).all()
    decayed = []
    for row in rows:
        old_conf = float(row.confidence_score or 0)
        if old_conf <= 0:
            continue
        days_stale = (now - row.last_updated).days
        periods = max(1, days_stale // DECAY_INTERVAL_DAYS)
        multiplier = DECAY_RATE ** periods
        new_conf = max(0.0, min(1.0, old_conf * multiplier))
        row.confidence_score = new_conf
        row.confidence_declining = 1
        # Do NOT update last_updated — decay continues each 30 days until new data arrives
        decayed.append((f"{row.keyword}" + (f" ({row.geo_phrase})" if row.geo_phrase else ""), old_conf, new_conf))
        log.info("decay_applied keyword=%s geo=%s old=%.3f new=%.3f periods=%s", row.keyword, row.geo_phrase, old_conf, new_conf, periods)
    if decayed:
        db.commit()
    return decayed


def get_declining_keywords(db) -> List[dict]:
    """Return keywords flagged with confidence_declining=1."""
    rows = db.query(KeywordPerformance).filter(KeywordPerformance.confidence_declining == 1).all()
    return [{"keyword": r.keyword, "geo_phrase": r.geo_phrase, "confidence_score": r.confidence_score} for r in rows]


def _as_string_list(val) -> list:
    """Normalize JSON/list to list of strings."""
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x).strip() for x in val if x is not None and str(x).strip()]
    if isinstance(val, str):
        return [val.strip()] if val.strip() else []
    return []


def _compute_confidence(impressions: int, clicks: int, calls: int, direction_requests: int) -> float:
    """
    Formula v1: (clicks*2 + calls*5 + direction_requests*3) / max(impressions, 1)
    Normalized to 0-1, capped at 1.0. Max raw = 10 when all impressions convert.
    """
    denom = max(impressions, 1)
    raw = (clicks * 2 + calls * 5 + direction_requests * 3) / denom
    normalized = min(raw / 10.0, 1.0)
    return max(0.0, min(1.0, normalized))


def _upsert_increment_keyword(
    db,
    keyword: str,
    geo_phrase: Optional[str],
    impressions: int,
    clicks: int,
    calls: int,
    direction_requests: int,
    last_updated: datetime,
) -> None:
    """
    Find or create keyword_performance row and increment totals.
    Updates confidence_score using formula v1.
    """
    kw_norm = keyword.strip() if keyword else ""
    geo_norm = geo_phrase.strip() if geo_phrase else None
    if geo_norm == "":
        geo_norm = None

    q = db.query(KeywordPerformance).filter(func.lower(KeywordPerformance.keyword) == kw_norm.lower())
    if geo_norm is None:
        q = q.filter(KeywordPerformance.geo_phrase.is_(None))
    else:
        q = q.filter(func.lower(KeywordPerformance.geo_phrase) == geo_norm.lower())
    row = q.first()

    if row:
        row.impressions = (row.impressions or 0) + impressions
        row.clicks = (row.clicks or 0) + clicks
        row.calls = (row.calls or 0) + calls
        row.direction_requests = (row.direction_requests or 0) + direction_requests
        row.confidence_score = _compute_confidence(
            row.impressions, row.clicks, row.calls, row.direction_requests
        )
        row.confidence_declining = 0  # Clear flag when new data arrives
        row.last_updated = last_updated
    else:
        conf = _compute_confidence(impressions, clicks, calls, direction_requests)
        db.add(
            KeywordPerformance(
                keyword=kw_norm,
                geo_phrase=geo_norm,
                impressions=impressions,
                clicks=clicks,
                calls=calls,
                direction_requests=direction_requests,
                confidence_score=conf,
                last_updated=last_updated,
            )
        )
