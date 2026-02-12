"""
Keyword Extractor — lightweight extraction from text.
Runs after Firecrawl + Ollama summarization.
Extracts keywords, stores in keyword_intelligence. No embeddings. Structured only.
Service-intent gate: only service nouns/verbs pass; adjectives and fluff are rejected.
"""

import re
from datetime import datetime
from typing import List, Optional

from keyword_filter import detect_and_normalize_geo_keyword, is_valid_keyword, score_keyword_confidence
from keyword_history import update_keyword as update_keyword_history

from database import KeywordIntelligence, SessionLocal

STOPWORDS = frozenset([
    "the", "and", "for", "with", "that", "this", "from", "your",
    "are", "was", "have", "has", "you", "not", "but", "they",
    "all", "can", "her", "his", "been", "has", "had", "its",
    "our", "out", "who", "how", "why", "what", "when", "where",
])


def extract_keywords(text_blob: str) -> List[str]:
    """Extract alphanumeric words 3+ chars, lowercase, excluding stopwords."""
    if not text_blob or not isinstance(text_blob, str):
        return []
    words = re.findall(r"\b[a-zA-Z]{3,}\b", text_blob.lower())
    return [w for w in words if w not in STOPWORDS]


def store_keywords(
    keywords: List[str],
    region: str,
    source: str,
    client_id: Optional[str] = None,
    keyword_type: Optional[str] = None,
    vertical: Optional[str] = None,
) -> int:
    """
    Upsert keywords into keyword_intelligence.
    On (keyword, region) conflict: increment frequency, update last_seen.
    Returns count of keywords processed.
    """
    if not keywords or not region or not source:
        return 0

    v = (vertical or "junk_removal").strip().lower()

    # Dedupe to avoid unique constraint when same keyword appears multiple times in batch
    unique_keywords = list(dict.fromkeys(keywords))

    # Post-processing filter: reject adjectives, fluff, generic nouns — keep only search-intent terms
    unique_keywords = [kw for kw in unique_keywords if is_valid_keyword(kw, vertical=v)]

    db = SessionLocal()
    try:
        count = 0
        geo_terms = [region] if region else []
        for kw in unique_keywords:
            # Geo phrase detection: normalize and detect service vs service_geo
            geo_info = detect_and_normalize_geo_keyword(kw)
            normalized_kw = geo_info["normalized_keyword"]
            kw_type = keyword_type or ("service_geo" if geo_info["is_geo_phrase"] else "service")
            geo_phrase_val = geo_info["geo"] or None

            conf_float = score_keyword_confidence(normalized_kw, geo_terms=geo_terms, vertical=v)
            if geo_info["is_geo_phrase"]:
                conf_float = max(conf_float, geo_info["confidence"])
            conf = int(round(conf_float * 100))  # store as 0-100

            existing = db.query(KeywordIntelligence).filter(
                KeywordIntelligence.keyword == normalized_kw,
                KeywordIntelligence.region == region,
            ).first()

            now = datetime.utcnow()
            if existing:
                existing.frequency += 1
                existing.last_seen = now
                existing.confidence_score = max(existing.confidence_score or 0, conf)
                existing.keyword_type = kw_type
                if geo_phrase_val:
                    existing.geo_phrase = geo_phrase_val
                if client_id:
                    existing.client_id = client_id
                existing.source = source
            else:
                db.add(KeywordIntelligence(
                    keyword=normalized_kw,
                    keyword_type=kw_type,
                    geo_phrase=geo_phrase_val,
                    region=region,
                    source=source,
                    client_id=client_id,
                    frequency=1,
                    confidence_score=conf,
                    first_seen=now,
                    last_seen=now,
                ))
            count += 1
            # Decay tracking: file-based history (no DB)
            try:
                update_keyword_history(normalized_kw, conf_float, region=region)
            except Exception:
                pass
        db.commit()
        return count
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def extract_and_store(
    text_blob: str,
    region: str,
    source: str,
    client_id: Optional[str] = None,
    keyword_type: Optional[str] = None,
) -> int:
    """Extract keywords from text and store. Returns count stored."""
    keywords = extract_keywords(text_blob)
    return store_keywords(keywords, region, source, client_id, keyword_type)
