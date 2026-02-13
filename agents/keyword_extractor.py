"""
Keyword Extractor — lightweight extraction from text.
Runs after Firecrawl + Ollama summarization.
Extracts keywords, stores in keyword_intelligence. No embeddings. Structured only.
Service-intent gate: only service nouns/verbs pass; adjectives and fluff are rejected.
"""

import re
from datetime import datetime
from typing import List, Optional

from keyword_filter import (
    calculate_keyword_confidence,
    compute_keyword_confidence_weighted,
    detect_and_normalize_geo_keyword,
    get_keyword_type_weight,
    is_valid_keyword,
    score_keyword_confidence,
)
from keyword_history import update_keyword as update_keyword_history

from sqlalchemy import func, or_

from database import Client, KeywordIntelligence, MarketSnapshot, SessionLocal

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
    source_url: Optional[str] = None,
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

            # Base confidence from intent (service+geo)
            base_conf = score_keyword_confidence(normalized_kw, geo_terms=geo_terms, vertical=v)
            if geo_info["is_geo_phrase"]:
                base_conf = max(base_conf, geo_info["confidence"])

            existing = db.query(KeywordIntelligence).filter(
                KeywordIntelligence.keyword == normalized_kw,
                KeywordIntelligence.region == region,
            ).first()

            now = datetime.utcnow()
            city_parsed, state_parsed = _parse_city_state(region)
            if existing:
                existing.frequency += 1
                existing.last_seen = now
                existing.keyword_type_weight = get_keyword_type_weight(kw_type)
                existing.last_confidence_update = now
                if city_parsed:
                    existing.city = city_parsed
                if state_parsed:
                    existing.state = state_parsed
                freq = existing.frequency or 1
                type_score = existing.keyword_type or kw_type
                conf = compute_keyword_confidence_weighted(
                    frequency=freq,
                    max_frequency=20,
                    keyword_type=type_score,
                    last_seen=now,
                )
                conf = max(conf, base_conf)  # never lower than intent-based
                stored = max(float(existing.confidence_score or 0.5), conf)
                if stored > 1:
                    stored = stored / 100.0
                existing.confidence_score = stored
                existing.keyword_confidence_score = min(1.0, max(0.0, stored))
                existing.keyword_type = kw_type
                if geo_phrase_val:
                    existing.geo_phrase = geo_phrase_val
                if client_id:
                    existing.client_id = client_id
                if source_url:
                    existing.source_url = source_url
                existing.source = source
            else:
                conf = compute_keyword_confidence_weighted(
                    frequency=1,
                    max_frequency=20,
                    keyword_type=kw_type,
                    last_seen=now,
                )
                conf = max(conf, base_conf)
                stored_conf = min(1.0, max(0.0, conf if conf <= 1 else conf / 100.0))
                db.add(KeywordIntelligence(
                    keyword=normalized_kw,
                    keyword_type=kw_type,
                    geo_phrase=geo_phrase_val,
                    region=region,
                    city=city_parsed or region,
                    state=state_parsed,
                    source=source,
                    source_url=source_url,
                    client_id=client_id,
                    frequency=1,
                    confidence_score=stored_conf,
                    keyword_confidence_score=stored_conf,
                    first_seen=now,
                    last_seen=now,
                    keyword_type_weight=get_keyword_type_weight(kw_type),
                    last_confidence_update=now,
                ))
            count += 1
            # Decay tracking: file-based history (no DB)
            try:
                update_keyword_history(normalized_kw, max(conf, base_conf), region=region)
            except Exception:
                pass
        db.commit()
        return count
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def recalculate_keyword_confidence(client_id: str) -> int:
    """
    Recalculate confidence scores for all keywords for a client.
    Runs after each research session. Updates confidence_score and last_confidence_update.
    Returns count of keywords updated.
    """
    db = SessionLocal()
    try:
        client = db.query(Client).filter(
            func.lower(Client.client_id) == client_id.lower()
        ).first()
        if not client:
            return 0

        cid = client.client_id

        # Regions = cities_served + snapshot cities
        regions = [c.strip() for c in (client.cities_served or []) if c and str(c).strip()]
        for s in db.query(MarketSnapshot).filter(MarketSnapshot.client_id == cid).all():
            if s.city and s.city.strip() and s.city.strip() not in regions:
                regions.append(s.city.strip())

        if regions:
            rows = db.query(KeywordIntelligence).filter(
                or_(
                    KeywordIntelligence.client_id == cid,
                    KeywordIntelligence.region.in_(regions),
                )
            ).all()
        else:
            rows = db.query(KeywordIntelligence).filter(
                KeywordIntelligence.client_id == cid
            ).all()

        now = datetime.utcnow()
        updated = 0
        for row in rows:
            new_conf = calculate_keyword_confidence(row)
            row.confidence_score = new_conf
            row.keyword_confidence_score = new_conf
            row.last_confidence_update = now
            updated += 1

        db.commit()
        return updated
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


def _parse_city_state(region: str) -> tuple[str, Optional[str]]:
    """Parse 'Charlotte NC' -> ('Charlotte', 'NC'). Returns (city, state)."""
    if not region or not isinstance(region, str):
        return ("", None)
    parts = region.strip().split()
    if len(parts) >= 2 and len(parts[-1]) == 2:
        return (" ".join(parts[:-1]), parts[-1].upper())
    return (region.strip(), None)


def upsert_keyword(
    db,
    keyword: str,
    region: str,
    keyword_type: str = "seo",
    *,
    source_url: Optional[str] = None,
    company_name: Optional[str] = None,
    client_id: Optional[str] = None,
    confidence: float = 0.5,
    vertical: Optional[str] = None,
    now: Optional[datetime] = None,
    source_quality: Optional[int] = None,
    competitor_strength: Optional[float] = None,
    in_title_or_h1: bool = False,
) -> bool:
    """
    Insert new keyword or increment frequency and update last_seen when it already exists.
    Uses (keyword, region) for uniqueness. Requires db session. Returns True if upserted.
    """
    if not keyword or not isinstance(keyword, str):
        return False
    normalized = keyword.strip().lower()
    if not normalized or len(normalized) < 2:
        return False
    if not region:
        return False

    v = (vertical or "junk_removal").strip().lower()
    if not is_valid_keyword(normalized, vertical=v):
        return False

    city, state = _parse_city_state(region)
    now = now or datetime.utcnow()

    # Weighted confidence when we have rich context
    if source_quality is not None or competitor_strength is not None:
        conf = compute_keyword_confidence_weighted(
            frequency=1,
            max_frequency=20,
            source_quality=float(source_quality) if source_quality is not None else None,
            keyword_type=keyword_type,
            competitor_strength=competitor_strength,
            last_seen=now,
        )
        confidence = max(confidence, conf)

    existing = db.query(KeywordIntelligence).filter(
        KeywordIntelligence.keyword == normalized,
        KeywordIntelligence.region == region,
    ).first()

    if existing:
        freq = (existing.frequency or 0) + 1
        existing.frequency = freq
        existing.last_seen = now

        # avg_source_quality: running average
        if source_quality is not None:
            old_avg = float(existing.avg_source_quality or 0)
            old_freq = freq - 1
            if old_freq <= 0:
                existing.avg_source_quality = float(source_quality)
            else:
                existing.avg_source_quality = (old_avg * old_freq + source_quality) / freq

        # top_competitor_count: increment when source website score > 70
        if source_quality is not None and source_quality > 70:
            existing.top_competitor_count = (existing.top_competitor_count or 0) + 1

        # keyword_type_weight and last_confidence_update
        existing.keyword_type_weight = get_keyword_type_weight(keyword_type)
        existing.last_confidence_update = now
        if in_title_or_h1:
            existing.in_title_h1_count = (existing.in_title_h1_count or 0) + 1

        # Recompute weighted confidence on update
        upd_conf = compute_keyword_confidence_weighted(
            frequency=freq,
            max_frequency=20,
            source_quality=float(source_quality) if source_quality is not None else None,
            keyword_type=keyword_type,
            competitor_strength=competitor_strength,
            last_seen=now,
        )
        final_conf = max(
            float(existing.confidence_score or 0.5),
            confidence,
            upd_conf,
        )
        final_conf = min(1.0, max(0.0, final_conf if final_conf <= 1 else final_conf / 100.0))
        existing.confidence_score = final_conf
        existing.keyword_confidence_score = final_conf
        if source_url:
            existing.source_url = source_url
        if company_name:
            existing.company_name = company_name
        if city:
            existing.city = city
        if state:
            existing.state = state
        if client_id:
            existing.client_id = client_id
    else:
        # New keyword: set avg_source_quality, top_competitor_count
        avg_qual = float(source_quality) if source_quality is not None else 0.0
        top_count = 1 if (source_quality is not None and source_quality > 70) else 0
        type_weight = get_keyword_type_weight(keyword_type)
        db.add(KeywordIntelligence(
            keyword=normalized,
            region=region,
            keyword_type=keyword_type,
            in_title_h1_count=1 if in_title_or_h1 else 0,
            source_url=source_url,
            company_name=company_name,
            city=city or region,
            state=state,
            client_id=client_id,
            frequency=1,
            confidence_score=min(1.0, max(0.0, confidence if confidence <= 1 else confidence / 100.0)),
            keyword_confidence_score=min(1.0, max(0.0, confidence if confidence <= 1 else confidence / 100.0)),
            source="competitor_site",
            first_seen=now,
            last_seen=now,
            avg_source_quality=avg_qual,
            top_competitor_count=top_count,
            keyword_type_weight=type_weight,
            last_confidence_update=now,
        ))
    return True


def upsert_keyword_standalone(
    keyword: str,
    region: str,
    keyword_type: str = "seo",
    *,
    source_url: Optional[str] = None,
    company_name: Optional[str] = None,
    client_id: Optional[str] = None,
    confidence: float = 0.5,
    vertical: Optional[str] = None,
) -> bool:
    """Standalone upsert_keyword that manages its own DB session."""
    db = SessionLocal()
    try:
        result = upsert_keyword(
            db, keyword, region, keyword_type=keyword_type,
            source_url=source_url, company_name=company_name, client_id=client_id,
            confidence=confidence, vertical=vertical,
        )
        if result:
            db.commit()
        return result
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def upsert_keywords_from_profile(
    extracted_profile: dict,
    region: str,
    *,
    client_id: Optional[str] = None,
    vertical: Optional[str] = "junk_removal",
    source_quality: Optional[int] = None,
    source_url: Optional[str] = None,
) -> int:
    """
    Upsert keywords from extracted_profile (Ollama JSON extraction result).
    Inserts new keywords, increments frequency and updates last_seen for existing ones.
    Returns count of keywords upserted.
    """
    if not extracted_profile or not isinstance(extracted_profile, dict):
        return 0
    if not region or not isinstance(region, str):
        return 0

    source_url = source_url or (extracted_profile.get("website_url") or "").strip() or None
    company_name = (extracted_profile.get("company_name") or "").strip() or None

    # Collect (keyword, type) pairs from the three lists
    items: list[tuple[str, str]] = []
    for kw in extracted_profile.get("seo_keywords") or []:
        if kw and isinstance(kw, str):
            items.append((kw.strip(), "seo"))
    for kw in extracted_profile.get("service_city_phrases") or []:
        if kw and isinstance(kw, str):
            items.append((kw.strip(), "service_city"))
    for kw in extracted_profile.get("geo_keywords") or []:
        if kw and isinstance(kw, str):
            items.append((kw.strip(), "geo"))

    # Title/H1 keywords from content_signals (Ollama extraction)
    content_signals = extracted_profile.get("content_signals") or {}
    title_keywords = {str(t).lower().strip() for t in (content_signals.get("title_keywords") or []) if t}

    # Dedupe by keyword, preferring service_city > geo > seo for type
    seen: dict[str, str] = {}
    for kw, kt in items:
        k = kw.lower().strip()
        if not k:
            continue
        if k not in seen or kt == "service_city":
            seen[k] = kt

    v = (vertical or "junk_removal").strip().lower()
    strength = 1.0 if (source_quality or 0) >= 70 else 0.5  # Top competitor = strong signal
    db = SessionLocal()
    try:
        count = 0
        now = datetime.utcnow()
        for kw, kt in seen.items():
            in_title = kw in title_keywords or any(kw in t for t in title_keywords) or any(t in kw for t in title_keywords)
            if upsert_keyword(
                db,
                kw,
                region,
                keyword_type=kt,
                source_url=source_url,
                company_name=company_name,
                client_id=client_id,
                confidence=0.5,
                vertical=v,
                now=now,
                source_quality=source_quality,
                competitor_strength=strength,
                in_title_or_h1=in_title,
            ):
                count += 1
        db.commit()
        return count
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
