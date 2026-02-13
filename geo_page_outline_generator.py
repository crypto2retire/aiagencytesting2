"""
Geo Page Outline Generator — generates and saves landing page outlines for high-confidence geo phrases.

For geo_phrases with confidence_score > 0.65:
- Generates page outline via Ollama
- Saves to geo_page_outlines

Auto-Write Workflow: For outlines with confidence > 0.65, runs full page generator and sets page_status='READY'.
"""

import time
from typing import Optional

from database import Client, CompetitorGeoCoverage, GeoPageOutline, GeoPhrase, SessionLocal
from agents.ollama_client import generate_full_page, generate_geo_page_outline

OUTLINE_CONFIDENCE_THRESHOLD = 0.65
SLEEP_BETWEEN_GENERATIONS = 1.0  # Rate limit Ollama


def generate_and_save_outlines(
    db=None,
    threshold: float = OUTLINE_CONFIDENCE_THRESHOLD,
    skip_existing: bool = True,
    client_id: Optional[str] = None,
) -> int:
    """
    For geo_phrases with confidence_score > threshold:
    - Generate page outline via Ollama
    - Save to geo_page_outlines

    client_id: optional, scopes outline to a client.
    Returns count of outlines generated and saved.
    """
    sess = db or SessionLocal()
    count = 0
    try:
        phrases = sess.query(GeoPhrase).filter(
            GeoPhrase.confidence_score > threshold,
        ).all()

        for phrase in phrases:
            city = (phrase.city or "").strip()
            state = (phrase.state or "").strip()
            service = (phrase.service or "").strip()
            geo_phrase = (phrase.geo_phrase or "").strip()
            conf = float(phrase.confidence_score or 0.5)
            if not service or not city:
                continue

            if skip_existing:
                existing = sess.query(GeoPageOutline).filter(
                    GeoPageOutline.city == city,
                    GeoPageOutline.state == state,
                    GeoPageOutline.service == service,
                ).first()
                if existing:
                    continue

            outline = generate_geo_page_outline(service=service, city=city, state=state)
            if not outline:
                continue

            page_title = outline.get("page_title")
            meta_description = outline.get("meta_description")
            h1 = outline.get("h1")
            sections = outline.get("sections") or []
            internal_links = outline.get("suggested_internal_links") or []

            sess.add(GeoPageOutline(
                client_id=client_id,
                city=city,
                state=state or None,
                service=service,
                geo_phrase=geo_phrase or None,
                page_title=page_title,
                meta_description=meta_description,
                h1=h1,
                section_outline=sections,
                internal_links=internal_links,
                confidence_score=conf,
                page_status="DRAFT",
            ))
            count += 1
            time.sleep(SLEEP_BETWEEN_GENERATIONS)

        if db is None:
            sess.commit()
    except Exception:
        if db is None:
            sess.rollback()
        raise
    finally:
        if db is None:
            sess.close()

    return count


def run_auto_write_workflow(
    db=None,
    threshold: float = OUTLINE_CONFIDENCE_THRESHOLD,
    client_id: Optional[str] = None,
    limit: int = 10,
) -> int:
    """
    For each geo_page_outline with confidence_score > threshold:
    - Run Ollama Full Page Generator
    - Save result (generated_sections, updated title/meta/h1)
    - Set page_status='READY'

    client_id: optional, only process outlines for this client.
    limit: max outlines to process per run.
    Returns count of outlines upgraded to READY.
    """
    sess = db or SessionLocal()
    count = 0
    try:
        q = sess.query(GeoPageOutline).filter(
            GeoPageOutline.confidence_score > threshold,
            GeoPageOutline.page_status != "READY",
        )
        if client_id:
            q = q.filter(GeoPageOutline.client_id == client_id)
        outlines = q.order_by(GeoPageOutline.confidence_score.desc()).limit(limit).all()

        for o in outlines:
            city = (o.city or "").strip()
            state = (o.state or "").strip()
            service = (o.service or "").strip()
            if not service or not city:
                continue

            # Get client name
            client_name = "Local Business"
            if o.client_id:
                client = sess.query(Client).filter(Client.client_id == o.client_id).first()
                if client:
                    client_name = (client.business_name or client.client_id or "").strip() or client_name

            # Get competitor context from same geo cluster (city, state, service)
            competitor_rows = sess.query(CompetitorGeoCoverage).filter(
                CompetitorGeoCoverage.city.ilike(city),
                CompetitorGeoCoverage.service.ilike(service),
                CompetitorGeoCoverage.page_exists == True,
            ).limit(5).all()

            competitor_context = ""
            if competitor_rows:
                parts = []
                for r in competitor_rows:
                    parts.append(f"- {r.competitor_name or 'Competitor'}: title={r.page_title or '—'}, h1={r.page_h1 or '—'}")
                competitor_context = "\n".join(parts)

            result = generate_full_page(
                client_name=client_name,
                service=service,
                city=city,
                state=state,
                page_title=o.page_title or "",
                meta_description=o.meta_description or "",
                h1=o.h1 or "",
                section_outline=o.section_outline or [],
                competitor_context=competitor_context,
            )
            if not result:
                continue

            o.page_title = result.get("page_title") or o.page_title
            o.meta_description = result.get("meta_description") or o.meta_description
            o.h1 = result.get("h1") or o.h1
            o.generated_sections = result.get("sections") or []
            o.confidence_score = result.get("confidence_score", o.confidence_score)
            o.page_status = "READY"
            count += 1
            time.sleep(SLEEP_BETWEEN_GENERATIONS)

        if db is None:
            sess.commit()
    except Exception:
        if db is None:
            sess.rollback()
        raise
    finally:
        if db is None:
            sess.close()

    return count
