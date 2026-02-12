"""
Strategist — content & positioning.
Never browses the web. Only reads research, thinks, writes drafts.
Tries Ollama first (local, free). Falls back to Claude only when output is insufficient.
"""

import json
import logging
import re
import requests
from datetime import datetime, timedelta
from typing import List, Optional

from config import (
    ANTHROPIC_API_KEY,
    ANTHROPIC_MODEL,
    ANTHROPIC_URL,
    MIN_CONFIDENCE_FOR_STRATEGIST,
    OLLAMA_MODEL,
    OLLAMA_URL,
    STRATEGIST_LOG,
)
from sqlalchemy import func

from sqlalchemy.orm import Session

from database import Client, ContentDraft, ContentStrategy, ResearchLog, SessionLocal

from verticals import get_vertical_config, is_excluded_from_content

from .opportunity_scorer import score_opportunities

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(STRATEGIST_LOG), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# Brand voice → concrete writing instructions for the LLM
BRAND_TONE_INSTRUCTIONS = {
    "friendly": "Write in a warm, approachable, conversational way. Use contractions, simple words, and a welcoming tone as if talking to a neighbor.",
    "no-BS": "Write in a direct, straightforward, no-fluff style. Be concise, honest, and punchy. Skip hype and filler; lead with value.",
    "premium": "Write in an elevated, polished tone. Emphasize quality, expertise, and premium service. Avoid casual slang; use refined language.",
    "professional": "Write in a clear, competent, business-appropriate tone. Balanced and trustworthy—neither stuffy nor casual.",
}


def _call_ollama(prompt: str) -> str:
    """Call local Ollama. Returns empty string on failure."""
    try:
        log.info("Trying Ollama first (local, no cost)")
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json().get("response", "").strip()
    except Exception as e:
        log.warning(f"Ollama failed: {e}")
        return ""


def _call_claude(prompt: str) -> str:
    """Call Claude via Anthropic API. Use only when Ollama is insufficient."""
    log.info("Falling back to Claude (API cost)")
    resp = requests.post(
        ANTHROPIC_URL,
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        },
        json={
            "model": ANTHROPIC_MODEL,
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    # Anthropic returns content as array of blocks; text is in content[0].text
    blocks = data.get("content", [])
    text = ""
    for b in blocks:
        if b.get("type") == "text":
            text += b.get("text", "")
    return text.strip()


CONTENT_DEDUP_DAYS = 30  # Same topic cannot be recommended twice within this window


def _topic_recently_drafted(db: Session, client_id: str, topic: str) -> bool:
    """True if we have drafts for this topic in the last N days (content duplication guard)."""
    cutoff = datetime.utcnow() - timedelta(days=CONTENT_DEDUP_DAYS)
    topic_lower = (topic or "").strip().lower()
    existing = (
        db.query(ContentDraft)
        .filter(
            ContentDraft.client_id == client_id,
            func.lower(ContentDraft.topic) == topic_lower,
            ContentDraft.created_at >= cutoff,
        )
        .first()
    )
    return existing is not None


def _is_semantically_same(topic_a: str, topic_b: str) -> bool:
    """True if topics are effectively the same (avoid 'hot tub removal' vs 'hot tub removal phoenix')."""
    a = (topic_a or "").strip().lower()
    b = (topic_b or "").strip().lower()
    if a == b:
        return True
    return a in b or b in a


def _save_strategy(db, client_id: str, city: str, opportunities: List[dict], vertical: str = "junk_removal") -> None:
    """Convert opportunities into clear action plans. Client-facing. Skips duplicates and excluded content."""
    db.query(ContentStrategy).filter(ContentStrategy.client_id == client_id).delete()
    seen_topics = set()
    for opp in opportunities:
        if opp.get("duplicate", False):
            continue
        topic = opp["service"]
        if is_excluded_from_content(topic, vertical):
            continue
        if any(_is_semantically_same(topic, s) for s in seen_topics):
            continue
        seen_topics.add((topic or "").strip().lower())
        score = opp["score"]
        actions = [
            f"Google Business Profile post targeting '{topic} in {city}'" if city else f"Google Business Profile post for '{topic}'",
            f"SEO blog page for '{topic}'",
            "Before/after photo post on Facebook",
        ]
        strat = ContentStrategy(
            client_id=client_id,
            topic=topic,
            recommended_actions=actions,
            priority_score=score,
        )
        db.add(strat)
    db.commit()


def _is_bad_draft(body: str, geo_candidates: List[str]) -> bool:
    """QC Rule 1: < 50 chars OR no geo reference → FAILED."""
    if not body or len(body) < 50:
        return True
    body_lower = body.lower()
    for geo in geo_candidates:
        if geo and str(geo).strip().lower() in body_lower:
            return False
    return bool(geo_candidates)


def _extract_draft(text: str, label: str) -> str:
    """Extract draft content from Claude output, handling markdown blocks."""
    # Look for ## Label or **Label** or "Label:"
    patterns = [
        rf"##\s*{label}[^\n]*\n(.*?)(?=##|$)",
        rf"\*\*{label}[^\n]*\*\*\s*\n(.*?)(?=\*\*|$)",
        rf"{label}:\s*\n(.*?)(?=\n\n|\Z)",
        rf"```\n(.*?)```",
    ]
    for p in patterns:
        m = re.search(p, text, re.DOTALL | re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return text.strip()


def generate_strategy(client_id: str) -> dict:
    """
    Load research_logs, send to Claude, save drafts.
    Never browses the web. Only reads DB, thinks, writes.
    Returns {draft_ids: [...]}.
    """
    log.info(f"Strategist starting for client={client_id}")
    db = SessionLocal()
    try:
        client = db.query(Client).filter(func.lower(Client.client_id) == client_id.lower()).first()
        if not client:
            log.error("Client not found")
            return {}

        client_id = client.client_id  # Use actual stored value for DB writes

        # Load research_logs, ignore low confidence (coalesce handles NULL)
        logs = (
            db.query(ResearchLog)
            .filter(
                ResearchLog.client_id == client_id,
                func.coalesce(ResearchLog.confidence_score, 0) >= MIN_CONFIDENCE_FOR_STRATEGIST,
            )
            .order_by(func.coalesce(ResearchLog.confidence_score, 0).desc())
            .all()
        )
        if not logs:
            log.warning("No research_logs above confidence threshold")
            return {}

        log.info(f"Loaded {len(logs)} research entries")

        # Opportunity scorer — ranked easy wins (saves to OpportunityScore)
        opportunities = score_opportunities(client_id)
        city = (client.cities_served or [""])[0] if client.cities_served else ""

        # Pick top unique opportunity not recently drafted (content duplication guard)
        vertical = (client.client_vertical or "junk_removal").strip().lower()
        vcfg = get_vertical_config(vertical)
        service_focus = (vcfg.get("core_services") or ["general service"])[0]
        for opp in opportunities:
            if opp.get("duplicate", False):
                continue
            topic = opp.get("service", "")
            if is_excluded_from_content(topic, vertical):
                log.info(f"Skipping '{topic}' — excluded from content")
                continue
            if _topic_recently_drafted(db, client_id, topic):
                log.info(f"Skipping '{topic}' — already drafted recently")
                continue
            service_focus = topic
            break
        log.info(f"Top opportunity: {service_focus}")

        # Build client-facing action plan (strategies table), excluding duplicates
        _save_strategy(db, client_id, city, opportunities[:10], vertical)

        research_summary = []
        for rl in logs[:10]:
            s = rl.extracted_services or []
            p = rl.pricing_mentions or []
            c = rl.complaints or []
            m = rl.missed_opportunities or []
            research_summary.append(
                f"{rl.competitor_name} ({rl.source_type}): services={s}, pricing={p}, complaints={c}, gaps={m}"
            )
        research_text = "\n".join(research_summary)

        city_instruction = f" MUST mention {city} by name (for local SEO)." if city else ""
        tone = (client.brand_tone or "friendly").strip().lower()
        tone_instruction = BRAND_TONE_INSTRUCTIONS.get(tone) or BRAND_TONE_INSTRUCTIONS["friendly"]
        differentiators = client.differentiators or ["reliable", "local", "transparent"]

        prompt = f"""You are a marketing copywriter for a local {client.business_name} business.

Research on competitors in the area:
{research_text}

Focus on ONE underserved service or gap: {service_focus}

Write exactly 2 posts:{city_instruction}
1. **Google Business Profile**: 150-300 words, local SEO friendly, include a clear CTA
2. **Facebook**: 80-150 words, engaging, shareable

**BRAND VOICE (follow strictly)**: {tone}
{tone_instruction}

Differentiators to emphasize: {differentiators}

Format your response as:
## Google Business Profile
[your post here]

## Facebook
[your post here]
"""

        MIN_WORDS = 50  # Use Claude only if Ollama output is below this

        # 1. Try Ollama first (local, no API cost)
        output = _call_ollama(prompt)
        gbp = _extract_draft(output, "Google Business Profile") if output else ""
        fb = _extract_draft(output, "Facebook") if output else ""

        # 2. Fall back to Claude only if Ollama failed or output insufficient
        if len(gbp.split()) < MIN_WORDS or len(fb.split()) < MIN_WORDS:
            log.info("Ollama output insufficient — using Claude")
            output = _call_claude(prompt)
            gbp = _extract_draft(output, "Google Business Profile") or gbp
            fb = _extract_draft(output, "Facebook") or fb

            # Retry Claude once if still too short
            if len(gbp.split()) < MIN_WORDS or len(fb.split()) < MIN_WORDS:
                log.warning("Claude output < 50 words, retrying once")
                output = _call_claude(prompt)
                gbp = _extract_draft(output, "Google Business Profile") or gbp
                fb = _extract_draft(output, "Facebook") or fb

        if len(gbp.split()) < 20 or len(fb.split()) < 20:
            log.error("Both Ollama and Claude produced insufficient output. No drafts saved.")
            return {}

        # Validation: < 50 chars → regenerate once
        if len(gbp.strip()) < 50 or len(fb.strip()) < 50:
            log.warning("Draft(s) < 50 characters — regenerating once via Claude")
            output = _call_claude(prompt)
            gbp = _extract_draft(output, "Google Business Profile") or gbp
            fb = _extract_draft(output, "Facebook") or fb

        # Geo candidates for QC: client cities + opportunity geos
        geo_candidates = list({g for g in (client.cities_served or []) + [city] + [o.get("geo", "") for o in opportunities[:5]] if g and str(g).strip()})

        draft_ids = []

        for platform, body, ctype in [
            ("google_business", gbp, "google_business"),
            ("facebook", fb, "social"),
        ]:
            status = "FAILED" if _is_bad_draft(body, geo_candidates) else "PENDING"
            if status == "FAILED":
                log.warning(f"QC: {platform} draft auto-flagged as FAILED (<50 chars or no geo)")

            draft = ContentDraft(
                client_id=client_id,
                topic=service_focus,
                content_type=ctype,
                platform=platform,
                title=None,
                body=body,
                body_refined=body,
                word_count=len(body.split()),
                change_notes=[],
                status=status,
            )
            db.add(draft)
            db.commit()
            draft_ids.append(draft.id)
            log.info(f"Saved {platform} draft (id={draft.id}, status={status})")

        log.info(f"Strategist done. {len(draft_ids)} drafts saved.")
        return {"draft_ids": draft_ids}
    except Exception as e:
        db.rollback()
        log.exception(str(e))
        raise
    finally:
        db.close()


def run_strategist(client_id: str) -> dict:
    """Alias for generate_strategy."""
    return generate_strategy(client_id)
