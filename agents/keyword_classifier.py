"""
Keyword Classifier — LLM-based classification of keywords.
Runs once per research batch, per city, per week, or manually.
Classifies into: service | geo | modifier | long_tail | brand
Uses Ollama first (free), Claude fallback.
"""

import json
import logging
import re
import requests
from typing import Callable, Dict, List, Optional

from config import (
    ANTHROPIC_API_KEY,
    ANTHROPIC_MODEL,
    ANTHROPIC_URL,
    KEYWORD_CLASSIFIER_LOG,
    OLLAMA_MODEL,
    OLLAMA_URL,
)
from database import KeywordIntelligence, SessionLocal

VALID_TYPES = frozenset({"service", "geo", "modifier", "long_tail", "brand"})
BATCH_SIZE = 40  # Keywords per LLM call

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(KEYWORD_CLASSIFIER_LOG), logging.StreamHandler()],
)
log = logging.getLogger(__name__)


def _call_ollama(prompt: str) -> str:
    """Call local Ollama. Returns empty string on failure."""
    try:
        log.info("Keyword classifier: trying Ollama (local, no cost)")
        resp = requests.post(
            f"{OLLAMA_URL}/api/generate",
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
            timeout=90,
        )
        resp.raise_for_status()
        return resp.json().get("response", "").strip()
    except Exception as e:
        log.warning(f"Ollama failed: {e}")
        return ""


def _call_claude(prompt: str) -> str:
    """Call Claude via Anthropic API."""
    log.info("Keyword classifier: falling back to Claude")
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
    blocks = resp.json().get("content", [])
    return "".join(b.get("text", "") for b in blocks if b.get("type") == "text").strip()


def _llm(prompt: str) -> str:
    """Try Ollama first, then Claude."""
    out = _call_ollama(prompt)
    if not out or len(out) < 10:
        out = _call_claude(prompt)
    return out


def _parse_json_response(text: str) -> Dict[str, str]:
    """Extract JSON from LLM response. Handles markdown code blocks."""
    text = text.strip()
    if "```" in text:
        m = re.search(r"```(?:json)?\s*(\{[^`]*\})\s*```", text, re.DOTALL)
        if m:
            text = m.group(1)
    match = re.search(r"\{[^{}]*\}", text, re.DOTALL)
    if not match:
        return {}
    try:
        raw = json.loads(match.group())
        return {str(k).lower().strip(): str(v).lower().strip() for k, v in raw.items()}
    except json.JSONDecodeError:
        return {}


def classify_keywords(keywords: List[str], llm: Optional[Callable[[str], str]] = None) -> Dict[str, str]:
    """
    Classify keywords into: service | geo | modifier | long_tail | brand.
    Uses Ollama first, Claude fallback if llm not provided.
    """
    if not keywords:
        return {}

    llm = llm or _llm
    all_classifications: Dict[str, str] = {}

    for i in range(0, len(keywords), BATCH_SIZE):
        batch = keywords[i : i + BATCH_SIZE]
        prompt = f"""You are classifying keywords for a junk removal business.

Group each keyword into exactly one of:
- service (e.g. removal, hauling, cleanout)
- geo (e.g. phoenix, scottsdale, arizona)
- modifier (e.g. same-day, cheap, emergency, fast)
- long_tail (e.g. estate sale cleanup, hoarder cleanup)
- brand (competitor or brand names)

Keywords:
{', '.join(batch)}

Return ONLY valid JSON: {{ "keyword": "type", "keyword2": "type2" }}
Use lowercase for both keyword and type. No explanation."""

        response = llm(prompt)
        parsed = _parse_json_response(response)
        for kw, t in parsed.items():
            if t in VALID_TYPES:
                all_classifications[kw] = t

    return all_classifications


def classify_region(region: str, client_id: Optional[str] = None) -> int:
    """
    Classify all unclassified keywords for a region. Updates DB.
    Returns count of keywords classified.
    """
    db = SessionLocal()
    try:
        q = db.query(KeywordIntelligence).filter(
            KeywordIntelligence.region == region,
            (KeywordIntelligence.keyword_type.is_(None)) | (KeywordIntelligence.keyword_type == ""),
        )
        if client_id:
            q = q.filter(KeywordIntelligence.client_id == client_id)
        keywords = list({r.keyword for r in q.all()})
        if not keywords:
            log.info(f"No unclassified keywords for region={region}")
            return 0

        log.info(f"Classifying {len(keywords)} keywords for region={region}")
        classifications = classify_keywords(keywords)

        count = 0
        for row in db.query(KeywordIntelligence).filter(
            KeywordIntelligence.region == region,
        ):
            if row.keyword in classifications:
                row.keyword_type = classifications[row.keyword]
                count += 1
        db.commit()
        log.info(f"Applied {len(classifications)} classifications, updated {count} rows")
        return count
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def run_classifier(region: str, client_id: Optional[str] = None) -> tuple[bool, str]:
    """
    Run classification for a region. For manual or batch use.
    Returns (success, message).
    """
    try:
        count = classify_region(region, client_id)
        return True, f"Classified {count} keywords for {region}."
    except Exception as e:
        log.exception(str(e))
        return False, str(e)
