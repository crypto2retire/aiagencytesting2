"""
Ollama Summarizer — factual extraction only. No guessing.
Uses prompts + run_ollama. Returns structured JSON only.
"""

import json
import re
import requests
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from config import OLLAMA_MODEL, OLLAMA_STREAM, OLLAMA_TIMEOUT, OLLAMA_URL

OLLAMA_GENERATE_URL = f"{OLLAMA_URL.rstrip('/')}/api/generate"
from llm import run_ollama
from prompts.content import get_full_page_prompt, get_page_outline_prompt
from prompts.extraction import get_prompt as get_extraction_prompt, get_summarize_prompt
from prompts.proposal import get_prompt as get_proposal_prompt
from prompts.seo import (
    GEO_PHRASE_EXTRACTION_PROMPT,
    SEO_KEYWORD_EXTRACTION_PROMPT,
)

KEYWORD_FEEDBACK_PATH = Path(__file__).resolve().parent.parent / "data" / "keyword_feedback.json"


def _load_keyword_feedback() -> Tuple[List[str], List[str]]:
    """Load valid/invalid keywords from file. Returns ([], []) if missing."""
    try:
        if KEYWORD_FEEDBACK_PATH.exists():
            data = json.loads(KEYWORD_FEEDBACK_PATH.read_text())
            return (data.get("valid") or [], data.get("invalid") or [])
    except Exception:
        pass
    return ([], [])


def generate_sales_proposal(
    client_name: str = "",
    city: str = "",
    niche: str = "",
    website: str = "",
    geo_phrases: Optional[List] = None,
    keywords: Optional[List] = None,
    competitor_coverage: Optional[str] = None,
    content_roadmap: Optional[str] = None,
) -> dict:
    """
    Generate sales proposal via Ollama. Returns JSON dict, or {} on failure.
    """
    findings = [
        f"Client: {client_name or 'N/A'}",
        f"City: {city or 'N/A'}",
        f"Niche: {niche or 'N/A'}",
        f"Website: {website or 'N/A'}",
        "",
        "Top geo_phrases & keywords:",
        *[f"  - {p}" if isinstance(p, str) else f"  - {p.get('geo_phrase', p.get('phrase', p))}" for p in (geo_phrases or [])[:15]],
        *[f"  - {k}" if isinstance(k, str) else f"  - {k.get('keyword', k)}" for k in (keywords or [])[:15]],
    ]
    if not geo_phrases and not keywords:
        findings.append("  (none)")
    findings.append("")
    findings.append("Recommended pages / content roadmap:")
    findings.append(content_roadmap or "  (none)")
    try:
        prompt = get_proposal_prompt(
            client_name=client_name or "N/A",
            findings="\n".join(findings),
            competitor_gaps=competitor_coverage or "  (none)",
        )
        data = run_ollama(prompt, model=OLLAMA_MODEL)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def generate_geo_page_outline(service: str, city: str, state: str = "") -> dict:
    """
    Generate landing page outline via Ollama. Returns JSON dict, or {} on failure.
    """
    try:
        city_str = f"{city or ''}, {state or ''}".strip(", ")
        prompt = get_page_outline_prompt(service or "", city_str)
        data = run_ollama(prompt, model=OLLAMA_MODEL)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    meta = data.get("meta_description") or ""
    if len(meta) > 155:
        data["meta_description"] = meta[:152] + "..."
    return data


def generate_full_page(
    client_name: str,
    service: str,
    city: str,
    state: str,
    page_title: str = "",
    meta_description: str = "",
    h1: str = "",
    section_outline: Optional[List] = None,
    competitor_context: str = "",
) -> Optional[dict]:
    """
    Generate fully written landing page via Ollama.
    Uses geo_page_outlines structure and competitor context.
    Returns {page_title, meta_description, h1, sections, seo_keywords, confidence_score} or None.
    """
    outline_json = ""
    if section_outline and isinstance(section_outline, list):
        outline_items = []
        for s in section_outline:
            if isinstance(s, dict):
                title = s.get("title") or s.get("heading") or ""
                bullets = s.get("bullets") or s.get("content") or []
                bullets_str = ", ".join(str(b) for b in bullets) if isinstance(bullets, list) else str(bullets)
                outline_items.append(f"  - {title}: {bullets_str}")
            else:
                outline_items.append(f"  - {s}")
        outline_json = "\n".join(outline_items) if outline_items else "  (none)"
    else:
        outline_json = "  (none)"

    context = f"""
Outline:
- page_title: {page_title or '(generate)'}
- meta_description: {meta_description or '(generate)'}
- h1: {h1 or '(generate)'}
- section_outline:
{outline_json}
"""
    if competitor_context.strip():
        context += f"\nCompetitor pages in this geo (for reference only — do not copy):\n{competitor_context}"
    else:
        context += "\nNo competitor pages provided — write original, locally-focused content."

    outline = f"""Client: {client_name or ''}
Service: {service or ''}
City: {city or ''}, {state or ''}
{context}"""
    try:
        prompt = get_full_page_prompt(outline)
        data = run_ollama(prompt, model=OLLAMA_MODEL)
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}
    meta = data.get("meta_description") or ""
    if len(meta) > 155:
        data["meta_description"] = meta[:152] + "..."
    conf = float(data.get("confidence_score", 0.7))
    data["confidence_score"] = max(0.0, min(1.0, conf))
    return data


def extract_geo_service_phrases(page_text: str) -> List[Dict]:
    """
    Extract geo-service phrases via Ollama.
    Returns list of {city, state, service, geo_phrase, confidence_score}.
    """
    prompt = GEO_PHRASE_EXTRACTION_PROMPT.replace("{{PAGE_TEXT}}", (page_text or "")[:6000])
    try:
        data = run_ollama(prompt, model=OLLAMA_MODEL)
    except (json.JSONDecodeError, requests.RequestException):
        return []
    if not isinstance(data, list):
        return []
    result = []
    for p in data:
        if not isinstance(p, dict) or not p.get("geo_phrase"):
            continue
        try:
            sc = float(p.get("confidence_score", 0.5))
            sc = max(0.0, min(1.0, sc))
        except (TypeError, ValueError):
            sc = 0.5
        result.append({
            "city": p.get("city"),
            "state": p.get("state"),
            "service": p.get("service"),
            "geo_phrase": p.get("geo_phrase"),
            "confidence_score": sc,
        })
    return result


def extract_competitive_intelligence(page_text: str, website_url: str = "") -> dict:
    """
    Extract structured competitive intelligence via Ollama. Returns JSON dict.
    """
    prompt = get_extraction_prompt(website_text=(page_text or "")[:6000])
    data = run_ollama(prompt, model=OLLAMA_MODEL)
    if not data or not isinstance(data, dict):
        return {}
    data.setdefault("website_url", website_url)
    # Map COMPETITOR schema to legacy: geo_phrases -> service_city_phrases, geo_keywords
    if "geo_phrases" in data and "service_city_phrases" not in data:
        data["service_city_phrases"] = data.get("geo_phrases", [])
    if "geo_phrases" in data and "geo_keywords" not in data:
        data["geo_keywords"] = data.get("geo_phrases", [])
    return data


def summarize_services(raw_text: str, competitor_name: str) -> dict:
    """
    Extract services + positioning from competitor text. Returns JSON dict.
    """
    prompt = get_summarize_prompt(raw_text=raw_text or "", competitor_name=competitor_name or "")
    try:
        data = run_ollama(prompt, model=OLLAMA_MODEL)
    except Exception:
        return {"extracted_services": [], "pricing_mentions": [], "complaints": [], "missed_opportunities": []}
    return data if isinstance(data, dict) else {"extracted_services": [], "pricing_mentions": [], "complaints": [], "missed_opportunities": []}


def extract_seo_keywords(raw_text: str) -> List[str]:
    """
    Extract SEO keywords for junk removal via Ollama.
    Uses keyword_feedback.json (file-based memory) to guide extraction.
    Returns list of keywords (one per line, 2-5 words, lowercase).
    """
    valid_kws, invalid_kws = _load_keyword_feedback()
    valid_block = "\n".join(f"- {k}" for k in valid_kws) if valid_kws else "(none loaded)"
    invalid_block = "\n".join(f"- {k}" for k in invalid_kws) if invalid_kws else "(none loaded)"

    prompt = SEO_KEYWORD_EXTRACTION_PROMPT.format(
        valid_block=valid_block,
        invalid_block=invalid_block,
    )
    prompt += f'"""\n{raw_text[:3500]}\n"""'

    try:
        response = requests.post(
            OLLAMA_GENERATE_URL,
            json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": OLLAMA_STREAM},
            timeout=OLLAMA_TIMEOUT,
        )
        response.raise_for_status()
        out = response.json().get("response", "").strip()
        keywords = []
        for line in out.split("\n"):
            kw = line.strip().lower().strip("-*• ")
            if kw and 2 <= len(kw.split()) <= 5 and not kw.startswith("#"):
                keywords.append(kw)
        return keywords
    except Exception:
        return []


def json_extraction_to_research_fields(data: dict) -> dict:
    """
    Map extract_competitive_intelligence JSON output to ResearchLog fields.
    Returns {extracted_services, pricing_mentions, complaints, missed_opportunities}.
    Supports both legacy (trust_signals) and COMPETITOR (local_trust_signals) schemas.
    """
    primary = data.get("primary_services") or []
    secondary = data.get("secondary_services") or []
    extracted_services = list(primary) + [s for s in (secondary or []) if s not in primary]
    missed = []
    trust = data.get("trust_signals") or {}
    local_trust = data.get("local_trust_signals") or []
    if isinstance(local_trust, list):
        has_license = any("licen" in str(s).lower() or "insur" in str(s).lower() for s in local_trust)
        has_guarantee = any("guarantee" in str(s).lower() for s in local_trust)
        if not has_license:
            missed.append("Licensing/insurance not prominently mentioned")
        if not has_guarantee:
            missed.append("No satisfaction guarantees stated")
    else:
        if trust.get("licenses_or_insurance") is False:
            missed.append("Licensing/insurance not prominently mentioned")
        if trust.get("guarantees") is False:
            missed.append("No satisfaction guarantees stated")
    return {
        "extracted_services": extracted_services,
        "pricing_mentions": [],
        "complaints": [],
        "missed_opportunities": missed,
    }


def parse_summary_to_fields(summary: str) -> dict:
    """
    Parses summarize_services output into ResearchLog fields.
    Returns {extracted_services, pricing_mentions, complaints, missed_opportunities}.
    """

    def extract_bullets(text: str, section: str) -> list:
        """Extract bullet items from a section."""
        pattern = rf"{re.escape(section)}:\s*\n(.*?)(?=\n[A-Z][A-Za-z/ ]+:|\Z)"
        m = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if not m:
            return []
        block = m.group(1).strip()
        items = []
        for line in block.split("\n"):
            line = line.strip()
            if line.startswith("-"):
                item = line[1:].strip()
                if item and item.lower() not in ("not mentioned", "none found", "n/a"):
                    items.append(item)
        return items

    services = extract_bullets(summary, "SERVICES OFFERED")
    pricing = extract_bullets(summary, "PRICING MENTIONS")
    complaints = extract_bullets(summary, "COMPLAINTS / GAPS")
    missed = complaints  # Gaps = missed opportunities for our client

    return {
        "extracted_services": services,
        "pricing_mentions": pricing,
        "complaints": complaints,
        "missed_opportunities": missed,
    }
