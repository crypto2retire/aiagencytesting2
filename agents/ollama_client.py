"""
Ollama Summarizer — factual extraction only. No guessing.

Guards: single-pass, timeout=45s, no streaming, no retries (fail fast).
"""

import json
import re
import requests
from pathlib import Path

from config import OLLAMA_MODEL, OLLAMA_URL, OLLAMA_TIMEOUT, OLLAMA_STREAM

KEYWORD_FEEDBACK_PATH = Path(__file__).resolve().parent.parent / "data" / "keyword_feedback.json"


def _load_keyword_feedback() -> tuple[list[str], list[str]]:
    """Load valid/invalid keywords from file. Returns ([], []) if missing."""
    try:
        if KEYWORD_FEEDBACK_PATH.exists():
            data = json.loads(KEYWORD_FEEDBACK_PATH.read_text())
            return (data.get("valid") or [], data.get("invalid") or [])
    except Exception:
        pass
    return ([], [])

OLLAMA_GENERATE_URL = f"{OLLAMA_URL.rstrip('/')}/api/generate"


def summarize_services(raw_text: str, competitor_name: str) -> str:
    """
    Sends competitor text to local Ollama and extracts services + positioning.
    Returns structured bullet output. Extract only what is explicitly stated.
    """

    prompt = f"""
You are a factual market research analyst.

Your task:
Extract ONLY what is explicitly stated in the text below.
DO NOT guess.
DO NOT invent services.
DO NOT assume pricing.

Output format (exactly):

COMPETITOR: {competitor_name}

SERVICES OFFERED:
- Bullet list of services

PRICING MENTIONS:
- Bullet list (or "Not mentioned")

POSITIONING / CLAIMS:
- Bullet list (speed, same-day, eco-friendly, etc.)

COMPLAINTS / GAPS (from reviews or weak areas):
- Bullet list (or "None found")

NOTES:
- If data is weak or unclear, say why.

TEXT TO ANALYZE:
\"\"\"
{raw_text[:4000]}
\"\"\"
"""

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": OLLAMA_STREAM,  # Guard: no streaming
    }

    try:
        response = requests.post(
            OLLAMA_GENERATE_URL,
            json=payload,
            timeout=OLLAMA_TIMEOUT,  # Guard: 45s hard limit
        )
        response.raise_for_status()
        return response.json().get("response", "").strip()
    except Exception as e:
        return f"Ollama summarization failed: {str(e)}"


def extract_seo_keywords(raw_text: str) -> list[str]:
    """
    Extract SEO keywords for junk removal via Ollama.
    Uses keyword_feedback.json (file-based memory) to guide extraction.
    Returns list of keywords (one per line, 2-5 words, lowercase).
    """
    valid_kws, invalid_kws = _load_keyword_feedback()
    valid_block = "\n".join(f"- {k}" for k in valid_kws) if valid_kws else "(none loaded)"
    invalid_block = "\n".join(f"- {k}" for k in invalid_kws) if invalid_kws else "(none loaded)"

    prompt = f"""Valid keywords examples:
{valid_block}

Invalid keyword examples:
{invalid_block}

Use these as guidance, not as a strict whitelist.

Extract ONLY valid SEO keywords for a junk removal business.

Rules:
- Must describe a service or service + location
- Ignore adjectives, branding, praise, or general business terms
- Return one keyword per line
- Return nothing if no valid keywords exist

Examples:
"Hot tub removal and garage cleanouts in Milwaukee WI"
→
hot tub removal
garage cleanout milwaukee wi

"Our friendly team provides great service"
→
(no output)

Now extract keywords from:
"""
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
