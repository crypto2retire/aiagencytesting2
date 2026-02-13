def get_prompt(**kwargs) -> str:
    website_text = kwargs.get("website_text", "")
    return f"""SYSTEM:
You are a data extraction engine.
You ONLY return valid JSON.
No commentary, no markdown, no explanations.

TASK:
Analyze the provided website content for a LOCAL SERVICE BUSINESS.

Extract the following fields as accurately as possible.

OUTPUT SCHEMA (strict):
{{
  "company_name": string | null,
  "primary_services": string[],
  "secondary_services": string[],
  "cities_served": string[],
  "geo_phrases": string[],
  "seo_keywords": string[],
  "unique_selling_points": string[],
  "contact_ctas": string[],
  "content_depth_score": number,
  "local_trust_signals": string[],
  "notes": string | null
}}

RULES:
- Use real phrases found in the content.
- Geo phrases must combine SERVICE + CITY (e.g. "junk removal phoenix").
- If data is missing, return empty arrays or null.
- content_depth_score must be 1–10 based on thoroughness.

INPUT:
{website_text}
"""


def get_summarize_prompt(**kwargs) -> str:
    """Summarize services from text — returns JSON schema."""
    raw_text = kwargs.get("raw_text", "")
    competitor_name = kwargs.get("competitor_name", "")
    return f"""SYSTEM:
You are a factual market research analyst. Return ONLY valid JSON.

TASK:
Extract ONLY what is explicitly stated. DO NOT guess or invent.

OUTPUT JSON:
{{
  "extracted_services": string[],
  "pricing_mentions": string[],
  "complaints": string[],
  "missed_opportunities": string[]
}}

COMPETITOR: {competitor_name}

TEXT:
\"\"\"
{raw_text[:4000]}
\"\"\"
"""
