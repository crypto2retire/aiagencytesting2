from prompts.seo import PAGE_OUTLINE_PROMPT
from prompts.generation import CONTENT_DRAFT_PROMPT_TEMPLATE, FULL_PAGE_WRITER_PROMPT


def get_content_draft_prompt(**kwargs) -> str:
    """Content draft (GBP + Facebook) — returns JSON only."""
    return f"""You are a marketing copywriter for a local {kwargs.get('business_name', 'service')} business.

Research on competitors:
{kwargs.get('research_text', '')}

Keyword intelligence: {kwargs.get('keyword_summary', '')}{kwargs.get('perf_section', '')}
{kwargs.get('landing_page_section', '')}
Focus on ONE underserved service or gap: {kwargs.get('service_focus', '')}

Write exactly 2 posts{kwargs.get('city_instruction', '')}:
1. Google Business Profile: 150-300 words, local SEO, clear CTA
2. Facebook: 80-150 words, engaging, shareable

**BRAND VOICE**: {kwargs.get('tone', 'friendly')}
{kwargs.get('tone_instruction', '')}
Differentiators: {kwargs.get('differentiators', '')}

Return ONLY valid JSON:
{{
  "google_business_profile": "string",
  "facebook": "string",
  "primary_keywords": ["keyword1", "keyword2"],
  "geo_phrases": ["city1 service1", "city2 service2"]
}}
"""


def get_page_outline_prompt(service: str, city: str) -> str:
    return PAGE_OUTLINE_PROMPT.replace("{{service}}", service or "").replace("{{city}}", city or "")


def get_full_page_prompt(outline: str) -> str:
    return FULL_PAGE_WRITER_PROMPT.replace("{{OUTLINE}}", outline or "")


def get_prompt(**kwargs) -> str:
    outline = kwargs.get("outline", "")
    return f"""SYSTEM:
You are a professional local SEO copywriter.

TASK:
Write a complete service page using the provided outline.

RULES:
- Conversational but professional tone
- Clear CTAs
- Emphasize local trust and speed
- Avoid keyword stuffing
- Write for humans first, SEO second

OUTLINE:
{outline}
"""
