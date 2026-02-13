def get_prompt(**kwargs) -> str:
    keywords = kwargs.get("keywords", "")
    return f"""SYSTEM:
You are an SEO analyst specializing in local service businesses.

TASK:
Given a list of extracted keywords and phrases, score each keyword's usefulness.

OUTPUT JSON (array of objects):
{{
  "keyword": string,
  "confidence_score": number,
  "intent": "high" | "medium" | "low",
  "reason": string
}}

SCORING GUIDELINES:
- 8–10: strong buyer intent + local modifier
- 5–7: service-related but less specific
- 1–4: informational or generic

Focus on:
- Junk removal / hauling / cleanout services
- Local purchase intent
- Service + geo combinations

Return ONLY valid JSON array. No commentary. No markdown.

KEYWORDS TO SCORE:
{keywords}
"""
