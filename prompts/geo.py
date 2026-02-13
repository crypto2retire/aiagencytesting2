def get_prompt(**kwargs) -> str:
    geo_phrases = kwargs.get("geo_phrases", "")
    return f"""SYSTEM:
You are a local SEO clustering engine.

TASK:
Group similar geo-service phrases into clusters.

INPUT:
List of geo phrases (service + city).

OUTPUT JSON (array of cluster objects):
{{
  "cluster_name": string,
  "primary_phrase": string,
  "variations": string[],
  "city": string,
  "service": string
}}

RULES:
- Group phrases with same intent.
- Choose the most SEO-clean phrase as primary.
- Do NOT invent phrases.

Return ONLY valid JSON array. No commentary. No markdown.

GEO PHRASES TO CLUSTER:
{geo_phrases}
"""
