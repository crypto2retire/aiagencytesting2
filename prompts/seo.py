"""
SEO prompts — geo phrase extraction, page outlines, keyword classification, keyword extraction.
"""

GEO_PHRASE_EXTRACTION_PROMPT = """Extract geo-service phrases from text.
Return JSON array of:
- city
- state
- service
- geo_phrase
- confidence_score
Only include phrases with clear local intent.

Output format: valid JSON array only. No commentary. No markdown.

Example:
[{"city":"Phoenix","state":"AZ","service":"junk removal","geo_phrase":"junk removal phoenix","confidence_score":0.9}]

TEXT:
{{PAGE_TEXT}}"""

GEO_PAGE_OUTLINE_PROMPT = """You are a local SEO expert.
Generate a high-converting landing page outline for:
Service: {{service}}
City: {{city}}, {{state}}

Return JSON with:
- page_title
- meta_description (<=155 chars)
- h1
- sections (titles + bullets)
- suggested_internal_links

Output format: valid JSON only. No commentary. No markdown.

Example:
{
  "page_title": "Junk Removal Madison WI | Same-Day Pickup",
  "meta_description": "Professional junk removal in Madison WI. Same-day service, free estimates. Furniture, appliances, debris. Call (555) 123-4567.",
  "h1": "Junk Removal Madison WI",
  "sections": [
    {"title": "Same-Day Junk Removal in Madison", "bullets": ["We haul furniture, appliances, and debris", "Serving all Madison neighborhoods", "Licensed and insured"]},
    {"title": "What We Remove", "bullets": ["Furniture and mattresses", "Appliances", "Yard waste and debris"]}
  ],
  "suggested_internal_links": ["/junk-removal", "/services", "/contact", "/about"]
}"""

# Content Strategist Agent — full page outline
PAGE_OUTLINE_PROMPT = """SYSTEM:
You are an SEO content strategist.

TASK:
Generate a full page outline for a LOCAL SERVICE landing page.

INPUT:
Service + City.

OUTPUT JSON:
{
  "page_title": string,
  "meta_description": string,
  "h1": string,
  "sections": [
    {
      "heading": string,
      "bullet_points": string[]
    }
  ],
  "cta": string
}

RULES:
- Optimize for conversions and local SEO.
- Use natural language.
- No fluff.

Return ONLY valid JSON. No commentary. No markdown.

SERVICE: {{service}}
CITY: {{city}}
"""

FULL_PAGE_GENERATION_PROMPT = """You are a local SEO copywriter.
Generate a fully written landing page for:
- Client: {{client_name}}
- Service: {{service}}
- City: {{city}}, {{state}}

Use the geo_page_outlines sections:
- page_title
- meta_description
- h1
- section_outline

{{competitor_context}}

Compare with competitor sites in the same geo_phrase cluster:
- Highlight advantages
- Avoid copying competitor text
- Include top local keywords

Return ONLY valid JSON. No commentary. No markdown.

Output format:
{
  "page_title": "string",
  "meta_description": "string",
  "h1": "string",
  "sections": [{"heading": "string", "body": "string"}],
  "seo_keywords": ["string"],
  "confidence_score": 0.0
}
"""

KEYWORD_CLASSIFICATION_PROMPT = """You are classifying keywords for a junk removal business.

Group each keyword into exactly one of:
- service (e.g. removal, hauling, cleanout)
- geo (e.g. phoenix, scottsdale, arizona)
- modifier (e.g. same-day, cheap, emergency, fast)
- long_tail (e.g. estate sale cleanup, hoarder cleanup)
- brand (competitor or brand names)

Keywords:
{keywords}

Return ONLY valid JSON: {{ "keyword": "type", "keyword2": "type2" }}
Use lowercase for both keyword and type. No explanation."""

SEO_KEYWORD_EXTRACTION_PROMPT = """Valid keywords examples:
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
# Appended: raw_text in triple quotes


# Keyword Intelligence Agent — score keyword usefulness
KEYWORD_CONFIDENCE_PROMPT = """SYSTEM:
You are an SEO analyst specializing in local service businesses.

TASK:
Given a list of extracted keywords and phrases, score each keyword's usefulness.

OUTPUT JSON (array of objects):
{
  "keyword": string,
  "confidence_score": number,
  "intent": "high" | "medium" | "low",
  "reason": string
}

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
{{KEYWORDS}}
"""
