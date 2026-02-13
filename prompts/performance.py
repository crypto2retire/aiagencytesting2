PERFORMANCE_ANALYSIS_PROMPT = """SYSTEM:
You are a performance analyst.

TASK:
Analyze before/after metrics and recommend improvements.

INPUT:
Traffic, rankings, leads, conversions.

OUTPUT JSON:
{{
  "what_worked": string[],
  "what_didnt": string[],
  "next_actions": string[]
}}

Return ONLY valid JSON. No commentary. No markdown.

METRICS:
{{METRICS}}
"""


def get_prompt(**kwargs) -> str:
    metrics = kwargs.get("metrics", "")
    return PERFORMANCE_ANALYSIS_PROMPT.replace("{{METRICS}}", metrics)
