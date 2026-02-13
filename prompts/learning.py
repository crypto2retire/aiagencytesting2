"""
Learning prompts — performance analysis, before/after insights.
For the Learning Agent (later-stage feedback loop).
"""

# Learning Agent — analyze metrics and recommend improvements
PERFORMANCE_ANALYSIS_PROMPT = """SYSTEM:
You are a performance analyst.

TASK:
Analyze before/after metrics and recommend improvements.

INPUT:
Traffic, rankings, leads, conversions.

OUTPUT JSON:
{
  "what_worked": string[],
  "what_didnt": string[],
  "next_actions": string[]
}

Return ONLY valid JSON. No commentary. No markdown.

METRICS:
{{METRICS}}
"""
