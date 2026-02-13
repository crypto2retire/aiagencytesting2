"""
Prompt library — centralized LLM prompts for extraction, SEO, scoring, generation.
Import from here or from submodules.
"""

from prompts.extraction import get_prompt as get_extraction_prompt, get_summarize_prompt
from prompts.seo import (
    FULL_PAGE_GENERATION_PROMPT,
    GEO_PHRASE_EXTRACTION_PROMPT,
    GEO_PAGE_OUTLINE_PROMPT,
    KEYWORD_CLASSIFICATION_PROMPT,
    KEYWORD_CONFIDENCE_PROMPT,
    PAGE_OUTLINE_PROMPT,
    SEO_KEYWORD_EXTRACTION_PROMPT,
)
from prompts.clustering import GEO_CLUSTERING_PROMPT
from prompts.learning import PERFORMANCE_ANALYSIS_PROMPT
from prompts.scoring import get_prompt as get_scoring_prompt
from prompts.generation import (
    CONTENT_DRAFT_PROMPT_TEMPLATE,
    FULL_PAGE_WRITER_PROMPT,
    PROPOSAL_PROMPT,
    SALES_PROPOSAL_PROMPT,
)

__all__ = [
    "get_extraction_prompt",
    "get_summarize_prompt",
    "GEO_PHRASE_EXTRACTION_PROMPT",
    "GEO_PAGE_OUTLINE_PROMPT",
    "FULL_PAGE_GENERATION_PROMPT",
    "KEYWORD_CLASSIFICATION_PROMPT",
    "KEYWORD_CONFIDENCE_PROMPT",
    "PAGE_OUTLINE_PROMPT",
    "SEO_KEYWORD_EXTRACTION_PROMPT",
    "GEO_CLUSTERING_PROMPT",
    "PERFORMANCE_ANALYSIS_PROMPT",
    "get_scoring_prompt",
    "SALES_PROPOSAL_PROMPT",
    "PROPOSAL_PROMPT",
    "CONTENT_DRAFT_PROMPT_TEMPLATE",
    "FULL_PAGE_WRITER_PROMPT",
]
