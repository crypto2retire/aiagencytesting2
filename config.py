"""
Single source of truth for configuration.
Everything imports from here. No hardcoded keys.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ─── Required API keys (fail loudly if missing) ───────────────────────────
REQUIRED_KEYS = ["TAVILY_API_KEY", "FIRECRAWL_API_KEY", "ANTHROPIC_API_KEY"]


def _validate_required():
    for key in REQUIRED_KEYS:
        val = os.getenv(key, "").strip()
        if not val:
            raise RuntimeError(
                f"Missing required config: {key}. Set it in .env. See .env.example."
            )


# Validate on import — fail loudly
_validate_required()

TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "").strip()
FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY", "").strip()
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "").strip()

# ─── Optional / with defaults ──────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./agency_ai.db")

OLLAMA_MODEL = (
    "junk-removal-seo"
    if os.getenv("USE_JUNK_MODEL", "false").lower() == "true"
    else "llama3.1:8b"
)
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
DEFAULT_NICHE = os.getenv("DEFAULT_NICHE", "Junk Removal")
SEARCH_RESULTS_LIMIT = int(os.getenv("SEARCH_RESULTS_LIMIT", "5"))

ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022")
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
MIN_CONFIDENCE_FOR_STRATEGIST = int(os.getenv("MIN_CONFIDENCE_FOR_STRATEGIST", "50"))

# ─── Project paths ────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(exist_ok=True)
PROPOSALS_DIR = PROJECT_ROOT / "proposals"
PROPOSALS_DIR.mkdir(exist_ok=True)

RESEARCHER_LOG = str(LOGS_DIR / "researcher.log")
STRATEGIST_LOG = str(LOGS_DIR / "strategist.log")
OPPORTUNITY_LOG = str(LOGS_DIR / "opportunity_scorer.log")
KEYWORD_CLASSIFIER_LOG = str(LOGS_DIR / "keyword_classifier.log")
PERFORMANCE_LOG = str(LOGS_DIR / "performance.log")

# ─── Researcher / LLM settings ───────────────────────────────────────────────
TAVILY_MAX_RESULTS = 5
TAVILY_SEARCH_DEPTH = "basic"
TAVILY_REVIEWS_MAX_RESULTS = 2
FIRECRAWL_TIMEOUT = 30
OLLAMA_TIMEOUT = 45
OLLAMA_STREAM = False
OLLAMA_FALLBACK_ON_TIMEOUT = True  # Deprecated: kept for import compat; no longer used by llm.py
SLEEP_BETWEEN_COMPETITORS = 2
RESEARCHER_MAX_PAGES_PER_SITE = int(os.getenv("RESEARCHER_MAX_PAGES_PER_SITE", "6"))
