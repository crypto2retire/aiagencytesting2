"""
Keyword Classifier — LLM-based classification of keywords.
Classifies into: service | geo | modifier | long_tail | brand
Returns structured JSON only.
"""

import logging
from typing import Dict, List, Optional

from database import KeywordIntelligence, SessionLocal
from llm import run_ollama
from prompts.seo import KEYWORD_CLASSIFICATION_PROMPT

from config import KEYWORD_CLASSIFIER_LOG, OLLAMA_MODEL

VALID_TYPES = frozenset({"service", "geo", "modifier", "long_tail", "brand"})
BATCH_SIZE = 40

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(KEYWORD_CLASSIFIER_LOG), logging.StreamHandler()],
)
log = logging.getLogger(__name__)


def classify_keywords(keywords: List[str]) -> Dict[str, str]:
    """
    Classify keywords into: service | geo | modifier | long_tail | brand.
    Returns dict of keyword -> type.
    """
    if not keywords:
        return {}

    all_classifications: Dict[str, str] = {}

    for i in range(0, len(keywords), BATCH_SIZE):
        batch = keywords[i : i + BATCH_SIZE]
        prompt = KEYWORD_CLASSIFICATION_PROMPT.format(keywords=", ".join(batch))
        try:
            data = run_ollama(prompt, model=OLLAMA_MODEL)
        except Exception as e:
            log.warning(f"Keyword classifier Ollama failed: {e}")
            continue
        if not isinstance(data, dict):
            continue
        for k, v in data.items():
            kw = str(k).lower().strip()
            t = str(v).lower().strip()
            if t in VALID_TYPES:
                all_classifications[kw] = t

    return all_classifications


def classify_region(region: str, client_id: Optional[str] = None) -> int:
    """
    Classify all unclassified keywords for a region. Updates DB.
    Returns count of keywords classified.
    """
    db = SessionLocal()
    try:
        q = db.query(KeywordIntelligence).filter(
            KeywordIntelligence.region == region,
            (KeywordIntelligence.keyword_type.is_(None)) | (KeywordIntelligence.keyword_type == ""),
        )
        if client_id:
            q = q.filter(KeywordIntelligence.client_id == client_id)
        keywords = list({r.keyword for r in q.all()})
        if not keywords:
            log.info(f"No unclassified keywords for region={region}")
            return 0

        log.info(f"Classifying {len(keywords)} keywords for region={region}")
        classifications = classify_keywords(keywords)

        count = 0
        for row in db.query(KeywordIntelligence).filter(
            KeywordIntelligence.region == region,
        ):
            if row.keyword in classifications:
                row.keyword_type = classifications[row.keyword]
                count += 1
        db.commit()
        log.info(f"Applied {len(classifications)} classifications, updated {count} rows")
        return count
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def run_classifier(region: str, client_id: Optional[str] = None) -> tuple[bool, str]:
    """
    Run classification for a region. For manual or batch use.
    Returns (success, message).
    """
    try:
        count = classify_region(region, client_id)
        return True, f"Classified {count} keywords for {region}."
    except Exception as e:
        log.exception(str(e))
        return False, str(e)
