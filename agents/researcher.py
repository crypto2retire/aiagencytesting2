"""
Researcher — FULL ORCHESTRATION.

Client → Tavily → Firecrawl/Reviews → Ollama → Database

Guards: Tavily max_results=5, Firecrawl 30s timeout, Ollama 45s no-retry, sleep(2) between.
"""

import logging
import time
from datetime import datetime
from typing import Optional

from config import (
    RESEARCHER_LOG,
    SLEEP_BETWEEN_COMPETITORS,
    TAVILY_MAX_RESULTS,
)
from sqlalchemy import func

from database import Client, MarketSnapshot, ResearchLog, SessionLocal

from .firecrawl_client import firecrawl_scrape
from verticals import get_niche

from .keyword_classifier import run_classifier as run_keyword_classifier
from .keyword_extractor import extract_keywords, store_keywords
from keyword_filter import is_valid_keyword
from .ollama_client import extract_seo_keywords, parse_summary_to_fields, summarize_services
from .tavily_client import find_local_competitors, get_services_from_reviews, has_real_website

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(RESEARCHER_LOG), logging.StreamHandler()],
)
log = logging.getLogger(__name__)


def gather_intelligence(client_id: str, city: Optional[str] = None) -> str:
    """
    End-to-end competitor research for a single client.
    Client → Tavily → Firecrawl/Reviews → Ollama → Database
    """
    log.info(f"Researcher starting for client_id={client_id}")
    db = SessionLocal()
    try:
        # Case-insensitive client lookup (CLI may pass "CTC" but DB has "ctc")
        client = db.query(Client).filter(func.lower(Client.client_id) == client_id.lower()).first()
        if not client:
            log.error(f"Client {client_id} not found")
            return ""

        client_id = client.client_id  # Use actual stored value for DB writes
        city = city or (client.cities_served[0] if client.cities_served else "Unknown")
        vertical = (client.client_vertical or "junk_removal").strip().lower()
        niche = get_niche(vertical)
        query = f"{niche} {city}" if city != "Unknown" else niche
        log.info(f"Step 1: Load client — city={city}, niche={niche}")

        # 2. Tavily — find local competitors (guard: max_results=5)
        competitors = find_local_competitors(
            business_type=niche,
            city=city,
            max_results=TAVILY_MAX_RESULTS,
        )

        if not competitors:
            log.warning("No competitors found from Tavily")
            run_id = f"{city.lower().replace(' ', '-')}-{datetime.utcnow().strftime('%Y-%m-%d-%H%M')}"
            return run_id

        run_id = f"{city.lower().replace(' ', '-')}-{datetime.utcnow().strftime('%Y-%m-%d-%H%M')}"
        seen_names = set()
        all_missed = []
        all_services = []

        for comp in competitors:
            name = comp.get("name", "").strip()
            url = comp.get("url", "").strip()
            content = comp.get("content", "").strip()

            if not name or name in seen_names:
                continue
            seen_names.add(name)
            log.info(f"Step 2: Processing — {name}")

            # 3. Firecrawl or Reviews
            if has_real_website(url):
                result = firecrawl_scrape(url)
                if result["success"]:
                    raw_text = result["content"]
                    source_type = "website"
                else:
                    log.warning(f"Firecrawl failed for {url}: {result['content']}")
                    raw_text = content
                    source_type = "website"
            else:
                raw_text = get_services_from_reviews(name, city, niche)
                if not raw_text:
                    raw_text = content
                source_type = "reviews"

            if len(raw_text.strip()) < 30:
                log.warning(f"Skipping {name}: insufficient text")
                continue

            # 4. Ollama — summarize
            summary = summarize_services(raw_text, name)
            if summary.startswith("Ollama summarization failed"):
                log.warning(summary)
                parsed = {"extracted_services": [], "pricing_mentions": [], "complaints": [], "missed_opportunities": []}
            else:
                parsed = parse_summary_to_fields(summary)

            services = parsed.get("extracted_services", [])
            pricing = parsed.get("pricing_mentions", [])
            complaints = parsed.get("complaints", [])
            missed = parsed.get("missed_opportunities", [])
            conf = 70 if (services or len(raw_text) > 200) else 50

            all_missed.extend(missed or [])
            all_services.extend(services or [])

            # 4b. Keyword extraction — Ollama SEO prompt first, fallback to regex; filter before save
            text_for_keywords = summary if not summary.startswith("Ollama summarization failed") else raw_text
            keywords = extract_seo_keywords(text_for_keywords)
            if not keywords:
                keywords = extract_keywords(text_for_keywords)
            keywords = [kw for kw in keywords if is_valid_keyword(kw, vertical=vertical)]
            if keywords:
                stored = store_keywords(
                    keywords=keywords,
                    region=city,
                    source="competitor_site",
                    client_id=client_id,
                    vertical=vertical,
                )
                if stored:
                    log.info(f"Stored {stored} keywords from {name}")

            # 5. Database — save
            db.add(ResearchLog(
                client_id=client_id,
                competitor_name=name,
                source_type=source_type,
                raw_text=raw_text[:10000],
                extracted_services=services,
                pricing_mentions=pricing,
                complaints=complaints,
                missed_opportunities=missed,
                confidence_score=conf,
                city=city,
            ))

            time.sleep(SLEEP_BETWEEN_COMPETITORS)  # Guard: cheap + polite

        db.commit()

        # 6. MarketSnapshot
        if seen_names:
            snapshot = MarketSnapshot(
                client_id=client_id,
                snapshot_id=run_id,
                city=city,
                primary_service=niche.lower(),
                strong_competitors=list(seen_names),
                content_gaps=list(dict.fromkeys(all_missed))[:10],
                common_messaging_themes=list(dict.fromkeys(all_services))[:10],
                snapshot_date=datetime.utcnow().strftime("%Y-%m-%d"),
            )
            db.add(snapshot)
            db.commit()

        # 7. Keyword classification — once per batch (optional, lightweight)
        if seen_names:
            ok, _ = run_keyword_classifier(city, client_id)
            if ok:
                log.info("Keyword classification complete")

        log.info(f"Researcher done. Saved {len(seen_names)} entries.")
        return run_id
    except Exception as e:
        db.rollback()
        log.exception(str(e))
        raise
    finally:
        db.close()


def run_researcher(client_id: str, city: Optional[str] = None) -> str:
    """Alias for gather_intelligence."""
    return gather_intelligence(client_id, city)
