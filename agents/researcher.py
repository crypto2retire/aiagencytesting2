"""
Researcher — FULL ORCHESTRATION.

Client → Tavily → Firecrawl/Reviews → Ollama → Database

- Deduplicates competitors by domain (one entry per domain)
- Extracts multiple pages per site, scores each, averages → competitor_comparison_score
- Includes client website in same calculation → Client.avg_page_quality_score

"""

import logging
import time
from datetime import datetime
from typing import List, Optional, Tuple
from urllib.parse import urlparse

from config import (
    RESEARCHER_LOG,
    RESEARCHER_MAX_PAGES_PER_SITE,
    SLEEP_BETWEEN_COMPETITORS,
    TAVILY_MAX_RESULTS,
)
from sqlalchemy import func

from database import Client, CompetitorGeoCoverage, CompetitorPageScore, CompetitorWebsite, MarketSnapshot, ResearchLog, SessionLocal

from .firecrawl_client import detect_competitor_geo_pages, firecrawl_map, firecrawl_scrape, reset_firecrawl_domain_counts
from verticals import get_niche

from .keyword_classifier import run_classifier as run_keyword_classifier
from .keyword_extractor import extract_keywords, recalculate_keyword_confidence, store_keywords, upsert_keywords_from_profile
from website_quality_scorer import score_website_quality
from keyword_filter import is_valid_keyword
from .ollama_client import (
    extract_competitive_intelligence,
    extract_seo_keywords,
    json_extraction_to_research_fields,
    summarize_services,
)
from .tavily_client import find_local_competitors, get_services_from_reviews, has_real_website, reset_tavily_query_count
from verticals import get_opportunity_services
from geo_coverage_aggregator import aggregate_competitor_geo_coverage
from backlink_opportunity_detector import detect_and_store_backlink_opportunities
from roadmap_generator import generate_roadmap

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(RESEARCHER_LOG), logging.StreamHandler()],
)
log = logging.getLogger(__name__)


def _domain_from_url(url: str) -> str:
    """Extract domain from URL for deduplication."""
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        host = (parsed.netloc or parsed.path or "").lower()
        return host.replace("www.", "").split(":")[0]
    except Exception:
        return ""


def _get_pages_to_score(base_url: str, max_pages: int) -> List[str]:
    """Map site and return base_url + internal links (up to max_pages)."""
    pages = [base_url.rstrip("/")]
    map_result = firecrawl_map(base_url, limit=min(max_pages * 3, 150))
    if not map_result.get("success") or not map_result.get("links"):
        return pages
    base_domain = _domain_from_url(base_url)
    for link in map_result["links"]:
        if len(pages) >= max_pages:
            break
        link_url = (link.get("url") or "").strip()
        if not link_url:
            continue
        if _domain_from_url(link_url) != base_domain:
            continue
        if link_url.rstrip("/") not in (p.rstrip("/") for p in pages):
            pages.append(link_url)
    return pages[:max_pages]


def _extract_and_score_pages(
    pages: List[str],
    primary_url: str,
    company_name: str,
) -> Tuple[Optional[dict], str, float, List[Tuple[str, float]]]:
    """
    Scrape each page, extract profile, score.
    Returns (primary_extracted_profile, primary_raw_text, avg_score, [(page_url, page_score), ...]).
    """
    page_scores: List[Tuple[str, float]] = []
    primary_profile = None
    primary_raw = ""
    for page_url in pages:
        result = firecrawl_scrape(page_url)
        if not result.get("success"):
            continue
        raw_text = result.get("content") or ""
        if len(raw_text.strip()) < 50:
            continue
        json_data = extract_competitive_intelligence(raw_text, page_url)
        if json_data and isinstance(json_data, dict):
            sq = score_website_quality(json_data)
            score = max(0.0, min(100.0, sq.total))
            page_scores.append((page_url, score))
            if primary_profile is None:
                primary_profile = json_data
                primary_raw = raw_text
        time.sleep(0.5)  # rate limit between pages
    avg_score = sum(s for _, s in page_scores) / len(page_scores) if page_scores else 0.0
    return primary_profile, primary_raw, avg_score, page_scores


def _parse_city_state(region: str) -> Tuple[str, str]:
    """Parse 'Charlotte NC' -> ('Charlotte', 'NC'). Returns (city, state)."""
    if not region or not isinstance(region, str):
        return ("", "")
    parts = region.strip().split()
    if len(parts) >= 2 and len(parts[-1]) == 2:
        return (" ".join(parts[:-1]), parts[-1].upper())
    return (region.strip(), "")


def gather_intelligence(client_id: str, city: Optional[str] = None) -> str:
    """
    End-to-end competitor research for a single client.
    Client → Tavily → Firecrawl/Reviews → Ollama → Database
    """
    log.info(f"Researcher starting for client_id={client_id}")
    reset_tavily_query_count()
    reset_firecrawl_domain_counts()
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
        seen_domains = set()
        all_missed = []
        all_services = []
        weak_competitor_names = []
        strong_competitor_names = []

        # Deduplicate competitors by domain — keep first occurrence per domain
        deduped_competitors = []
        for comp in competitors:
            name = comp.get("name", "").strip()
            url = comp.get("url", "").strip()
            if not name:
                continue
            domain = _domain_from_url(url)
            if domain and domain in seen_domains:
                log.info(f"Skipping duplicate domain: {name} ({domain})")
                continue
            if domain:
                seen_domains.add(domain)
            deduped_competitors.append(comp)

        # Include client website in same multi-page scoring (if has real site)
        client_url = (client.website_url or "").strip()
        if has_real_website(client_url):
            deduped_competitors.insert(0, {"name": client.business_name or client_id, "url": client_url, "content": "", "is_client": True})
        else:
            client_url = None

        for comp in deduped_competitors:
            name = comp.get("name", "").strip()
            url = comp.get("url", "").strip()
            content = comp.get("content", "").strip()
            is_client = comp.get("is_client", False)

            if not name or (not is_client and name in seen_names):
                continue
            if not is_client:
                seen_names.add(name)
            log.info(f"Step 2: Processing — {name}{' (client)' if is_client else ''}")

            # 3. Multi-page extraction and scoring, or Reviews fallback
            extracted_profile = None
            raw_text = ""
            source_type = "website"
            quality_score = None
            competitor_comparison_score = None
            page_scores_list: List[Tuple[str, float]] = []

            if has_real_website(url):
                pages = _get_pages_to_score(url, RESEARCHER_MAX_PAGES_PER_SITE)
                log.info(f"  Extracting {len(pages)} pages from {url}")
                primary_profile, primary_raw, avg_score, page_scores_list = _extract_and_score_pages(pages, url, name)
                competitor_comparison_score = avg_score
                quality_score = max(0, min(100, int(round(avg_score))))

                if primary_profile:
                    extracted_profile = primary_profile
                    raw_text = primary_raw
                    parsed = json_extraction_to_research_fields(primary_profile)
                    keywords = list(dict.fromkeys(
                        (primary_profile.get("seo_keywords") or [])
                        + (primary_profile.get("service_city_phrases") or [])
                        + (primary_profile.get("geo_keywords") or [])
                    ))
                else:
                    raw_text = content or primary_raw
                    if len(raw_text.strip()) < 30:
                        raw_text = firecrawl_scrape(url).get("content", "") or content
                    try:
                        json_data = extract_competitive_intelligence(raw_text, url)
                        if json_data and isinstance(json_data, dict):
                            extracted_profile = json_data
                            parsed = json_extraction_to_research_fields(json_data)
                            keywords = list(dict.fromkeys(
                                (json_data.get("seo_keywords") or [])
                                + (json_data.get("service_city_phrases") or [])
                                + (json_data.get("geo_keywords") or [])
                            ))
                        else:
                            raise ValueError("No valid extraction")
                    except (ValueError, TypeError, KeyError, Exception):
                        summary = summarize_services(raw_text, name)
                        parsed = summary if isinstance(summary, dict) else {"extracted_services": [], "pricing_mentions": [], "complaints": [], "missed_opportunities": []}
                        keywords = extract_seo_keywords(raw_text) or extract_keywords(raw_text)
                        extracted_profile = {
                            "company_name": name,
                            "website_url": url,
                            "primary_services": parsed.get("extracted_services") or [],
                            "secondary_services": [],
                            "seo_keywords": keywords or [],
                            "geo_keywords": [],
                            "service_city_phrases": [],
                            "missed_opportunities": parsed.get("missed_opportunities") or [],
                        }
            else:
                raw_text = get_services_from_reviews(name, city, niche)
                if not raw_text:
                    raw_text = content
                source_type = "reviews"
                summary = summarize_services(raw_text, name)
                parsed = summary if isinstance(summary, dict) else {"extracted_services": [], "pricing_mentions": [], "complaints": [], "missed_opportunities": []}
                keywords = extract_seo_keywords(raw_text) or extract_keywords(raw_text)
                extracted_profile = {
                    "company_name": name,
                    "website_url": url,
                    "primary_services": parsed.get("extracted_services") or [],
                    "secondary_services": [],
                    "seo_keywords": keywords or [],
                    "geo_keywords": [],
                    "service_city_phrases": [],
                    "missed_opportunities": parsed.get("missed_opportunities") or [],
                }

            if len((raw_text or "").strip()) < 30 and not is_client:
                log.warning(f"Skipping {name}: insufficient text")
                continue

            services = parsed.get("extracted_services", [])
            pricing = parsed.get("pricing_mentions", [])
            complaints = parsed.get("complaints", [])
            missed = parsed.get("missed_opportunities", [])
            conf = 70 if (services or len((raw_text or "")) > 200) else 50

            if not is_client:
                all_missed.extend(missed or [])
                all_services.extend(services or [])

            # Client: update avg_page_quality_score only (no ResearchLog)
            if is_client:
                if competitor_comparison_score is not None:
                    client.avg_page_quality_score = competitor_comparison_score
                    db.add(client)
                time.sleep(SLEEP_BETWEEN_COMPETITORS)
                continue

            # 4b. Keyword extraction
            if extracted_profile:
                stored = upsert_keywords_from_profile(
                    extracted_profile,
                    region=city,
                    client_id=client_id,
                    vertical=vertical,
                    source_quality=quality_score,
                    source_url=url or None,
                )
            else:
                keywords = [kw for kw in (keywords or []) if is_valid_keyword(kw, vertical=vertical)]
                stored = store_keywords(keywords=keywords, region=city, source="competitor_site", client_id=client_id, vertical=vertical, source_url=url or None) if keywords else 0
            if stored:
                log.info(f"Stored {stored} keywords from {name}")

            # 4a. Upsert CompetitorWebsite (one per domain) + CompetitorPageScore; site_score = avg(page_scores)
            domain = _domain_from_url(url)
            if not is_client and domain and (page_scores_list or competitor_comparison_score is not None):
                cw = db.query(CompetitorWebsite).filter(
                    CompetitorWebsite.client_id == client_id,
                    func.lower(CompetitorWebsite.domain) == domain.lower(),
                ).first()
                if not cw:
                    cw = CompetitorWebsite(
                        client_id=client_id,
                        domain=domain,
                        competitor_name=name,
                        base_url=url,
                        site_score=float(competitor_comparison_score) if competitor_comparison_score is not None else None,
                    )
                    db.add(cw)
                    db.flush()
                else:
                    cw.competitor_name = name
                    cw.base_url = url
                if page_scores_list:
                    for page_url, ps in page_scores_list:
                        existing = db.query(CompetitorPageScore).filter(
                            CompetitorPageScore.competitor_website_id == cw.id,
                            CompetitorPageScore.page_url == page_url,
                        ).first()
                        if existing:
                            existing.page_score = ps
                        else:
                            db.add(CompetitorPageScore(competitor_website_id=cw.id, page_url=page_url, page_score=ps))
                    avg_ps = sum(s for _, s in page_scores_list) / len(page_scores_list)
                    cw.site_score = avg_ps
                elif competitor_comparison_score is not None:
                    cw.site_score = float(competitor_comparison_score)

            # 5a. Competitor geo coverage
            if has_real_website(url) and url:
                city_only, state_only = _parse_city_state(city)
                opportunity_svcs = get_opportunity_services(vertical) or []
                svcs = list(dict.fromkeys(
                    [s.strip().lower() for s in (services or []) if s] +
                    [s.strip().lower() for s in opportunity_svcs[:10] if s]
                ))
                geo_rows = detect_competitor_geo_pages(
                    base_url=url,
                    competitor_name=name,
                    city=city_only or city,
                    state=state_only or "",
                    services=svcs,
                    max_pages_to_scrape=2,
                    page_quality_score=float(quality_score) if quality_score is not None else None,
                )
                for row in geo_rows:
                    db.add(CompetitorGeoCoverage(
                        competitor_name=name,
                        website=url,
                        city=row.get("city"),
                        state=row.get("state"),
                        service=row.get("service"),
                        ranking_position=None,
                        page_exists=bool(row.get("page_exists")),
                        page_quality_score=row.get("page_quality_score"),
                        page_url=row.get("url"),
                        page_title=row.get("title"),
                        page_h1=row.get("h1"),
                    ))
                if geo_rows:
                    log.info(f"Saved {len(geo_rows)} geo coverage rows for {name}")

            # Quality differential vs client — for weak/strong classification
            client_avg = getattr(client, "avg_page_quality_score", None)
            if competitor_comparison_score is not None and client_avg is not None:
                diff = float(competitor_comparison_score) - float(client_avg)
                if diff < -10:
                    weak_competitor_names.append(name)
                elif diff > 10:
                    strong_competitor_names.append(name)

            # 5b. Database — save ResearchLog with competitor_comparison_score (avg of all pages)
            db.add(ResearchLog(
                client_id=client_id,
                competitor_name=name,
                source_type=source_type,
                raw_text=(raw_text or "")[:10000],
                extracted_services=services,
                pricing_mentions=pricing,
                complaints=complaints,
                missed_opportunities=missed,
                extracted_profile=extracted_profile,
                website_quality_score=quality_score,
                competitor_comparison_score=float(competitor_comparison_score) if competitor_comparison_score is not None else None,
                confidence_score=conf,
                city=city,
            ))

            time.sleep(SLEEP_BETWEEN_COMPETITORS)  # Guard: cheap + polite

        db.commit()

        # 7. MarketSnapshot (weak/strong from quality differential vs client)
        if seen_names:
            snapshot = MarketSnapshot(
                client_id=client_id,
                snapshot_id=run_id,
                city=city,
                primary_service=niche.lower(),
                strong_competitors=strong_competitor_names or list(seen_names),
                weak_competitors=weak_competitor_names,
                content_gaps=list(dict.fromkeys(all_missed))[:10],
                common_messaging_themes=list(dict.fromkeys(all_services))[:10],
                snapshot_date=datetime.utcnow().strftime("%Y-%m-%d"),
            )
            db.add(snapshot)
            db.commit()

        # 8. Keyword classification — once per batch (optional, lightweight)
        if seen_names:
            ok, _ = run_keyword_classifier(city, client_id)
            if ok:
                log.info("Keyword classification complete")

        # 9. Recalculate confidence scores for all keywords (runs after each research session)
        if seen_names:
            updated = recalculate_keyword_confidence(client_id)
            if updated:
                log.info(f"Recalculated confidence for {updated} keywords")

        # 10. Aggregate competitor geo coverage into City × Service density + avg quality
        try:
            agg_count = aggregate_competitor_geo_coverage(db)
            if agg_count:
                db.commit()
                log.info(f"Aggregated {agg_count} geo coverage density rows")
        except Exception as e:
            log.warning(f"Geo coverage aggregation failed: {e}")

        # 11. Detect local backlink opportunities (directories, chambers, etc.) — compare competitor vs client
        try:
            city_only, state_only = _parse_city_state(city)
            bl_count = detect_and_store_backlink_opportunities(
                client_id, city_only or city, state_only or "", db=db, max_sources_to_scrape=6
            )
            if bl_count:
                db.commit()
                log.info(f"Detected {bl_count} backlink opportunities")
        except Exception as e:
            log.warning(f"Backlink opportunity detection failed: {e}")

        # 12. Generate client roadmap (missing geo pages, weak competitors, backlinks, website quality)
        try:
            rm_count = generate_roadmap(client_id, db=db)
            if rm_count:
                db.commit()
                log.info(f"Generated {rm_count} roadmap items")
        except Exception as e:
            log.warning(f"Roadmap generation failed: {e}")

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
