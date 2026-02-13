"""
Backlink Opportunity Detector — detect local backlink sources and compare competitor vs client.

Source types: directory, chamber, city_business_list, local_blog.
Uses Tavily + Firecrawl; no backlink API required.
"""

import re
import time
from typing import Optional
from urllib.parse import urlparse

from sqlalchemy import func

from database import BacklinkOpportunity, Client, ResearchLog, SessionLocal
from agents.tavily_client import reset_tavily_query_count, tavily_search
from agents.firecrawl_client import firecrawl_scrape, is_supported_url, reset_firecrawl_domain_counts

SOURCE_TYPE_PATTERNS = {
    "directory": [
        r"directory", r"listing", r"yellow.?pages", r"yelp",
        r"homeadvisor", r"angie.?list", r"thumbtack", r"find.?local",
    ],
    "chamber": [
        r"chamber", r"chamberofcommerce", r"cofc",
    ],
    "city_business_list": [
        r"business.?list", r"local.?business", r"small.?business",
        r"city.?guide", r"city.?business", r"economic.?development",
    ],
    "local_blog": [
        r"blog", r"news", r"journal", r"times", r"post",
        r"tribune", r"herald", r"daily", r"weekly",
    ],
}


def _classify_source_type(url: str, title: str = "", content: str = "") -> str:
    """Classify source as directory, chamber, city_business_list, or local_blog."""
    combined = f"{url} {title} {content}".lower()
    for stype, patterns in SOURCE_TYPE_PATTERNS.items():
        if any(re.search(p, combined) for p in patterns):
            return stype
    return "other"


def _domain_from_url(url: str) -> str:
    """Extract domain from URL."""
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        host = (parsed.netloc or parsed.path or "").lower()
        return host.replace("www.", "").split(":")[0]
    except Exception:
        return ""


def _mentioned_in_text(text: str, names: list[str], domains: list[str]) -> list[str]:
    """Return list of names/domains that appear in text (case-insensitive)."""
    if not text:
        return []
    text_lower = text.lower()
    found = []
    for n in (names or []):
        if n and len(str(n).strip()) > 2 and str(n).strip().lower() in text_lower:
            found.append(n)
    for d in (domains or []):
        if d and len(d) > 4 and d.lower() in text_lower:
            found.append(d)
    return found


def discover_local_backlink_sources(city: str, state: str = "", max_per_type: int = 5) -> list[dict]:
    """
    Discover potential local backlink sources via Tavily.
    Returns list of {url, title, content, source_type, domain}.
    """
    city = (city or "").strip()
    state = (state or "").strip()
    if not city:
        return []

    queries = [
        f'"{city}" business directory',
        f'"{city}" chamber of commerce',
        f'"{city}" local business list',
        f'"{city}" local blog news',
    ]

    seen_urls = set()
    results = []
    for q in queries:
        hits = tavily_search(q, max_results=max_per_type)
        for h in hits:
            url = (h.get("url") or "").strip()
            if not url or url in seen_urls:
                continue
            if not is_supported_url(url):
                continue
            seen_urls.add(url)
            stype = _classify_source_type(
                url,
                h.get("title", ""),
                h.get("content", ""),
            )
            results.append({
                "url": url,
                "title": h.get("title", ""),
                "content": h.get("content", ""),
                "source_type": stype,
                "domain": _domain_from_url(url),
            })
        time.sleep(0.5)

    return results


def detect_and_store_backlink_opportunities(
    client_id: str,
    city: str,
    state: str = "",
    db=None,
    max_sources_to_scrape: int = 8,
) -> int:
    """
    Discover local backlink sources, compare competitor vs client, store opportunities.
    Returns count of opportunities stored.

    Logic: For each source, scrape and check who is mentioned.
    If competitors are linked/mentioned but client is not → backlink opportunity.
    """
    reset_tavily_query_count()
    reset_firecrawl_domain_counts()
    sess = db or SessionLocal()
    try:
        client = sess.query(Client).filter(Client.client_id == client_id).first()
        if not client:
            return 0

        client_domain = _domain_from_url(client.website_url or "")
        client_name = (client.business_name or "").strip()

        logs = sess.query(ResearchLog).filter(ResearchLog.client_id == client_id).all()
        competitor_names = list({(rl.competitor_name or "").strip() for rl in logs if (rl.competitor_name or "").strip()})
        competitor_domains = []
        for rl in logs:
            profile = rl.extracted_profile or {}
            url = (profile.get("website_url") or "").strip()
            if url:
                competitor_domains.append(_domain_from_url(url))

        if not competitor_names and not competitor_domains:
            return 0

        sources = discover_local_backlink_sources(city, state, max_per_type=5)
        count = 0

        for i, src in enumerate(sources):
            if i >= max_sources_to_scrape:
                break

            url = src.get("url", "")
            domain = src.get("domain", "")
            stype = src.get("source_type", "other")
            content = src.get("content", "")

            # Scrape for fuller content
            if url:
                result = firecrawl_scrape(url)
                if result.get("success"):
                    content = (content or "") + " " + (result.get("content", "") or "")
                time.sleep(1)

            linked_competitors = _mentioned_in_text(
                content,
                competitor_names,
                [d for d in competitor_domains if d and d != client_domain],
            )
            client_mentioned = _mentioned_in_text(
                content,
                [client_name] if client_name else [],
                [client_domain] if client_domain else [],
            )

            if linked_competitors and not client_mentioned:
                confidence = 0.5 + min(0.4, len(linked_competitors) * 0.1)
                if stype == "chamber":
                    confidence += 0.1
                elif stype == "directory":
                    confidence += 0.05
                confidence = min(1.0, confidence)

                existing = sess.query(BacklinkOpportunity).filter(
                    BacklinkOpportunity.client_id == client_id,
                    BacklinkOpportunity.domain == domain,
                    BacklinkOpportunity.city == city,
                    func.coalesce(BacklinkOpportunity.state, "") == (state or ""),
                ).first()

                if not existing:
                    sess.add(BacklinkOpportunity(
                        client_id=client_id,
                        domain=domain,
                        source_type=stype,
                        city=city,
                        state=state or None,
                        linked_competitors=list(set(linked_competitors))[:20],
                        confidence_score=confidence,
                    ))
                    count += 1

        if db is None:
            sess.commit()
        return count
    except Exception:
        if db is None:
            sess.rollback()
        raise
    finally:
        if db is None:
            sess.close()
