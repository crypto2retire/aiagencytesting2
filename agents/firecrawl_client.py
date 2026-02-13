"""
Firecrawl client — scrape and map. Never used for Facebook, Google Maps, Yelp, etc.
"""

import re
import logging
import requests
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from config import FIRECRAWL_API_KEY, FIRECRAWL_TIMEOUT

log = logging.getLogger(__name__)


def reset_firecrawl_domain_counts() -> None:
    """No-op; kept for API compatibility."""
    pass


def _domain_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "").lower() or ""
    except Exception:
        return ""


FIRECRAWL_BASE_URL = "https://api.firecrawl.dev/v1/scrape"
# v2 map returns links with url/title/description; v1 returns links as string[]
FIRECRAWL_MAP_URL = "https://api.firecrawl.dev/v1/map"

HEADERS = {
    "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
    "Content-Type": "application/json",
}

# Block bad URLs early — Firecrawl should never touch listings/reviews
BLOCKED_DOMAINS = [
    "facebook.com", "fb.com", "instagram.com", "yelp.com",
    "google.com/maps", "maps.google.com", "goo.gl/maps",
    "linkedin.com", "youtube.com", "tripadvisor.com",
    "bing.com/maps", "yellowpages.com", "angieslist.com",
    "homeadvisor.com", "nextdoor.com", "thumbtack.com",
]


def is_supported_url(url: str) -> bool:
    """True if URL is a scrapable business site. False for listings/reviews."""
    if not url or len(url) < 10:
        return False
    return not any(domain in url.lower() for domain in BLOCKED_DOMAINS)


def firecrawl_scrape(url: str, timeout: int = None) -> dict:
    """
    Scrapes a single business website using Firecrawl.
    Returns dict with: success, content, source.
    """
    if not url or not is_supported_url(url):
        return {"success": False, "content": "Unsupported or missing website URL", "source": "firecrawl"}

    payload = {
        "url": url,
        "formats": ["markdown"],
        "onlyMainContent": True,
    }

    try:
        response = requests.post(
            FIRECRAWL_BASE_URL,
            headers=HEADERS,
            json=payload,
            timeout=timeout or FIRECRAWL_TIMEOUT,
        )
        if response.status_code != 200:
            return {"success": False, "content": f"Firecrawl error: {response.status_code}", "source": "firecrawl"}

        data = response.json()
        markdown = data.get("data", {}).get("markdown", "").strip()
        if not markdown:
            return {"success": False, "content": "Empty scrape result", "source": "firecrawl"}

        return {"success": True, "content": markdown, "source": "firecrawl"}
    except requests.exceptions.Timeout:
        log.warning(f"Firecrawl scrape timeout: {url[:50]}")
        return {"success": False, "content": "Firecrawl timeout", "source": "firecrawl"}
    except Exception as e:
        log.warning(f"Firecrawl scrape failed: {e}")
        return {"success": False, "content": f"Firecrawl exception: {str(e)}", "source": "firecrawl"}


def firecrawl_map(url: str, search: str = "", limit: int = 200, timeout: int = None) -> dict:
    """
    Map internal URLs on a website using Firecrawl.
    Returns dict with success, links.
    """
    if not url or not is_supported_url(url):
        return {"success": False, "links": []}

    payload = {"url": url, "limit": min(limit, 5000)}
    if search:
        payload["search"] = search

    try:
        response = requests.post(
            FIRECRAWL_MAP_URL,
            headers=HEADERS,
            json=payload,
            timeout=timeout or FIRECRAWL_TIMEOUT,
        )
        if response.status_code != 200:
            return {"success": False, "links": []}
        data = response.json()
        raw = data.get("links", []) or data.get("data", {}).get("links", [])
        if not isinstance(raw, list):
            return {"success": True, "links": []}
        links = []
        for item in raw:
            if isinstance(item, str):
                links.append({"url": item, "title": "", "description": ""})
            elif isinstance(item, dict):
                links.append({
                    "url": item.get("url", ""),
                    "title": item.get("title", ""),
                    "description": item.get("description", ""),
                })
        return {"success": True, "links": links}
    except requests.exceptions.Timeout:
        log.warning(f"Firecrawl map timeout: {url[:50]}")
        return {"success": False, "links": []}
    except Exception as e:
        log.warning(f"Firecrawl map failed: {e}")
        return {"success": False, "links": []}


def _extract_h1_from_markdown(markdown: str) -> Optional[str]:
    """Extract first H1 (# header) from markdown."""
    if not markdown:
        return None
    m = re.search(r"^#\s+(.+)$", markdown, re.MULTILINE)
    return m.group(1).strip() if m else None


def detect_competitor_geo_pages(
    base_url: str,
    competitor_name: str,
    city: str,
    state: str,
    services: List[str],
    max_pages_to_scrape: int = 2,
    page_quality_score: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """
    Detect geo pages (service + city) on competitor site.
    Returns list of {url, title, h1, city, state, service, page_exists, page_quality_score}.
    Uses Firecrawl map + optional scrape for h1.
    """
    city_lower = (city or "").lower().strip()
    city_slug = re.sub(r"[^a-z0-9]+", "-", city_lower).strip("-")
    state_upper = (state or "").strip().upper()

    map_result = firecrawl_map(base_url, search=city_lower, limit=150)
    if not map_result.get("success") or not map_result.get("links"):
        return _no_page_rows(competitor_name, base_url, city, state, services, page_quality_score)

    matches = []
    for link in map_result["links"]:
        link_url = (link.get("url") or "").strip()
        link_title = (link.get("title") or "").strip()
        link_desc = (link.get("description") or "").strip()
        combined = f"{link_url} {link_title} {link_desc}".lower()

        if city_lower not in combined and city_slug not in combined:
            continue

        for svc in (services or []):
            svc_lower = (svc or "").lower().strip()
            if not svc_lower:
                continue
            svc_words = svc_lower.replace("-", " ").split()
            if any(w in combined for w in svc_words if len(w) > 2):
                matches.append({
                    "url": link_url,
                    "title": link_title or None,
                    "h1": None,
                    "city": city,
                    "state": state,
                    "service": svc,
                    "page_exists": True,
                    "page_quality_score": page_quality_score,
                })
                break

    # Dedupe matches by service (keep first match per service)
    seen_svc = set()
    deduped = []
    for m in matches:
        k = (m["service"] or "").lower()
        if k and k not in seen_svc:
            seen_svc.add(k)
            deduped.append(m)

    # Build full result: one row per service (found or not)
    found_services = {(m["service"] or "").lower() for m in deduped}
    result = list(deduped)

    for svc in (services or []):
        if not svc:
            continue
        if (svc or "").lower() not in found_services:
            result.append({
                "url": None, "title": None, "h1": None,
                "city": city, "state": state, "service": svc,
                "page_exists": False, "page_quality_score": page_quality_score,
            })

    # Optionally scrape top N pages for h1 (limit cost)
    scraped = 0
    for m in result:
        m.setdefault("page_quality_score", page_quality_score)
        if scraped < max_pages_to_scrape and m.get("page_exists") and m.get("url"):
            scrape_result = firecrawl_scrape(m["url"])
            if scrape_result.get("success"):
                m["h1"] = _extract_h1_from_markdown(scrape_result.get("content") or "")
            scraped += 1

    return result if result else _no_page_rows(competitor_name, base_url, city, state, services, page_quality_score)


def _no_page_rows(
    competitor_name: str, website: str, city: str, state: str, services: list,
    page_quality_score: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """Return rows for services with no geo page found."""
    return [
        {
            "url": None, "title": None, "h1": None,
            "city": city, "state": state, "service": svc,
            "page_exists": False, "page_quality_score": page_quality_score,
        }
        for svc in (services or []) if svc
    ]
