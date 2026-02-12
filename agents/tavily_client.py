"""
Tavily client — competitor discovery and review-based fallback.

Guards: max_results=5, search_depth=basic (no loops, no advanced on main).
"""

from typing import Dict, List

from config import (
    TAVILY_API_KEY,
    TAVILY_MAX_RESULTS,
    TAVILY_REVIEWS_MAX_RESULTS,
    TAVILY_SEARCH_DEPTH,
)

# URLs where we should NOT use Firecrawl — use reviews instead
NON_WEBSITE_DOMAINS = (
    "facebook.com", "fb.com", "instagram.com", "linkedin.com",
    "yelp.com", "youtube.com", "tripadvisor.com",
    "google.com/maps", "maps.google.com", "goo.gl/maps",
    "bing.com/maps", "yellowpages.com", "angieslist.com",
    "homeadvisor.com", "nextdoor.com", "thumbtack.com",
)


def has_real_website(url: str) -> bool:
    """True if URL is a scrapable business site. False for listings/reviews."""
    if not url or len(url) < 10:
        return False
    return not any(d in url.lower() for d in NON_WEBSITE_DOMAINS)


def find_local_competitors(
    business_type: str,
    city: str,
    max_results: int = None,
) -> List[Dict]:
    """
    Tavily search for local competitors.
    Returns list of {name, url, content} dicts.
    Guard: max_results=5, search_depth=basic.
    """
    max_results = min(max_results or TAVILY_MAX_RESULTS, TAVILY_MAX_RESULTS)
    try:
        from tavily import TavilyClient
        client = TavilyClient(api_key=TAVILY_API_KEY)
        query = f"{business_type} {city}"
        response = client.search(
            query,
            max_results=max_results,
            search_depth=TAVILY_SEARCH_DEPTH,
        )
        results = []
        if isinstance(response, dict):
            results = response.get("results", [])
        elif hasattr(response, "results"):
            results = getattr(response, "results", None) or []
        if not isinstance(results, list):
            return []

        competitors = []
        seen = set()
        for r in results:
            if not isinstance(r, dict):
                continue
            name = (r.get("title") or r.get("name") or "").strip()
            url = (r.get("url") or r.get("link") or "").strip()
            content = (r.get("content") or r.get("snippet") or "").strip()
            if name and name not in seen:
                seen.add(name)
                competitors.append({"name": name, "url": url, "content": content})
        return competitors
    except Exception as e:
        return []


def get_services_from_reviews(competitor_name: str, city: str, niche: str = None) -> str:
    """
    Tavily search for reviews when competitor has no scrapable website.
    Returns raw text from review snippets.
    Guard: max_results=2, search_depth=basic (fallback only).
    """
    try:
        from tavily import TavilyClient
        client = TavilyClient(api_key=TAVILY_API_KEY)
        niche = niche or "Junk Removal"
        query = f'"{competitor_name}" {niche} reviews'
        response = client.search(
            query,
            max_results=TAVILY_REVIEWS_MAX_RESULTS,
            search_depth=TAVILY_SEARCH_DEPTH,
        )
        results = []
        if isinstance(response, dict):
            results = response.get("results", [])
        elif hasattr(response, "results"):
            results = getattr(response, "results", None) or []
        if not isinstance(results, list):
            return ""

        parts = []
        for r in results:
            c = r.get("content", r.get("snippet", ""))
            if c:
                parts.append(str(c))
        return " ".join(parts).strip() if parts else ""
    except Exception as e:
        return ""
