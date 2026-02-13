"""
Geo Phrase Extractor — extracts clean (service, city) pairs from SEO keywords and service_city phrases.

Takes seo_keywords + service_city_phrases, matches known service terms and known cities
from research location, returns deduplicated (service, city) pairs.

Normalization: lowercase, remove stop words, canonical city names (e.g. phx → phoenix).
"""

import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

from keyword_filter import detect_and_normalize_geo_keyword
from services_taxonomy import SERVICE_NOUNS, SERVICE_VERBS

# Stop words to remove from extracted phrases (service + city)
STOPWORDS = frozenset([
    "the", "and", "for", "with", "that", "this", "from", "your",
    "are", "was", "have", "has", "you", "not", "but", "they",
    "all", "can", "her", "his", "been", "had", "its", "our", "out",
    "who", "how", "why", "what", "when", "where", "into", "some",
    "just", "than", "then", "only", "about", "like", "over", "more",
])

# City abbreviations/nicknames → canonical name (lowercase)
CITY_CANONICAL = {
    "phx": "phoenix",
    "la": "los angeles",
    "lax": "los angeles",
    "l.a.": "los angeles",
    "sf": "san francisco",
    "sfo": "san francisco",
    "nyc": "new york",
    "chi": "chicago",
    "ord": "chicago",
    "hou": "houston",
    "iah": "houston",
    "philly": "philadelphia",
    "phl": "philadelphia",
    "sd": "san diego",
    "dc": "washington",
    "dca": "washington",
    "mia": "miami",
    "atl": "atlanta",
    "den": "denver",
    "sea": "seattle",
    "slc": "salt lake city",
    "vegas": "las vegas",
    "lv": "las vegas",
    "tpa": "tampa",
    "stl": "st louis",
    "mil": "milwaukee",
    "mke": "milwaukee",
    "clt": "charlotte",
    "scotts": "scottsdale",
    "temp": "tempe",
}


def _has_service_term(phrase: str) -> bool:
    """True if phrase contains a known service noun or verb."""
    if not phrase or not isinstance(phrase, str):
        return False
    p = phrase.lower().strip()
    return any(n in p for n in SERVICE_NOUNS) or any(v in p for v in SERVICE_VERBS)


def _looks_like_place(name: str) -> bool:
    """Reject 'cities' that are actually service terms."""
    if not name or len(name) < 2:
        return False
    n = name.lower().strip()
    if n in SERVICE_NOUNS or n in SERVICE_VERBS:
        return False
    if any(t in n for t in ("removal", "haul", "cleanout", "pickup", "disposal")):
        return False
    return True


def _normalize_city_for_match(city: str) -> str:
    """Lowercase, strip, collapse whitespace for matching."""
    if not city or not isinstance(city, str):
        return ""
    return re.sub(r"\s+", " ", city.lower().strip())


def _remove_stopwords(phrase: str) -> str:
    """Remove stop words, collapse whitespace. Keeps word order."""
    if not phrase or not isinstance(phrase, str):
        return ""
    words = phrase.lower().strip().split()
    kept = [w for w in words if w not in STOPWORDS]
    return re.sub(r"\s+", " ", " ".join(kept)).strip()


def _canonicalize_city(city: str) -> str:
    """Map city abbreviations to canonical names (e.g. phx → phoenix)."""
    if not city or not isinstance(city, str):
        return ""
    c = city.lower().strip()
    # Whole-phrase match first
    if c in CITY_CANONICAL:
        return CITY_CANONICAL[c]
    # Word-by-word for "phx az" → "phoenix az"
    words = c.split()
    canonical = []
    for w in words:
        clean = re.sub(r"[,.]", "", w)
        if clean in CITY_CANONICAL:
            canonical.append(CITY_CANONICAL[clean])
        else:
            canonical.append(w)
    return re.sub(r"\s+", " ", " ".join(canonical)).strip()


def _normalize_phrase(phrase: str) -> str:
    """Full normalization: lowercase, remove stop words, collapse whitespace."""
    if not phrase or not isinstance(phrase, str):
        return ""
    lowered = phrase.lower().strip()
    return _remove_stopwords(lowered)


def _clean_service(raw: str) -> str:
    """Remove trailing prepositions (in, near, around) from service part."""
    if not raw:
        return ""
    return re.sub(r"\s+(in|near|around)\s*$", "", raw, flags=re.IGNORECASE).strip()


def _find_city_in_phrase(phrase: str, known_cities: Set[str]) -> Optional[Tuple[str, str]]:
    """
    If phrase contains a known city, return (service_part, city).
    known_cities should be normalized (lowercase).
    """
    if not phrase or not known_cities:
        return None
    p = phrase.lower().strip()
    if not p:
        return None

    # Prefer longer matches first (e.g. "phoenix az" before "phoenix")
    sorted_cities = sorted(known_cities, key=len, reverse=True)
    for city in sorted_cities:
        if not city:
            continue
        # Whole-word match
        pattern = r"\b" + re.escape(city) + r"\b"
        if re.search(pattern, p):
            parts = re.split(pattern, p, maxsplit=1)
            before = (parts[0] or "").strip()
            after = (parts[1] or "").strip() if len(parts) > 1 else ""
            if before and _has_service_term(before):
                service = _clean_service(re.sub(r"\s+", " ", before))
                if service:
                    return (service, city)
            if after and _has_service_term(after):
                service = _clean_service(re.sub(r"\s+", " ", after))
                if service:
                    return (service, city)
            if before:
                cleaned = _clean_service(before)
                if cleaned and _has_service_term(cleaned):
                    return (cleaned, city)
    return None


def extract_geo_phrases(
    seo_keywords: Iterable[str],
    service_city_phrases: Iterable[str],
    known_cities: Iterable[str],
) -> List[Tuple[str, str]]:
    """
    Extract clean (service, city) pairs from keywords and phrases.

    - Uses detect_and_normalize_geo_keyword for phrases with embedded geo
    - Matches known service terms (SERVICE_NOUNS, SERVICE_VERBS)
    - Matches known cities from research location (city names, city+state)
    - Returns deduplicated list of (service, city) tuples

    Args:
        seo_keywords: SEO keywords from extracted_profile
        service_city_phrases: service+city phrases from extracted_profile
        known_cities: Cities from research location (e.g. cities_served, snapshot cities)

    Returns:
        List of (service, city) pairs. Service and city are lowercase, trimmed.
    """
    all_phrases: List[str] = []
    for src in (seo_keywords, service_city_phrases):
        for p in src or []:
            if p and isinstance(p, str) and p.strip():
                all_phrases.append(p.strip())

    known_set: Set[str] = set()
    for c in known_cities or []:
        norm = _normalize_city_for_match(c)
        if norm:
            known_set.add(norm)
        parts = norm.split()
        if len(parts) >= 2 and len(parts[-1]) == 2:
            # "phoenix az" -> also add "phoenix" for flexible matching
            known_set.add(" ".join(parts[:-1]))
    # Add abbreviations that map to known cities (e.g. phx when Phoenix is known)
    for abbrev, canonical in CITY_CANONICAL.items():
        for k in list(known_set):
            if k == canonical:
                known_set.add(abbrev)
                break
            if " " in k and k.startswith(canonical + " "):
                known_set.add(abbrev + " " + k.split(" ", 1)[1])
                break

    seen: Set[Tuple[str, str]] = set()
    result: List[Tuple[str, str]] = []

    for phrase in all_phrases:
        if not phrase:
            continue

        # 1. Try detect_and_normalize (handles "service in city state", "service city st")
        info = detect_and_normalize_geo_keyword(phrase)
        if info.get("is_geo_phrase") and info.get("service") and info.get("geo"):
            if _has_service_term(info["service"]) and _looks_like_place(info["geo"]):
                service = _normalize_phrase(info["service"])
                city = _canonicalize_city(_normalize_city_for_match(info["geo"]))
                if service and city:
                    key = (service, city)
                    if key not in seen:
                        seen.add(key)
                        result.append((service, city))
                continue
            # Invalid geo (e.g. "in" parsed as Indiana) — fall through to known_cities match

        # 2. Try matching known cities (handles "service city", "city service", "service in city")
        if not known_set:
            continue
        match = _find_city_in_phrase(phrase, known_set)
        if match:
            service, city = match
            if _looks_like_place(city):
                service = _normalize_phrase(service)
                city = _canonicalize_city(city)
                if service and city:
                    key = (service, city)
                    if key not in seen:
                        seen.add(key)
                        result.append((service, city))

    return result


def extract_geo_phrases_from_profile(
    extracted_profile: dict,
    known_cities: Iterable[str],
) -> List[Tuple[str, str]]:
    """
    Convenience: extract from extracted_profile (Ollama extraction result).

    Args:
        extracted_profile: Dict with seo_keywords, service_city_phrases
        known_cities: Cities from research location

    Returns:
        List of (service, city) pairs
    """
    if not extracted_profile or not isinstance(extracted_profile, dict):
        return []
    seo = extracted_profile.get("seo_keywords") or []
    scp = extracted_profile.get("service_city_phrases") or []
    return extract_geo_phrases(seo, scp, known_cities)


# ─── City-level clusters ─────────────────────────────────────────────────────


@dataclass
class CityCluster:
    """City-level cluster: grouped phrases, service counts, missing combinations."""

    city: str
    phrases: List[Tuple[str, str]] = field(default_factory=list)
    service_counts: Dict[str, int] = field(default_factory=dict)
    missing_services: List[str] = field(default_factory=list)


# Generic terms that appear in many services — exclude from overlap matching
_SERVICE_GENERIC = frozenset({"removal", "haul", "hauling", "cleaning", "cleanout", "clean", "pickup", "disposal", "dispose", "repair", "rental"})

# Service synonym groups — phrases in same group cluster together (e.g. junk removal = waste removal)
_SERVICE_SYNONYM_GROUPS = [
    frozenset({"junk", "waste", "trash", "debris", "garbage"}),  # junk/waste/trash removal
    frozenset({"cleanout", "clean out", "clean-up", "cleanup"}),
    frozenset({"haul", "hauling"}),
]


def _canonical_service_key(service: str) -> str:
    """Return stable key for clustering. 'junk removal' and 'waste removal' → same key."""
    if not service:
        return ""
    s = service.lower().strip()
    if not s:
        return ""
    words = set(re.sub(r"\s+", " ", s).split())
    for group in _SERVICE_SYNONYM_GROUPS:
        overlap = words & group
        if overlap:
            return " ".join(sorted(group))  # stable key per group
    return s


def _services_in_same_cluster(a: str, b: str) -> bool:
    """True if services refer to same cluster (junk removal ≈ waste removal)."""
    if _service_matches(a, b):
        return True
    key_a = _canonical_service_key(a)
    key_b = _canonical_service_key(b)
    return bool(key_a and key_a == key_b)


def _service_matches(extracted: str, reference: str) -> bool:
    """True if extracted service matches reference (substring or distinctive word overlap)."""
    if not extracted or not reference:
        return False
    ex = extracted.lower().strip()
    ref = reference.lower().strip()
    if ex == ref:
        return True
    if ref in ex or ex in ref:
        return True
    # Distinctive word match: "estate cleanout" matches "estate clean out", but
    # "appliance removal" does not match "junk removal" (generic "removal" excluded)
    ref_words = [w for w in ref.split() if w not in _SERVICE_GENERIC]
    ex_words = set(ex.split())
    if not ref_words:
        return False
    return any(rw in ex or rw in ex_words for rw in ref_words)


def cluster_geo_phrases_by_city(
    phrases: Iterable[Tuple[str, str]],
    reference_services: Optional[Iterable[str]] = None,
    vertical: Optional[str] = None,
) -> Dict[str, CityCluster]:
    """
    Create city-level clusters from geo phrases.

    - Groups phrases by city
    - Counts services per city (distinct service + frequency)
    - Identifies missing service combinations (services in reference set or
      seen in other cities but not in this city)

    Args:
        phrases: List of (service, city) pairs from extract_geo_phrases
        reference_services: Optional canonical services (e.g. from get_opportunity_services).
            If None, uses vertical's opportunity_services when vertical is set.
        vertical: Optional vertical (junk_removal, plumbing, etc.) to load reference services

    Returns:
        Dict[city, CityCluster] with phrases, service_counts, missing_services
    """
    # Build reference set
    ref_set: Set[str] = set()
    if reference_services is not None:
        ref_set = {s.lower().strip() for s in reference_services if s}
    elif vertical:
        try:
            from verticals import get_opportunity_services
            ref_set = {s.lower().strip() for s in get_opportunity_services(vertical) if s}
        except Exception:
            pass

    # Group by city
    by_city: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for service, city in phrases or []:
        if service and city:
            by_city[city].append((service, city))

    # All services seen across cities (for missing-detection when no reference)
    all_services: Set[str] = set()
    for pairs in by_city.values():
        for svc, _ in pairs:
            all_services.add(svc)

    if not ref_set:
        ref_set = all_services

    clusters: Dict[str, CityCluster] = {}

    for city, pairs in by_city.items():
        service_counts: Dict[str, int] = defaultdict(int)
        for svc, _ in pairs:
            service_counts[svc] += 1

        # Find which reference services are missing (no match in this city's services)
        city_services = set(service_counts.keys())
        missing: List[str] = []
        for ref in ref_set:
            if not ref:
                continue
            matched = any(_service_matches(extracted, ref) for extracted in city_services)
            if not matched:
                missing.append(ref)

        clusters[city] = CityCluster(
            city=city,
            phrases=pairs,
            service_counts=dict(service_counts),
            missing_services=sorted(missing),
        )

    return dict(clusters)


def cluster_geo_phrases_from_profile(
    extracted_profile: dict,
    known_cities: Iterable[str],
    vertical: Optional[str] = None,
) -> Dict[str, CityCluster]:
    """
    Extract geo phrases and cluster by city in one call.

    Args:
        extracted_profile: Dict with seo_keywords, service_city_phrases
        known_cities: Cities from research location
        vertical: Optional vertical for reference services (missing detection)

    Returns:
        Dict[city, CityCluster]
    """
    phrases = extract_geo_phrases_from_profile(extracted_profile, known_cities)
    return cluster_geo_phrases_by_city(phrases, vertical=vertical)


# ─── Similar phrase clustering (junk removal madison ≈ waste removal madison) ──


def cluster_similar_geo_phrases(
    phrases: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Cluster similar geo phrases (e.g. 'junk removal madison wi' + 'waste removal madison' → same).
    Returns strongest version per cluster by confidence_score.

    Input: list of {city, state, service, geo_phrase, confidence_score, source_url?}
    Output: list of same shape, one per cluster, with highest confidence; source_urls merged.
    """
    result: List[Dict[str, Any]] = []
    seen: List[Dict[str, Any]] = []
    for p in phrases or []:
        if not isinstance(p, dict) or not p.get("geo_phrase"):
            continue
        city = _normalize_city_for_match(str(p.get("city") or ""))
        state = (str(p.get("state") or "").strip().upper() or None)
        service = (str(p.get("service") or "").strip().lower() or "")
        geo = (str(p.get("geo_phrase") or "").strip().lower() or "")
        conf = float(p.get("confidence_score", 0.5) or 0.5)
        conf = max(0.0, min(1.0, conf))
        url = p.get("source_url")
        url = str(url).strip() if url else None

        city_canon = _canonicalize_city(city) or city
        svc_key = _canonical_service_key(service) or service

        merged: Optional[Dict[str, Any]] = None
        for existing in seen:
            ecity = _normalize_city_for_match(str(existing.get("city") or ""))
            ecity_canon = _canonicalize_city(ecity) or ecity
            estate = (str(existing.get("state") or "").strip().upper() or "")
            esvc = str(existing.get("service") or "").strip().lower()
            if (ecity_canon == city_canon and
                (estate == (state or "") or not estate or not state) and
                _services_in_same_cluster(service, esvc)):
                merged = existing
                break

        if merged is not None:
            if conf > float(merged.get("confidence_score", 0) or 0):
                merged["geo_phrase"] = geo
                merged["service"] = service or merged.get("service")
                merged["city"] = p.get("city") or merged.get("city")
                merged["state"] = p.get("state") or merged.get("state")
                merged["confidence_score"] = conf
            urls = merged.setdefault("source_urls", [])
            if url and url not in urls:
                urls.append(url)
        else:
            new: Dict[str, Any] = {
                "city": city or p.get("city"),
                "state": state or p.get("state"),
                "service": service or p.get("service"),
                "geo_phrase": geo,
                "confidence_score": conf,
                "source_urls": [url] if url else [],
            }
            seen.append(new)

    return seen


def upsert_geo_phrase_clusters(
    phrases: Iterable[Dict[str, Any]],
    db=None,
) -> int:
    """
    Cluster similar phrases and upsert strongest per cluster into geo_phrases table.
    Matches existing rows by (city, state, same cluster service). Returns count upserted.
    """
    clustered = cluster_similar_geo_phrases(phrases)
    if not clustered:
        return 0

    try:
        from database import GeoPhrase, SessionLocal
    except ImportError:
        return 0

    sess = db or SessionLocal()
    count = 0
    try:
        for p in clustered:
            city = (str(p.get("city") or "").strip() or None)
            state = (str(p.get("state") or "").strip().upper() or None)
            service = (str(p.get("service") or "").strip() or None)
            geo = (str(p.get("geo_phrase") or "").strip() or None)
            conf = float(p.get("confidence_score", 0.5) or 0.5)
            urls = list(p.get("source_urls") or [])
            if not geo:
                continue

            city_canon = _canonicalize_city(_normalize_city_for_match(city or "")) or (city or "").lower().strip()

            q = sess.query(GeoPhrase)
            if city_canon:
                q = q.filter(GeoPhrase.city.ilike(f"%{city_canon}%"))
            candidates = q.all()

            def state_matches(row) -> bool:
                rs = str(row.state or "").strip().upper()
                s = (state or "").strip().upper()
                return rs == s or (not rs and not s)

            existing = None
            for row in candidates:
                if _services_in_same_cluster(service or "", str(row.service or "")) and state_matches(row):
                    existing = row
                    break

            if existing:
                if conf > float(existing.confidence_score or 0):
                    existing.geo_phrase = geo
                    existing.service = service or existing.service
                    existing.confidence_score = conf
                all_urls = list(existing.source_urls or [])
                for u in urls:
                    if u and str(u) not in all_urls:
                        all_urls.append(str(u))
                existing.source_urls = all_urls
                count += 1
            else:
                sess.add(GeoPhrase(
                    city=city,
                    state=state,
                    service=service,
                    geo_phrase=geo,
                    confidence_score=conf,
                    source_urls=urls,
                ))
                count += 1
        if db is None:
            sess.commit()
    except Exception:
        if db is None:
            sess.rollback()
        raise
    finally:
        if db is None:
            sess.close()

    return count


# ─── Service-level clusters ──────────────────────────────────────────────────


@dataclass
class ServiceCluster:
    """Service-level cluster: grouped phrases, city counts, underserved cities."""

    service: str
    phrases: List[Tuple[str, str]] = field(default_factory=list)
    city_counts: Dict[str, int] = field(default_factory=dict)
    underserved_cities: List[str] = field(default_factory=list)


def cluster_geo_phrases_by_service(
    phrases: Iterable[Tuple[str, str]],
    known_cities: Optional[Iterable[str]] = None,
) -> Dict[str, ServiceCluster]:
    """
    Create service-level clusters from geo phrases.

    - Groups phrases by service
    - Counts cities per service (distinct city + frequency)
    - Identifies underserved cities (cities in known_cities that have no coverage
      for this service)

    Args:
        phrases: List of (service, city) pairs from extract_geo_phrases
        known_cities: Cities from research location. Underserved = cities in this
            set that have no phrases for the service.

    Returns:
        Dict[service, ServiceCluster] with phrases, city_counts, underserved_cities
    """
    # Normalize known cities for comparison
    known_set: Set[str] = set()
    if known_cities:
        for c in known_cities:
            if c and isinstance(c, str):
                known_set.add(c.lower().strip())

    # Group by service
    by_service: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for service, city in phrases or []:
        if service and city:
            city_norm = city.lower().strip()
            by_service[service].append((service, city_norm))

    clusters: Dict[str, ServiceCluster] = {}

    for service, pairs in by_service.items():
        city_counts: Dict[str, int] = defaultdict(int)
        for _, city in pairs:
            city_counts[city] += 1

        cities_with_service = set(city_counts.keys())
        # Underserved: known cities without this service
        underserved = sorted(known_set - cities_with_service) if known_set else []

        clusters[service] = ServiceCluster(
            service=service,
            phrases=pairs,
            city_counts=dict(city_counts),
            underserved_cities=underserved,
        )

    return dict(clusters)


def cluster_geo_phrases_by_service_from_profile(
    extracted_profile: dict,
    known_cities: Iterable[str],
) -> Dict[str, ServiceCluster]:
    """
    Extract geo phrases and cluster by service in one call.

    Args:
        extracted_profile: Dict with seo_keywords, service_city_phrases
        known_cities: Cities from research location (for underserved detection)

    Returns:
        Dict[service, ServiceCluster]
    """
    phrases = extract_geo_phrases_from_profile(extracted_profile, known_cities)
    return cluster_geo_phrases_by_service(phrases, known_cities=known_cities)
