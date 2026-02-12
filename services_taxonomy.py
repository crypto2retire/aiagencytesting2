"""
Service-Intent Taxonomy — guardrails for junk removal keywords.
Before any keyword is saved, it must pass the service-intent gate.
Not ML — controlled vocabulary that removes ~70% of garbage.
"""

# ✅ Allowed: keywords containing these pass the gate
SERVICE_NOUNS = frozenset({
    "junk", "trash", "debris", "furniture", "appliance",
    "mattress", "sofa", "couch", "hot tub", "hottub", "tub",
    "shed", "garage", "basement", "attic", "estate",
    "construction", "yard", "waste", "recycling",
    "metal", "scrap", "e waste", "ewaste",
})

SERVICE_VERBS = frozenset({
    "remove", "removal", "haul", "hauling",
    "cleanout", "clean out", "dispose", "disposal",
    "pickup", "pick up", "take away",
})

# ❌ Blocked: automatically reject (single adjectives, business fluff, generic nouns)
EXCLUDED_TERMS = frozenset({
    "professional", "friendly", "great", "best",
    "quality", "local", "team", "service", "work",
    "company", "job", "family", "owned",
    "fast", "reliable", "affordable", "trusted",
    "family-owned", "locally owned",
})


def passes_service_intent_gate(keyword: str) -> bool:
    """
    Service-intent gate: only allow keywords with search intent.
    Passes if keyword contains a service noun OR service verb.
    Fails if keyword is in EXCLUDED_TERMS or has no service content.
    """
    if not keyword or not isinstance(keyword, str):
        return False
    kw = keyword.lower().strip()
    if not kw:
        return False
    if kw in EXCLUDED_TERMS:
        return False
    has_service_noun = any(n in kw for n in SERVICE_NOUNS)
    has_service_verb = any(v in kw for v in SERVICE_VERBS)
    return has_service_noun or has_service_verb
