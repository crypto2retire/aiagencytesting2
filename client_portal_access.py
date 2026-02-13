"""
Client Portal Access — read-only, client-scoped data layer.
- Restricts all data to a single client_id
- No write permissions (add, delete, update, commit blocked)
- Optional shareable magic-link access

Usage:
    # Direct client_id access
    portal = create("client-slug")
    client = portal.get_client()
    opps = portal.get_opportunities(status="OPEN")
    portal.close()

    # Magic-link access (e.g. from ?token=xxx in URL)
    portal = create_from_token(request_token)
    if portal:
        data = portal.to_dict_summary()
        portal.close()

    # Generate shareable link (admin only)
    url = generate_magic_link("client-slug", "https://app.example.com/portal", expires_days=30)
"""

import secrets
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session


class ClientPortalAccessError(Exception):
    """Raised on invalid access."""

    pass


class ClientPortalAccess:
    """
    Read-only access layer scoped to a single client_id.
    Use create(client_id) or create_from_token(token).
    All queries are restricted to this client. No add/delete/update/commit.
    Call close() when done if portal was created without an external db session.
    """

    def __init__(self, client_id: str, db: Session, _owns_session: bool = False):
        self._client_id = client_id
        self._db = db
        self._owns_session = _owns_session

    @property
    def client_id(self) -> str:
        return self._client_id

    def close(self) -> None:
        """Close the underlying session if this portal owns it."""
        if self._owns_session:
            self._db.close()

    def __enter__(self) -> "ClientPortalAccess":
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def get_client(self):
        """Get the client record (read-only)."""
        from database import Client

        return (
            self._db.query(Client)
            .filter(func.lower(Client.client_id) == self._client_id.lower())
            .first()
        )

    def get_branding(self) -> Dict[str, Any]:
        """
        Client branding for portal/PDF: logo, primary_color, portal_title, anonymize_competitors.
        From client.asset_links. Defaults when missing.
        """
        client = self.get_client()
        if not client:
            return {}
        assets = client.asset_links or {}
        return {
            "logo": assets.get("logo") or assets.get("logo_url") or "",
            "primary_color": (assets.get("primary_color") or "#1e40af").strip(),
            "portal_title": assets.get("portal_title") or "Client Portal",
            "anonymize_competitors": assets.get("anonymize_competitors", True),
        }

    def get_research_logs(self, limit: int = 100) -> List:
        from database import ResearchLog

        return (
            self._db.query(ResearchLog)
            .filter(func.lower(ResearchLog.client_id) == self._client_id.lower())
            .order_by(ResearchLog.created_at.desc())
            .limit(limit)
            .all()
        )

    def get_opportunities(self, status: Optional[str] = None, limit: int = 100) -> List:
        from database import Opportunity

        q = (
            self._db.query(Opportunity)
            .filter(Opportunity.client_id == self._client_id)
            .order_by(Opportunity.opportunity_score.desc())
        )
        if status:
            q = q.filter(Opportunity.status == status)
        return q.limit(limit).all()

    def get_content_drafts(self, status: Optional[str] = None, limit: int = 100) -> List:
        from database import ContentDraft

        q = (
            self._db.query(ContentDraft)
            .filter(func.lower(ContentDraft.client_id) == self._client_id.lower())
            .order_by(ContentDraft.created_at.desc())
        )
        if status:
            q = q.filter(ContentDraft.status == status)
        return q.limit(limit).all()

    def get_client_roadmap(self, limit: int = 50) -> List:
        from database import ClientRoadmap

        return (
            self._db.query(ClientRoadmap)
            .filter(ClientRoadmap.client_id == self._client_id)
            .order_by(ClientRoadmap.confidence_score.desc().nullslast())
            .limit(limit)
            .all()
        )

    def get_roadmap_grouped(self) -> Dict[str, Any]:
        """
        Roadmap items grouped for 30/60/90 day plan, completed vs upcoming, locked vs optional.
        Returns: {
            by_period: { 30: [...], 60: [...], 90: [...] },
            completed: [...],
            upcoming: [...],
            locked: [...],
            optional: [...],
        }
        Derives plan_period from priority (1-10→30, 11-20→60, 21+→90) when null.
        """
        items = self.get_client_roadmap(limit=50)
        by_period: Dict[int, List] = {30: [], 60: [], 90: []}
        completed = []
        upcoming = []
        locked = []
        optional = []

        for r in items:
            plan = getattr(r, "plan_period", None)
            if plan is None:
                p = r.priority or 99
                plan = 30 if p <= 10 else (60 if p <= 20 else 90)
            plan = 30 if plan == 30 else (60 if plan == 60 else 90)
            status = (getattr(r, "status", None) or "PENDING").upper()
            is_locked = getattr(r, "is_locked", None)
            if is_locked is None:
                is_locked = (r.confidence_score or 0) >= 0.6

            rec = {
                "id": r.id,
                "priority": r.priority,
                "task_type": r.task_type,
                "title": r.title,
                "description": r.description,
                "expected_impact": r.expected_impact,
                "confidence_score": r.confidence_score,
                "plan_period": plan,
                "status": status,
                "is_locked": bool(is_locked),
            }

            if plan in by_period:
                by_period[plan].append(rec)

            if status == "COMPLETED":
                completed.append(rec)
            else:
                upcoming.append(rec)

            if is_locked:
                locked.append(rec)
            else:
                optional.append(rec)

        for k in by_period:
            by_period[k].sort(key=lambda x: (-(x.get("confidence_score") or 0), x.get("priority") or 99))

        return {
            "by_period": by_period,
            "completed": completed,
            "upcoming": upcoming,
            "locked": locked,
            "optional": optional,
        }

    def get_latest_website_gap_proposal(self, anonymize_competitors: Optional[bool] = None) -> Dict[str, Any]:
        """
        Build latest Website Gap Proposal from gap analysis + proposals.
        Populates websites if needed, runs analyzer + mapper + generator.
        anonymize_competitors: when True, use "Top Local Competitors". Default from client.asset_links.
        Returns: { gap, proposals, markdown } or {} if no client site.
        """
        from website_gap_analyzer import WebsiteGapAnalyzer, populate_websites_from_research
        from proposal_mapper import ProposalMapper
        from auto_proposal_generator import generate_proposal

        client = self.get_client()
        if not client:
            return {}

        from database import Website

        assets = client.asset_links or {}
        if anonymize_competitors is None:
            anonymize_competitors = assets.get("anonymize_competitors", True)

        client_id = self._client_id
        populate_websites_from_research(self._db, client_id)
        client_site = self._db.query(Website).filter(Website.client_id == client_id).first()
        if not client_site:
            return {}

        comp_sites = (
            self._db.query(Website)
            .filter(Website.research_log_id.isnot(None))
            .limit(20)
            .all()
        )
        comp_ids = [w.id for w in comp_sites]
        if not comp_ids:
            return {}

        analyzer = WebsiteGapAnalyzer(self._db)
        gap = analyzer.analyze(client_site.id, comp_ids)
        if gap.get("error"):
            return {}

        mapper = ProposalMapper()
        proposals = mapper.map_gaps_to_proposals(gap, self._db)
        markdown = generate_proposal(
            gap,
            proposals,
            client_name=client.business_name or client_id,
            client_domain=gap.get("client_domain"),
            format="markdown",
            anonymize_competitors=anonymize_competitors,
        )
        return {"gap": gap, "proposals": proposals, "markdown": markdown}

    def get_latest_website_gap_pdf(self) -> Optional[Dict[str, Any]]:
        """Get latest WEBSITE_GAP PDF export for this client. Returns dict with pdf_file_path or None."""
        from pdf_utils import get_exports

        exports = get_exports(self._client_id)
        for ex in exports:
            if (ex.get("export_type") or "").upper() == "WEBSITE_GAP" and ex.get("pdf_file_path"):
                return ex
        return None

    def submit_proposal_response(self, action: str) -> tuple:
        """
        Submit client response to Website Gap Proposal: 'accept' | 'request_changes'.
        Requires proposals from get_latest_website_gap_proposal. Returns (success, message).
        """
        from proposal_outcomes import log_proposal_outcome

        if action not in ("accept", "request_changes"):
            return False, "Invalid action."
        proposal_data = self.get_latest_website_gap_proposal()
        proposals = proposal_data.get("proposals", [])
        if not proposals:
            return False, "No proposal data. Your agency must generate a Website Gap Proposal first."
        out = "accepted" if action == "accept" else "requested_changes"
        try:
            log_proposal_outcome(self._db, self._client_id, proposals, outcome=out)
            return True, "Response recorded. Thank you!"
        except Exception as e:
            return False, str(e)

    def get_sales_proposals(self, limit: int = 10) -> List:
        from database import SalesProposal

        return (
            self._db.query(SalesProposal)
            .filter(SalesProposal.client_id == self._client_id)
            .order_by(SalesProposal.proposal_date.desc().nullslast())
            .limit(limit)
            .all()
        )

    def get_geo_page_outlines(self, limit: int = 50) -> List:
        from database import GeoPageOutline
        from sqlalchemy import or_

        return (
            self._db.query(GeoPageOutline)
            .filter(
                or_(
                    GeoPageOutline.client_id.is_(None),
                    func.lower(GeoPageOutline.client_id) == self._client_id.lower(),
                )
            )
            .order_by(GeoPageOutline.confidence_score.desc().nullslast())
            .limit(limit)
            .all()
        )

    def get_website_gap_proposal_outcomes(self, limit: int = 20) -> List:
        from database import WebsiteGapProposalOutcome

        return (
            self._db.query(WebsiteGapProposalOutcome)
            .filter(WebsiteGapProposalOutcome.client_id == self._client_id)
            .order_by(WebsiteGapProposalOutcome.created_at.desc())
            .limit(limit)
            .all()
        )

    def get_market_snapshots(self, limit: int = 20) -> List:
        from database import MarketSnapshot

        return (
            self._db.query(MarketSnapshot)
            .filter(MarketSnapshot.client_id == self._client_id)
            .order_by(MarketSnapshot.created_at.desc().nullslast())
            .limit(limit)
            .all()
        )

    def get_keywords(self, regions: Optional[List[str]] = None, limit: int = 200) -> List:
        from database import KeywordIntelligence
        from sqlalchemy import or_

        q = self._db.query(KeywordIntelligence)
        if regions:
            q = q.filter(
                or_(
                    KeywordIntelligence.client_id == self._client_id,
                    KeywordIntelligence.region.in_(regions),
                )
            )
        else:
            q = q.filter(KeywordIntelligence.client_id == self._client_id)
        return (
            q.order_by(
                func.coalesce(KeywordIntelligence.keyword_confidence_score, 0).desc(),
                KeywordIntelligence.frequency.desc(),
            )
            .limit(limit)
            .all()
        )

    def to_dict_summary(self) -> Dict[str, Any]:
        """Summary of portal data counts for this client."""
        client = self.get_client()
        if not client:
            return {"client_id": self._client_id, "exists": False}

        return {
            "client_id": self._client_id,
            "business_name": client.business_name,
            "exists": True,
            "research_logs": len(self.get_research_logs(limit=1000)),
            "opportunities": len(self.get_opportunities(limit=1000)),
            "content_drafts": len(self.get_content_drafts(limit=1000)),
            "roadmap_items": len(self.get_client_roadmap(limit=1000)),
            "sales_proposals": len(self.get_sales_proposals(limit=100)),
        }

    def get_overview_metrics(self, days: int = 30) -> Dict[str, Any]:
        """
        Compute Overview scores (0-100) and 30-day trend deltas.
        Returns: overall_website_score, local_seo_score, content_coverage_score, competitor_percentile
        Each with optional _delta for 30-day change (None if no historical data).
        """
        from database import (
            ContentDraft,
            GeoPageOutline,
            KeywordIntelligence,
            Opportunity,
            ResearchLog,
            Website,
        )

        client = self.get_client()
        if not client:
            return {}

        cutoff = datetime.utcnow() - timedelta(days=days)
        client_avg = float(client.avg_page_quality_score or 0)

        # ─── Overall Website Score (0-100) ─────────────────────────────────
        # Prefer Website.quality_score for client site, else client.avg_page_quality_score
        website_row = (
            self._db.query(Website)
            .filter(Website.client_id == self._client_id)
            .order_by(Website.created_at.desc())
            .first()
        )
        overall_score = round(max(0, min(100, float(website_row.quality_score or client_avg) if website_row else client_avg)))
        # No stored history for website score — delta from Website.created_at not meaningful for score itself
        overall_delta = None

        # ─── Local SEO Visibility Score (0-100) ─────────────────────────────
        # Geo keywords (have city, region, or geo-type), geo outlines, geo opportunities
        kws = self.get_keywords(limit=500)
        geo_kws = [k for k in kws if (k.city or k.region or (k.keyword_type or "").lower() in ("geo", "service_city", "service_geo"))]
        outlines = self.get_geo_page_outlines(limit=100)
        opps = self.get_opportunities(limit=200)
        geo_opps = [o for o in opps if (o.geo or "").strip()]

        geo_kw_count = len(geo_kws)
        outline_count = len(outlines)
        geo_opp_count = len(geo_opps)
        local_seo_raw = min(100.0, geo_kw_count * 2 + outline_count * 4 + geo_opp_count * 3)
        local_seo_score = round(local_seo_raw)

        # 30d ago counts (keywords with first_seen before cutoff)
        kw_30d = self._db.query(KeywordIntelligence).filter(
            KeywordIntelligence.client_id == self._client_id,
            KeywordIntelligence.first_seen <= cutoff,
        ).count()
        draft_count_30d = self._db.query(ContentDraft).filter(
            func.lower(ContentDraft.client_id) == self._client_id.lower(),
            ContentDraft.created_at <= cutoff,
        ).count()
        outline_count_30d = self._db.query(GeoPageOutline).filter(
            (GeoPageOutline.client_id.is_(None)) | (func.lower(GeoPageOutline.client_id) == self._client_id.lower()),
            GeoPageOutline.created_at <= cutoff,
        ).count()
        geo_opp_30d = self._db.query(Opportunity).filter(
            Opportunity.client_id == self._client_id,
            Opportunity.geo.isnot(None),
            Opportunity.geo != "",
            Opportunity.created_at <= cutoff,
        ).count()
        local_seo_raw_30d = min(100.0, kw_30d * 2 + outline_count_30d * 4 + geo_opp_30d * 3)
        local_seo_score_30d = round(local_seo_raw_30d)
        local_seo_delta = local_seo_score - local_seo_score_30d if local_seo_score_30d > 0 or local_seo_score > 0 else None

        # ─── Content Coverage Score (0-100) ─────────────────────────────────
        drafts = self.get_content_drafts(limit=200)
        draft_count = len(drafts)
        content_raw = min(100.0, draft_count * 3 + outline_count * 2)
        content_score = round(content_raw)

        draft_count_30d_val = self._db.query(ContentDraft).filter(
            func.lower(ContentDraft.client_id) == self._client_id.lower(),
            ContentDraft.created_at <= cutoff,
        ).count()
        content_raw_30d = min(100.0, draft_count_30d_val * 3 + outline_count_30d * 2)
        content_score_30d = round(content_raw_30d)
        content_delta = content_score - content_score_30d

        # ─── Competitor Percentile Rank (0-100) ─────────────────────────────
        # % of competitors you outrank (higher = better than more competitors)
        logs = self.get_research_logs(limit=500)
        comp_scores = []
        for rl in logs:
            q = getattr(rl, "competitor_comparison_score", None) or rl.website_quality_score
            if q is not None:
                comp_scores.append(float(q))
        num_below = sum(1 for s in comp_scores if s < client_avg)
        competitor_percentile = round((num_below / len(comp_scores)) * 100) if comp_scores else 50
        competitor_percentile = max(0, min(100, competitor_percentile))

        logs_30d = (
            self._db.query(ResearchLog)
            .filter(
                func.lower(ResearchLog.client_id) == self._client_id.lower(),
                ResearchLog.created_at <= cutoff,
            )
            .all()
        )
        comp_scores_30d = []
        for rl in logs_30d:
            q = getattr(rl, "competitor_comparison_score", None) or rl.website_quality_score
            if q is not None:
                comp_scores_30d.append(float(q))
        num_below_30d = sum(1 for s in comp_scores_30d if s < client_avg)
        percentile_30d = round((num_below_30d / len(comp_scores_30d)) * 100) if comp_scores_30d else 50
        percentile_30d = max(0, min(100, percentile_30d))
        competitor_delta = competitor_percentile - percentile_30d if (comp_scores_30d or comp_scores) else None

        return {
            "overall_website_score": overall_score,
            "overall_website_delta": overall_delta,
            "local_seo_score": local_seo_score,
            "local_seo_delta": local_seo_delta,
            "content_coverage_score": content_score,
            "content_coverage_delta": content_delta,
            "competitor_percentile": competitor_percentile,
            "competitor_percentile_delta": competitor_delta,
        }

    def get_competitive_position_metrics(self) -> Dict[str, Any]:
        """
        Client vs competitor average by category (0-100 scale).
        Categories: SEO, Geo Coverage, Content Depth, Trust Signals, Conversion Elements.
        Returns: { overall: { client, competitor_avg }, categories: { cat: { client_score, competitor_avg } } }
        """
        from database import ResearchLog, Website

        client = self.get_client()
        if not client:
            return {}

        def _safe_list(obj):
            if obj is None:
                return []
            return list(obj) if isinstance(obj, (list, tuple)) else []

        def _build_client_profile():
            """Client profile from Website or synthetic from Client."""
            w = (
                self._db.query(Website)
                .filter(Website.client_id == self._client_id)
                .order_by(Website.created_at.desc())
                .first()
            )
            if w and w.extracted_profile and isinstance(w.extracted_profile, dict):
                return w.extracted_profile
            # Synthetic from Client
            svc = _safe_list(client.services_offered)
            cities = _safe_list(client.cities_served)
            return {
                "primary_services": svc,
                "secondary_services": [],
                "content_signals": {
                    "service_pages_count_estimate": max(1, len(svc)),
                    "blog_present": False,
                    "location_pages_present": len(cities) > 0,
                },
                "local_signals": {
                    "address_mentioned": True,
                    "phone_mentioned": bool((client.phone_number or "").strip()),
                    "city_mentions_count": len(cities),
                },
                "seo_keywords": svc,
                "service_city_phrases": [],
                "geo_keywords": [],
                "trust_signals": {},
                "calls_to_action": [],
                "conversion_signals": {},
                "technical_signals": {},
            }

        from website_gap_analyzer import _extract_category_scores

        client_profile = _build_client_profile()
        client_scores = _extract_category_scores(client_profile)

        logs = self.get_research_logs(limit=100)
        comp_scores_list = []
        for rl in logs:
            profile = rl.extracted_profile or {}
            if profile:
                comp_scores_list.append(_extract_category_scores(profile))

        comp_avgs = {}
        cats = ["technical_seo", "content_depth", "geo_coverage", "trust_signals", "conversion_elements"]
        for cat in cats:
            vals = [s.get(cat, 0) for s in comp_scores_list if cat in s]
            comp_avgs[cat] = sum(vals) / len(vals) if vals else 0.0

        # Scale 0-20 -> 0-100 for display
        def scale(v):
            return round((v or 0) * 5)

        # Map internal keys to display names
        display_cats = [
            ("technical_seo", "SEO"),
            ("geo_coverage", "Geo Coverage"),
            ("content_depth", "Content Depth"),
            ("trust_signals", "Trust Signals"),
            ("conversion_elements", "Conversion Elements"),
        ]

        categories = {}
        client_total = 0.0
        comp_total = 0.0
        for key, label in display_cats:
            cv = client_scores.get(key, 0) or 0
            cc = comp_avgs.get(key, 0) or 0
            categories[label] = {"client_score": scale(cv), "competitor_avg": scale(cc)}
            client_total += cv
            comp_total += cc

        # Overall: sum of 5 categories (each 0-20) = 0-100
        client_overall = round(client_total) if client_total else int(client.avg_page_quality_score or 0)
        competitor_overall = round(comp_total) if comp_scores_list else 0

        return {
            "overall": {"client_score": client_overall, "competitor_avg": competitor_overall},
            "categories": categories,
            "competitor_count": len(comp_scores_list),
        }

    def get_website_health_metrics(self) -> Dict[str, Any]:
        """
        Website Health category scores: client vs market with gap severity.
        Categories: Technical SEO, Content Quality, UX/Conversion, Trust Signals.
        Returns: { categories: [ { label, client_score, market_avg, severity } ] }
        Severity: critical | major | minor | none (when client behind).
        """
        from database import ResearchLog, Website
        from website_gap_analyzer import _extract_category_scores, _build_gap_category

        client = self.get_client()
        if not client:
            return {}

        def _safe_list(obj):
            if obj is None:
                return []
            return list(obj) if isinstance(obj, (list, tuple)) else []

        def _build_client_profile():
            w = (
                self._db.query(Website)
                .filter(Website.client_id == self._client_id)
                .order_by(Website.created_at.desc())
                .first()
            )
            if w and w.extracted_profile and isinstance(w.extracted_profile, dict):
                return w.extracted_profile
            svc = _safe_list(client.services_offered)
            cities = _safe_list(client.cities_served)
            return {
                "primary_services": svc,
                "secondary_services": [],
                "content_signals": {
                    "service_pages_count_estimate": max(1, len(svc)),
                    "blog_present": False,
                    "location_pages_present": len(cities) > 0,
                },
                "local_signals": {
                    "address_mentioned": True,
                    "phone_mentioned": bool((client.phone_number or "").strip()),
                    "city_mentions_count": len(cities),
                },
                "seo_keywords": svc,
                "service_city_phrases": [],
                "geo_keywords": [],
                "trust_signals": {},
                "calls_to_action": [],
                "conversion_signals": {},
                "technical_signals": {},
            }

        client_profile = _build_client_profile()
        client_scores = _extract_category_scores(client_profile)

        logs = self.get_research_logs(limit=100)
        comp_scores_list = []
        for rl in logs:
            profile = rl.extracted_profile or {}
            if profile:
                comp_scores_list.append(_extract_category_scores(profile))

        comp_avgs = {}
        health_cats = [
            ("technical_seo", "Technical SEO"),
            ("content_depth", "Content Quality"),
            ("conversion_elements", "UX / Conversion"),
            ("trust_signals", "Trust Signals"),
        ]
        for key, _ in health_cats:
            vals = [s.get(key, 0) for s in comp_scores_list if key in s]
            comp_avgs[key] = sum(vals) / len(vals) if vals else 0.0

        categories = []
        for key, label in health_cats:
            cv = client_scores.get(key, 0) or 0
            cc = comp_avgs.get(key, 0) or 0
            built = _build_gap_category(key, cv, cc, scale_100=True)
            # Severity only when client is behind (gap < 0)
            sev = built["severity"] if built["gap"] < 0 else "none"
            categories.append({
                "label": label,
                "client_score": built["client_score"],
                "market_avg": built["competitor_avg"],
                "gap": built["gap"],
                "severity": sev,
            })

        return {"categories": categories, "competitor_count": len(comp_scores_list)}

    def get_content_seo_time_series(self, days: int = 90) -> Dict[str, Any]:
        """
        Time-series data for Content & SEO charts (bucketed by week).
        Returns: pages_created, keywords_targeted, geo_phrases_covered, rankings (impressions if available).
        Each is a list of { date: "YYYY-MM-DD", value: N } sorted by date.
        """
        from collections import defaultdict
        from database import (
            ContentDraft,
            ContentPerformance,
            GeoPageOutline,
            KeywordIntelligence,
        )
        from sqlalchemy import or_

        cutoff = datetime.utcnow() - timedelta(days=days)

        def _week_key(dt) -> str:
            if dt is None:
                return ""
            d = dt if hasattr(dt, "date") else dt
            # Monday as week start
            offset = (d.weekday() if hasattr(d, "weekday") else 0)
            start = d - timedelta(days=offset)
            return start.strftime("%Y-%m-%d")

        pages_by_week: Dict[str, int] = defaultdict(int)
        keywords_by_week: Dict[str, int] = defaultdict(int)
        geo_by_week: Dict[str, int] = defaultdict(int)
        impressions_by_week: Dict[str, int] = defaultdict(int)

        # Pages: GeoPageOutline + ContentDraft (both count as "content created")
        outlines = (
            self._db.query(GeoPageOutline)
            .filter(
                or_(
                    GeoPageOutline.client_id.is_(None),
                    func.lower(GeoPageOutline.client_id) == self._client_id.lower(),
                ),
                GeoPageOutline.created_at >= cutoff,
            )
            .all()
        )
        for o in outlines:
            k = _week_key(o.created_at)
            if k:
                pages_by_week[k] += 1

        drafts = (
            self._db.query(ContentDraft)
            .filter(
                func.lower(ContentDraft.client_id) == self._client_id.lower(),
                ContentDraft.created_at >= cutoff,
            )
            .all()
        )
        for d in drafts:
            k = _week_key(d.created_at)
            if k:
                pages_by_week[k] += 1

        # Keywords: KeywordIntelligence first_seen
        kws = (
            self._db.query(KeywordIntelligence)
            .filter(
                KeywordIntelligence.client_id == self._client_id,
                KeywordIntelligence.first_seen >= cutoff,
            )
            .all()
        )
        for k in kws:
            wk = _week_key(k.first_seen)
            if wk:
                keywords_by_week[wk] += 1

        # Geo phrases: GeoPageOutline (each outline = one geo phrase covered)
        for o in outlines:
            wk = _week_key(o.created_at)
            if wk:
                geo_by_week[wk] += 1

        # Rankings / Impressions: ContentPerformance for client's drafts
        draft_ids = [d.id for d in self._db.query(ContentDraft).filter(
            func.lower(ContentDraft.client_id) == self._client_id.lower(),
        ).all()]
        if draft_ids:
            perfs = (
                self._db.query(ContentPerformance)
                .filter(
                    ContentPerformance.content_id.in_(draft_ids),
                    ContentPerformance.recorded_at >= cutoff,
                )
                .all()
            )
            for p in perfs:
                wk = _week_key(p.recorded_at)
                if wk:
                    impressions_by_week[wk] += p.impressions or 0

        def _to_sorted_list(d: Dict[str, int], cumulative: bool = False) -> List[Dict[str, Any]]:
            keys = sorted(d.keys())
            cum = 0
            out = []
            for k in keys:
                v = d.get(k, 0)
                cum += v
                out.append({"date": k, "value": cum if cumulative else v})
            return out

        return {
            "pages_created": _to_sorted_list(pages_by_week, cumulative=True),
            "keywords_targeted": _to_sorted_list(keywords_by_week, cumulative=True),
            "geo_phrases_covered": _to_sorted_list(geo_by_week, cumulative=True),
            "impressions": _to_sorted_list(impressions_by_week, cumulative=False),
        }


def create(client_id: str, db: Optional[Session] = None) -> ClientPortalAccess:
    """
    Create read-only portal access for a client.
    db: optional session; if not provided, creates one (call close() when done).
    """
    from database import SessionLocal

    session = db or SessionLocal()
    return ClientPortalAccess(client_id, session, _owns_session=(db is None))


def create_from_token(
    token: str,
    db: Optional[Session] = None,
) -> Optional[ClientPortalAccess]:
    """
    Create portal access from a valid magic-link token.
    Returns None if token invalid or expired.
    Call close() when done if db was not provided.
    """
    from database import ClientPortalToken, SessionLocal

    session = db or SessionLocal()
    row = (
        session.query(ClientPortalToken)
        .filter(ClientPortalToken.token == token)
        .first()
    )
    if not row or (row.expires_at and row.expires_at < datetime.utcnow()):
        if db is None:
            session.close()
        return None
    return ClientPortalAccess(row.client_id, session, _owns_session=(db is None))


def generate_magic_link(
    client_id: str,
    base_url: str,
    db: Optional[Session] = None,
    expires_days: int = 30,
) -> str:
    """
    Generate a shareable magic-link URL for client portal access.
    Creates a token, stores it, returns full URL.
    base_url: e.g. "https://app.example.com/portal" (no trailing slash)
    """
    from database import ClientPortalToken, SessionLocal

    session = db or SessionLocal()
    try:
        token = secrets.token_urlsafe(32)
        row = ClientPortalToken(
            client_id=client_id,
            token=token,
            expires_at=datetime.utcnow() + timedelta(days=expires_days),
        )
        session.add(row)
        session.commit()
        return f"{base_url.rstrip('/')}?token={token}"
    finally:
        if db is None:
            session.close()


def revoke_token(token: str, db: Optional[Session] = None) -> bool:
    """Revoke a magic-link token (delete from DB). Returns True if found and deleted."""
    from database import ClientPortalToken, SessionLocal

    session = db or SessionLocal()
    try:
        row = session.query(ClientPortalToken).filter(ClientPortalToken.token == token).first()
        if not row:
            return False
        session.delete(row)
        session.commit()
        return True
    finally:
        if db is None:
            session.close()


def list_tokens(client_id: str, db: Optional[Session] = None) -> List[Dict[str, Any]]:
    """List magic-link tokens for a client (for admin/revocation)."""
    from database import ClientPortalToken, SessionLocal

    session = db or SessionLocal()
    try:
        rows = (
            session.query(ClientPortalToken)
            .filter(ClientPortalToken.client_id == client_id)
            .order_by(ClientPortalToken.created_at.desc())
            .all()
        )
        return [
            {
                "id": r.id,
                "token_preview": f"{r.token[:8]}..." if r.token else "",
                "expires_at": r.expires_at.isoformat() if r.expires_at else None,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ]
    finally:
        if db is None:
            session.close()


def revoke_token_by_id(token_id: int, db: Optional[Session] = None) -> bool:
    """Revoke a magic-link token by its database id. Returns True if found and deleted."""
    from database import ClientPortalToken, SessionLocal

    session = db or SessionLocal()
    try:
        row = session.query(ClientPortalToken).filter(ClientPortalToken.id == token_id).first()
        if not row:
            return False
        session.delete(row)
        session.commit()
        return True
    finally:
        if db is None:
            session.close()
