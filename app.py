"""
Agency AI Dashboard — Outcomes over activity. Simple, calm, trustworthy.
"""

import json
import streamlit as st
import sys
from pathlib import Path
from itertools import groupby

sys.path.insert(0, str(Path(__file__).resolve().parent))

from database import init_db, SessionLocal, Client, ContentStrategy, KeywordIntelligence, MarketSnapshot, Opportunity, OpportunityScore, ContentDraft, ResearchLog
from main import run_keyword_classifier, run_researcher, run_strategist
from agents.keyword_extractor import store_keywords
from verticals import list_verticals
from proposal_generator import generate_proposal, get_proposal_path, save_proposal
from roi_projection import compute_roi_projection
from verticals import get_average_job_value
from sqlalchemy import func, or_

st.set_page_config(page_title="Agency AI", page_icon="📋", layout="wide")
init_db()

# ─── Layout Helpers ─────────────────────────────────────────────────────────

def get_client_context(clients):
    """Client selector. Returns (client_id, client). Ensures selection is valid."""
    if not clients:
        return None, None
    options = [c.client_id for c in clients]
    # If stored selection is invalid (e.g. client removed), reset to first
    stored = st.session_state.get("client_select")
    if stored not in options:
        st.session_state["client_select"] = options[0]
    client_id = st.sidebar.selectbox(
        "Client",
        options,
        key="client_select",
        format_func=lambda x: next((c.business_name or x for c in clients if c.client_id == x), x),
    )
    client = next((c for c in clients if c.client_id == client_id), None)
    return client_id, client


def render_status_ribbon(client, city):
    """Top status ribbon: Client Name | City | Status."""
    status = "🟢 Active" if _has_recent_activity(client) else "🟡 Needs Review"
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        st.markdown(f"### {client.business_name or client.client_id}")
    with col2:
        st.markdown(f"**{city or '—'}**")
    with col3:
        st.success(status)
    st.divider()


def _has_recent_activity(client):
    """True if client has recent research/drafts."""
    db = SessionLocal()
    try:
        has_research = db.query(ResearchLog).filter(ResearchLog.client_id == client.client_id).first()
        has_drafts = db.query(ContentDraft).filter(ContentDraft.client_id == client.client_id).first()
        return bool(has_research or has_drafts)
    finally:
        db.close()


# ─── Overview ──────────────────────────────────────────────────────────────

def page_overview(client_id, client, db):
    """Prove value in under 10 seconds."""
    city = (client.cities_served or [""])[0] if client.cities_served else "—"
    render_status_ribbon(client, city)

    # Run Research / Strategist buttons
    st.markdown("**Get started**")
    city_in = st.text_input("City for research", value=city if city != "—" else "", placeholder="e.g. Phoenix AZ", key=f"overview_city_{client_id}")
    r1, r2, _ = st.columns([1, 1, 4])
    with r1:
        if st.button("▶ Run Research", type="primary"):
            c = (city_in or "").strip() or city
            if c and c != "—":
                st.session_state["run_researcher"] = (client_id, c)
                st.rerun()
            else:
                st.error("Enter a city for research.")
    with r2:
        if st.button("▶ Find Opportunities"):
            st.session_state["run_strategist"] = client_id
            st.rerun()
    st.divider()

    # KPI Cards (4 only)
    opp_open = db.query(Opportunity).filter(
        Opportunity.client_id == client_id,
        Opportunity.status == "OPEN",
        Opportunity.opportunity_score >= 40,
    ).count()
    drafts = db.query(ContentDraft).filter(ContentDraft.client_id == client_id).all()
    content_pending = sum(1 for d in drafts if d.status in ("PENDING", "draft", None))
    regions = _get_regions(client, db, client_id)
    kws = _get_keywords(db, client_id, regions)
    market_gaps = db.query(Opportunity.service).filter(
        Opportunity.client_id == client_id,
        Opportunity.opportunity_score >= 50,
    ).distinct().count()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Opportunities Found", opp_open, "Low-competition wins identified")
    with col2:
        st.metric("Content Ready", content_pending, "Posts pending approval")
    with col3:
        st.metric("Keywords Tracked", len(kws), "Regional variations saved")
    with col4:
        st.metric("Market Gaps Identified", market_gaps, "Services competitors ignore")

    st.divider()
    st.subheader("What We Did For You")
    timeline = _build_timeline(client_id, db)
    for item in timeline:
        st.markdown(f"- {item}")


def _build_timeline(client_id, db):
    """Plain English activity feed from research_logs, opportunities, content_drafts."""
    timeline = []
    logs = db.query(ResearchLog).filter(ResearchLog.client_id == client_id).all()
    if logs:
        n_competitors = len(set(rl.competitor_name for rl in logs))
        city = logs[0].city if logs else ""
        city_phrase = f" in {city}" if city else ""
        timeline.append(f"Scanned {n_competitors} competitors{city_phrase}")
    opps = db.query(Opportunity).filter(
        Opportunity.client_id == client_id,
        Opportunity.status == "OPEN",
        Opportunity.opportunity_score >= 40,
    ).order_by(Opportunity.opportunity_score.desc()).limit(3).all()
    for o in opps:
        timeline.append(f"Identified **{o.service}** as low-competition in {o.geo}")
    if not opps:
        scores = db.query(OpportunityScore).filter(OpportunityScore.client_id == client_id).all()
        for sc in scores:
            for t in (sc.tier_1_topics or [])[:2]:
                timeline.append(f"Identified **{t}** as an underused opportunity")
    drafts = db.query(ContentDraft).filter(
        ContentDraft.client_id == client_id,
        ContentDraft.status != "FAILED",
    ).order_by(ContentDraft.created_at.desc()).limit(3).all()
    for d in drafts:
        timeline.append(f"Drafted {d.platform.replace('_', ' ').title()} post: *{d.topic}*")
    kws = db.query(KeywordIntelligence).filter(KeywordIntelligence.client_id == client_id).count()
    if kws:
        timeline.append(f"Saved {kws} regional keyword variations")
    if not timeline:
        timeline.append("Run research and strategist to see your activity here")
    return timeline


# ─── Market Intelligence ───────────────────────────────────────────────────

def page_market_intelligence(client_id, client, db):
    """Competitive advantage without raw scraping."""
    city = (client.cities_served or [""])[0] if client.cities_served else "—"
    render_status_ribbon(client, city)

    st.subheader("Market Intelligence")
    logs = db.query(ResearchLog).filter(ResearchLog.client_id == client_id).order_by(ResearchLog.confidence_score.desc()).all()

    if not logs:
        st.info("No market data yet. Refresh market data from Overview or Settings.")
        return

    for rl in logs:
        presence = "Website" if rl.source_type == "website" else "Google Only"
        services = ", ".join((rl.extracted_services or [])[:5]) or "General services"
        weakness = ", ".join(rl.missed_opportunities or rl.complaints or ["—"])[:60] or "—"
        conf = "High" if rl.confidence_score >= 70 else ("Medium" if rl.confidence_score >= 50 else "Low")
        with st.expander(f"**{rl.competitor_name}** | {presence} | {conf}"):
            st.markdown("**Services mentioned** · " + services)
            st.markdown("**Weakness / gap** · " + weakness)
            if rl.complaints:
                st.caption("Customer feedback: " + ", ".join(rl.complaints[:3]))


# ─── Opportunities ─────────────────────────────────────────────────────────

def page_opportunities(client_id, client, db):
    """Easy wins engine — ranked opportunity cards with filters."""
    city = (client.cities_served or [""])[0] if client.cities_served else ""
    render_status_ribbon(client, city)

    st.subheader("Opportunities")

    # Filters (default: hide repeats, confidence >= 0.65, prefer geo)
    with st.expander("🔍 Filters", expanded=False):
        conf_min = st.slider(
            "Confidence (min)",
            min_value=0.0,
            max_value=1.0,
            value=0.65,
            step=0.05,
            help="Keyword confidence 0–1. Higher = stronger SEO signal.",
        )
        geo_filter = st.radio(
            "Geo specificity",
            ["All", "Local only", "Non-local"],
            horizontal=True,
            help="Local = has city/state; Non-local = service only.",
        )
        novelty_filter = st.radio(
            "Novelty",
            ["New opportunities only", "Include repeats"],
            index=0,
            horizontal=True,
            help="New = from most recent run; Repeats = previously recommended.",
        )
    st.caption("Default: confidence ≥ 0.65 · New only · Prefer geo-targeted")

    # Base query
    opps = db.query(Opportunity).filter(
        Opportunity.client_id == client_id,
        Opportunity.opportunity_score >= 40,
    ).order_by(Opportunity.opportunity_score.desc()).all()

    # Apply confidence filter (opportunity_score is 0–100)
    conf_min_score = int(conf_min * 100)
    opps = [o for o in opps if (o.opportunity_score or 0) >= conf_min_score]

    # Apply geo filter
    if geo_filter == "Local only":
        opps = [o for o in opps if o.geo and str(o.geo).strip()]
    elif geo_filter == "Non-local":
        opps = [o for o in opps if not (o.geo and str(o.geo).strip())]

    # Apply novelty filter (new = created in same run as most recent OpportunityScore)
    if novelty_filter == "New opportunities only" and opps:
        latest_run = (
            db.query(OpportunityScore)
            .filter(OpportunityScore.client_id == client_id)
            .order_by(OpportunityScore.created_at.desc())
            .first()
        )
        if latest_run:
            from datetime import timedelta
            cutoff = latest_run.created_at - timedelta(seconds=120)
            opps = [o for o in opps if o.created_at and o.created_at >= cutoff]

    # Prefer geo-targeted: sort local first when viewing All
    if geo_filter == "All" and opps:
        opps.sort(key=lambda o: ((1 if (o.geo and str(o.geo).strip()) else 0), o.opportunity_score or 0), reverse=True)

    # Generate Proposal — 1 click to client-ready markdown
    if opps:
        def _enrich_roi(o):
            r = getattr(o, "roi_projection", None)
            if isinstance(r, dict) and r:
                return r
            from roi_projection import compute_roi_projection
            from verticals import get_average_job_value
            v = getattr(client, "client_vertical", None) or "junk_removal"
            return compute_roi_projection(
                opportunity_score=getattr(o, "opportunity_score", 0) or 0,
                has_geo=bool(getattr(o, "geo", None) and str(o.geo).strip()),
                service=getattr(o, "service", "") or "",
                avg_job_value=get_average_job_value(v),
            )
        proposal_md = generate_proposal(
            business_name=client.business_name or "Your Business",
            opportunities=opps,
            top_n=5,
            enrich_roi=_enrich_roi,
        )
        if st.button("📄 Generate Proposal", key="gen-proposal"):
            try:
                path = save_proposal(client_id, proposal_md)
                st.session_state["show_proposal"] = True
                st.session_state["proposal_path"] = str(path)
            except Exception as e:
                st.error(f"Could not save: {e}")
        if st.session_state.get("show_proposal"):
            with st.expander("📄 Client-Ready Proposal", expanded=True):
                st.markdown(proposal_md)
                path = get_proposal_path(client_id)
                st.caption(f"Saved to `{path}`")
                st.download_button(
                    "Download as Markdown",
                    data=proposal_md,
                    file_name=f"proposal-{client_id}.md".replace(" ", "-"),
                    mime="text/markdown",
                    key="dl-proposal",
                )

    if not opps:
        strategies = db.query(ContentStrategy).filter(ContentStrategy.client_id == client_id).order_by(ContentStrategy.priority_score.desc()).all()
        if strategies:
            st.caption("Showing from strategy (run Strategist to populate opportunities table)")
            for s in strategies:
                with st.container():
                    st.markdown(f"### {s.topic}" + (f" — {city}" if city else ""))
                    st.caption(f"Score: {s.priority_score}")
                    st.markdown("**Why it's easy** · Low competition · High search intent")
                    st.markdown("**Action** · " + "; ".join((s.recommended_actions or [])[:2]))
                    if st.button("▶ Generate Content", key=f"gen-{s.id}"):
                        st.session_state["run_strategist"] = client_id
                        st.rerun()
                    st.divider()
        else:
            st.info("No opportunities yet. Run Strategist to identify easy wins.")
        return

    for o in opps:
        with st.container():
            status_badge = "🟢 OPEN" if o.status == "OPEN" else "✓ USED"
            has_geo = bool(o.geo and str(o.geo).strip())
            geo_badge = "📍 Local" if has_geo else "🌐 Broad"
            score = o.opportunity_score or 0
            st.markdown(f"### {o.service} — {o.geo or '—'} [{status_badge}]")
            st.caption(f"Score: {score} · Confidence: {score/100:.0%} · {geo_badge}")
            st.markdown(f"**Why it's easy** · {o.reason or 'Low competition, high intent'}")
            why = o.why_recommended or {}
            if isinstance(why, str):
                try:
                    why = json.loads(why) if why else {}
                except Exception:
                    why = {}
            with st.expander("📋 Why this was recommended", expanded=False):
                for k, v in (why or {}).items():
                    if v:
                        st.markdown(f"**{k.replace('_', ' ').title()}** · {v}")
                if not why:
                    st.caption("No breakdown available.")
            with st.expander("📊 Confidence, Geo & Novelty", expanded=False):
                st.markdown(f"**Confidence** · {why.get('confidence') or 'Strong search intent supported by data'}")
                st.markdown(f"**Geo specificity** · {why.get('geo') or (geo_badge + ' — local targeting')}")
                st.markdown(f"**Novelty status** · {why.get('novelty') or 'Not previously recommended'}")
                seas = o.seasonality or {}
                if isinstance(seas, str):
                    try:
                        seas = json.loads(seas) if seas else {}
                    except Exception:
                        seas = {}
                if seas.get("match"):
                    st.markdown(f"**Seasonality** · {seas.get('current_season', '—').title()} demand (+{int((seas.get('boost_applied') or 0)*100)}% boost)")
                else:
                    st.markdown(f"**Seasonality** · No current-season match")
            roi = o.roi_projection or {}
            if isinstance(roi, str):
                try:
                    roi = json.loads(roi) if roi else {}
                except Exception:
                    roi = {}
            if not roi:
                vertical = getattr(client, "client_vertical", None) or "junk_removal"
                roi = compute_roi_projection(
                    opportunity_score=o.opportunity_score or 0,
                    has_geo=bool(o.geo and str(o.geo).strip()),
                    service=o.service or "",
                    avg_job_value=get_average_job_value(vertical),
                )
            with st.expander("📈 ROI projection (estimates only)", expanded=False):
                st.caption("Conservative model · No guarantees")
                m = roi.get("monthly_searches")
                leads = roi.get("estimated_leads", {})
                rev = roi.get("estimated_revenue", {})
                if m is not None:
                    st.metric("Est. monthly searches", f"{m:,}", "")
                st.markdown("**Est. leads** · " + f"Low: {leads.get('low', 0)} · Expected: {leads.get('expected', 0)} · High: {leads.get('high', 0)}")
                rl, rh = rev.get("low", 0) or 0, rev.get("high", 0) or 0
                st.markdown(f"**Est. revenue** · ${rl:,} – ${rh:,} /mo")
                for a in roi.get("assumptions", [])[:3]:
                    st.caption(f"• {a}")
            st.markdown(f"**Competition** · {o.competition_level or 'Low'}")
            st.markdown(f"**Action** · {o.recommended_action or 'Google Business Post'}")
            if o.status == "OPEN":
                c1, c2, _ = st.columns([1, 1, 4])
                with c1:
                    if st.button("Mark as Used", key=f"used-{o.id}"):
                        o.status = "USED"
                        db.commit()
                        st.success("Marked as used.")
                        st.rerun()
                with c2:
                    if st.button("Generate Content", key=f"gen-{o.id}"):
                        st.session_state["run_strategist"] = client_id
                        st.rerun()
            st.divider()


# ─── Content Studio ────────────────────────────────────────────────────────

def page_content_studio(client_id, client, db):
    """Human-in-the-loop approval zone — content queue grouped by opportunity."""
    city = (client.cities_served or [""])[0] if client.cities_served else "—"
    render_status_ribbon(client, city)

    st.subheader("Content Studio")
    all_drafts = db.query(ContentDraft).filter(ContentDraft.client_id == client_id).order_by(ContentDraft.created_at.desc()).all()

    if not all_drafts:
        st.info("No content yet. Run Strategist to generate drafts.")
        return

    pending = [d for d in all_drafts if d.status in ("PENDING", "draft", None)]
    approved = [d for d in all_drafts if d.status == "approved"]

    def by_topic(d):
        return d.topic or "General"

    st.markdown("**Content queue** (filter: Pending)")
    if pending:
        for topic, group in groupby(sorted(pending, key=by_topic), key=by_topic):
            st.markdown(f"**{topic}**")
            for d in group:
                _render_draft_card(d, db)
    else:
        st.caption("No pending drafts.")

    if approved:
        st.divider()
        st.markdown("**Approved**")
        for topic, group in groupby(sorted(approved, key=by_topic), key=by_topic):
            st.markdown(f"**{topic}**")
            for d in group:
                _render_draft_card(d, db)


def _render_draft_card(d, db):
    """Single draft: editable text, why it works, Approve / Edit / Regenerate."""
    is_pending = d.status in ("PENDING", "draft", None)
    status_badge = "🟡 Pending" if is_pending else "✅ Approved"
    with st.expander(f"**{d.platform.replace('_', ' ').title()}** — {d.topic} [{status_badge}]", expanded=is_pending):
        edited = st.text_area(
            "Edit content",
            value=d.body_refined or d.body,
            height=180,
            key=f"draft-{d.id}",
        )
        st.caption("Why this works: Targets your opportunity · Matches local intent")
        c1, c2, c3, _ = st.columns([1, 1, 1, 4])
        with c1:
            if st.button("✅ Approve", key=f"approve-{d.id}") and is_pending:
                text = (edited or d.body or "").strip()
                if len(text) < 50:
                    st.error("Draft too short (min 50 chars). Edit or regenerate.")
                else:
                    d.status = "approved"
                    d.body_refined = edited or d.body
                    d.word_count = len((edited or d.body).split())
                    db.commit()
                    st.success("Approved.")
                    st.rerun()
        with c2:
            if st.button("✏️ Save edits", key=f"save-{d.id}"):
                d.body_refined = edited or d.body
                d.word_count = len((edited or d.body).split())
                db.commit()
                st.success("Saved.")
                st.rerun()
        with c3:
            if st.button("🔄 Regenerate", key=f"regen-{d.id}"):
                st.session_state["run_strategist"] = d.client_id
                st.rerun()


# ─── SEO & Keywords ───────────────────────────────────────────────────────

def page_seo_keywords(client_id, client, db):
    """Proprietary intelligence without overwhelming."""
    city = (client.cities_served or [""])[0] if client.cities_served else "—"
    render_status_ribbon(client, city)

    st.subheader("SEO & Keywords")
    regions = _get_regions(client, db, client_id)
    kws = _get_keywords(db, client_id, regions)

    # Summary
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total keywords", len(kws), "collected")
    with col2:
        st.metric("Regional variations", len(set((k.keyword, k.region) for k in kws)))
    high_conf = [k for k in kws if (k.confidence_score or 0) >= 70]
    with col3:
        st.metric("High-confidence", len(high_conf), "score ≥ 70")

    # Classify keywords (LLM)
    if regions:
        if st.button("🏷️ Classify keywords"):
            with st.status("Classifying…"):
                ok, msg = run_keyword_classifier(regions[0], client_id)
                st.success(msg) if ok else st.error(msg)
            st.rerun()

    # Add keywords
    with st.expander("➕ Add keywords"):
        with st.form("add_kw"):
            kw_in = st.text_input("Keywords (comma-separated)", placeholder="appliance removal, haul away")
            reg_in = st.text_input("Region", value=regions[0] if regions else "", placeholder="Phoenix")
            typ_in = st.selectbox("Type", ["", "service", "geo", "modifier", "long_tail", "brand"])
            if st.form_submit_button("Add"):
                klist = [k.strip() for k in kw_in.split(",") if k.strip()]
                if klist and reg_in.strip():
                    store_keywords(klist, reg_in.strip(), "client", client_id, typ_in if typ_in else None, vertical=getattr(client, "client_vertical", None) or "junk_removal")
                    st.success(f"Added {len(klist)} keywords.")
                    st.rerun()

    # Table
    if kws:
        import pandas as pd
        df = pd.DataFrame([{
            "Keyword": k.keyword,
            "Type": k.keyword_type or "—",
            "Geo": getattr(k, "geo_phrase", None) or "—",
            "Region": k.region,
            "Score": k.confidence_score or 0,
            "Frequency": k.frequency,
        } for k in sorted(kws, key=lambda x: (x.confidence_score or 0, x.frequency), reverse=True)[:100]])
        st.dataframe(df, width="stretch", hide_index=True)

    # Regional insights
    if kws:
        st.markdown("---")
        st.markdown("**Regional language**")
        high_freq = [k for k in kws if k.frequency >= 3]
        if high_freq:
            top = high_freq[0]
            st.info(f"'{top.keyword}' appears often in {top.region} — consider emphasizing it.")


# ─── Performance ──────────────────────────────────────────────────────────

def page_performance(client_id, client, db):
    """Performance tracking placeholder."""
    city = (client.cities_served or [""])[0] if client.cities_served else "—"
    render_status_ribbon(client, city)
    st.subheader("Performance")
    st.info("Performance tracking — coming in Phase 2.")


# ─── Settings ──────────────────────────────────────────────────────────────

def page_settings(client_id, client, db):
    """Business info, services, brand voice."""
    city = (client.cities_served or [""])[0] if client.cities_served else "—"
    render_status_ribbon(client, city)

    st.subheader("Settings")
    with st.form("settings"):
        business_name = st.text_input("Business Name", value=client.business_name or "")
        vertical_opts = list_verticals()
        vertical_idx = vertical_opts.index(client.client_vertical) if getattr(client, "client_vertical", None) in vertical_opts else 0
        vertical = st.selectbox("Vertical (industry)", vertical_opts, index=vertical_idx, help="Determines opportunity services, ROI model, and filtering")
        website_url = st.text_input("Website URL", value=client.website_url or "")
        phone = st.text_input("Phone", value=client.phone_number or "")
        cities = st.text_input("Cities served (comma-separated)", value=", ".join(client.cities_served or []), placeholder="Phoenix, Scottsdale")
        services = st.text_input("Services (comma-separated)", value=", ".join(client.services_offered or []), placeholder="Junk removal, Estate cleanout")
        tone_opts = ["friendly", "no-BS", "premium", "professional"]
        tone_idx = tone_opts.index(client.brand_tone) if client.brand_tone in tone_opts else 0
        tone = st.selectbox("Brand voice", tone_opts, index=tone_idx)
        if st.form_submit_button("Save"):
            client.business_name = business_name
            client.client_vertical = vertical
            client.website_url = website_url or None
            client.phone_number = phone or None
            client.cities_served = [c.strip() for c in cities.split(",") if c.strip()]
            client.services_offered = [s.strip() for s in services.split(",") if s.strip()]
            client.brand_tone = tone
            db.commit()
            st.success("Settings saved.")

    st.divider()
    st.markdown("**Refresh data**")
    city_in = st.text_input("City for research", value=city, placeholder="Phoenix AZ", key=f"settings_city_{client_id}")
    col1, col2, _ = st.columns([1, 1, 4])
    with col1:
        if st.button("Refresh market data"):
            st.session_state["run_researcher"] = (client_id, city_in or city)
            st.rerun()
    with col2:
        if st.button("Find opportunities"):
            st.session_state["run_strategist"] = client_id
            st.rerun()


def _get_regions(client, db, client_id):
    regions = [c.strip() for c in (client.cities_served or []) if c and str(c).strip()]
    for s in db.query(MarketSnapshot).filter(MarketSnapshot.client_id == client_id).all():
        if s.city and s.city.strip() and s.city.strip() not in regions:
            regions.append(s.city.strip())
    return regions


def _get_keywords(db, client_id, regions):
    q = db.query(KeywordIntelligence)
    if regions:
        q = q.filter(or_(KeywordIntelligence.client_id == client_id, KeywordIntelligence.region.in_(regions)))
    else:
        q = q.filter(KeywordIntelligence.client_id == client_id)
    return q.order_by(func.coalesce(KeywordIntelligence.confidence_score, 0).desc(), KeywordIntelligence.frequency.desc()).all()


# ─── Add Client ────────────────────────────────────────────────────────────

def page_add_client(db):
    st.subheader("Add Client")
    if st.button("← Back to dashboard"):
        st.session_state.pop("nav_add_client", None)
        st.rerun()
    st.divider()
    with st.form("onboarding"):
        client_id = st.text_input("Client ID (slug)", placeholder="junk-away-phoenix")
        business_name = st.text_input("Business Name", placeholder="Junk Away Phoenix")
        website_url = st.text_input("Website URL")
        phone = st.text_input("Phone")
        cities = st.text_input("Cities (comma-separated)", placeholder="Phoenix, Scottsdale, Tempe")
        services = st.text_input("Services (comma-separated)", placeholder="Junk removal, Estate cleanout")
        tone = st.selectbox("Brand voice", ["friendly", "no-BS", "premium", "professional"])
        if st.form_submit_button("Save Client"):
            if client_id and business_name:
                if db.query(Client).filter(Client.client_id == client_id).first():
                    st.warning("Client ID already exists.")
                else:
                    client = Client(
                        client_id=client_id,
                        business_name=business_name,
                        website_url=website_url or None,
                        phone_number=phone or None,
                        cities_served=[c.strip() for c in cities.split(",") if c.strip()],
                        services_offered=[s.strip() for s in services.split(",") if s.strip()],
                        brand_tone=tone,
                    )
                    db.add(client)
                    db.commit()
                    city = (client.cities_served or [""])[0].strip() if client.cities_served else None
                    st.session_state.pop("nav_add_client", None)
                    if city:
                        st.session_state["run_researcher_new"] = (client_id, city)
                        st.success(f"Client added. Refreshing market data…")
                    else:
                        st.success(f"Client added. Go to Overview and run research with a city.")
                    st.rerun()
            else:
                st.error("Client ID and Business Name required.")


# ─── Main ─────────────────────────────────────────────────────────────────

def main():
    st.sidebar.title("Agency AI")
    st.sidebar.caption("Outcomes over activity.")

    db = SessionLocal()
    try:
        clients = db.query(Client).all()

        # Run Researcher (from Settings or new client)
        if st.session_state.get("run_researcher"):
            cid, city = st.session_state.pop("run_researcher")
            st.session_state["client_select"] = cid  # Stay on this client after run
            with st.status("Refreshing market data…"):
                ok, msg = run_researcher(cid, city)
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)
            st.rerun()
        if st.session_state.get("run_researcher_new"):
            cid, city = st.session_state.pop("run_researcher_new")
            st.session_state["client_select"] = cid  # Stay on new client after run
            with st.status("Refreshing market data…"):
                run_researcher(cid, city)
            st.rerun()

        # Run Strategist
        if st.session_state.get("run_strategist"):
            cid = st.session_state.pop("run_strategist")
            st.session_state["client_select"] = cid  # Stay on this client after run
            with st.status("Finding opportunities…"):
                ok, msg = run_strategist(cid)
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)
            st.rerun()

        if not clients:
            page_add_client(db)
            return

        client_id, client = get_client_context(clients)
        if not client:
            return

        st.sidebar.divider()
        if st.sidebar.button("➕ Add New Client", width="stretch"):
            st.session_state["nav_add_client"] = True
            st.rerun()

        nav = st.sidebar.radio(
            "Page",
            [
                "🏠 Overview",
                "🔍 Market Intelligence",
                "🎯 Opportunities",
                "✍️ Content Studio",
                "📍 SEO & Keywords",
                "📊 Performance",
                "⚙️ Settings",
            ],
            label_visibility="collapsed",
        )

        if st.session_state.get("nav_add_client"):
            page_add_client(db)
        elif nav == "🏠 Overview":
            page_overview(client_id, client, db)
        elif nav == "🔍 Market Intelligence":
            page_market_intelligence(client_id, client, db)
        elif nav == "🎯 Opportunities":
            page_opportunities(client_id, client, db)
        elif nav == "✍️ Content Studio":
            page_content_studio(client_id, client, db)
        elif nav == "📍 SEO & Keywords":
            page_seo_keywords(client_id, client, db)
        elif nav == "📊 Performance":
            page_performance(client_id, client, db)
        else:  # Settings
            page_settings(client_id, client, db)
    except Exception as e:
        st.error(str(e))
        raise

    finally:
        db.close()


if __name__ == "__main__":
    main()
