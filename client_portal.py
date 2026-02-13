"""
Client Portal — read-only dashboard for clients via magic link.
Access: streamlit run client_portal.py (with ?token=xxx in URL)
"""

import sys
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# Suppress Streamlit plotly_chart kwargs deprecation (use Streamlit>=1.40 to fix properly)
import logging

class _PlotlyDeprecationFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage() or ""
        if "keyword arguments" in msg and "deprecated" in msg:
            return False  # filter out plotly_chart kwargs deprecation
        return True

for name in ("streamlit", "streamlit.elements"):
    logging.getLogger(name).addFilter(_PlotlyDeprecationFilter())

warnings.filterwarnings("ignore", message=".*keyword arguments.*deprecated.*config.*")

import streamlit as st

from client_portal_access import create_from_token

st.set_page_config(page_title="Client Portal", page_icon="🔐", layout="wide")


def _confidence_pct(score) -> float:
    """Clamp to 0–1 for percentage display."""
    v = float(score or 0)
    if v > 1:
        v = v / 100.0 if v > 2 else 1.0
    return max(0.0, min(1.0, v))


def _quality_badge(score):
    """Color-coded quality badge."""
    if score is None:
        return "—"
    if score < 40:
        color, label = "#dc3545", "Low"
    elif score < 60:
        color, label = "#fd7e14", "Fair"
    elif score < 80:
        color, label = "#ffc107", "Good"
    else:
        color, label = "#28a745", "Strong"
    return f'<span style="background:{color};color:white;padding:2px 8px;border-radius:4px;font-size:0.85em;">{int(round(score))}/100 {label}</span>'


def _get_competitor_quality(rl):
    return getattr(rl, "competitor_comparison_score", None) or rl.website_quality_score


def _apply_branding(portal):
    """Inject client branding: logo, primary color CSS."""
    branding = portal.get_branding()
    if not branding:
        return
    primary = branding.get("primary_color", "#1e40af")
    # Ensure valid hex
    if not primary.startswith("#") or len(primary) not in (4, 7):
        primary = "#1e40af"
    st.markdown(
        f"""
        <style>
            /* Client brand primary color */
            .stMetric label {{ color: {primary}; }}
            .stExpander summary {{ color: {primary}; }}
            div[data-testid="stHeader"] {{ background: linear-gradient(90deg, {primary}22, transparent); }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_portal_header(portal, client):
    """Render branded header: logo + title."""
    branding = portal.get_branding()
    logo_url = (branding.get("logo") or "").strip()
    portal_title = branding.get("portal_title") or "Client Portal"
    name = client.business_name or client.client_id

    if logo_url:
        st.markdown(
            f'<div style="display:flex;align-items:center;gap:1rem;margin-bottom:1rem;">'
            f'<img src="{logo_url}" alt="Logo" style="max-height:48px;max-width:180px;object-fit:contain;" />'
            f'<div><h1 style="margin:0;font-size:1.5rem;">{portal_title} · {name}</h1>'
            f'<p style="margin:0.25rem 0 0 0;font-size:0.9rem;color:#6b7280;">Read-only view of your marketing intelligence.</p></div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.title(f"{portal_title} · {name}")
        st.caption("Read-only view of your marketing intelligence. Share any questions with your agency.")


# ─── Section 1: Overview ───────────────────────────────────────────────────

def _format_delta(delta, suffix=""):
    """Format delta for st.metric: positive = green up, negative = red down."""
    if delta is None:
        return None
    d = float(delta)
    s = f"{int(d):+d}" if d == int(d) else f"{d:+.1f}"
    return f"{s}{suffix} vs 30d ago"


def _section_overview(portal, client):
    """Overview: 4 score cards with 30-day trend arrows, then activity summary."""
    st.subheader("Overview")

    metrics = portal.get_overview_metrics(days=30)
    city = (client.cities_served or [""])[0] if (client and client.cities_served) else "—"

    # Primary score cards with trend arrows
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "Overall Website Score",
            f"{int(metrics.get('overall_website_score', 0))}/100",
            _format_delta(metrics.get("overall_website_delta")),
        )
    with col2:
        st.metric(
            "Local SEO Visibility Score",
            f"{int(metrics.get('local_seo_score', 0))}/100",
            _format_delta(metrics.get("local_seo_delta")),
        )
    with col3:
        st.metric(
            "Content Coverage Score",
            f"{int(metrics.get('content_coverage_score', 0))}/100",
            _format_delta(metrics.get("content_coverage_delta")),
        )
    with col4:
        st.metric(
            "Competitor Percentile Rank",
            f"{int(metrics.get('competitor_percentile', 0))}%",
            _format_delta(metrics.get("competitor_percentile_delta")),
        )

    st.caption("Trends show change vs 30 days ago. Higher percentile = outranking more competitors.")
    st.divider()

    # Activity summary
    summary = portal.to_dict_summary()
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Opportunities", summary.get("opportunities", 0), "Identified")
    with col2:
        st.metric("Content Drafts", summary.get("content_drafts", 0), "Ready for review")
    with col3:
        st.metric("Keywords", len(portal.get_keywords(limit=500)), "Regional variations")
    with col4:
        st.metric("Roadmap Items", summary.get("roadmap_items", 0), "Prioritized tasks")

    st.divider()
    st.caption(f"**{client.business_name or client.client_id}** · {city}")


# ─── Section 2: Competitive Position ──────────────────────────────────────────

PARITY_THRESHOLD = 5  # pts within market = near parity (yellow)


def _position_color(client_score: float, market_avg: float) -> str:
    """Green = ahead, Yellow = near parity, Red = behind."""
    diff = client_score - market_avg
    if diff > PARITY_THRESHOLD:
        return "#22c55e"  # green
    if diff < -PARITY_THRESHOLD:
        return "#ef4444"  # red
    return "#eab308"  # yellow


def _section_competitive_position(portal, client):
    """Client vs competitor average, category bars, ahead/parity/behind highlighting."""
    st.subheader("Competitive Position")

    metrics = portal.get_competitive_position_metrics()
    if not metrics or not metrics.get("categories"):
        st.info("No market data yet. Your agency will populate this section after research.")
        return

    overall = metrics.get("overall", {})
    client_score = overall.get("client_score", 0)
    comp_avg = overall.get("competitor_avg", 0)
    n_comp = metrics.get("competitor_count", 0)

    # Overall: Client vs competitor average
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Your Score", f"{client_score}/100", None)
    with col2:
        st.metric("Market Average", f"{comp_avg}/100", None)
    with col3:
        diff = client_score - comp_avg
        delta_str = f"{diff:+d} vs market"
        st.metric("Position", delta_str, None)
    st.caption(f"Compared to {n_comp} competitors. Green = ahead · Yellow = near parity · Red = behind.")
    st.divider()

    # Category bars
    st.markdown("**Category comparison**")
    for label, data in metrics.get("categories", {}).items():
        c_score = data.get("client_score", 0)
        m_avg = data.get("competitor_avg", 0)
        color = _position_color(c_score, m_avg)
        diff = c_score - m_avg
        status = "▲ ahead" if diff > PARITY_THRESHOLD else ("▼ behind" if diff < -PARITY_THRESHOLD else "◆ near parity")

        st.markdown(f"**{label}** — You: **{c_score}** / Market: **{m_avg}** — <span style='color:{color};font-weight:600'>{status}</span>", unsafe_allow_html=True)
        max_val = max(c_score, m_avg, 1)
        client_pct = (c_score / max_val) * 100
        market_pct = (m_avg / max_val) * 100
        st.markdown(
            f"""
            <div style="margin-bottom:16px;">
                <div style="display:flex;gap:8px;align-items:center;font-size:0.85em;">
                    <span style="width:120px;">You</span>
                    <div style="flex:1;background:#e5e7eb;border-radius:4px;height:16px;overflow:hidden;">
                        <div style="width:{client_pct}%;height:100%;background:{color};border-radius:4px;transition:width 0.3s;"></div>
                    </div>
                    <span style="min-width:36px;">{c_score}</span>
                </div>
                <div style="display:flex;gap:8px;align-items:center;font-size:0.85em;margin-top:6px;">
                    <span style="width:120px;">Market</span>
                    <div style="flex:1;background:#e5e7eb;border-radius:4px;height:16px;overflow:hidden;">
                        <div style="width:{market_pct}%;height:100%;background:#94a3b8;border-radius:4px;"></div>
                    </div>
                    <span style="min-width:36px;">{m_avg}</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ─── Section 3: Opportunities ───────────────────────────────────────────────

def _impact_label(score: int) -> str:
    """Convert opportunity_score to High/Medium/Low."""
    if score >= 70:
        return "High"
    if score >= 40:
        return "Medium"
    return "Low"


def _traffic_gain_pct(score: int, roi: dict) -> str:
    """Estimate traffic gain % from score or roi_projection."""
    if roi and isinstance(roi, dict):
        m = roi.get("monthly_searches")
        if m and m > 0:
            # Heuristic: higher score = more potential gain (15–45% range)
            pct = min(45, max(10, int(score * 0.5)))
            return f"+{pct}%"
    pct = min(45, max(10, int((score or 0) * 0.5)))
    return f"+{pct}%"


def _section_opportunities(portal):
    """Top 5 ranked opportunities with cards: what's missing, why it matters, impact, difficulty."""
    st.subheader("Opportunities")

    opps = portal.get_opportunities(status="OPEN", limit=5)

    if not opps:
        st.info("No open opportunities yet. Your agency will identify wins after strategist runs.")
        return

    for i, o in enumerate(opps, 1):
        score = o.opportunity_score or 0
        roi = o.roi_projection if isinstance(o.roi_projection, dict) else {}
        if not roi and hasattr(o, "roi_projection"):
            try:
                import json
                roi = json.loads(o.roi_projection) if isinstance(o.roi_projection, str) else {}
            except Exception:
                roi = {}

        # Title: e.g. "Add City-Specific Junk Removal Pages"
        svc = (o.service or "").strip() or "Service"
        geo = (o.geo or "").strip()
        if geo:
            title = f"Add {svc} in {geo} Pages"
        else:
            title = f"Add {svc} Pages"

        impact = _impact_label(score)
        comp = (o.competition_level or "low").lower().capitalize()
        difficulty = "Easy" if comp == "Low" else ("Medium" if comp == "Medium" else "Hard")
        traffic = _traffic_gain_pct(score, roi)

        # What's missing: one-liner
        whats_missing = f"Dedicated {svc} pages for {geo}" if geo else f"Dedicated {svc} content"
        if o.recommended_action:
            whats_missing = o.recommended_action

        with st.container():
            st.markdown(f"### {i}. {title}")
            st.markdown(f"**What's missing:** {whats_missing}")
            if o.reason:
                st.markdown("**Why it matters:** " + o.reason)

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Impact", impact, None)
            with col2:
                st.metric("Competition", comp, f"Difficulty: {difficulty}")
            with col3:
                st.metric("Est. Traffic Gain", traffic, None)

            st.divider()


# ─── Section 4: Content & SEO Progress ──────────────────────────────────────

def _render_line_chart(series: list, title: str, y_label: str = "Count"):
    """Simple line chart from [{date, value}] series."""
    if not series:
        st.caption(f"No data yet for {title}.")
        return
    import plotly.express as px
    import pandas as pd
    df = pd.DataFrame(series)
    if df.empty:
        st.caption(f"No data yet for {title}.")
        return
    fig = px.line(df, x="date", y="value", title=title, labels={"value": y_label, "date": "Week"})
    fig.update_layout(height=220, margin=dict(t=40, b=40, l=50, r=20))
    st.plotly_chart(fig)


def _section_content_seo(portal, client):
    """Content & SEO: metrics over time with line charts, plus raw lists."""
    st.subheader("Content & SEO Progress")

    ts = portal.get_content_seo_time_series(days=90)
    col1, col2, col3, col4 = st.columns(4)
    # Cumulative series: last value = total. Impressions: sum of weekly values.
    pages_total = ts["pages_created"][-1]["value"] if ts.get("pages_created") else 0
    kws_total = ts["keywords_targeted"][-1]["value"] if ts.get("keywords_targeted") else 0
    geo_total = ts["geo_phrases_covered"][-1]["value"] if ts.get("geo_phrases_covered") else 0
    imp_total = sum(d["value"] for d in ts.get("impressions", []))

    with col1:
        st.metric("Pages Created", pages_total, "Last 90 days")
    with col2:
        st.metric("Keywords Targeted", kws_total, "Last 90 days")
    with col3:
        st.metric("Geo Phrases Covered", geo_total, "Last 90 days")
    with col4:
        st.metric("Impressions", f"{imp_total:,}" if imp_total else "—", "If available")

    st.divider()
    st.markdown("**Over time**")

    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        _render_line_chart(ts.get("pages_created", []), "Pages Created", "Cumulative")
        _render_line_chart(ts.get("geo_phrases_covered", []), "Geo Phrases Covered", "Cumulative")
    with chart_col2:
        _render_line_chart(ts.get("keywords_targeted", []), "Keywords Targeted", "Cumulative")
        if ts.get("impressions"):
            _render_line_chart(ts.get("impressions", []), "Impressions (Rankings / Visibility)", "Per week")
        else:
            st.caption("**Rankings gained** — No impression data yet. Your agency can add performance metrics.")

    st.divider()
    st.markdown("**Details**")
    drafts = portal.get_content_drafts(limit=30)
    outlines = portal.get_geo_page_outlines(limit=30)
    regions = [c.strip() for c in (client.cities_served or []) if c and str(c).strip()]
    kws = portal.get_keywords(regions=regions, limit=100) if regions else portal.get_keywords(limit=100)

    tab1, tab2, tab3 = st.tabs(["Content Drafts", "Keywords", "Geo Page Outlines"])

    with tab1:
        if not drafts:
            st.caption("No content drafts yet. Your agency will create drafts from opportunities.")
        else:
            for d in drafts[:20]:
                status = (d.status or "draft").lower()
                st.markdown(f"• **{d.topic}** — {d.platform} · {status}")

    with tab2:
        if not kws:
            st.caption("No keywords tracked yet.")
        else:
            sorted_kws = sorted(
                kws,
                key=lambda k: (float(k.keyword_confidence_score or k.confidence_score or 0), k.frequency or 0),
                reverse=True,
            )[:40]
            for k in sorted_kws:
                conf = float(k.keyword_confidence_score or k.confidence_score or 0)
                if conf > 1:
                    conf = conf / 100.0
                geo = f"{k.city or ''} {k.state or ''}".strip() or "—"
                st.caption(f"• **{k.keyword}** · {geo} · {conf:.0%}")

    with tab3:
        if not outlines:
            st.caption("No geo page outlines yet.")
        else:
            for o in outlines[:15]:
                phrase = o.geo_phrase or f"{o.service} in {o.city or ''}"
                conf = f"{_confidence_pct(o.confidence_score):.0%}"
                st.markdown(f"• **{phrase}** — {conf} · {o.page_status or 'DRAFT'}")


# ─── Section 5: Website Health ──────────────────────────────────────────────

def _severity_badge(severity: str) -> str:
    """Color-coded severity badge."""
    if not severity or severity == "none":
        return ""
    colors = {"minor": "#eab308", "major": "#f97316", "critical": "#ef4444"}
    color = colors.get(severity, "#6b7280")
    label = severity.capitalize()
    return f'<span style="background:{color};color:white;padding:2px 8px;border-radius:4px;font-size:0.85em;">{label}</span>'


def _section_website_health(portal, client):
    """Website Health: Technical SEO, Content Quality, UX/Conversion, Trust Signals — client vs market with gap severity."""
    st.subheader("Website Health")

    metrics = portal.get_website_health_metrics()
    categories = metrics.get("categories", [])
    n_comp = metrics.get("competitor_count", 0)

    if not categories:
        st.info("No market data yet. Your agency will populate this section after research.")
        return

    st.caption(f"Compared to {n_comp} competitors. Gap severity: Minor (5–10 pts behind) · Major (10–20) · Critical (20+).")
    st.divider()

    for cat in categories:
        label = cat.get("label", "")
        client_score = cat.get("client_score", 0)
        market_avg = cat.get("market_avg", 0)
        severity = cat.get("severity", "none")
        gap = cat.get("gap", 0)

        sev_badge = _severity_badge(severity) if severity and severity != "none" else ""
        gap_str = f"{gap:+d}" if gap != 0 else "0"

        st.markdown(f"**{label}**")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Client Score", f"{client_score}/100", None)
        with col2:
            st.metric("Market Average", f"{market_avg}/100", None)
        with col3:
            st.metric("Gap", gap_str, None)
        with col4:
            if sev_badge:
                st.markdown(sev_badge, unsafe_allow_html=True)
            else:
                st.caption("On track")
        st.divider()

    # Recent gap proposal outcomes
    outcomes = portal.get_website_gap_proposal_outcomes(limit=5)
    if outcomes:
        st.markdown("**Recent gap analysis outcomes**")
        for o in outcomes:
            gaps = ", ".join(o.gap_types or []) or "—"
            severities = (o.gap_severities or {})
            sev_str = ", ".join(f"{k}: {v}" for k, v in list(severities.items())[:3]) if severities else "—"
            st.markdown(f"- **{gaps}** · {o.outcome or 'pending'}")
            if sev_str:
                st.caption(f"  Severity: {sev_str}")


# ─── Section 6: Roadmap & Next Actions ──────────────────────────────────────

def _render_roadmap_item(rec: dict, show_impact: bool, badge: str = ""):
    """Render a single roadmap item (expandable card)."""
    title = rec.get("title") or "—"
    task_type = rec.get("task_type") or "—"
    conf = f"{_confidence_pct(rec.get('confidence_score')):.0%}"
    header = f"{badge} [{task_type}] **{title}** — {conf}"
    with st.expander(header.strip(), expanded=False):
        if rec.get("description"):
            st.markdown(rec["description"])
        if show_impact and rec.get("expected_impact"):
            st.caption(f"**Estimated impact:** {rec['expected_impact']}")


def _section_roadmap(portal):
    """30/60/90 day plan, completed vs upcoming, locked vs optional, estimated impact toggle."""
    st.subheader("Roadmap & Next Actions")

    grouped = portal.get_roadmap_grouped()
    by_period = grouped.get("by_period", {})
    completed = grouped.get("completed", [])
    upcoming = grouped.get("upcoming", [])
    locked = grouped.get("locked", [])
    optional = grouped.get("optional", [])

    if not by_period and not completed and not upcoming:
        st.info("No roadmap items yet. Your agency will add prioritized tasks after research.")
        return

    show_impact = st.checkbox("Show estimated impact", value=True, key="roadmap_show_impact")
    st.caption("🔒 Locked = must-do · ◐ Optional = nice-to-have")
    st.divider()

    # 30 / 60 / 90 day plan
    st.markdown("**30 / 60 / 90 day plan**")
    p30, p60, p90 = st.tabs(["30 days", "60 days", "90 days"])
    with p30:
        items = by_period.get(30, [])
        if not items:
            st.caption("No tasks in this period.")
        for rec in items:
            badge = "🔒 " if rec.get("is_locked") else "◐ "
            _render_roadmap_item(rec, show_impact, badge)
    with p60:
        items = by_period.get(60, [])
        if not items:
            st.caption("No tasks in this period.")
        for rec in items:
            badge = "🔒 " if rec.get("is_locked") else "◐ "
            _render_roadmap_item(rec, show_impact, badge)
    with p90:
        items = by_period.get(90, [])
        if not items:
            st.caption("No tasks in this period.")
        for rec in items:
            badge = "🔒 " if rec.get("is_locked") else "◐ "
            _render_roadmap_item(rec, show_impact, badge)

    st.divider()

    # Completed vs Upcoming
    col_completed, col_upcoming = st.columns(2)
    with col_completed:
        st.markdown("**Completed**")
        if not completed:
            st.caption("None yet.")
        for rec in completed:
            _render_roadmap_item(rec, show_impact, "✓ ")

    with col_upcoming:
        st.markdown("**Upcoming**")
        if not upcoming:
            st.caption("All done — or no tasks yet.")
        for rec in upcoming[:15]:
            badge = "🔒 " if rec.get("is_locked") else "◐ "
            _render_roadmap_item(rec, show_impact, badge)


# ─── Section 7: Proposal ────────────────────────────────────────────────────

CATEGORY_LABELS_PROPOSAL = {
    "technical_seo": "Technical SEO",
    "content_depth": "Content Depth",
    "geo_coverage": "Geo Coverage",
    "keyword_overlap": "Keyword Overlap",
    "trust_signals": "Trust Signals",
    "conversion_elements": "Conversion Elements",
}


def _section_proposal(portal, client):
    """View latest Website Gap Proposal, download PDF, recommended upgrades, Accept / Request changes."""
    st.subheader("Proposal")

    proposal_data = portal.get_latest_website_gap_proposal()
    pdf_export = portal.get_latest_website_gap_pdf()

    if not proposal_data:
        st.info("No Website Gap Proposal yet. Your agency will generate one from the main app.")
        return

    gap = proposal_data.get("gap", {})
    proposals = proposal_data.get("proposals", [])
    markdown = proposal_data.get("markdown", "")

    # View latest proposal
    st.markdown("**Latest Website Gap Proposal**")
    with st.expander("View full proposal", expanded=True):
        st.markdown(markdown or "No content.")

    # Download PDF
    st.markdown("**Download PDF**")
    if pdf_export and pdf_export.get("pdf_file_path"):
        from pathlib import Path
        path = Path(pdf_export["pdf_file_path"])
        if path.exists():
            try:
                pdf_bytes = path.read_bytes()
                st.download_button(
                    "📥 Download Website Gap Proposal PDF",
                    data=pdf_bytes,
                    file_name=path.name,
                    mime="application/pdf",
                    key="proposal_dl_pdf",
                )
            except Exception as e:
                st.caption(f"Could not load file: {e}")
        else:
            st.caption("PDF file not found. Ask your agency to regenerate.")
    else:
        st.caption("No PDF export yet. Your agency can generate one from the main app.")

    # Recommended upgrades
    st.divider()
    st.markdown("**Recommended upgrades**")
    if proposals:
        for p in proposals:
            cat = CATEGORY_LABELS_PROPOSAL.get(p.get("category", ""), p.get("category", ""))
            sev = (p.get("severity") or "").lower()
            sev_tag = {"critical": "🔴", "major": "🟠", "minor": "🟡"}.get(sev, "")
            with st.expander(f"{sev_tag} **{cat}** — {p.get('suggested_price_range', '')}", expanded=False):
                st.markdown(f"**Problem:** {p.get('problem_statement', '')}")
                st.markdown(f"**Impact:** {p.get('business_impact', '')}")
                st.markdown(f"**Solution:** {p.get('recommended_solution', '')}")
    else:
        st.caption("No gaps flagged. Your site is competitive.")

    # Accept / Request changes
    st.divider()
    st.markdown("**Your response**")
    col1, col2, _ = st.columns([1, 1, 4])
    with col1:
        if st.button("✓ Accept", type="primary", key="proposal_accept"):
            ok, msg = portal.submit_proposal_response("accept")
            if ok:
                st.success(msg)
            else:
                st.error(msg)
            st.rerun()
    with col2:
        if st.button("✎ Request changes", key="proposal_request"):
            ok, msg = portal.submit_proposal_response("request_changes")
            if ok:
                st.success(msg)
            else:
                st.error(msg)
            st.rerun()
    st.caption("Accept = you're good with the proposal. Request changes = you'd like revisions.")


# ─── Main ───────────────────────────────────────────────────────────────────

def main():
    raw = st.query_params.get("token")
    token = raw[0] if isinstance(raw, list) and raw else (raw if isinstance(raw, str) else None)

    if not token:
        st.title("Client Portal")
        st.info("Access this portal via the magic link shared by your agency. The link contains a secure token.")
        st.caption("If you received a link, ensure the full URL including `?token=...` is loaded.")
        return

    portal = create_from_token(token)
    if not portal:
        st.title("Invalid or Expired Link")
        st.error("This link is invalid or has expired. Please request a new portal link from your agency.")
        return

    try:
        client = portal.get_client()
        if not client:
            st.error("Client not found.")
            return

        _apply_branding(portal)
        _render_portal_header(portal, client)
        st.divider()

        sections = [
            ("Overview", _section_overview),
            ("Competitive Position", _section_competitive_position),
            ("Opportunities", _section_opportunities),
            ("Content & SEO Progress", _section_content_seo),
            ("Website Health", _section_website_health),
            ("Roadmap & Next Actions", _section_roadmap),
            ("Proposal", _section_proposal),
        ]

        for title, render_fn in sections:
            with st.container():
                render_fn(portal, client)
                st.divider()

    finally:
        portal.close()


if __name__ == "__main__":
    main()
