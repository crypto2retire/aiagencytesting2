"""
Auto Proposal Generator — assembles gap analysis into a full client-ready proposal.
Outputs: Executive Summary, Competitive Comparison, Gap Breakdown, Roadmap, Investment Ranges, CTA.
Formats: Markdown, HTML, PDF-ready HTML.
"""

import re
from typing import Any, Dict, List, Optional

CATEGORY_LABELS = {
    "technical_seo": "Technical SEO",
    "content_depth": "Content Depth",
    "geo_coverage": "Geo Coverage",
    "keyword_overlap": "Keyword Overlap",
    "trust_signals": "Trust Signals",
    "conversion_elements": "Conversion Elements",
}


def _label(cat: str) -> str:
    return CATEGORY_LABELS.get(cat, cat.replace("_", " ").title())


def _escape_html(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _markdown_heading(level: int, text: str) -> str:
    return f"{'#' * level} {text}\n"


def _markdown_table(headers: List[str], rows: List[List[str]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(c) for c in row) + " |")
    return "\n".join(lines) + "\n"


class AutoProposalGenerator:
    """
    Assembles gap analysis + proposals into a full proposal document.
    Input: gap_analysis (from WebsiteGapAnalyzer), proposals (from ProposalMapper),
           optional client_name, competitor_sites for richer context.
    anonymize_competitors: when True, use "Top Local Competitors" instead of "N competitors"
    logo_url: optional client logo URL for branded header
    """

    def __init__(
        self,
        gap_analysis: Dict[str, Any],
        proposals: List[Dict[str, Any]],
        *,
        client_name: Optional[str] = None,
        client_domain: Optional[str] = None,
        competitor_sites: Optional[List[Dict[str, Any]]] = None,
        anonymize_competitors: bool = False,
        logo_url: Optional[str] = None,
        primary_color: Optional[str] = None,
    ):
        self.gap_analysis = gap_analysis
        self.proposals = proposals
        self.client_name = client_name or "Your Business"
        self.client_domain = client_domain or (gap_analysis.get("client_domain") or "")
        self.competitor_sites = competitor_sites or []
        self.anonymize_competitors = anonymize_competitors
        self.logo_url = (logo_url or "").strip() or None
        pc = (primary_color or "").strip()
        self.primary_color = pc if pc and pc.startswith("#") else "#1e40af"

    def _build_executive_summary(self) -> str:
        categories = [
            "technical_seo", "content_depth", "geo_coverage",
            "keyword_overlap", "trust_signals", "conversion_elements",
        ]
        gap_count = sum(1 for c in categories if (self.gap_analysis.get(c) or {}).get("flagged"))
        comp_count = self.gap_analysis.get("competitor_count") or 0
        comp_ref = "top local competitors" if self.anonymize_competitors else f"{comp_count} competitors"
        if gap_count == 0:
            return (
                f"We compared {self.client_domain or self.client_name} against {comp_ref}. "
                "Your website performs competitively. The recommendations below focus on incremental improvements."
            )
        return (
            f"We analyzed {self.client_domain or self.client_name} against {comp_ref} and found "
            f"{gap_count} key areas where your site lags the market. Closing these gaps will help you capture more "
            "local search traffic and convert more visitors into leads."
        )

    def _build_comparison_rows(self) -> List[List[str]]:
        rows = []
        categories = [
            "technical_seo", "content_depth", "geo_coverage",
            "keyword_overlap", "trust_signals", "conversion_elements",
        ]
        for cat in categories:
            data = self.gap_analysis.get(cat) or {}
            if not isinstance(data, dict):
                continue
            cs = data.get("client_score", 0)
            ca = data.get("competitor_avg", 0)
            gap = data.get("gap", 0)
            gap_str = f"{gap:+d}" if gap != 0 else "—"
            rows.append([_label(cat), str(cs), str(int(ca)), gap_str])
        return rows

    def _comparison_header(self) -> List[str]:
        """Table header for comparison; anonymized uses 'Market Avg' instead of 'Competitor Avg'."""
        if self.anonymize_competitors:
            return ["Category", "Your Score", "Market Avg", "Gap"]
        return ["Category", "Your Score", "Competitor Avg", "Gap"]

    def _build_roadmap_phases(self) -> Dict[str, List[Dict[str, Any]]]:
        """Group proposals into 30 / 60 / 90 day phases by effort."""
        phases = {"30": [], "60": [], "90": []}
        effort_to_phase = {"Low": "30", "Medium": "60", "High": "90"}
        for p in self.proposals:
            effort = (p.get("estimated_effort") or "Medium")
            phase = effort_to_phase.get(effort, "60")
            phases[phase].append(p)
        return phases

    def _build_investment_ranges(self) -> str:
        if not self.proposals:
            return "Investment range will be determined based on scope."
        total_min = 0
        total_max = 0
        for p in self.proposals:
            pr = p.get("suggested_price_range") or ""
            m = re.findall(r"\$?([\d,]+)", pr)
            if len(m) >= 2:
                total_min += int(m[0].replace(",", ""))
                total_max += int(m[1].replace(",", ""))
            elif len(m) == 1:
                v = int(m[0].replace(",", ""))
                total_min += v
                total_max += v
        if total_min == 0 and total_max == 0:
            return "Contact us for a detailed quote based on your priorities."
        return f"${total_min:,} – ${total_max:,} (total for all recommended improvements)"

    def _build_optional_addons(self) -> List[Dict[str, Any]]:
        """Proposals with minor severity or lower impact as optional add-ons."""
        return [p for p in self.proposals if (p.get("severity") or "").lower() == "minor"]

    def to_markdown(self) -> str:
        """Render proposal as Markdown."""
        sections = []

        # Title
        sections.append(_markdown_heading(1, f"Website Growth Proposal — {self.client_name}"))
        sections.append("")

        # Executive Summary
        sections.append(_markdown_heading(2, "Executive Summary"))
        sections.append(self._build_executive_summary())
        sections.append("")
        sections.append("---")
        sections.append("")

        # Competitive Landscape
        sections.append(_markdown_heading(2, "Competitive Landscape"))
        rows = self._build_comparison_rows()
        if rows:
            sections.append(_markdown_table(self._comparison_header(), rows))
        else:
            sections.append("*No comparative data available.*")
        sections.append("")
        sections.append("---")
        sections.append("")

        # Website Gap Findings
        sections.append(_markdown_heading(2, "Website Gap Findings"))
        if self.proposals:
            for p in self.proposals:
                cat_label = _label(p.get("category", ""))
                sections.append(_markdown_heading(3, cat_label))
                sections.append(f"**Problem:** {p.get('problem_statement', '')}")
                sections.append(f"**Business impact:** {p.get('business_impact', '')}")
                sections.append(f"**Recommended solution:** {p.get('recommended_solution', '')}")
                sections.append(f"**Effort:** {p.get('estimated_effort', '')} | **Investment:** {p.get('suggested_price_range', '')}")
                sections.append("")
        else:
            sections.append("*No gaps flagged. Your website is competitive.*")
        sections.append("---")
        sections.append("")

        # Recommended Improvements
        sections.append(_markdown_heading(2, "Recommended Improvements"))
        for p in self.proposals:
            sections.append(f"- **{_label(p.get('category', ''))}:** {p.get('recommended_solution', '')}")
        if not self.proposals:
            sections.append("*No recommendations at this time.*")
        sections.append("")
        sections.append("---")
        sections.append("")

        # 30 / 60 / 90 Day Roadmap
        sections.append(_markdown_heading(2, "30 / 60 / 90 Day Roadmap"))
        phases = self._build_roadmap_phases()
        for days, label in [("30", "First 30 Days"), ("60", "60 Days"), ("90", "90 Days")]:
            items = phases.get(days, [])
            sections.append(_markdown_heading(3, label))
            if items:
                for p in items:
                    sections.append(f"- {p.get('recommended_solution', '')} — {p.get('suggested_price_range', '')}")
            else:
                sections.append("*No items scheduled for this phase.*")
            sections.append("")
        sections.append("---")
        sections.append("")

        # Investment Ranges
        sections.append(_markdown_heading(2, "Investment Ranges"))
        sections.append(self._build_investment_ranges())
        sections.append("")

        # Optional Add-ons
        addons = self._build_optional_addons()
        if addons:
            sections.append(_markdown_heading(3, "Optional Add-ons"))
            for p in addons:
                sections.append(f"- {p.get('recommended_solution', '')} — {p.get('suggested_price_range', '')}")
            sections.append("")
        sections.append("---")
        sections.append("")

        # Next Steps CTA
        sections.append(_markdown_heading(2, "Next Steps"))
        sections.append(
            "Ready to close the gap? Schedule a call to discuss priorities, timeline, and get started. "
            "We'll focus on the highest-impact improvements first."
        )

        return "\n".join(sections)

    def to_html(self) -> str:
        """Render proposal as styled HTML."""
        return self._render_html(pdf_ready=False)

    def to_pdf_html(self) -> str:
        """Render proposal as PDF-ready HTML (print-optimized)."""
        return self._render_html(pdf_ready=True)

    def _render_html(self, pdf_ready: bool) -> str:
        """Shared HTML renderer; pdf_ready adds print styles and page-break hints."""
        sections = []

        def h(n: int, t: str) -> str:
            tag = f"h{n}"
            return f"<{tag}>{_escape_html(t)}</{tag}>"

        def para(text: str) -> str:
            return f"<p>{_escape_html(text)}</p>"

        def ul(items: List[str]) -> str:
            lis = "".join(f"<li>{_escape_html(i)}</li>" for i in items)
            return f"<ul>{lis}</ul>"

        def table(headers: List[str], rows: List[List[str]]) -> str:
            ths = "".join(f"<th>{_escape_html(hdr)}</th>" for hdr in headers)
            trs = []
            for row in rows:
                tds = "".join(f"<td>{_escape_html(str(c))}</td>" for c in row)
                trs.append(f"<tr>{tds}</tr>")
            return f"<table><thead><tr>{ths}</tr></thead><tbody>{''.join(trs)}</tbody></table>"

        page_break = '<div class="page-break"></div>' if pdf_ready else ""
        css = self._get_html_styles(pdf_ready)

        body_parts = []

        # Executive Summary
        body_parts.append(h(1, f"Website Growth Proposal — {self.client_name}"))
        body_parts.append(h(2, "Executive Summary"))
        body_parts.append(para(self._build_executive_summary()))
        body_parts.append(page_break)

        # Competitive Landscape
        body_parts.append(h(2, "Competitive Landscape"))
        rows = self._build_comparison_rows()
        if rows:
            body_parts.append(table(self._comparison_header(), rows))
        else:
            body_parts.append(para("No comparative data available."))
        body_parts.append(page_break)

        # Website Gap Findings
        body_parts.append(h(2, "Website Gap Findings"))
        if self.proposals:
            for prop in self.proposals:
                body_parts.append(h(3, _label(prop.get("category", ""))))
                body_parts.append(f'<p><strong>Problem:</strong> {_escape_html(prop.get("problem_statement", ""))}</p>')
                body_parts.append(f'<p><strong>Business impact:</strong> {_escape_html(prop.get("business_impact", ""))}</p>')
                body_parts.append(f'<p><strong>Recommended solution:</strong> {_escape_html(prop.get("recommended_solution", ""))}</p>')
                body_parts.append(para(f"Effort: {prop.get('estimated_effort', '')} | Investment: {prop.get('suggested_price_range', '')}"))
        else:
            body_parts.append(para("No gaps flagged. Your website is competitive."))
        body_parts.append(page_break)

        # Recommended Improvements
        body_parts.append(h(2, "Recommended Improvements"))
        recs = [f"{_label(p.get('category', ''))}: {p.get('recommended_solution', '')}" for p in self.proposals]
        body_parts.append(ul(recs) if recs else para("No recommendations at this time."))
        body_parts.append(page_break)

        # Roadmap
        body_parts.append(h(2, "30 / 60 / 90 Day Roadmap"))
        phases = self._build_roadmap_phases()
        for days, label in [("30", "First 30 Days"), ("60", "60 Days"), ("90", "90 Days")]:
            body_parts.append(h(3, label))
            items = phases.get(days, [])
            if items:
                body_parts.append(ul([f"{p.get('recommended_solution', '')} — {p.get('suggested_price_range', '')}" for p in items]))
            else:
                body_parts.append(para("No items scheduled for this phase."))
        body_parts.append(page_break)

        # Investment Ranges
        body_parts.append(h(2, "Investment Ranges"))
        body_parts.append(para(self._build_investment_ranges()))

        addons = self._build_optional_addons()
        if addons:
            body_parts.append(h(3, "Optional Add-ons"))
            body_parts.append(ul([f"{p.get('recommended_solution', '')} — {p.get('suggested_price_range', '')}" for p in addons]))

        body_parts.append(h(2, "Next Steps"))
        body_parts.append(para(
            "Ready to close the gap? Schedule a call to discuss priorities, timeline, and get started. "
            "We'll focus on the highest-impact improvements first."
        ))

        # Header with optional logo
        header_html = ""
        if self.logo_url:
            header_html = f'<div class="proposal-header"><img src="{_escape_html(self.logo_url)}" alt="Logo" class="proposal-logo" /></div>'
        else:
            header_html = '<div class="proposal-header"></div>'

        body_html = "\n".join(body_parts)

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Website Growth Proposal — {_escape_html(self.client_name)}</title>
<style>{css}</style>
</head>
<body>
{header_html}
<div class="proposal">
{body_html}
</div>
</body>
</html>"""

    def _get_html_styles(self, pdf_ready: bool) -> str:
        pc = self.primary_color
        # Lighter accent for borders
        accent = pc
        # Use variable for page-break rule to avoid f-string colon parsing issues
        page_break_rule = "page-break-after: always;"
        base = f"""
:root {{ --brand-primary: {pc}; --brand-accent: {accent}; --text-primary: #1f2937; --text-muted: #6b7280; }}
body {{ margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', sans-serif; color: var(--text-primary); background: #fff; }}
.proposal-header {{ padding: 1.5rem 2rem; border-bottom: 1px solid #e5e7eb; min-height: 60px; }}
.proposal-logo {{ max-height: 48px; max-width: 200px; object-fit: contain; }}
.proposal {{ max-width: 800px; margin: 0 auto; padding: 2rem; line-height: 1.6; }}
.proposal h1 {{ font-size: 1.75rem; margin-top: 0; margin-bottom: 0.5rem; color: var(--brand-primary); border-bottom: 3px solid var(--brand-accent); padding-bottom: 0.5rem; font-weight: 700; }}
.proposal h2 {{ font-size: 1.35rem; margin-top: 1.75rem; margin-bottom: 0.5rem; color: var(--brand-primary); font-weight: 600; }}
.proposal h3 {{ font-size: 1.1rem; margin-top: 1.25rem; margin-bottom: 0.25rem; color: var(--text-primary); font-weight: 600; }}
.proposal p {{ margin: 0.5rem 0; color: var(--text-primary); }}
.proposal ul {{ margin: 0.5rem 0; padding-left: 1.5rem; }}
.proposal li {{ margin: 0.25rem 0; }}
.proposal table {{ width: 100%; border-collapse: collapse; margin: 1rem 0; font-size: 0.95rem; box-shadow: 0 1px 3px rgba(0,0,0,0.08); border-radius: 6px; overflow: hidden; }}
.proposal th, .proposal td {{ border: 1px solid #e5e7eb; padding: 0.6rem 0.85rem; text-align: left; }}
.proposal th {{ background: var(--brand-primary); color: #fff; font-weight: 600; }}
.proposal tr:nth-child(even) {{ background: #f9fafb; }}
.proposal .page-break {{ {page_break_rule} }}
"""
        if pdf_ready:
            base += """
@media print { body { -webkit-print-color-adjust: exact; print-color-adjust: exact; } .page-break { page-break-after: always; break-after: page; } }
"""
        return base


def generate_proposal(
    gap_analysis: Dict[str, Any],
    proposals: Optional[List[Dict[str, Any]]] = None,
    *,
    client_name: Optional[str] = None,
    client_domain: Optional[str] = None,
    competitor_sites: Optional[List[Dict[str, Any]]] = None,
    anonymize_competitors: bool = False,
    logo_url: Optional[str] = None,
    primary_color: Optional[str] = None,
    db=None,
    format: str = "markdown",
) -> str:
    """
    Convenience: run ProposalMapper if proposals not provided, then render.
    format: 'markdown' | 'html' | 'pdf_html'
    anonymize_competitors: use "Top Local Competitors" instead of competitor count
    logo_url: optional client logo URL for branded header
    db: optional session for learned price/close-rate correlations
    """
    from proposal_mapper import ProposalMapper

    props = proposals
    if props is None:
        mapper = ProposalMapper()
        props = mapper.map_gaps_to_proposals(gap_analysis, db)

    gen = AutoProposalGenerator(
        gap_analysis=gap_analysis,
        proposals=props,
        client_name=client_name,
        client_domain=client_domain or gap_analysis.get("client_domain"),
        primary_color=primary_color,
        competitor_sites=competitor_sites,
        anonymize_competitors=anonymize_competitors,
        logo_url=logo_url,
    )
    if format == "html":
        return gen.to_html()
    if format == "pdf_html":
        return gen.to_pdf_html()
    return gen.to_markdown()
