"""
PDF Generator — exports content or proposals to PDF.
Returns file path to the generated PDF.
Website Gap: HTML→PDF with weasyprint, branded styling, logo, competitor anonymization.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import func


def generate_website_gap_pdf(
    client_id: int,
    output_path: Optional[str] = None,
    *,
    anonymize_competitors: bool = False,
    logo_url: Optional[str] = None,
    primary_color: Optional[str] = None,
) -> str:
    """
    Generate Website Gap Proposal PDF from HTML.
    Uses weasyprint for HTML→PDF. Branded styling, optional client logo.
    anonymize_competitors: when True, use "Top Local Competitors" instead of competitor names/count
    logo_url: optional client logo URL for header (e.g. from Client.asset_links["logo"])
    Returns path to generated PDF.
    """
    from config import PROJECT_ROOT
    from database import Client, ResearchLog, SessionLocal, Website
    from website_gap_analyzer import WebsiteGapAnalyzer, populate_websites_from_research
    from proposal_mapper import ProposalMapper
    from auto_proposal_generator import AutoProposalGenerator, generate_proposal

    db = SessionLocal()
    try:
        client = db.query(Client).filter(Client.id == client_id).first()
        if not client:
            raise ValueError(f"Client not found: {client_id}")

        cid = client.client_id
        safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in (cid or "")) or "client"
        exports_dir = PROJECT_ROOT / "exports"
        exports_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        pdf_path = output_path or str(exports_dir / f"{safe_id}_website_gap_{timestamp}.pdf")

        populate_websites_from_research(db, cid)
        client_site = db.query(Website).filter(Website.client_id == cid).first()
        if not client_site:
            raise ValueError("No client website. Add website URL in Settings and run research.")

        comp_ids = []
        for rl in db.query(ResearchLog).filter(ResearchLog.client_id == cid).all():
            w = db.query(Website).filter(Website.research_log_id == rl.id).first()
            if w:
                comp_ids.append(w.id)

        analyzer = WebsiteGapAnalyzer(db)
        gap = analyzer.analyze(client_site.id, comp_ids)
        if gap.get("error"):
            raise ValueError("Gap analysis failed.")

        mapper = ProposalMapper()
        proposals = mapper.map_gaps_to_proposals(gap, db)

        html = generate_proposal(
            gap,
            proposals,
            client_name=client.business_name or cid,
            client_domain=gap.get("client_domain"),
            anonymize_competitors=anonymize_competitors,
            logo_url=logo_url,
            primary_color=primary_color,
            format="pdf_html",
        )

        if not html.strip():
            raise ValueError("Generated HTML is empty.")

        _html_to_pdf(html, pdf_path)
        return pdf_path
    finally:
        db.close()


def _html_to_pdf(html_content: str, output_path: str) -> None:
    """Convert HTML to PDF using weasyprint."""
    try:
        from weasyprint import HTML
        from weasyprint.text.fonts import FontConfiguration
    except ImportError:
        raise RuntimeError(
            "Install weasyprint for HTML→PDF: pip install weasyprint"
        )

    font_config = FontConfiguration()
    html_doc = HTML(string=html_content)
    html_doc.write_pdf(output_path, font_config=font_config)


def generate_pdf(
    client_id: int,
    export_type: str = "CONTENT",
    *,
    anonymize_competitors: bool = False,
    logo_url: Optional[str] = None,
    primary_color: Optional[str] = None,
) -> str:
    """
    Generates PDF and returns file path.
    client_id: Client.id (primary key)
    export_type: 'CONTENT' | 'PROPOSAL' | 'WEBSITE_GAP'
    For WEBSITE_GAP: anonymize_competitors, logo_url, primary_color used for HTML→PDF (weasyprint).
    """
    from config import PROJECT_ROOT
    from database import Client, ContentDraft, GeoPageOutline, SalesProposal, SessionLocal

    if export_type.upper() == "WEBSITE_GAP":
        return generate_website_gap_pdf(
            client_id,
            anonymize_competitors=anonymize_competitors,
            logo_url=logo_url,
            primary_color=primary_color,
        )

    db = SessionLocal()
    try:
        client = db.query(Client).filter(Client.id == client_id).first()
        if not client:
            raise ValueError(f"Client not found: {client_id}")

        cid = client.client_id
        safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in (cid or "")) or "client"
        exports_dir = PROJECT_ROOT / "exports"
        exports_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        pdf_path = exports_dir / f"{safe_id}_{export_type.lower()}_{timestamp}.pdf"

        if export_type.upper() == "CONTENT":
            content = _build_content_export(db, cid)
        elif export_type.upper() == "PROPOSAL":
            content = _build_proposal_export(db, cid)
        else:
            raise ValueError(f"Invalid export_type: {export_type}")

        if not content.strip():
            raise ValueError(f"No {export_type} content to export for client {cid}")

        _write_pdf(content, str(pdf_path))
        return str(pdf_path)
    finally:
        db.close()


def _build_content_export(db, client_id: str) -> str:
    """Build markdown content from approved drafts + geo outlines."""
    from sqlalchemy import or_

    parts = []
    approved = db.query(ContentDraft).filter(
        func.lower(ContentDraft.client_id) == client_id.lower(),
        ContentDraft.status == "approved",
    ).all()
    for d in approved:
        parts.append(f"## {d.topic} — {d.platform}\n\n")
        parts.append((d.body_refined or d.body or "") + "\n\n")

    q = db.query(GeoPageOutline).filter(
        or_(GeoPageOutline.client_id.is_(None), func.lower(GeoPageOutline.client_id) == client_id.lower())
    ).order_by(GeoPageOutline.confidence_score.desc().nullslast()).limit(50)
    for o in q.all():
        city = (o.city or "").title()
        service = (o.service or "").title()
        parts.append(f"## {service} in {city}\n\n")
        parts.append(f"**Title:** {o.page_title or '—'}\n")
        parts.append(f"**Meta:** {o.meta_description or '—'}\n")
        parts.append(f"**H1:** {o.h1 or '—'}\n\n")
        for s in (o.generated_sections or o.section_outline or []):
            if isinstance(s, dict):
                heading = s.get("heading") or s.get("title") or "—"
                body = s.get("body")
                parts.append(f"### {heading}\n")
                if body:
                    parts.append(f"{body}\n")
                else:
                    for b in (s.get("bullets") or s.get("content") or []):
                        parts.append(f"- {b}\n")
            else:
                parts.append(f"- {s}\n")
        parts.append("\n")
    return "".join(parts)


def _build_proposal_export(db, client_id: str) -> str:
    """Build markdown from latest sales proposal."""
    prop = (
        db.query(SalesProposal)
        .filter(SalesProposal.client_id == client_id)
        .order_by(SalesProposal.proposal_date.desc())
        .first()
    )
    if not prop or not prop.generated_document:
        return ""
    return prop.generated_document


def _write_pdf(markdown_content: str, output_path: str) -> None:
    """Convert markdown to PDF using reportlab."""
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer
        from reportlab.lib.units import inch
    except ImportError:
        raise RuntimeError("Install reportlab: pip install reportlab")

    def _safe_html(s: str) -> str:
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("**", "")

    doc = SimpleDocTemplate(output_path, pagesize=letter, topMargin=0.75 * inch, bottomMargin=0.75 * inch)
    styles = getSampleStyleSheet()
    story = []

    for block in markdown_content.split("\n\n"):
        block = block.strip()
        if not block:
            story.append(Spacer(1, 12))
            continue
        for line in block.split("\n"):
            line = line.strip()
            if not line:
                continue
            if line.startswith("### "):
                story.append(Paragraph(_safe_html(line[4:]), styles["Heading3"]))
            elif line.startswith("## "):
                story.append(Paragraph(_safe_html(line[3:]), styles["Heading2"]))
            elif line.startswith("# "):
                story.append(Paragraph(_safe_html(line[2:]), styles["Heading1"]))
            elif line.startswith("- "):
                story.append(Paragraph("• " + _safe_html(line[2:]), styles["Normal"]))
            else:
                story.append(Paragraph(_safe_html(line), styles["Normal"]))
        story.append(Spacer(1, 6))

    doc.build(story)
