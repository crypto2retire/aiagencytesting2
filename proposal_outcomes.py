"""
Proposal Outcomes — log accepted/won/lost website gap proposals.
Correlates gap types → deal size, gap severity → close rate.
Results improve future proposals (price ranges, prioritization).
"""

import re
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session


def _parse_proposed_total(proposals: List[Dict[str, Any]]) -> tuple:
    """Parse proposed_total_low, proposed_total_high from proposals' suggested_price_range."""
    total_min, total_max = 0.0, 0.0
    for p in proposals:
        pr = p.get("suggested_price_range") or ""
        m = re.findall(r"[\d,]+", pr)
        if len(m) >= 2:
            total_min += float(m[0].replace(",", ""))
            total_max += float(m[1].replace(",", ""))
        elif len(m) == 1:
            v = float(m[0].replace(",", ""))
            total_min += v
            total_max += v
    return (total_min, total_max)


def log_proposal_outcome(
    db: Session,
    client_id: str,
    proposals: List[Dict[str, Any]],
    outcome: str,
    deal_size: Optional[float] = None,
) -> int:
    """
    Log a website gap proposal with outcome (accepted | won | lost).
    Extracts gap_types, gap_severities from proposals.
    deal_size required when outcome=won.
    Returns outcome id.
    """
    from database import WebsiteGapProposalOutcome

    gap_types = []
    gap_severities = {}
    for p in proposals:
        cat = p.get("category")
        sev = (p.get("severity") or "").strip()
        if cat:
            gap_types.append(cat)
            if sev:
                gap_severities[cat] = sev.lower()
    total_low, total_high = _parse_proposed_total(proposals)

    out = (outcome or "").strip().lower()
    if out not in ("accepted", "won", "lost", "requested_changes"):
        out = "accepted"

    row = WebsiteGapProposalOutcome(
        client_id=client_id,
        gap_types=gap_types,
        gap_severities=gap_severities,
        proposed_total_low=total_low if total_low > 0 else None,
        proposed_total_high=total_high if total_high > 0 else None,
        outcome=out,
        deal_size=float(deal_size) if out == "won" and deal_size is not None else None,
    )
    db.add(row)
    db.flush()
    db.commit()
    return row.id


def record_outcome(
    db: Session,
    outcome_id: int,
    outcome: str,
    deal_size: Optional[float] = None,
) -> bool:
    """
    Record outcome for a proposal: accepted | won | lost.
    deal_size required when outcome=won.
    """
    from database import WebsiteGapProposalOutcome

    row = db.query(WebsiteGapProposalOutcome).filter(WebsiteGapProposalOutcome.id == outcome_id).first()
    if not row:
        return False
    out = (outcome or "").strip().lower()
    if out not in ("accepted", "won", "lost", "requested_changes"):
        return False
    row.outcome = out
    if out == "won" and deal_size is not None:
        row.deal_size = float(deal_size)
    elif out == "won" and deal_size is None:
        row.deal_size = None  # allow later update
    db.commit()
    return True


def get_gap_type_deal_stats(db: Session) -> Dict[str, Dict[str, float]]:
    """
    Gap type → deal size correlation.
    Returns {gap_type: {avg_deal_size: float, count: int, total_deal_size: float}}
    Only from outcome='won' with deal_size.
    """
    from database import WebsiteGapProposalOutcome

    rows = (
        db.query(WebsiteGapProposalOutcome)
        .filter(
            WebsiteGapProposalOutcome.outcome == "won",
            WebsiteGapProposalOutcome.deal_size.isnot(None),
            WebsiteGapProposalOutcome.deal_size > 0,
        )
        .all()
    )
    # Aggregate by gap_type: each outcome contributes deal_size / len(gap_types) to each gap type
    agg: Dict[str, List[float]] = {}
    for r in rows:
        ds = float(r.deal_size or 0)
        gts = r.gap_types or []
        if not gts or ds <= 0:
            continue
        share = ds / len(gts)
        for gt in gts:
            agg.setdefault(gt, []).append(share)
    result = {}
    for gt, values in agg.items():
        n = len(values)
        total = sum(values)
        result[gt] = {"avg_deal_size": total / n if n else 0, "count": n, "total_deal_size": total}
    return result


def get_severity_close_rates(db: Session) -> Dict[str, Dict[str, Any]]:
    """
    Gap severity → close rate.
    Returns {severity: {close_rate: float, won_count: int, total_count: int}}
    """
    from database import WebsiteGapProposalOutcome

    rows = (
        db.query(WebsiteGapProposalOutcome)
        .filter(WebsiteGapProposalOutcome.outcome.in_(["won", "lost", "accepted"]))
        .all()
    )
    # For each outcome, each (gap_type, severity) pair contributes to that severity's counts
    won_by_sev: Dict[str, int] = {}
    total_by_sev: Dict[str, int] = {}
    for r in rows:
        sevs = r.gap_severities or {}
        is_won = (r.outcome or "").lower() == "won"
        for gt, sev in sevs.items():
            s = (sev or "").lower()
            if not s:
                continue
            total_by_sev[s] = total_by_sev.get(s, 0) + 1
            if is_won:
                won_by_sev[s] = won_by_sev.get(s, 0) + 1
    result = {}
    for sev, total in total_by_sev.items():
        won = won_by_sev.get(sev, 0)
        result[sev] = {
            "close_rate": won / total if total else 0,
            "won_count": won,
            "total_count": total,
        }
    return result


def get_learned_price_adjustments(db: Session) -> Dict[str, tuple]:
    """
    Learned price ranges per gap type from won deals.
    Returns {gap_type: (suggested_low, suggested_high)} for use in ProposalMapper.
    Uses 25th–75th percentile of deal share per gap type if enough data.
    """
    stats = get_gap_type_deal_stats(db)
    result = {}
    for gt, s in stats.items():
        if s["count"] < 2:
            continue
        avg = s["avg_deal_size"]
        # Use avg ± 30% as suggested range (simplified; could use percentiles with more data)
        low = max(0, int(avg * 0.7))
        high = int(avg * 1.3)
        result[gt] = (low, high)
    return result
