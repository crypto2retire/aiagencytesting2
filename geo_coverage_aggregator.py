"""
Geo Coverage Aggregator — aggregates competitor_geo_coverage into City × Service density and avg quality.

- competitor_count: # competitors with page_exists for (city, state, service)
- avg_quality_score: average page_quality_score of those pages
"""

from typing import Optional

from sqlalchemy import case, cast, func, Integer

from database import CompetitorGeoCoverage, GeoCoverageDensity, SessionLocal


def aggregate_competitor_geo_coverage(db=None) -> int:
    """
    Aggregate competitor_geo_coverage into geo_coverage_density.

    For each (city, state, service):
    - competitor_count = count of competitors with page_exists=True
    - avg_quality_score = avg(page_quality_score) where page_exists=True and score is not null

    Returns count of rows upserted.
    """
    sess = db or SessionLocal()
    try:
        agg_q = sess.query(
            CompetitorGeoCoverage.city,
            CompetitorGeoCoverage.state,
            CompetitorGeoCoverage.service,
            func.sum(cast(CompetitorGeoCoverage.page_exists, Integer)).label("competitor_count"),
            func.avg(
                case(
                    (CompetitorGeoCoverage.page_exists == True, CompetitorGeoCoverage.page_quality_score),
                    else_=None,
                )
            ).label("avg_quality"),
        ).filter(
            CompetitorGeoCoverage.city.isnot(None),
            CompetitorGeoCoverage.city != "",
            CompetitorGeoCoverage.service.isnot(None),
            CompetitorGeoCoverage.service != "",
        ).group_by(
            CompetitorGeoCoverage.city,
            CompetitorGeoCoverage.state,
            CompetitorGeoCoverage.service,
        )

        rows = agg_q.all()
        count = 0
        for city, state, service, comp_count, avg_qual in rows:
            if not city or not service:
                continue
            comp_count = int(comp_count or 0)
            avg_qual = float(avg_qual) if avg_qual is not None else None

            state_val = (state or "").strip() or ""
            existing = sess.query(GeoCoverageDensity).filter(
                GeoCoverageDensity.city == city,
                GeoCoverageDensity.service == service,
                func.coalesce(GeoCoverageDensity.state, "") == state_val,
            ).first()

            if existing:
                existing.competitor_count = comp_count
                existing.avg_quality_score = avg_qual
            else:
                sess.add(GeoCoverageDensity(
                    city=city,
                    state=state or None,
                    service=service,
                    competitor_count=comp_count,
                    avg_quality_score=avg_qual,
                ))
            count += 1

        if db is None:
            sess.commit()
        return count
    except Exception:
        if db is None:
            sess.rollback()
        raise
    finally:
        if db is None:
            sess.close()


def get_geo_coverage_density(
    city: Optional[str] = None,
    service: Optional[str] = None,
    db=None,
) -> list[dict]:
    """
    Get aggregated City × Service coverage density.
    Optional filters: city, service.
    Returns list of {city, state, service, competitor_count, avg_quality_score}.
    """
    sess = db or SessionLocal()
    try:
        q = sess.query(GeoCoverageDensity)
        if city:
            q = q.filter(GeoCoverageDensity.city.ilike(f"%{city}%"))
        if service:
            q = q.filter(GeoCoverageDensity.service.ilike(f"%{service}%"))
        rows = q.order_by(
            GeoCoverageDensity.city,
            GeoCoverageDensity.service,
        ).all()

        return [
            {
                "city": r.city,
                "state": r.state,
                "service": r.service,
                "competitor_count": r.competitor_count or 0,
                "avg_quality_score": float(r.avg_quality_score) if r.avg_quality_score is not None else None,
            }
            for r in rows
        ]
    finally:
        if db is None:
            sess.close()
