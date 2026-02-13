"""
Agency AI Pipeline — simple, readable orchestration.
Take client_id → run agents in order → stop if something fails.
Replaces n8n for v1.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

from sqlalchemy import func

from database import Client, ResearchLog, SessionLocal, init_db


def _resolve_client_id(client_id: str) -> Optional[str]:
    """Resolve client_id (case-insensitive) to actual stored value."""
    db = SessionLocal()
    try:
        client = db.query(Client).filter(func.lower(Client.client_id) == client_id.lower()).first()
        return client.client_id if client else None
    finally:
        db.close()


def run_researcher(client_id: str, city: str = None) -> tuple[bool, str]:
    """Run Researcher only. Returns (success, message)."""
    from config import _validate_required
    _validate_required()
    try:
        resolved = _resolve_client_id(client_id)
        if not resolved:
            return False, f"Client '{client_id}' not found. Check client ID (case-insensitive)."
        from agents.researcher import gather_intelligence
        run_id = gather_intelligence(client_id=resolved, city=city)
        if not run_id:
            return False, "Researcher returned no run_id (client not found?)."
        db = SessionLocal()
        try:
            count = db.query(ResearchLog).filter(ResearchLog.client_id == resolved).count()
        finally:
            db.close()
        if count == 0:
            return False, "No research saved. Check Tavily API key, ensure client has cities_served, and see logs/researcher.log"
        return True, f"Research complete ({count} entries). Review logs, then run Strategist."
    except Exception as e:
        return False, str(e)


def run_keyword_classifier(region: str, client_id: str = None) -> tuple[bool, str]:
    """Run LLM keyword classification for a region. Optional — once per batch/city/week."""
    from agents.keyword_classifier import run_classifier
    return run_classifier(region, client_id)


def run_strategist(client_id: str) -> tuple[bool, str]:
    """Run Strategist only (reads research_logs). Returns (success, message)."""
    from config import _validate_required
    _validate_required()
    from sqlalchemy import func
    from config import MIN_CONFIDENCE_FOR_STRATEGIST

    resolved = _resolve_client_id(client_id)
    if not resolved:
        return False, f"Client '{client_id}' not found. Check client ID (case-insensitive)."
    db = SessionLocal()
    try:
        count = db.query(ResearchLog).filter(ResearchLog.client_id == resolved).count()
        if count == 0:
            return False, "No research logs. Run Researcher first for this client."

        # Check how many pass confidence threshold
        qualified = db.query(ResearchLog).filter(
            ResearchLog.client_id == resolved,
            func.coalesce(ResearchLog.confidence_score, 0) >= MIN_CONFIDENCE_FOR_STRATEGIST,
        ).count()
        if qualified == 0:
            return False, f"No research logs meet confidence threshold ({MIN_CONFIDENCE_FOR_STRATEGIST}+). Lower MIN_CONFIDENCE_FOR_STRATEGIST in .env or re-run Research."
    finally:
        db.close()
    try:
        from agents.strategist import generate_strategy
        result = generate_strategy(client_id=resolved)
        if not result:
            return False, "Strategist produced no output. Check logs/strategist.log."
        actions = result.get("action_count", 0)
        pages = result.get("page_count", 0)
        upsells = result.get("upsell_count", 0)
        return True, f"Strategist done: {actions} actions, {pages} pages, {upsells} upsell flags."
    except Exception as e:
        return False, str(e)


def run_pipeline(client_id: str, city: str = None) -> tuple[bool, str]:
    """Run Researcher → Strategist (skips review step). For CLI convenience."""
    ok, msg = run_researcher(client_id, city)
    if not ok:
        return False, msg
    return run_strategist(client_id)


def main():
    parser = argparse.ArgumentParser(
        description="Agency AI pipeline — run Researcher → Strategist for a client",
    )
    parser.add_argument("client_id", type=str, nargs="?", help="Client ID to process")
    parser.add_argument("--city", type=str, help="City for Researcher (optional if client has cities_served)")
    parser.add_argument("--init-db", action="store_true", help="Initialize database tables")
    parser.add_argument("--researcher-only", action="store_true", help="Run only Researcher")
    parser.add_argument("--strategist-only", action="store_true", help="Run only Strategist")
    args = parser.parse_args()

    if args.init_db:
        init_db()
        print("Database initialized.")
        print("Next: streamlit run app.py for onboarding & review")
        return 0

    if args.researcher_only:
        if not args.client_id:
            print("Error: client_id required")
            return 1
        resolved = _resolve_client_id(args.client_id)
        if not resolved:
            print(f"Error: Client '{args.client_id}' not found")
            return 1
        from agents.researcher import gather_intelligence
        run_id = gather_intelligence(client_id=resolved, city=args.city)
        print(f"Researcher done. run_id={run_id}")
        return 0 if run_id else 1

    if args.strategist_only:
        if not args.client_id:
            print("Error: client_id required")
            return 1
        resolved = _resolve_client_id(args.client_id)
        if not resolved:
            print(f"Error: Client '{args.client_id}' not found")
            return 1
        from agents.strategist import generate_strategy
        result = generate_strategy(client_id=resolved)
        print(f"Strategist done. {result}")
        return 0 if result else 1

    # Full pipeline
    if not args.client_id:
        parser.print_help()
        print("\nExample: python main.py junk-away-phoenix --city 'Oshkosh WI'")
        return 1

    success, _ = run_pipeline(client_id=args.client_id, city=args.city)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
