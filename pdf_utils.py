"""
PDF export utilities — CRUD for pdf_exports table.
Safe to call from Streamlit: each function manages its own DB session.
"""

from typing import Any, Dict, List, Optional

from sqlalchemy import func

from database import PdfExport, SessionLocal, engine
from sqlalchemy.orm import Session


def _ensure_tables():
    """Ensure pdf_exports table exists."""
    from database import Base
    Base.metadata.create_all(bind=engine)


def get_exports(client_id: str) -> List[Dict[str, Any]]:
    """
    Return list of PDF exports for the client, newest first.
    Each item: {id, client_id, export_type, pdf_file_path, status, created_at}
    """
    _ensure_tables()
    db: Optional[Session] = None
    try:
        db = SessionLocal()
        rows = (
            db.query(PdfExport)
            .filter(func.lower(PdfExport.client_id) == str(client_id).lower())
            .order_by(PdfExport.created_at.desc())
            .all()
        )
        return [
            {
                "id": r.id,
                "client_id": r.client_id,
                "export_type": r.export_type,
                "pdf_file_path": r.pdf_file_path,
                "status": r.status,
                "created_at": r.created_at,
            }
            for r in rows
        ]
    finally:
        if db:
            db.close()


def create_export(client_id: str, export_type: str) -> Dict[str, Any]:
    """
    Create a new PdfExport row. Status defaults to READY (caller may update after PDF generation).
    Returns the created record: {id, client_id, export_type, pdf_file_path, status, created_at}
    """
    _ensure_tables()
    db: Optional[Session] = None
    try:
        db = SessionLocal()
        record = PdfExport(
            client_id=client_id,
            export_type=export_type.upper() if export_type else "CONTENT",
            status="READY",
        )
        db.add(record)
        db.commit()
        db.refresh(record)
        return {
            "id": record.id,
            "client_id": record.client_id,
            "export_type": record.export_type,
            "pdf_file_path": record.pdf_file_path,
            "status": record.status,
            "created_at": record.created_at,
        }
    finally:
        if db:
            db.close()


def update_export_status(
    export_id: int,
    status: str,
    file_path: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Update an export's status and optionally pdf_file_path.
    Returns updated record dict or None if not found.
    """
    _ensure_tables()
    db: Optional[Session] = None
    try:
        db = SessionLocal()
        record = db.query(PdfExport).filter(PdfExport.id == export_id).first()
        if not record:
            return None
        record.status = status
        if file_path is not None:
            record.pdf_file_path = file_path
        db.commit()
        db.refresh(record)
        return {
            "id": record.id,
            "client_id": record.client_id,
            "export_type": record.export_type,
            "pdf_file_path": record.pdf_file_path,
            "status": record.status,
            "created_at": record.created_at,
        }
    finally:
        if db:
            db.close()
