from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import datetime
from pydantic import BaseModel
from src.core.engine import EmailSearchEngine
import logging

router = APIRouter()
search_engine = EmailSearchEngine()

class SearchResponse(BaseModel):
    total: int
    page: int
    page_size: int
    total_pages: int
    results: list

@router.get("/search", response_model=SearchResponse)
async def search_emails(
    email_id: Optional[str] = Query(None, description="Search by email ID"),
    subject: Optional[str] = Query(None, description="Search in email subject"),
    body: Optional[str] = Query(None, description="Search in email body"),
    from_addr: Optional[str] = Query(None, description="Filter by sender email"),
    to_addr: Optional[str] = Query(None, description="Filter by recipient email"),
    from_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    has_attachment: Optional[bool] = Query(None, description="Filter by attachment presence"),
    labels: Optional[str] = Query(None, description="Comma-separated list of labels"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(10, ge=1, le=100, description="Results per page")
):
    try:
        # Convert comma-separated labels to list
        label_list = labels.split(',') if labels else None
        
        results = search_engine.search(
            email_id=email_id,
            subject=subject,
            body=body,
            from_addr=from_addr,
            to_addr=to_addr,
            from_date=from_date,
            to_date=to_date,
            has_attachment=has_attachment,
            labels=label_list,
            page=page,
            page_size=page_size
        )
        return results
    except Exception as e:
        logging.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
