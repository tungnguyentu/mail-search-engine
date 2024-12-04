from fastapi import APIRouter, Query, HTTPException, Request
from typing import Optional
from datetime import datetime
from pydantic import BaseModel
from src.config.settings import settings
from src.core.engine import EmailSearchEngine
import logging
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import os
from whoosh import index
from whoosh.reading import IndexReader
from itertools import islice

router = APIRouter()
search_engine = EmailSearchEngine()
templates = Jinja2Templates(directory="templates")

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

@router.get("/index-viewer", response_class=HTMLResponse)
async def view_index(request: Request):
    index_stats = get_index_stats()
    return templates.TemplateResponse(
        "index_viewer.html", 
        {"request": request, "stats": index_stats}
    )

def get_index_stats():
    ix = index.open_dir(settings.INDEX_DIR)
    reader = ix.reader()
    
    stats = {
        'doc_count': reader.doc_count(),
        'fields': list(reader.schema.names()),
        'latest_docs': get_latest_documents(reader, limit=10),
        'field_stats': get_field_stats(reader),
    }
    reader.close()
    return stats

def get_latest_documents(reader: IndexReader, limit: int = 10):
    return [dict(doc) for doc in islice(reader.all_stored_fields(), limit)]

def get_field_stats(reader: IndexReader):
    stats = {}
    for field in reader.schema.names():
        stats[field] = {
            'unique_terms': len(list(reader.lexicon(field))),
            'sample_terms': list(reader.lexicon(field))[:5]
        }
    return stats
