"""
FastAPI application for Index Builder
Provides endpoints to construct and query equal-weighted index
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from datetime import datetime, timedelta
from typing import Optional, List
import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fast_app.models import (
    BuildIndexRequest,
    BuildIndexResponse,
    IndexPerformanceResponse,
    IndexCompositionResponse,
    CompositionChangesResponse,
    DailyPerformance,
    StockComposition,
    CompositionChange,
)
from fast_app.services import IndexBuilderService
from fast_app.config import settings

app = FastAPI(
    title="Index Builder API",
    description="API for constructing and querying equal-weighted stock indices",
    version="1.0.0",
    debug=settings.DEBUG
)

# Initialize service
index_service = IndexBuilderService(
    db_type=settings.DB_TYPE,
    db_path=settings.DB_PATH
)


@app.get("/")
def root():
    """Root endpoint with API information"""
    return {
        "name": "Index Builder API",
        "version": "1.0.0",
        "description": "Construct and query equal-weighted stock indices",
        "endpoints": [
            "POST /build-index",
            "GET /index-performance",
            "GET /index-composition",
            "GET /composition-changes",
            "GET /export-excel"
        ]
    }


@app.post("/build-index", response_model=BuildIndexResponse)
async def build_index(request: BuildIndexRequest):
    """
    Construct the equal-weighted index dynamically for the given date range

    - Select top N stocks daily by market cap
    - Assign equal weights
    - Persist compositions and performance
    """
    try:
        result = await index_service.build_index(
            start_date=request.start_date,
            end_date=request.end_date,
            top_n=request.top_n,
            initial_nav=request.initial_nav
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/index-performance", response_model=IndexPerformanceResponse)
async def get_index_performance(
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """
    Return daily returns and cumulative returns for the index
    Results are cached for performance
    """
    try:
        result = await index_service.get_performance(start_date, end_date)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/index-composition", response_model=IndexCompositionResponse)
async def get_index_composition(
    date: str = Query(..., description="Date (YYYY-MM-DD)")
):
    """
    Return the stock composition for a given date
    Results are cached for performance
    """
    try:
        result = await index_service.get_composition(date)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/composition-changes", response_model=CompositionChangesResponse)
async def get_composition_changes(
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """
    List days when composition changed, with stocks entered/exited
    Results are cached for performance
    """
    try:
        result = await index_service.get_composition_changes(start_date, end_date)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/export-excel")
async def export_to_excel(
    start_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """
    Export all index data to a well-formatted Excel file
    Includes performance, compositions, and composition changes
    """
    try:
        file_path = await index_service.export_to_excel(start_date, end_date)
        return FileResponse(
            path=file_path,
            filename=os.path.basename(file_path),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.delete("/clear-cache")
async def clear_cache():
    """Clear all cached data"""
    try:
        index_service.clear_cache()
        return {"message": "Cache cleared successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error clearing cache: {str(e)}")

@app.delete("/reset-database")
async def reset_database():
    """Reset the database by deleting all index-related data"""
    try:
        index_service.reset_database()
        return {"message": "Database reset successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error resetting database: {str(e)}")



if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=settings.DEBUG)

