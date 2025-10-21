"""
FastAPI application for Index Builder
Provides endpoints to construct and query equal-weighted index
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from typing import Optional
import os
import sys
import time

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fast_app.models import (
    BuildIndexRequest,
    BuildIndexResponse,
    IndexPerformanceResponse,
    IndexCompositionResponse,
    CompositionChangesResponse,
)
from fast_app.services import IndexBuilderService
from fast_app.config import settings
from data_pipeline.base_logging import Logger

# Setup logger using base_logging module
logger = Logger("fast_app.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown events"""
    # Startup
    logger.info("=" * 80)
    logger.info("Index Builder API starting up...")
    logger.info(f"Debug mode: {settings.DEBUG}")
    logger.info(f"Database: {settings.DB_TYPE} at {settings.DB_PATH}")
    logger.info(f"Cache enabled: {settings.CACHE_ENABLED}")
    logger.info(f"Export directory: {settings.EXPORT_DIR}")
    logger.info("=" * 80)

    yield

    # Shutdown
    logger.info("Index Builder API shutting down...")


app = FastAPI(
    title="Index Builder API",
    description="API for constructing and querying equal-weighted stock indices",
    version="1.0.0",
    debug=settings.DEBUG,
    lifespan=lifespan
)

# Initialize service
logger.info("Initializing IndexBuilderService...")
index_service = IndexBuilderService(
    db_type=settings.DB_TYPE,
    db_path=settings.DB_PATH
)
logger.info(f"IndexBuilderService initialized with DB_TYPE={settings.DB_TYPE}, DB_PATH={settings.DB_PATH}")


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all HTTP requests and responses"""
    start_time = time.time()

    logger.info(f"Incoming request: {request.method} {request.url.path}")

    try:
        response = await call_next(request)
        process_time = time.time() - start_time

        logger.info(
            f"Completed request: {request.method} {request.url.path} - "
            f"Status: {response.status_code} - Duration: {process_time:.3f}s"
        )

        response.headers["X-Process-Time"] = str(process_time)
        return response
    except Exception as e:
        process_time = time.time() - start_time
        logger.error(
            f"Request failed: {request.method} {request.url.path} - "
            f"Error: {str(e)} - Duration: {process_time:.3f}s"
        )
        raise


@app.get("/")
def root():
    """Root endpoint with API information"""
    logger.debug("Root endpoint accessed")
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
    logger.info(
        f"Building index - Start: {request.start_date}, End: {request.end_date}, "
        f"Top N: {request.top_n}, Initial NAV: {request.initial_nav}"
    )

    try:
        result = await index_service.build_index(
            start_date=request.start_date,
            end_date=request.end_date,
            top_n=request.top_n,
            initial_nav=request.initial_nav
        )
        logger.info(
            f"Index built successfully - Days processed: {result.days_processed}, "
            f"Final NAV: {result.final_nav:.2f}, Total return: {result.total_return:.2f}%"
        )
        return result
    except ValueError as e:
        logger.warning(f"Invalid request for build_index: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error building index: {str(e)}")
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
    logger.info(f"Fetching index performance - Start: {start_date}, End: {end_date}")

    try:
        result = await index_service.get_performance(start_date, end_date)
        logger.info(f"Performance data retrieved - {result.summary['total_days']} days")
        return result
    except ValueError as e:
        logger.warning(f"Invalid request for get_index_performance: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error fetching performance: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/index-composition", response_model=IndexCompositionResponse)
async def get_index_composition(
    date: str = Query(..., description="Date (YYYY-MM-DD)")
):
    """
    Return the stock composition for a given date
    Results are cached for performance
    """
    logger.info(f"Fetching index composition for date: {date}")

    try:
        result = await index_service.get_composition(date)
        logger.info(f"Composition data retrieved - {result.total_stocks} stocks")
        return result
    except ValueError as e:
        logger.warning(f"Invalid request for get_index_composition: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error fetching composition: {str(e)}")
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
    logger.info(f"Fetching composition changes - Start: {start_date}, End: {end_date}")

    try:
        result = await index_service.get_composition_changes(start_date, end_date)
        logger.info(f"Composition changes retrieved - {result.total_change_days} days with changes")
        return result
    except ValueError as e:
        logger.warning(f"Invalid request for get_composition_changes: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error fetching composition changes: {str(e)}")
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
    logger.info(f"Exporting to Excel - Start: {start_date}, End: {end_date}")

    try:
        file_path = await index_service.export_to_excel(start_date, end_date)
        logger.info(f"Excel file created successfully: {file_path}")
        return FileResponse(
            path=file_path,
            filename=os.path.basename(file_path),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except ValueError as e:
        logger.warning(f"Invalid request for export_to_excel: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error exporting to Excel: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.delete("/clear-cache")
async def clear_cache():
    """Clear all cached data"""
    logger.info("Clearing cache...")

    try:
        index_service.clear_cache()
        logger.info("Cache cleared successfully")
        return {"message": "Cache cleared successfully"}
    except Exception as e:
        logger.error(f"Error clearing cache: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error clearing cache: {str(e)}")

@app.delete("/reset-database")
async def reset_database():
    """Reset the database by deleting all index-related data"""
    logger.warning("Resetting database - all index data will be deleted")

    try:
        index_service.reset_database()
        logger.info("Database reset successfully")
        return {"message": "Database reset successfully"}
    except Exception as e:
        logger.error(f"Error resetting database: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error resetting database: {str(e)}")





if __name__ == "__main__":
    import uvicorn
    logger.info("Starting uvicorn server...")
    uvicorn.run("main:app", host="0.0.0.0", port=8001, reload=settings.DEBUG)
