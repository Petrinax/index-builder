"""
Pydantic models for request/response validation
"""

from pydantic import BaseModel, Field, validator
from typing import Optional, List
from datetime import datetime, date


class BuildIndexRequest(BaseModel):
    """Request model for building an index"""
    start_date: str = Field(..., description="Start date in YYYY-MM-DD format")
    end_date: Optional[str] = Field(None, description="End date in YYYY-MM-DD format")
    top_n: int = Field(100, description="Number of top stocks to include", ge=1, le=500)
    initial_nav: float = Field(1000.0, description="Initial NAV value", gt=0)

    @validator('start_date', 'end_date')
    def validate_date_format(cls, v):
        if v is None:
            return v
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError('Date must be in YYYY-MM-DD format')

    @validator('end_date')
    def validate_end_after_start(cls, v, values):
        if v and 'start_date' in values:
            start = datetime.strptime(values['start_date'], '%Y-%m-%d')
            end = datetime.strptime(v, '%Y-%m-%d')
            if end < start:
                raise ValueError('end_date must be after start_date')
        return v


class StockComposition(BaseModel):
    """Model for a single stock in the index composition"""
    symbol: str
    exchange: str
    market_cap: float
    price: float
    shares: float
    weight: float
    notional_value: float


class DailyPerformance(BaseModel):
    """Model for daily performance metrics"""
    date: str
    nav: float
    daily_return: Optional[float] = None
    cumulative_return: float


class BuildIndexResponse(BaseModel):
    """Response model for build-index endpoint"""
    message: str
    start_date: str
    end_date: str
    top_n: int
    initial_nav: float
    final_nav: float
    total_return: float
    days_processed: int


class IndexPerformanceResponse(BaseModel):
    """Response model for index performance"""
    start_date: str
    end_date: str
    performance: List[DailyPerformance]
    summary: dict


class IndexCompositionResponse(BaseModel):
    """Response model for index composition"""
    date: str
    composition: List[StockComposition]
    total_stocks: int
    total_market_cap: float


class CompositionChange(BaseModel):
    """Model for composition changes on a specific date"""
    date: str
    stocks_added: List[dict]
    stocks_removed: List[dict]
    num_added: int
    num_removed: int


class CompositionChangesResponse(BaseModel):
    """Response model for composition changes"""
    start_date: str
    end_date: str
    changes: List[CompositionChange]
    total_change_days: int

