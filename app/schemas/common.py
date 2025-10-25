from pydantic import BaseModel, Field, ConfigDict, field_validator
from typing import Optional, List, Generic, TypeVar, Any, Dict
from datetime import datetime, date
from decimal import Decimal
from dateutil.relativedelta import relativedelta

T = TypeVar('T')

class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response for all list endpoints"""
    count: int = Field(..., example=123, description="Total number of items")
    next: Optional[str] = Field(
        None, 
        example="http://api.example.org/factories/?offset=50&limit=50"
    )
    previous: Optional[str] = Field(
        None, 
        example="http://api.example.org/factories/?offset=0&limit=50"
    )
    results: List[T] = Field(..., description="Array of items")

class ResponseMessage(BaseModel):
    """Standard message response"""
    message: str = Field(..., example="Operation completed successfully")
    success: bool = Field(default=True)
    data: Optional[Dict[str, Any]] = None

# Base types for common SQL result patterns
class BaseRecord(BaseModel):
    """Base for SQL query results"""
    model_config = ConfigDict(
        from_attributes=True,
        arbitrary_types_allowed=True,
        json_encoders={
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
            Decimal: lambda v: float(v)
        }
    )


class DateRangeParams(BaseModel):
    date__gte: date = date.today().replace(day=1)  # First day of this month
    date__lte: date = date.today()  # Today
    
    @field_validator('date__lte')
    def validate_date_range(cls, v, info):
        if 'date__gte' in info.data and v < info.data['date__gte']:
            raise ValueError('End date must be after or equal to start date')
        return v


class DateRangeTargetParams(BaseModel):
    date_target__gte: date = (date.today().replace(day=1) - relativedelta(months=1))  # First day of Today - 1 month
    date_target__lte: date = (date.today() - relativedelta(months=1))  # Today - 1 month

    @field_validator('date_target__lte')
    def validate_target_date_range(cls, v, info):
        if 'date_target__gte' in info.data and v < info.data['date_target__gte']:
            raise ValueError('Target end date must be after or equal to target start date')
        return v


TIME_GROUP_BY_MAPPING = {
    "year": "dd.year",
    "quarter": "dd.quarter",
    "month": "dd.month",
    "week_of_year": "dd.week_of_year",
    "day_of_week": "dd.day_of_week",
    "date": "dd.date",
    "day": "dd.day",
}