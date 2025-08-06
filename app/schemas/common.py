from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Generic, TypeVar, Any, Dict
from datetime import datetime, date
from decimal import Decimal

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