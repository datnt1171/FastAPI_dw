from pydantic import Field
from .common import BaseRecord, PaginatedResponse
from typing import Optional

class Factory(BaseRecord):
    """Factory item for list view - matches your SQL SELECT"""
    factory_code: str = Field(..., description="Factory code")
    factory_name: str = Field(..., description="Factory name")
    is_active: bool = Field(..., description="Active status")
    has_onsite: bool = Field(..., description="Has onsite facilities")

class FactoryDetail(BaseRecord):
    """Detailed factory view - for single factory endpoint"""
    factory_code: str = Field(..., description="Factory code")
    factory_name: str = Field(..., description="Factory name")
    is_active: bool = Field(..., description="Active status")
    has_onsite: bool = Field(..., description="Has onsite facilities")

# Paginated responses
PaginatedFactoryList = PaginatedResponse[Factory]

class FactoryUpdate(BaseRecord):
    is_active: Optional[bool] = Field(None, description="Active status")
    has_onsite: Optional[bool] = Field(None, description="Has onsite facilities")