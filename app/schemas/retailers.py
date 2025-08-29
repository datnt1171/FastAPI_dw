from uuid import UUID
from pydantic import Field
from .common import BaseRecord, PaginatedResponse

class Retailer(BaseRecord):
    """Retailer item for list view"""
    id: UUID = Field(..., description="Retailer UUID")
    name: str = Field(..., description="Retailer name")

class RetailerDetail(BaseRecord):
    """Detailed retailer view - for single retailer endpoint"""
    id: UUID = Field(..., description="Retailer UUID")
    name: str = Field(..., description="Retailer name")

class RetailerCreate(BaseRecord):
    """Schema for creating a new retailer"""
    name: str = Field(..., description="Retailer name", min_length=1, max_length=255)

class RetailerUpdate(BaseRecord):
    """Schema for updating a retailer"""
    name: str = Field(..., description="Retailer name", min_length=1, max_length=255)

# Paginated responses
PaginatedRetailerList = PaginatedResponse[Retailer]