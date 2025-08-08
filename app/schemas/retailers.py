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

# Paginated responses
PaginatedRetailerList = PaginatedResponse[Retailer]