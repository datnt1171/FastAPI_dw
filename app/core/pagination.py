# app/core/pagination.py
from typing import Optional, List, Dict, Any, Generic, TypeVar
from urllib.parse import urlencode
from fastapi import Request
from pydantic import BaseModel

T = TypeVar('T')

class PaginatedResponse(BaseModel, Generic[T]):
    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[T]

class Paginator:
    def __init__(self, request: Request, offset: int = 0, limit: int = 50):
        self.request = request
        self.offset = offset
        self.limit = limit
        self.base_url = str(request.url).split('?')[0]
        self.query_params = dict(request.query_params)
    
    def paginate(self, results: List[Dict[str, Any]], total_count: int) -> Dict[str, Any]:
        """
        Create paginated response matching Django DRF format
        """
        # Calculate next and previous URLs
        next_url = self._get_next_url(total_count)
        previous_url = self._get_previous_url()
        
        return {
            "count": total_count,
            "next": next_url,
            "previous": previous_url,
            "results": results
        }
    
    def _get_next_url(self, total_count: int) -> Optional[str]:
        """Generate next page URL if there are more results"""
        if self.offset + self.limit >= total_count:
            return None
        
        params = self.query_params.copy()
        params['offset'] = self.offset + self.limit
        params['limit'] = self.limit
        
        return f"{self.base_url}?{urlencode(params)}"
    
    def _get_previous_url(self) -> Optional[str]:
        """Generate previous page URL if not on first page"""
        if self.offset <= 0:
            return None
        
        params = self.query_params.copy()
        previous_offset = max(0, self.offset - self.limit)
        
        if previous_offset == 0:
            # For first page, only include limit if it's not the default
            params.pop('offset', None)
            if self.limit != 50:  # Assuming 50 is your default
                params['limit'] = self.limit
        else:
            params['offset'] = previous_offset
            params['limit'] = self.limit
        
        return f"{self.base_url}?{urlencode(params)}"