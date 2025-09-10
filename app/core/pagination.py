# app/core/pagination.py
from typing import Optional, List, Dict, Any, Generic, TypeVar
from urllib.parse import urlencode
from fastapi import Request
from pydantic import BaseModel
from math import ceil

T = TypeVar('T')

class PaginatedResponse(BaseModel, Generic[T]):
    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[T]

class Paginator:
    def __init__(self, request: Request, page: int = 1, page_size: int = 50):
        self.request = request
        self.page = page
        self.page_size = page_size
        self.base_url = str(request.url).split('?')[0]
        self.query_params = dict(request.query_params)
    
    @property
    def offset(self) -> int:
        """Convert page-based pagination to offset"""
        return (self.page - 1) * self.page_size
    
    @property
    def limit(self) -> int:
        """Return the page size as limit"""
        return self.page_size
    
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
        total_pages = ceil(total_count / self.page_size)
        
        if self.page >= total_pages:
            return None
        
        params = self.query_params.copy()
        params['page'] = self.page + 1
        params['page_size'] = self.page_size
        
        return f"{self.base_url}?{urlencode(params)}"
    
    def _get_previous_url(self) -> Optional[str]:
        """Generate previous page URL if not on first page"""
        if self.page <= 1:
            return None
        
        params = self.query_params.copy()
        previous_page = self.page - 1
        
        if previous_page == 1:
            # For first page, remove page parameter but keep page_size if not default
            params.pop('page', None)
            if self.page_size != 50:  # Assuming 50 is your default
                params['page_size'] = self.page_size
        else:
            params['page'] = previous_page
            params['page_size'] = self.page_size
        
        return f"{self.base_url}?{urlencode(params)}"