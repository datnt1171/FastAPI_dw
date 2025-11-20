from fastapi import APIRouter, Depends, HTTPException, Request, Query
import asyncio
import logging
from app.core.auth import has_permission
from app.core.database import execute_query
from app.core.pagination import Paginator
from app.schemas.products import (PaginatedProductList, Product,)
from app.schemas.schema_helpers import validate_sql_results

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/crm/products", tags=["products"])


@router.get("", response_model=PaginatedProductList)
async def get_products(
    request: Request,
    product_type: str | None = None,
    search: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1),
    permitted = Depends(has_permission())
) -> PaginatedProductList:

    try:
        # Create paginator to get offset and limit
        paginator = Paginator(request, page, page_size)
        limit = paginator.limit
        offset = paginator.offset

        # Split comma-separated types into array for PostgreSQL ANY clause
        product_type_array = product_type.split(',') if product_type else None

        products_query = """
        SELECT
            id,
            product_name,
            product_type,
            qc
        FROM dim_product
        WHERE ($1::text[] IS NULL OR product_type = ANY($1))
        AND ($2::text IS NULL OR product_name ILIKE '%' || $2 || '%')
        LIMIT $3 OFFSET $4
        """
        
        products_task = execute_query(
            query=products_query,
            params=(product_type_array, search, limit, offset),
            fetch_all=True
        )
        
        count_query = """
        SELECT COUNT(*)                     
        FROM dim_product
        WHERE ($1::text[] IS NULL OR product_type = ANY($1))
        AND ($2::text IS NULL OR product_name ILIKE '%' || $2 || '%')
        """

        count_task = execute_query(
            query=count_query,
            params=(product_type_array, search),
            fetch_one=True
        )

        products_data, count_result = await asyncio.gather(products_task, count_task)
        
        # Validate data against schema
        products = validate_sql_results(products_data or [], Product)
        total_count = count_result.get('count', 0) if count_result else 0
        
        paginated_response = paginator.paginate(
            [item.model_dump() for item in products], 
            total_count
        )
        
        return PaginatedProductList(
            count=paginated_response['count'],
            next=paginated_response.get('next'),
            previous=paginated_response.get('previous'),
            results=products
        )
        
    except Exception as e:
        logger.error(f"Error retrieving products: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve products"
        )