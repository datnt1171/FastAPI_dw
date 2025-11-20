from fastapi import APIRouter, Depends, HTTPException, Request, Query
import asyncio
import logging
from app.core.auth import has_permission
from app.core.database import execute_query
from app.core.pagination import Paginator
from app.schemas.products import (PaginatedFormularList, Formular,)
from app.schemas.schema_helpers import validate_sql_results

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/crm/formulars", tags=["formulars"])


@router.get("", response_model=PaginatedFormularList)
async def get_formulars(
    request: Request,
    product_name: str | None = None,
    material_name: str | None = None,
    is_current: bool = True,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1),
    permitted = Depends(has_permission())
) -> PaginatedFormularList:

    try:
        # Create paginator to get offset and limit
        paginator = Paginator(request, page, page_size)
        limit = paginator.limit
        offset = paginator.offset

        # Split comma-separated types into array for PostgreSQL ANY clause
        product_name_array = product_name.split(',') if product_name else None
        material_name_array = material_name.split(',') if material_name else None

        formulars_query = """
        SELECT
            product_name,
            material_name,
            ratio,
            version_number,
            effective_date,
            end_date,
            is_current
        FROM bridge_product_material
        WHERE ($1::text[] IS NULL OR product_name = ANY($1))
        AND ($2::text[] IS NULL OR material_name = ANY($2))
        AND ($3::bool IS NULL OR is_current = $3)
        LIMIT $4 OFFSET $5
        """
        
        formulars_task = execute_query(
            query=formulars_query,
            params=(
                product_name_array, 
                material_name_array,
                is_current,
                limit, 
                offset
            ),
            fetch_all=True
        )
        
        count_query = """
        SELECT COUNT(*)                     
        FROM bridge_product_material
        WHERE ($1::text[] IS NULL OR product_name = ANY($1))
        AND ($2::text[] IS NULL OR material_name = ANY($2))
        AND ($3::bool IS NULL OR is_current = $3)
        """

        count_task = execute_query(
            query=count_query,
            params=(
                product_name_array, 
                material_name_array,
                is_current,
            ),
            fetch_one=True
        )

        formulars_data, count_result = await asyncio.gather(formulars_task, count_task)
        
        # Validate data against schema
        formulars = validate_sql_results(formulars_data or [], Formular)
        total_count = count_result.get('count', 0) if count_result else 0
        
        paginated_response = paginator.paginate(
            [item.model_dump() for item in formulars], 
            total_count
        )
        
        return PaginatedFormularList(
            count=paginated_response['count'],
            next=paginated_response.get('next'),
            previous=paginated_response.get('previous'),
            results=formulars
        )
        
    except Exception as e:
        logger.error(f"Error retrieving formulars: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve formulars"
        )
