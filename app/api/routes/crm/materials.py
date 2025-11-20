from fastapi import APIRouter, Depends, HTTPException, Request, Query
import asyncio
import logging
from app.core.auth import has_permission
from app.core.database import execute_query
from app.core.pagination import Paginator
from app.schemas.products import (PaginatedMaterialList, Material,)
from app.schemas.schema_helpers import validate_sql_results

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/crm/materials", tags=["materials"])


@router.get("", response_model=PaginatedMaterialList)
async def get_materials(
    request: Request,
    search: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1),
    permitted = Depends(has_permission())
) -> PaginatedMaterialList:

    try:
        # Create paginator to get offset and limit
        paginator = Paginator(request, page, page_size)
        limit = paginator.limit
        offset = paginator.offset


        materials_query = """
        SELECT
            id,
            name,
            qc,
            unit
        FROM dim_material
        WHERE ($1::text IS NULL OR name ILIKE '%' || $1 || '%')
        LIMIT $2 OFFSET $3
        """
        
        materials_task = execute_query(
            query=materials_query,
            params=(search, limit, offset),
            fetch_all=True
        )
        
        count_query = """
        SELECT COUNT(*)                     
        FROM dim_material
        WHERE ($1::text IS NULL OR name ILIKE '%' || $1 || '%')
        """

        count_task = execute_query(
            query=count_query,
            params=(search,),
            fetch_one=True
        )

        materials_data, count_result = await asyncio.gather(materials_task, count_task)
        
        # Validate data against schema
        materials = validate_sql_results(materials_data or [], Material)
        total_count = count_result.get('count', 0) if count_result else 0
        
        paginated_response = paginator.paginate(
            [item.model_dump() for item in materials], 
            total_count
        )
        
        return PaginatedMaterialList(
            count=paginated_response['count'],
            next=paginated_response.get('next'),
            previous=paginated_response.get('previous'),
            results=materials
        )
        
    except Exception as e:
        logger.error(f"Error retrieving materials: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve materials"
        )