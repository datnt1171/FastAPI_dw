from fastapi import APIRouter, Depends, HTTPException, Request
import asyncio
import logging
from app.core.auth import has_permission
from app.core.database import execute_query
from app.core.pagination import Paginator
from app.schemas.retailers import PaginatedRetailerList, Retailer, RetailerDetail
from app.schemas.schema_helpers import validate_sql_results

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/crm/retailers", tags=["retailers"])

@router.get("", response_model=PaginatedRetailerList)
async def get_retailers(
    request: Request,
    search: str = None,
    offset: int = 0,
    limit: int = 50,
    permitted = Depends(has_permission())
) -> PaginatedRetailerList:

    try:
        retailers_query = """
        SELECT
            id,
            name
        FROM dim_retailer
        WHERE ($1::text IS NULL OR name ILIKE '%' || $1 || '%')
        LIMIT $2 OFFSET $3
        """
        
        retailers_task = execute_query(
            query=retailers_query,
            params=(search, limit, offset),
            fetch_all=True
        )
        
        count_query = """
        SELECT COUNT (*)                     
        FROM dim_retailer
        WHERE ($1::text IS NULL OR name ILIKE '%' || $1 || '%')
        """

        count_task = execute_query(
            query=count_query,
            params=(search,),
            fetch_one=True
        )

        retailers_data, count_result = await asyncio.gather(retailers_task, count_task)
        
        # Validate data against schema
        retailers = validate_sql_results(retailers_data or [], Retailer)
        total_count = count_result.get('count', 0) if count_result else 0
        
        # Create paginator and return paginated response
        paginator = Paginator(request, offset, limit)
        paginated_response = paginator.paginate(
            [item.model_dump() for item in retailers], 
            total_count
        )
        
        return PaginatedRetailerList(
            count=paginated_response['count'],
            next=paginated_response.get('next'),
            previous=paginated_response.get('previous'),
            results=retailers
        )
        
    except Exception as e:
        logger.error(f"Error retrieving retailers: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve factories"
        )

@router.get("/{id}", response_model=RetailerDetail)
async def get_retailer_by_id(
    id: str,
    permitted = Depends(has_permission())
) -> RetailerDetail:
    """
    Get a specific retailer by id
    """
    try:
        query = """
        SELECT
            id,
            name                        
        FROM dim_retailer
        WHERE id = $1
        """
        
        retailer = await execute_query(
            query=query,
            params=(id,),
            fetch_one=True,
            fetch_all=False
        )
        
        if not retailer:
            logger.info(f"Retailer with code {id} not found")
            raise HTTPException(
                status_code=404,
                detail=f"Retailer with code '{id}' not found"
            )
        
        retailer = RetailerDetail.model_validate(retailer)
        return retailer
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving retailer {id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve retailer"
        )