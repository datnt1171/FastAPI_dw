from fastapi import APIRouter, Depends, HTTPException, Request
import asyncio
import logging
from app.core.auth import has_permission
from app.core.database import execute_query
from app.core.pagination import Paginator
from app.schemas.factories import PaginatedFactoryList, Factory, FactoryDetail
from app.schemas.schema_helpers import validate_sql_results

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/factories/", response_model=PaginatedFactoryList)
async def get_factories(
    request: Request,
    is_active: bool = None,
    has_onsite: bool = None,
    offset: int = 0,
    limit: int = 50,
    permitted = Depends(has_permission())
) -> PaginatedFactoryList:

    try:
        factories_query = """
        SELECT
            factory_code,
            factory_name,
            is_active,
            has_onsite
        FROM dim_factory
        WHERE ($1::boolean IS NULL OR is_active = $1)
        AND ($2::boolean IS NULL OR has_onsite = $2)
        LIMIT $3 OFFSET $4
        """
        
        factories_task = execute_query(
            query=factories_query,
            params=(is_active, has_onsite, limit, offset),
            fetch_all=True
        )
        
        count_query = """
        SELECT COUNT (*)                     
        FROM dim_factory
        WHERE ($1::boolean IS NULL OR is_active = $1)
        AND ($2::boolean IS NULL OR has_onsite = $2)
        """

        count_task = execute_query(
            query=count_query,
            params=(is_active, has_onsite),
            fetch_one=True
        )

        factories_data, count_result = await asyncio.gather(factories_task, count_task)
        
        # Validate data against schema
        factories = validate_sql_results(factories_data or [], Factory)
        total_count = count_result.get('count', 0) if count_result else 0
        logger.info(f"Total factories count: {total_count}")
        
        # Create paginator and return paginated response
        paginator = Paginator(request, offset, limit)
        paginated_response = paginator.paginate(
            [item.model_dump() for item in factories], 
            total_count
        )
        
        return PaginatedFactoryList(
            count=paginated_response['count'],
            next=paginated_response.get('next'),
            previous=paginated_response.get('previous'),
            results=factories
        )
        
    except Exception as e:
        logger.error(f"Error retrieving factories: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve factories"
        )

@router.get("/factories/{factory_id}/", response_model=FactoryDetail)
async def get_factory_by_id(
    factory_id: str,
    permitted = Depends(has_permission())
) -> FactoryDetail:
    """
    Get a specific factory by factory_code
    """
    try:
        query = """
        SELECT
            factory_code,
            factory_name,
            is_active,
            has_onsite                         
        FROM dim_factory
        WHERE factory_code = $1
        """
        
        factory = await execute_query(
            query=query,
            params=(factory_id,),
            fetch_one=True,
            fetch_all=False
        )
        
        if not factory:
            logger.info(f"Factory with code {factory_id} not found")
            raise HTTPException(
                status_code=404,
                detail=f"Factory with code '{factory_id}' not found"
            )
        
        factory = FactoryDetail.model_validate(factory)
        return factory
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving factory {factory_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve factory"
        )