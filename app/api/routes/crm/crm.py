from fastapi import APIRouter, Depends, HTTPException, Request
from typing import List, Dict, Any, Union
import logging
import asyncio
from app.core.auth import has_permission
from app.core.database import execute_sql_file
from app.core.pagination import Paginator

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/factories/")
async def get_factories(
    request: Request,
    is_active: bool = True,
    has_onsite: bool = True,
    offset: int = 0,
    limit: int = 50,
    permitted = Depends(has_permission())
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:

    try:
        # Execute queries concurrently
        factories_task = execute_sql_file(
            "app/api/routes/crm/sql/get_factories.sql",
            params={
                "is_active": is_active,
                "has_onsite": has_onsite,
                "offset": offset,
                "limit": limit
            },
            fetch_all=True
        )
        
        count_task = execute_sql_file(
            "app/api/routes/crm/sql/count_factories.sql",
            params={
                "is_active": is_active,
                "has_onsite": has_onsite,
            },
            fetch_one=True,
            fetch_all=False
        )
        
        # Await
        factories, count_result = await asyncio.gather(factories_task, count_task)
        
        total_count = count_result.get('count', 0) if count_result else 0
        logger.info(f"Total factories count: {total_count}")
        
        # Create paginator and return paginated response
        paginator = Paginator(request, offset, limit)
        paginated_response = paginator.paginate(factories or [], total_count)
        
        logger.info(f"Retrieved {len(factories or [])} factories out of {total_count}")
        return paginated_response
        
    except Exception as e:
        logger.error(f"Error retrieving factories: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve factories"
        )

@router.get("/factories/{factory_id}/", response_model=Dict[str, Any])
async def get_factory_by_id(
    factory_id: str,
    permitted = Depends(has_permission())
):
    """
    Get a specific factory by factory_code
    """
    try:
        factory = await execute_sql_file(
            "app/api/routes/crm/sql/get_factory_by_id.sql",
            params={"factory_id": factory_id},
            fetch_one=True,
            fetch_all=False
        )
        
        if not factory:
            logger.info(f"Factory with code {factory_id} not found")
            raise HTTPException(
                status_code=404,
                detail=f"Factory with code '{factory_id}' not found"
            )
        
        logger.info(f"Retrieved factory: {factory_id}")
        return factory
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving factory {factory_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve factory"
        )