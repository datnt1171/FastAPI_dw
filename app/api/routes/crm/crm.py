from fastapi import APIRouter, Depends, HTTPException
from typing import List, Dict, Any
import logging

from app.core.auth import has_permission
from app.core.database import execute_sql_file

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/factories/", response_model=List[Dict[str, Any]])
async def get_factories(permitted = Depends(has_permission())):
    """
    Get all distinct factories from the data warehouse
    """
    try:
        factories = await execute_sql_file(
            "app/api/routes/crm/sql/get_factories.sql",
            fetch_all=True
        )
        
        if not factories:
            logger.info("No factories found in database")
            return []
        
        logger.info(f"Retrieved {len(factories)} factories")
        return factories
        
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