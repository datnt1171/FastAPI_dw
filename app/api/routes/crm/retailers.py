from fastapi import APIRouter, Depends, HTTPException, Request, status, Query
import asyncio
import logging

from app.core.auth import has_permission
from app.core.database import execute_query
from app.core.pagination import Paginator
from app.schemas.retailers import (
    PaginatedRetailerList, 
    Retailer, 
    RetailerDetail,
    RetailerCreate,
    RetailerUpdate
)
from app.schemas.schema_helpers import validate_sql_results

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/crm/retailers", tags=["retailers"])

@router.get("", response_model=PaginatedRetailerList)
async def get_retailers(
    request: Request,
    search: str = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1),
    permitted = Depends(has_permission())
) -> PaginatedRetailerList:

    try:
        # Create paginator to get offset and limit
        paginator = Paginator(request, page, page_size)
        limit = paginator.limit
        offset = paginator.offset

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
            detail="Failed to retrieve retailers"
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
            logger.info(f"Retailer with id {id} not found")
            raise HTTPException(
                status_code=404,
                detail=f"Retailer with id '{id}' not found"
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

@router.post("", response_model=RetailerDetail, status_code=status.HTTP_201_CREATED)
async def create_retailer(
    retailer_data: RetailerCreate,
    permitted = Depends(has_permission())
) -> RetailerDetail:
    try:
        # Check if retailer with same name already exists
        check_query = """
        SELECT id FROM dim_retailer WHERE LOWER(name) = LOWER($1)
        """
        
        existing_retailer = await execute_query(
            query=check_query,
            params=(retailer_data.name,),
            fetch_one=True
        )
        
        if existing_retailer:
            raise HTTPException(
                status_code=400,
                detail=f"Retailer with name '{retailer_data.name}' already exists"
            )
        
        insert_query = """
        INSERT INTO dim_retailer (name)
        VALUES ($1)
        RETURNING id, name
        """
        
        new_retailer = await execute_query(
            query=insert_query,
            params=(retailer_data.name,),
            fetch_one=True
        )
        
        if not new_retailer:
            raise HTTPException(
                status_code=500,
                detail="Failed to create retailer"
            )
        
        return RetailerDetail.model_validate(new_retailer)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating retailer: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to create retailer"
        )

@router.patch("/{id}", response_model=RetailerDetail)
async def update_retailer(
    id: str,
    retailer_data: RetailerUpdate,
    permitted = Depends(has_permission())
) -> RetailerDetail:
    try:
        # Check if retailer exists
        check_query = """
        SELECT id FROM dim_retailer WHERE id = $1
        """
        
        existing_retailer = await execute_query(
            query=check_query,
            params=(id,),
            fetch_one=True
        )
        
        if not existing_retailer:
            raise HTTPException(
                status_code=404,
                detail=f"Retailer with id '{id}' not found"
            )
        
        # Check if another retailer with same name already exists (excluding current one)
        name_check_query = """
        SELECT id FROM dim_retailer 
        WHERE LOWER(name) = LOWER($1) AND id != $2
        """
        
        name_conflict = await execute_query(
            query=name_check_query,
            params=(retailer_data.name, id),
            fetch_one=True
        )
        
        if name_conflict:
            raise HTTPException(
                status_code=400,
                detail=f"Retailer with name '{retailer_data.name}' already exists"
            )
        
        # Update retailer
        update_query = """
        UPDATE dim_retailer 
        SET name = $1
        WHERE id = $2
        RETURNING id, name
        """
        
        updated_retailer = await execute_query(
            query=update_query,
            params=(retailer_data.name, id),
            fetch_one=True
        )
        
        if not updated_retailer:
            raise HTTPException(
                status_code=500,
                detail="Failed to update retailer"
            )
        
        return RetailerDetail.model_validate(updated_retailer)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating retailer {id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to update retailer"
        )

@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_retailer(
    id: str,
    permitted = Depends(has_permission())
):
    """
    Delete a retailer
    """
    try:
        # Check if retailer exists
        check_query = """
        SELECT id FROM dim_retailer WHERE id = $1
        """
        
        existing_retailer = await execute_query(
            query=check_query,
            params=(id,),
            fetch_one=True
        )
        
        if not existing_retailer:
            raise HTTPException(
                status_code=404,
                detail=f"Retailer with id '{id}' not found"
            )
        
        # Delete retailer
        delete_query = """
        DELETE FROM dim_retailer WHERE id = $1
        """
        
        await execute_query(
            query=delete_query,
            params=(id,),
            fetch_one=False,
            fetch_all=False
        )
        
        logger.info(f"Retailer {id} deleted successfully")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting retailer {id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to delete retailer"
        )