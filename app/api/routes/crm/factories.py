from fastapi import APIRouter, Depends, HTTPException, Request, Query
import asyncio
import logging
from app.core.auth import has_permission
from app.core.database import execute_query
from app.core.pagination import Paginator
from app.schemas.factories import PaginatedFactoryList, Factory, FactoryDetail, FactoryUpdate
from app.schemas.schema_helpers import validate_sql_results

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/crm/factories", tags=["factories"])

@router.get("", response_model=PaginatedFactoryList)
async def get_factories(
    request: Request,
    is_active: bool = None,
    has_onsite: bool = None,
    search: str = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1),
    permitted = Depends(has_permission())
) -> PaginatedFactoryList:

    try:
        # Create paginator to get offset and limit
        paginator = Paginator(request, page, page_size)
        limit = paginator.limit
        offset = paginator.offset

        factories_query = """
        SELECT
            factory_code,
            factory_name,
            salesman,
            is_active,
            has_onsite
        FROM dim_factory
        WHERE ($1::boolean IS NULL OR is_active = $1)
        AND ($2::boolean IS NULL OR has_onsite = $2)
        AND ($3::text IS NULL OR factory_name ILIKE '%' || $3 || '%')
        LIMIT $4 OFFSET $5
        """
        
        factories_task = execute_query(
            query=factories_query,
            params=(is_active, has_onsite, search, limit, offset),
            fetch_all=True
        )
        
        count_query = """
        SELECT COUNT (*)                     
        FROM dim_factory
        WHERE ($1::boolean IS NULL OR is_active = $1)
        AND ($2::boolean IS NULL OR has_onsite = $2)
        AND ($3::text IS NULL OR factory_name ILIKE '%' || $3 || '%')
        """

        count_task = execute_query(
            query=count_query,
            params=(is_active, has_onsite, search),
            fetch_one=True
        )

        factories_data, count_result = await asyncio.gather(factories_task, count_task)
        
        # Validate data against schema
        factories = validate_sql_results(factories_data or [], Factory)
        total_count = count_result.get('count', 0) if count_result else 0
        
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

@router.post("", response_model=Factory, status_code=201)
async def create_factory(
    factory: Factory,
    permitted = Depends(has_permission())
) -> Factory:
    """
    Create a new factory
    """
    try:
        # Check if factory already exists
        check_query = """
        SELECT factory_code FROM dim_factory WHERE factory_code = $1
        """
        
        existing_factory = await execute_query(
            query=check_query,
            params=(factory.factory_code,),
            fetch_one=True
        )
        
        if existing_factory:
            logger.warning(f"Factory with code {factory.factory_code} already exists")
            raise HTTPException(
                status_code=409,
                detail=f"Factory with code '{factory.factory_code}' already exists"
            )
        
        # Insert new factory
        insert_query = """
        INSERT INTO dim_factory (factory_code, factory_name, is_active, has_onsite)
        VALUES ($1, $2, $3, $4)
        RETURNING factory_code, factory_name, is_active, has_onsite
        """
        
        new_factory = await execute_query(
            query=insert_query,
            params=(factory.factory_code, factory.factory_name, factory.is_active, factory.has_onsite),
            fetch_one=True
        )
        
        if not new_factory:
            raise HTTPException(
                status_code=500,
                detail="Failed to create factory"
            )
        
        logger.info(f"Successfully created factory with code {factory.factory_code}")
        return Factory.model_validate(new_factory)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating factory: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to create factory"
        )

@router.get("/{factory_id}", response_model=FactoryDetail)
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
            salesman,
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
    
@router.patch("/{factory_id}", response_model=Factory)
async def update_factory(
    factory_id: str,
    factory_update: FactoryUpdate,
    permitted = Depends(has_permission())
) -> Factory:
    """
    Update a factory's is_active and/or has_onsite status
    Only is_active and has_onsite fields can be modified
    """
    try:
        # Check if factory exists
        check_query = """
        SELECT factory_code FROM dim_factory WHERE factory_code = $1
        """
        
        existing_factory = await execute_query(
            query=check_query,
            params=(factory_id,),
            fetch_one=True
        )
        
        if not existing_factory:
            logger.info(f"Factory with code {factory_id} not found")
            raise HTTPException(
                status_code=404,
                detail=f"Factory with code '{factory_id}' not found"
            )
        
        # Check if there are any fields to update
        update_data = factory_update.model_dump(exclude_unset=True)
        if not update_data:
            logger.warning(f"No valid fields provided for factory {factory_id} update")
            raise HTTPException(
                status_code=400,
                detail="At least one field (is_active or has_onsite) must be provided for update"
            )
        
        # Build dynamic UPDATE query based on provided fields
        set_clauses = []
        params = []
        param_count = 1
        
        if factory_update.is_active is not None:
            set_clauses.append(f"is_active = ${param_count}")
            params.append(factory_update.is_active)
            param_count += 1
            
        if factory_update.has_onsite is not None:
            set_clauses.append(f"has_onsite = ${param_count}")
            params.append(factory_update.has_onsite)
            param_count += 1
        
        # Add factory_id as the last parameter for WHERE clause
        params.append(factory_id)
        
        update_query = f"""
        UPDATE dim_factory 
        SET {', '.join(set_clauses)}
        WHERE factory_code = ${param_count}
        RETURNING factory_code, factory_name, salesman, is_active, has_onsite
        """
        
        updated_factory = await execute_query(
            query=update_query,
            params=params,
            fetch_one=True
        )
        
        if not updated_factory:
            raise HTTPException(
                status_code=500,
                detail="Failed to update factory"
            )
        
        logger.info(f"Successfully updated factory {factory_id} with fields: {list(update_data.keys())}")
        return Factory.model_validate(updated_factory)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating factory {factory_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to update factory"
        )