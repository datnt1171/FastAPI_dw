from fastapi import APIRouter, Depends, HTTPException, status, Form, UploadFile, File
from typing import List, Optional
import logging
import os
import uuid
from pathlib import Path
from app.core.auth import has_permission
from app.core.database import execute_query
from app.schemas.blueprints import Blueprint, BlueprintCreateForm, BlueprintUpdate
from app.schemas.schema_helpers import validate_sql_results

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/crm/blueprints", tags=["blueprints"])

# Configuration
UPLOAD_DIR = Path("media/blueprints")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
MAX_FILE_SIZE = 100 * 1024 * 1024

def validate_svg_file(file: UploadFile) -> None:
    """Validate uploaded SVG file"""
    if not file.filename.lower().endswith('.svg'):
        raise HTTPException(status_code=400, detail="Only SVG files are allowed")
    
    if file.content_type not in ['image/svg+xml', 'text/xml', 'application/xml']:
        raise HTTPException(status_code=400, detail="Invalid file type. Must be SVG")

async def save_uploaded_file(file: UploadFile, blueprint_id: str) -> tuple[str, int]:
    """Save uploaded file and return (file_path, file_size)"""
    # Generate unique filename
    file_extension = Path(file.filename).suffix
    unique_filename = f"{blueprint_id}{file_extension}"
    file_path = UPLOAD_DIR / unique_filename
    
    # Read and save file
    content = await file.read()
    file_size = len(content)
    
    if file_size > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="File too large. Maximum size is 100MB")
    
    with open(file_path, "wb") as f:
        f.write(content)
    
    return str(file_path), file_size

@router.get("", response_model=List[Blueprint])
async def get_blueprints(
    factory: str = None,
    permitted = Depends(has_permission())
) -> List[Blueprint]:
    """Get all blueprints with optional factory filter"""
    try:
        query = """
        SELECT
            id,
            factory,
            name,
            type,
            description,
            file_path,
            filename,
            file_size,
            created_at,
            updated_at
        FROM blueprint
        WHERE ($1::text IS NULL OR factory = $1)
        ORDER BY created_at DESC
        """
        
        blueprints_data = await execute_query(
            query=query,
            params=(factory,),
            fetch_all=True
        )
        
        blueprints = validate_sql_results(blueprints_data or [], Blueprint)
        return blueprints
        
    except Exception as e:
        logger.error(f"Error retrieving blueprints: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve blueprints"
        )

@router.get("/{blueprint_id}", response_model=Blueprint)
async def get_blueprint(
    blueprint_id: str,
    permitted = Depends(has_permission())
) -> Blueprint:
    """Get a specific blueprint by ID"""
    try:
        query = """
        SELECT
            id,
            factory,
            name,
            type,
            description,
            file_path,
            filename,
            file_size,
            created_at,
            updated_at
        FROM blueprint
        WHERE id = $1
        """
        
        blueprint_data = await execute_query(
            query=query,
            params=(blueprint_id,),
            fetch_one=True
        )
        
        if not blueprint_data:
            raise HTTPException(
                status_code=404,
                detail="Blueprint not found"
            )
            
        return Blueprint(**blueprint_data)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving blueprint {blueprint_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve blueprint"
        )

@router.post("", response_model=Blueprint, status_code=status.HTTP_201_CREATED)
async def create_blueprint(
    factory: str = Form(...),
    name: str = Form(...),
    type: str = Form(...),
    description: Optional[str] = Form(None),
    file: UploadFile = File(...),
    permitted = Depends(has_permission())
) -> Blueprint:
    """Create a new blueprint with file upload"""
    try:
        # Validate form data
        blueprint_form = BlueprintCreateForm(
            factory=factory,
            name=name,
            type=type,
            description=description
        )
        
        # Validate file
        validate_svg_file(file)
        
        # Check if factory exists
        factory_check = await execute_query(
            query="SELECT factory_code FROM dim_factory WHERE factory_code = $1",
            params=(blueprint_form.factory,),
            fetch_one=True
        )
        
        if not factory_check:
            raise HTTPException(
                status_code=400,
                detail=f"Factory '{blueprint_form.factory}' not found"
            )
        
        # Generate UUID for blueprint
        blueprint_id = str(uuid.uuid4())
        
        # Save file
        file_path, file_size = await save_uploaded_file(file, blueprint_id)
        
        # Insert blueprint record
        query = """
        INSERT INTO blueprint (
            factory, name, type, description, 
            file_path, filename, file_size
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING
            id,
            factory,
            name,
            type,
            description,
            file_path,
            filename,
            file_size,
            created_at,
            updated_at
        """
        
        blueprint_data = await execute_query(
            query=query,
            params=(
                blueprint_form.factory,
                blueprint_form.name,
                blueprint_form.type.value,
                blueprint_form.description,
                file_path,
                file.filename,
                file_size
            ),
            fetch_one=True
        )
        
        return Blueprint(**blueprint_data)
        
    except HTTPException:
        raise
    except Exception as e:
        # Clean up file if database insert fails
        if 'file_path' in locals() and os.path.exists(file_path):
            os.remove(file_path)
        
        logger.error(f"Error creating blueprint: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to create blueprint"
        )

@router.put("/{blueprint_id}", response_model=Blueprint)
async def update_blueprint(
    blueprint_id: str,
    blueprint: BlueprintUpdate,
    permitted = Depends(has_permission())
) -> Blueprint:
    """Update an existing blueprint"""
    try:
        # Check if blueprint exists
        existing = await execute_query(
            query="SELECT id FROM blueprint WHERE id = $1",
            params=(blueprint_id,),
            fetch_one=True
        )
        
        if not existing:
            raise HTTPException(
                status_code=404,
                detail="Blueprint not found"
            )
        
        # Build dynamic update query
        update_fields = []
        params = []
        param_count = 1
        
        if blueprint.name is not None:
            update_fields.append(f"name = ${param_count}")
            params.append(blueprint.name)
            param_count += 1
            
        if blueprint.type is not None:
            update_fields.append(f"type = ${param_count}")
            params.append(blueprint.type.value)
            param_count += 1
            
        if blueprint.description is not None:
            update_fields.append(f"description = ${param_count}")
            params.append(blueprint.description)
            param_count += 1
        
        if not update_fields:
            raise HTTPException(
                status_code=400,
                detail="No fields to update"
            )
        
        params.append(blueprint_id)
        
        query = f"""
        UPDATE blueprint
        SET {', '.join(update_fields)}
        WHERE id = ${param_count}
        RETURNING
            id,
            factory,
            name,
            type,
            description,
            file_path,
            filename,
            file_size,
            created_at,
            updated_at
        """
        
        blueprint_data = await execute_query(
            query=query,
            params=params,
            fetch_one=True
        )
        
        return Blueprint(**blueprint_data)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating blueprint {blueprint_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to update blueprint"
        )

@router.delete("/{blueprint_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_blueprint(
    blueprint_id: str,
    permitted = Depends(has_permission())
):
    """Delete a blueprint"""
    try:
        # First, get the file path before deleting the record
        blueprint_data = await execute_query(
            query="SELECT file_path FROM blueprint WHERE id = $1",
            params=(blueprint_id,),
            fetch_one=True
        )
        
        if not blueprint_data:
            raise HTTPException(
                status_code=404,
                detail="Blueprint not found"
            )
        
        file_path = blueprint_data['file_path']
        
        # Delete from database first
        await execute_query(
            query="DELETE FROM blueprint WHERE id = $1",
            params=(blueprint_id,),
            fetch_one=False
        )
        
        # Then delete the file
        try:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted file: {file_path}")
            else:
                logger.warning(f"File not found for deletion: {file_path}")
        except OSError as file_error:
            # Log but don't fail the API call - database record is already deleted
            logger.error(f"Failed to delete file {file_path}: {str(file_error)}")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting blueprint {blueprint_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to delete blueprint"
        )