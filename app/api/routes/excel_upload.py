from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File
import logging
import os
from pathlib import Path
from datetime import datetime
from app.core.auth import has_permission

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/etl", tags=["etl"])

# Configuration
UPLOAD_DIR = Path("/app/media/dw")
SALES_DIR = UPLOAD_DIR / "sales"
ORDER_DIR = UPLOAD_DIR / "order"

# Create directories if they don't exist
SALES_DIR.mkdir(parents=True, exist_ok=True)
ORDER_DIR.mkdir(parents=True, exist_ok=True)

# File configuration
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
ALLOWED_EXTENSIONS = {'.xlsx', '.xls'}

def validate_excel_file(file: UploadFile) -> None:
    """Validate uploaded Excel file"""
    file_extension = Path(file.filename).suffix.lower()
    
    if file_extension not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400, 
            detail=f"Only Excel files are allowed (.xlsx, .xls). Got: {file_extension}"
        )
    
    # Check content type (optional, as Excel files can have various MIME types)
    valid_content_types = [
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',  # .xlsx
        'application/vnd.ms-excel',  # .xls
        'application/octet-stream'  # Sometimes Excel files are detected as this
    ]
    
    if file.content_type not in valid_content_types:
        logger.warning(f"Unexpected content type: {file.content_type} for file: {file.filename}")

async def save_uploaded_file(file: UploadFile, upload_dir: Path) -> tuple[str, int]:
    """
    Save uploaded file and return (file_path, file_size)
    
    Args:
        file: The uploaded file
        upload_dir: Directory to save the file
    """
    # Generate timestamped filename to avoid overwrites
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_extension = Path(file.filename).suffix
    original_name = Path(file.filename).stem
    unique_filename = f"{original_name}_{timestamp}{file_extension}"
    file_path = upload_dir / unique_filename
    
    # Read and validate file size
    content = await file.read()
    file_size = len(content)
    
    if file_size > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=400, 
            detail=f"File too large. Maximum size is {MAX_FILE_SIZE // (1024*1024)}MB"
        )
    
    if file_size == 0:
        raise HTTPException(status_code=400, detail="File is empty")
    
    # Save file
    with open(file_path, "wb") as f:
        f.write(content)
    
    logger.info(f"File saved: {file_path} ({file_size} bytes)")
    return str(file_path), file_size

@router.post("/sales", status_code=status.HTTP_201_CREATED)
async def upload_sales_file(
    file: UploadFile = File(..., description="Sales Excel file to upload"),
    permitted = Depends(has_permission())
) -> dict:
    """
    Upload and process sales Excel file for ETL
    
    - **file**: Excel file (.xlsx or .xls) containing sales data
    
    Returns processing results including rows inserted
    """
    file_path = None
    
    try:
        # Validate Excel file
        validate_excel_file(file)
        
        # Save file
        file_path, file_size = await save_uploaded_file(file, SALES_DIR)
        
        # Import the processor
        from app.utils.etl.sales_processor import process_sales_file
        from app.core.database import db_manager
        
        # Process the file
        async with db_manager.get_connection() as conn:
            processing_stats = await process_sales_file(file_path, conn)
        
        return {
            "status": "success",
            "message": "Sales file processed successfully",
            "data": {
                "file_type": "sales",
                "filename": file.filename,
                "file_path": file_path,
                "file_size": file_size,
                "file_size_mb": round(file_size / (1024 * 1024), 2),
                "uploaded_at": datetime.now().isoformat(),
                "processing_stats": processing_stats
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        # Clean up file if something goes wrong
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass
        
        logger.error(f"Error processing sales file: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process sales file: {str(e)}"
        )

@router.post("/order", status_code=status.HTTP_201_CREATED)
async def upload_order_file(
    file: UploadFile = File(..., description="Order Excel file to upload"),
    permitted = Depends(has_permission())
) -> dict:
    """
    Upload and process order Excel file for ETL
    
    - **file**: Excel file (.xlsx or .xls) containing order data
    
    Returns processing results including rows inserted
    """
    file_path = None
    
    try:
        # Validate Excel file
        validate_excel_file(file)
        
        # Save file
        file_path, file_size = await save_uploaded_file(file, ORDER_DIR)
        
        # Import the processor
        from app.utils.etl.order_processor import process_order_file
        from app.core.database import db_manager
        
        # Process the file
        async with db_manager.get_connection() as conn:
            processing_stats = await process_order_file(file_path, conn)
        
        return {
            "status": "success",
            "message": "Order file processed successfully",
            "data": {
                "file_type": "order",
                "filename": file.filename,
                "file_path": file_path,
                "file_size": file_size,
                "file_size_mb": round(file_size / (1024 * 1024), 2),
                "uploaded_at": datetime.now().isoformat(),
                "processing_stats": processing_stats
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        # Clean up file if something goes wrong
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass
        
        logger.error(f"Error processing order file: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process order file: {str(e)}"
        )