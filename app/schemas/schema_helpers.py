from typing import List, Dict, Any, Type, Optional
from datetime import date, datetime
from pydantic import BaseModel, create_model
from app.schemas.common import BaseRecord
import logging

logger = logging.getLogger(__name__)

def create_dynamic_schema(data: List[Dict[str, Any]], schema_name: str) -> Type[BaseModel]:
    """Create a Pydantic schema from SQL query results dynamically"""
    if not data:
        return BaseModel
    
    # Analyze first row to determine field types
    sample_row = data[0]
    fields = {}
    
    for key, value in sample_row.items():
        if value is None:
            fields[key] = (Optional[str], None)
        elif isinstance(value, str):
            fields[key] = (str, ...)
        elif isinstance(value, int):
            fields[key] = (int, ...)
        elif isinstance(value, float):
            fields[key] = (float, ...)
        elif isinstance(value, bool):
            fields[key] = (bool, ...)
        elif isinstance(value, datetime):
            fields[key] = (datetime, ...)
        elif isinstance(value, date):
            fields[key] = (date, ...)
        else:
            fields[key] = (Any, ...)
    
    return create_model(schema_name, **fields, __base__=BaseRecord)

def validate_sql_results(data: List[Dict[str, Any]], schema: Type[BaseModel]) -> List[BaseModel]:
    """Validate and convert SQL results to Pydantic models"""
    try:
        return [schema.model_validate(row) for row in data or []]
    except Exception as e:
        logger.error(f"Schema validation failed: {e}")
        logger.error(f"Sample data: {data[:1] if data else 'No data'}")
        raise