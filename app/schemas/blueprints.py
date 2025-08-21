from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from enum import Enum
from app.schemas.common import BaseRecord

class ProductionLineType(str, Enum):
    PALLET = "PALLET"
    HANGING = "HANGING"
    ROLLER = "ROLLER"

class BlueprintBase(BaseRecord):
    factory: str = Field(..., description="Factory code")
    name: str = Field(..., max_length=255, description="Blueprint name")
    type: ProductionLineType = Field(..., description="Production line type")
    description: Optional[str] = Field(None, description="Blueprint description")

# For form data - not used as Pydantic model for file upload
class BlueprintCreateForm:
    def __init__(
        self,
        factory: str,
        name: str,
        type: str,
        description: Optional[str] = None
    ):
        self.factory = factory
        self.name = name
        self.type = ProductionLineType(type)
        self.description = description

class BlueprintUpdate(BaseRecord):
    name: Optional[str] = Field(None, max_length=255, description="Blueprint name")
    type: Optional[ProductionLineType] = Field(None, description="Production line type")
    description: Optional[str] = Field(None, description="Blueprint description")

class Blueprint(BaseRecord):
    id: str = Field(..., description="Blueprint UUID")
    factory: str = Field(..., description="Factory code")
    name: str = Field(..., description="Blueprint name")
    type: ProductionLineType = Field(..., description="Production line type")
    description: Optional[str] = Field(None, description="Blueprint description")
    file_path: str = Field(..., description="File storage path")
    filename: str = Field(..., description="Original filename")
    file_size: int = Field(..., description="File size in bytes")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")