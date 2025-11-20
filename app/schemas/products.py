from pydantic import Field
from .common import BaseRecord, PaginatedResponse
from typing import Optional
from datetime import date
from decimal import Decimal


class Formular(BaseRecord):
    product_name: str = Field(..., description="Product name")
    material_name: str = Field(..., description="Material name")
    ratio: Decimal = Field(..., description="Ratio")
    version_number: int = Field(..., description="Formular version")
    effective_date: date = Field(..., description="Start effective date")
    end_date: Optional[date] = Field(default=None, description="End effective date")
    is_current: bool = Field(..., description="Formular version")


PaginatedFormularList = PaginatedResponse[Formular]


class Product(BaseRecord):
    id: int = Field(...)
    product_name: str = Field(...)
    product_type: Optional[str] = Field(default="")
    qc: Optional[str] = Field(default="")


PaginatedProductList = PaginatedResponse[Product]


class Material(BaseRecord):
    id: int = Field(...)
    name: str = Field(..., description="Material name")
    qc: Optional[str] = Field(default="")
    unit: Optional[str] = Field(default="")


PaginatedMaterialList = PaginatedResponse[Material]