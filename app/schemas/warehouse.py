from pydantic import Field
from .common import BaseRecord
from pydantic import Field
from typing import Optional


class Overall(BaseRecord):
    month: int = Field(...)
    sales_quantity: Optional[float] = Field(default=0.0)
    exclude_factory_sales_quantity: Optional[float] = Field(default=0.0)
    remain_sales_quantity: Optional[float] = Field(default=0.0)
    order_quantity: Optional[float] = Field(default=0.0)
    exclude_factory_order_quantity: Optional[float] = Field(default=0.0)
    remain_order_quantity: Optional[float] = Field(default=0.0)
    sales_target_value: Optional[float] = Field(default=0.0)
    order_target_value: Optional[float] = Field(default=0.0)
    sales_target_pct: Optional[float] = Field(default=0.0)
    order_target_pct: Optional[float] = Field(default=0.0)