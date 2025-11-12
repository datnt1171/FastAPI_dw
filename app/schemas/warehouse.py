from pydantic import Field
from .common import BaseRecord
from pydantic import Field
from typing import Optional, Literal, List, Dict, Any, Union
from datetime import date, datetime

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


class FactorySalesRangeDiff(BaseRecord):
    factory_code: str = Field(...)
    factory_name: str = Field(...)
    salesman: str = Field(...)
    sales_quantity: float = Field(...)
    sales_quantity_target: float = Field(...)
    quantity_diff: float = Field(...)
    quantity_diff_abs: float = Field(...)
    quantity_diff_pct: float = Field(...)
    whole_month_sales_quantity: float = Field(...)
    planned_deliveries: float = Field(...)


class FactoryOrderRangeDiff(BaseRecord):
    factory_code: str = Field(...)
    factory_name: str = Field(...)
    salesman: str = Field(...)
    order_quantity: float = Field(...)
    order_quantity_target: float = Field(...)
    quantity_diff: float = Field(...)
    quantity_diff_abs: float = Field(...)
    quantity_diff_pct: float = Field(...)
    whole_month_order_quantity: float = Field(...)
    planned_deliveries: float = Field(...)


class ProductSalesRangeDiff(BaseRecord):
    product_name: str = Field(...)
    sales_quantity: float = Field(...)
    sales_quantity_target: float = Field(...)
    quantity_diff: float = Field(...)
    quantity_diff_abs: float = Field(...)


class ProductOrderRangeDiff(BaseRecord):
    product_name: str = Field(...)
    order_quantity: float = Field(...)
    order_quantity_target: float = Field(...)
    quantity_diff: float = Field(...)
    quantity_diff_abs: float = Field(...)


class ScheduledAndActualSales(BaseRecord):
    scheduled_month: int = Field(...)
    scheduled_quantity: float = Field(...)
    sales_quantity: float = Field(...)
    sales_pct: float = Field(...)


class IsSameMonth(BaseRecord):
    year: int = Field(...)
    month: int = Field(...)
    same_month_sales: float = Field(...)
    diff_month_sales: float = Field(...)
    total_sales: float = Field(...)
    total_order: float = Field(...)


class SalesOrderPctDiff(BaseRecord):
    year: int = Field(...)
    month: int = Field(...)
    sales_quantity: float = Field(...)
    sales_pct_diff: float = Field(...)
    remain_sales_quantity: float = Field(...)
    remain_sales_pct_diff: float = Field(...)
    order_quantity: float = Field(...)
    order_pct_diff: float = Field(...)
    remain_order_quantity: float = Field(...)
    remain_order_pct_diff: float = Field(...)

class ThinnerPaintRatio(BaseRecord):
    factory_code: str = Field(...)
    factory_name: str = Field(...)
    month: int = Field(...)
    sales_thinner_quantity: float = Field(...)
    sales_paint_quantity: float = Field(...)
    ratio: float = Field(...)


ProductType = Literal[
    "成品溶劑DUNG MOI TP",
    "底漆 LOT",
    "水性SON NUOC",
    "色母SON CAI",
    "木調色PM GO",
    "補土 BOT TRET",
    "半成品BAN THANHPHAM",
    "助劑PHU GIA",
    "原料溶劑 NL DUNG MOI",
    "面漆 BONG",
    "色精TINH MAU",
    "粉類 BOT",
    "硬化劑chat cung",
    "烤調色PM HAP"
]

class PivotThinnerPaintRatio(BaseRecord):
    thinner_data: List[Dict[str, Any]]
    paint_data: List[Dict[str, Any]]
    ratio_data: List[Dict[str, Any]]


class FactOrder(BaseRecord):
    order_date: Optional[date] = Field(default=None)
    order_code: Optional[str] = Field(default=None)
    ct_date: Optional[date] = Field(default=None)
    factory_code: Optional[str] = Field(default=None)
    factory_order_code: Optional[str] = Field(default=None)
    tax_type: Optional[str] = Field(default=None)
    department: Optional[str] = Field(default=None)
    salesman: Optional[str] = Field(default=None)
    deposit_rate: Optional[float] = Field(default=0.0)
    payment_registration_code: Optional[str] = Field(default=None)
    payment_registration_name: Optional[str] = Field(default=None)
    delivery_address: Optional[str] = Field(default=None)
    product_code: Optional[str] = Field(default=None)
    product_name: Optional[str] = Field(default=None)
    qc: Optional[str] = Field(default=None)
    warehouse_type: Optional[str] = Field(default=None)
    order_quantity: Optional[float] = Field(default=0.0)
    delivered_quantity: Optional[float] = Field(default=0.0)
    package_order_quantity: Optional[float] = Field(default=0.0)
    delivered_package_order_quantity: Optional[float] = Field(default=0.0)
    unit: Optional[str] = Field(default=None)
    package_unit: Optional[str] = Field(default=None)
    estimated_delivery_date: Optional[date] = Field(default=None)
    original_estimated_delivery_date: Optional[date] = Field(default=None)
    pre_ct: Optional[str] = Field(default=None)
    finish_code: Optional[str] = Field(default=None)
    import_timestamp: Optional[datetime] = Field(default=None)
    import_wh_timestamp: Optional[datetime] = Field(default=None)
    factory_name: Optional[str] = Field(default=None)


class FactSales(BaseRecord):
    product_code: Optional[str] = Field(default=None)
    product_name: Optional[str] = Field(default=None)
    qc: Optional[str] = Field(default=None)
    factory_code: Optional[str] = Field(default=None)
    sales_date: Optional[date] = Field(default=None)
    sales_code: Optional[str] = Field(default=None)
    order_code: Optional[str] = Field(default=None)
    sales_quantity: Optional[float] = Field(default=0.0)
    unit: Optional[str] = Field(default=None)
    package_sales_quantity: Optional[float] = Field(default=0.0)
    package_unit: Optional[str] = Field(default=None)
    department: Optional[str] = Field(default=None)
    salesman: Optional[str] = Field(default=None)
    warehouse_code: Optional[str] = Field(default=None)
    warehouse_type: Optional[str] = Field(default=None)
    import_code: Optional[str] = Field(default=None)
    factory_order_code: Optional[str] = Field(default=None)
    import_timestamp: Optional[datetime] = Field(default=None)
    import_wh_timestamp: Optional[datetime] = Field(default=None)
    factory_name: Optional[str] = Field(default=None)