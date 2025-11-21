from fastapi import APIRouter, Depends, HTTPException, Query
import logging
from typing import List, Optional
from app.core.auth import has_permission
from app.core.database import execute_query
from app.schemas.warehouse import (Overall,
                                   FactorySalesRangeDiff, FactoryOrderRangeDiff,
                                   ProductSalesRangeDiff, ProductOrderRangeDiff,
                                   ScheduledAndActualSales,
                                   IsSameMonth, SalesOrderPctDiff,
                                   ThinnerPaintRatio, ProductType, PivotThinnerPaintRatio,
                                   FactOrder, FactSales,
                                   SalesBOM, OrderBOM,
                                   )
from app.schemas.common import DateRangeParams, DateRangeTargetParams, TIME_GROUP_BY_MAPPING
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/warehouse", tags=["warehouse"])


@router.get("/max-sales-date", response_model=str)
async def get_max_sales_date(
    permitted = Depends(has_permission())
) -> str:
    """Get the maximum sales date from fact_sales"""
    try:
        query = "SELECT MAX(sales_date) as max_sales_date FROM fact_sales"
       
        result = await execute_query(
            query=query,
            fetch_all=False,
            fetch_one=True
        )
       
        # Format date as string
        max_sales_date = result['max_sales_date']
        return max_sales_date.strftime('%Y-%m-%d')


    except Exception as e:
        logger.error(f"Error retrieving max sales date: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve max sales date: {str(e)}"
        )

@router.get("/overall", response_model=List[Overall])
async def get_overall(
    day__gte: int = Query(1, ge=1, le=31, description="Start day"),
    day__lte: int = Query(31, ge=1, le=31, description="End day"),
    month__gte: int = Query(1, ge=1, le=12, description="Start month"),
    month__lte: int = Query(12, ge=1, le=12, description="End month"),
    year: int = Query(datetime.now().year, ge=2020, le=datetime.now().year, description="Year"),
    target_month: int = Query(5, ge=1, le=12, description="Target month"),
    target_year: int = Query(2022, ge=2020, le=datetime.now().year, description="Target year"),
    exclude_factory: str = Query('30673', description="Factory code to exclude"),
    permitted = Depends(has_permission())
) -> List[Overall]:
    """
    Get overall warehouse data with sales and order statistics.
    Returns aggregated data by month with target comparisons.
    """
    try:
        query = """WITH filtered_dates AS (
                    SELECT date, month
                    FROM dim_date
                    WHERE day BETWEEN $1 AND $2
                    AND month BETWEEN $3 AND $4
                    AND year = $5
                ),
                total_sales AS (
                    SELECT fd.month, COALESCE(sum(fs.sales_quantity), 0) AS sales_quantity
                    FROM filtered_dates fd
                    LEFT JOIN fact_sales fs ON fs.sales_date = fd."date"
                    GROUP BY fd."month"
                ),
                exclude_factory_sales AS (
                    SELECT fd.month, COALESCE(sum(fs.sales_quantity), 0) AS exclude_factory_sales_quantity
                    FROM filtered_dates fd
                    LEFT JOIN fact_sales fs ON fs.sales_date = fd."date" AND fs.factory_code = $8
                    GROUP BY fd."month"
                ),
                total_order AS (
                    SELECT fd."month", COALESCE(sum(fo.order_quantity), 0) AS order_quantity
                    FROM filtered_dates fd
                    LEFT JOIN fact_order fo ON fo.order_date = fd."date"
                    GROUP BY fd."month"
                ),
                exclude_factory_order AS (
                    SELECT fd."month", COALESCE(sum(fo.order_quantity), 0) AS exclude_factory_order_quantity
                    FROM filtered_dates fd
                    LEFT JOIN fact_order fo ON fo.order_date = fd."date" AND fo.factory_code = $8
                    GROUP BY fd."month"
                ),
                sales_order_quantity AS (
                    SELECT 
                        COALESCE(ts."month", tod."month") AS month,
                        COALESCE(ts.sales_quantity, 0) AS sales_quantity,
                        COALESCE(tod.order_quantity, 0) AS order_quantity
                    FROM total_sales ts
                    FULL OUTER JOIN total_order tod ON ts."month" = tod."month"
                ),
                sales_order_detail AS (
                    SELECT 
                        soc.month,
                        soc.sales_quantity,
                        COALESCE(efs.exclude_factory_sales_quantity, 0) AS exclude_factory_sales_quantity,
                        COALESCE(soc.sales_quantity - efs.exclude_factory_sales_quantity, 0) AS remain_sales_quantity,
                        soc.order_quantity,
                        COALESCE(efo.exclude_factory_order_quantity, 0) AS exclude_factory_order_quantity,
                        COALESCE(soc.order_quantity - efo.exclude_factory_order_quantity, 0) AS remain_order_quantity
                    FROM sales_order_quantity soc
                    LEFT JOIN exclude_factory_sales efs ON soc.month = efs.month
                    LEFT JOIN exclude_factory_order efo ON soc.month = efo.month
                ),
                target_date AS (
                    SELECT date
                    FROM dim_date
                    WHERE day BETWEEN $1 AND $2
                    AND month = $6
                    AND year = $7
                ),
                sales_target AS (
                    SELECT COALESCE(SUM(sales_quantity), 0) AS sales_target_value
                    FROM fact_sales fs
                    JOIN target_date td ON fs.sales_date = td."date"
                    WHERE factory_code != $8
                ),
                order_target AS (
                    SELECT COALESCE(SUM(order_quantity), 0) AS order_target_value
                    FROM fact_order fo
                    JOIN target_date td ON fo.order_date = td."date"
                    WHERE factory_code != $8
                )
                SELECT 
                    sod.month,
                    sod.sales_quantity,
                    sod.exclude_factory_sales_quantity,
                    sod.remain_sales_quantity,
                    sod.order_quantity,
                    sod.exclude_factory_order_quantity,
                    sod.remain_order_quantity,
                    st.sales_target_value,
                    ot.order_target_value,
                    CASE 
                        WHEN st.sales_target_value > 0 THEN sod.remain_sales_quantity / st.sales_target_value 
                        ELSE 0 
                    END AS sales_target_pct,
                    CASE 
                        WHEN ot.order_target_value > 0 THEN sod.remain_order_quantity / ot.order_target_value 
                        ELSE 0 
                    END AS order_target_pct
                FROM sales_order_detail sod
                CROSS JOIN sales_target st
                CROSS JOIN order_target ot
                ORDER BY sod.month"""
        
        overall_result = await execute_query(
            query=query,
            params=(day__gte,
                    day__lte,
                    month__gte,
                    month__lte,
                    year,
                    target_month,
                    target_year,
                    exclude_factory),
            fetch_all=True
        )

        if not overall_result:
            logger.warning("No data found for the specified criteria")
            return []

        return overall_result

    except Exception as e:
        logger.error(f"Error retrieving overall_data: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve overall_data: {str(e)}"
        )


@router.get("/factory-sales-range-diff", response_model=List[FactorySalesRangeDiff])
async def get_factory_sales_range_diff(
    date_range: DateRangeParams = Depends(),
    date_range_target: DateRangeTargetParams = Depends(),
    threshold: int = 1000,
    increase: bool = False,
    permitted = Depends(has_permission())
) -> List[FactorySalesRangeDiff]:
    """
    Get sales by factories for 2 date range, whole month sales and scheduled delivery
    Return the diff of these 2 date range
    """
    quantity_filter = "sd.quantity_diff > 0" if increase else "sd.quantity_diff < 0"
    order_clause = "sd.quantity_diff DESC" if increase else "sd.quantity_diff ASC"

    try:
        query = """WITH date_range_sales AS (
                    SELECT factory_code, SUM(sales_quantity) AS sales_quantity
                    FROM fact_sales
                    WHERE sales_date BETWEEN $1 AND $2
                    GROUP BY factory_code
                ),
                date_range_target_sales AS (
                    SELECT factory_code, SUM(sales_quantity) AS sales_quantity_target
                    FROM fact_sales
                    WHERE sales_date BETWEEN $3 AND $4
                    GROUP BY factory_code
                ),
                sales_diff AS (
                    SELECT
                        COALESCE(drs.factory_code, drts.factory_code) AS factory_code,
                        COALESCE(drs.sales_quantity, 0) AS sales_quantity,
                        COALESCE(drts.sales_quantity_target, 0) AS sales_quantity_target,
                        (COALESCE(drs.sales_quantity, 0) - COALESCE(drts.sales_quantity_target, 0)) AS quantity_diff,
                        ABS(COALESCE(drs.sales_quantity, 0) - COALESCE(drts.sales_quantity_target, 0)) AS quantity_diff_abs
                    FROM date_range_sales drs
                    FULL OUTER JOIN date_range_target_sales drts
                        ON drs.factory_code = drts.factory_code
                    WHERE ABS(COALESCE(drs.sales_quantity, 0) - COALESCE(drts.sales_quantity_target, 0)) >= $5
                ),
                whole_month_sales AS (
                    SELECT fs.factory_code, SUM(fs.sales_quantity) AS whole_month_sales_quantity
                    FROM fact_sales fs
                    JOIN dim_date dd ON fs.sales_date = dd.date
                    WHERE dd.year = EXTRACT(YEAR FROM CAST($3 AS DATE))
                        AND dd.month = EXTRACT(MONTH FROM CAST($3 AS DATE))
                        AND fs.factory_code IN (SELECT factory_code FROM sales_diff)
                    GROUP BY fs.factory_code
                ),
                planned_deliveries AS (
                    SELECT factory_code, SUM(order_quantity) AS planned_deliveries
                    FROM fact_order
                    WHERE estimated_delivery_date BETWEEN
                        CAST($2 AS DATE) + 1
                        AND (DATE_TRUNC('month', CAST($2 AS DATE)) + INTERVAL '1 month' - INTERVAL '1 day')::DATE
                        AND factory_code IN (SELECT factory_code FROM sales_diff)
                    GROUP BY factory_code
                )
                SELECT
                    df.factory_code,
                    df.factory_name,
                    df.salesman,
                    sd.sales_quantity,
                    sd.sales_quantity_target,
                    sd.quantity_diff,
                    sd.quantity_diff_abs,
                    COALESCE(sd.quantity_diff / NULLIF(sd.sales_quantity_target, 0), 1) AS quantity_diff_pct,
                    COALESCE(wms.whole_month_sales_quantity, 0) AS whole_month_sales_quantity,
                    COALESCE(pd.planned_deliveries, 0) AS planned_deliveries
                FROM sales_diff sd
                JOIN dim_factory df ON sd.factory_code = df.factory_code
                LEFT JOIN whole_month_sales wms ON sd.factory_code = wms.factory_code
                LEFT JOIN planned_deliveries pd ON sd.factory_code = pd.factory_code
                WHERE {quantity_filter}
                ORDER BY {order_clause}
                """.format(quantity_filter=quantity_filter, order_clause=order_clause)
        
        result = await execute_query(
            query=query,
            params=(date_range.date__gte,
                    date_range.date__lte,
                    date_range_target.date_target__gte,
                    date_range_target.date_target__lte,
                    threshold
                    ),
            fetch_all=True
        )

        if not result:
            logger.warning("No data found for the specified criteria")
            return []

        return result

    except Exception as e:
        logger.error(f"Error retrieving factory-sales-range-diff: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve factory-sales-range-diff: {str(e)}"
        )


@router.get("/factory-order-range-diff", response_model=List[FactoryOrderRangeDiff])
async def get_factory_order_range_diff(
    date_range: DateRangeParams = Depends(),
    date_range_target: DateRangeTargetParams = Depends(),
    threshold: int = 1000,
    increase: bool = False,
    permitted = Depends(has_permission())
) -> List[FactoryOrderRangeDiff]:
    """
    Get order by factories for 2 date range, whole month order and scheduled delivery
    Return the diff of these 2 date range
    """
    quantity_filter = "od.quantity_diff > 0" if increase else "od.quantity_diff < 0"
    order_clause = "od.quantity_diff DESC" if increase else "od.quantity_diff ASC"

    try:
        query = """WITH date_range_order AS (
                    SELECT factory_code, SUM(order_quantity) AS order_quantity
                    FROM fact_order
                    WHERE order_date BETWEEN $1 AND $2
                    GROUP BY factory_code
                ),
                date_range_target_order AS (
                    SELECT factory_code, SUM(order_quantity) AS order_quantity_target
                    FROM fact_order
                    WHERE order_date BETWEEN $3 AND $4
                    GROUP BY factory_code
                ),
                order_diff AS (
                    SELECT 
                        COALESCE(dro.factory_code, drto.factory_code) AS factory_code,
                        COALESCE(dro.order_quantity, 0) AS order_quantity,
                        COALESCE(drto.order_quantity_target, 0) AS order_quantity_target,
                        (COALESCE(dro.order_quantity, 0) - COALESCE(drto.order_quantity_target, 0)) AS quantity_diff,
                        ABS(COALESCE(dro.order_quantity, 0) - COALESCE(drto.order_quantity_target, 0)) AS quantity_diff_abs
                    FROM date_range_order dro
                    FULL OUTER JOIN date_range_target_order drto
                        ON dro.factory_code = drto.factory_code
                    WHERE ABS(COALESCE(dro.order_quantity, 0) - COALESCE(drto.order_quantity_target, 0)) >= $5
                ),
                whole_month_order AS (
                    SELECT fo.factory_code, SUM(fo.order_quantity) AS whole_month_order_quantity
                    FROM fact_order fo
                    JOIN dim_date dd ON fo.order_date = dd.date
                    WHERE dd.year = EXTRACT(YEAR FROM CAST($3 AS DATE))
                    AND dd.month = EXTRACT(MONTH FROM CAST($3 AS DATE))
                    AND fo.factory_code IN (SELECT factory_code FROM order_diff)
                    GROUP BY fo.factory_code
                ),
                planned_deliveries AS (
                    SELECT factory_code, SUM(order_quantity) AS planned_deliveries
                    FROM fact_order
                    WHERE estimated_delivery_date BETWEEN
                        CAST($2 AS DATE) + 1
                        AND (DATE_TRUNC('month', CAST($2 AS DATE)) + INTERVAL '1 month' - INTERVAL '1 day')::DATE
                    AND factory_code IN (SELECT factory_code FROM order_diff)
                    GROUP BY factory_code
                )
                SELECT 
                    df.factory_code, 
                    df.factory_name, 
                    df.salesman,
                    od.order_quantity,
                    od.order_quantity_target,
                    od.quantity_diff,
                    od.quantity_diff_abs,
                    COALESCE(od.quantity_diff / NULLIF(od.order_quantity_target, 0), 1) AS quantity_diff_pct,
                    COALESCE(wmo.whole_month_order_quantity, 0) AS whole_month_order_quantity,
                    COALESCE(pd.planned_deliveries, 0) AS planned_deliveries
                FROM order_diff od
                JOIN dim_factory df ON od.factory_code = df.factory_code
                LEFT JOIN whole_month_order wmo ON od.factory_code = wmo.factory_code
                LEFT JOIN planned_deliveries pd ON od.factory_code = pd.factory_code
                WHERE {quantity_filter}
                ORDER BY {order_clause}
                """.format(quantity_filter=quantity_filter, order_clause=order_clause)
        
        result = await execute_query(
            query=query,
            params=(date_range.date__gte,
                    date_range.date__lte,
                    date_range_target.date_target__gte,
                    date_range_target.date_target__lte,
                    threshold
                    ),
            fetch_all=True
        )

        if not result:
            logger.warning("No data found for the specified criteria")
            return []

        return result

    except Exception as e:
        logger.error(f"Error retrieving factory-order-range-diff: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve factory-order-range-diff: {str(e)}"
        )


@router.get("/product-sales-range-diff", response_model=List[ProductSalesRangeDiff])
async def get_factory_sales_range_diff(
    date_range: DateRangeParams = Depends(),
    date_range_target: DateRangeTargetParams = Depends(),
    factory: Optional[str] = None,
    permitted = Depends(has_permission())
) -> List[ProductSalesRangeDiff]:
    """
    Get sales by product for 2 date range
    Return the diff of these 2 date range
    """
    # Parse factory codes if provided
    factory_codes = []
    if factory:
        factory_codes = [code.strip() for code in factory.split(',')]

    factory_filter = ""
    params = [
        date_range.date__gte,
        date_range.date__lte,
        date_range_target.date_target__gte,
        date_range_target.date_target__lte
    ]
    
    if factory_codes:
        # Create placeholders for factory codes: $5, $6, $7, etc. 1-4 is date range and date range target
        placeholders = ', '.join([f'${i+5}' for i in range(len(factory_codes))])
        factory_filter = f"AND factory_code IN ({placeholders})"
        params.extend(factory_codes)

    try:
        query = """WITH date_range_sales AS (
                    SELECT product_name, SUM(sales_quantity) AS sales_quantity
                    FROM fact_sales
                    WHERE sales_date BETWEEN $1 AND $2 
                        {factory_filter}
                    GROUP BY product_name
                ),
                date_range_target_sales AS (
                    SELECT product_name, SUM(sales_quantity) AS sales_quantity_target
                    FROM fact_sales
                    WHERE sales_date BETWEEN $3 AND $4 
                        {factory_filter}
                    GROUP BY product_name
                )
                SELECT 
                    COALESCE(drs.product_name, drts.product_name) AS product_name,
                    COALESCE(drs.sales_quantity, 0) AS sales_quantity,
                    COALESCE(drts.sales_quantity_target, 0) AS sales_quantity_target,
                    (COALESCE(drs.sales_quantity, 0) - COALESCE(drts.sales_quantity_target, 0)) AS quantity_diff,
                    ABS(COALESCE(drs.sales_quantity, 0) - COALESCE(drts.sales_quantity_target, 0)) AS quantity_diff_abs
                FROM date_range_sales drs
                FULL OUTER JOIN date_range_target_sales drts 
                    ON drs.product_name = drts.product_name
                ORDER BY quantity_diff
                """.format(factory_filter=factory_filter)
        
        result = await execute_query(
            query=query,
            params=tuple(params),
            fetch_all=True
        )

        if not result:
            logger.warning("No data found for the specified criteria")
            return []

        return result

    except Exception as e:
        logger.error(f"Error retrieving product-sales-range-diff: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve product-sales-range-diff: {str(e)}"
        )


@router.get("/product-order-range-diff", response_model=List[ProductOrderRangeDiff])
async def get_factory_order_range_diff(
    date_range: DateRangeParams = Depends(),
    date_range_target: DateRangeTargetParams = Depends(),
    factory: Optional[str] = None,
    permitted = Depends(has_permission())
) -> List[ProductOrderRangeDiff]:
    """
    Get order by product for 2 date range
    Return the diff of these 2 date range
    """
    # Parse factory codes if provided
    factory_codes = []
    if factory:
        factory_codes = [code.strip() for code in factory.split(',')]

    factory_filter = ""
    params = [
        date_range.date__gte,
        date_range.date__lte,
        date_range_target.date_target__gte,
        date_range_target.date_target__lte
    ]
    
    if factory_codes:
        # Create placeholders for factory codes: $5, $6, $7, etc. 1-4 is date range and date range target
        placeholders = ', '.join([f'${i+5}' for i in range(len(factory_codes))])
        factory_filter = f"AND factory_code IN ({placeholders})"
        params.extend(factory_codes)

    try:
        query = """WITH date_range_order AS (
                    SELECT product_name, SUM(order_quantity) AS order_quantity
                    FROM fact_order
                    WHERE order_date BETWEEN $1 AND $2 
                        {factory_filter}
                    GROUP BY product_name
                ),
                date_range_target_order AS (
                    SELECT product_name, SUM(order_quantity) AS order_quantity_target
                    FROM fact_order
                    WHERE order_date BETWEEN $3 AND $4 
                        {factory_filter}
                    GROUP BY product_name
                )
                SELECT 
                    COALESCE(drs.product_name, drts.product_name) AS product_name,
                    COALESCE(drs.order_quantity, 0) AS order_quantity,
                    COALESCE(drts.order_quantity_target, 0) AS order_quantity_target,
                    (COALESCE(drs.order_quantity, 0) - COALESCE(drts.order_quantity_target, 0)) AS quantity_diff,
                    ABS(COALESCE(drs.order_quantity, 0) - COALESCE(drts.order_quantity_target, 0)) AS quantity_diff_abs
                FROM date_range_order drs
                    FULL OUTER JOIN date_range_target_order drts ON drs.product_name = drts.product_name
                ORDER BY quantity_diff
                """.format(factory_filter=factory_filter)
        
        result = await execute_query(
            query=query,
            params=tuple(params),
            fetch_all=True
        )

        if not result:
            logger.warning("No data found for the specified criteria")
            return []

        return result

    except Exception as e:
        logger.error(f"Error retrieving product-order-range-diff: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve product-order-range-diff: {str(e)}"
        )
    

@router.get("/scheduled-and-actual-sales", response_model=List[ScheduledAndActualSales])
async def get_scheduled_and_actual_sales(
    year: int = Query(datetime.now().year, ge=2020, le=datetime.now().year, description="Year"),
    factory: Optional[str] = None,
    permitted = Depends(has_permission())
) -> List[ScheduledAndActualSales]:
    """
    Compare scheduled deriveries and actual sales group by month
    """

    try:
        # Build query conditionally
        factory_filter_sales = "AND fs.factory_code = $2" if factory else ""
        factory_filter_order = "AND fo.factory_code = $2" if factory else ""
        
        query = f"""WITH actual_sales AS (
                    SELECT 
                        EXTRACT(MONTH FROM fs.sales_date) AS sales_month,
                        SUM(fs.sales_quantity) AS sales_quantity
                    FROM fact_sales fs
                    JOIN dim_date dd ON dd.date = fs.sales_date
                    WHERE dd.year = $1
                    {factory_filter_sales}
                    GROUP BY EXTRACT(MONTH FROM fs.sales_date)
                ),
                scheduled_delivery AS (
                    SELECT 
                        EXTRACT(MONTH FROM fo.estimated_delivery_date) AS scheduled_month,
                        SUM(fo.order_quantity) AS scheduled_quantity
                    FROM fact_order fo
                    JOIN dim_date dd ON dd.date = fo.estimated_delivery_date
                    WHERE dd.year = $1
                    {factory_filter_order}
                    GROUP BY EXTRACT(MONTH FROM fo.estimated_delivery_date)
                )
                SELECT 
                    sd.scheduled_month,
                    sd.scheduled_quantity,
                    COALESCE(acs.sales_quantity, 0) AS sales_quantity,
                    (COALESCE(acs.sales_quantity, 0) / sd.scheduled_quantity) AS sales_pct
                FROM scheduled_delivery sd
                LEFT JOIN actual_sales acs ON sd.scheduled_month = acs.sales_month
                ORDER BY sd.scheduled_month;
                """
        
        params = (year, factory) if factory else (year,)
        
        result = await execute_query(
            query=query,
            params=params,
            fetch_all=True
        )

        if not result:
            logger.warning("No data found for the specified criteria")
            return []

        return result

    except Exception as e:
        logger.error(f"Error retrieving scheduled-and-actual-sales: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve scheduled-and-actual-sales: {str(e)}"
        )


@router.get("/sales-overtime")
async def get_sales_pivot(
    year: str = Query(str(datetime.now().year)),
    group_by: str = Query("month", description="Comma-separated group by fields (e.g., year,quarter)"),
    factory: Optional[str] = None,
    product: Optional[str] = None,
    permitted = Depends(has_permission())
):
    """
    Pivot table for sales data with dynamic grouping
    """
    try:
        # Parse comma-separated values
        years_list = [int(y.strip()) for y in year.split(",")]
        group_by_list = [field.strip() for field in group_by.split(",")]
        
        # Validate group_by fields
        if not group_by_list:
            raise HTTPException(status_code=400, detail="At least one group_by field required")
        
        invalid_fields = [f for f in group_by_list if f not in TIME_GROUP_BY_MAPPING]
        if invalid_fields:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid group_by fields: {', '.join(invalid_fields)}. Valid options: {', '.join(TIME_GROUP_BY_MAPPING.keys())}"
            )
        
        # Build SELECT clause
        select_fields = [TIME_GROUP_BY_MAPPING[field] for field in group_by_list]
        select_clause = ", ".join(select_fields)
        
        # Build GROUP BY clause
        group_by_fields = [TIME_GROUP_BY_MAPPING[field] for field in group_by_list]
        group_by_clause = ", ".join(group_by_fields)
        
        # Build ORDER BY clause (same as GROUP BY)
        order_by_clause = ", ".join(group_by_fields)
        
        # Build params list and filters dynamically
        params = [years_list]  # $1
        param_index = 2
        
        factory_filter = ""
        if factory:
            factory_filter = f"AND fs.factory_code = ${param_index}"
            params.append(factory)
            param_index += 1
        
        product_filter = ""
        if product:
            product_filter = f"AND fs.product_name = ${param_index}"
            params.append(product)
            param_index += 1
        
        query = f"""
            SELECT
                {select_clause},
                SUM(fs.sales_quantity) as sales_quantity
            FROM fact_sales fs
            JOIN dim_factory df ON fs.factory_code = df.factory_code
            JOIN dim_product dp ON fs.product_name = dp.product_name
            JOIN dim_date dd ON fs.sales_date = dd.date
            WHERE dd.year = ANY($1)
            {factory_filter}
            {product_filter}
            GROUP BY {group_by_clause}
            ORDER BY {order_by_clause}
        """
        
        result = await execute_query(
            query=query,
            params=tuple(params),
            fetch_all=True
        )
        return result
    
    except Exception as e:
        logger.error(f"Error retrieving scheduled-and-actual-sales: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve scheduled-and-actual-sales: {str(e)}"
        )

@router.get("/is-same-month", response_model=List[IsSameMonth])
async def get_sales_pivot(
    date_range: DateRangeParams = Depends(),
    date_range_target: DateRangeTargetParams = Depends(),
    permitted = Depends(has_permission())
) -> List[IsSameMonth]:
    
    try:

        query = """WITH date_series AS (
                    -- Generate all months from both date ranges
                    SELECT DISTINCT dd.year, dd.month
                    FROM dim_date dd
                    WHERE dd.date BETWEEN $1 AND $2
                       OR dd.date BETWEEN $3 AND $4
                ),
                base_data AS (
                    SELECT
                        d_sales.year,
                        d_sales.month,
                        SUM(fs.sales_quantity) AS sales_quantity,
                        CASE
                            WHEN d_sales.month = d_order.month AND d_sales.year = d_order.year THEN 1
                            ELSE 0
                        END AS is_same_month
                    FROM fact_sales fs
                        JOIN fact_order fo ON fs.order_code = fo.order_code
                        JOIN dim_date d_sales ON fs.sales_date = d_sales.date
                        JOIN dim_date d_order ON fo.order_date = d_order.date
                    WHERE d_sales.date BETWEEN $1 AND $2
                        OR d_sales.date BETWEEN $3 AND $4
                    GROUP BY d_sales.year, d_sales.month, is_same_month
                ),
                aggregated_data AS (
                    SELECT
                        year,
                        month,
                        SUM(CASE WHEN is_same_month = 1 THEN sales_quantity ELSE 0 END) AS same_month_sales,
                        SUM(CASE WHEN is_same_month = 0 THEN sales_quantity ELSE 0 END) AS diff_month_sales,
                        SUM(sales_quantity) AS total_sales
                    FROM base_data
                    GROUP BY year, month
                ),
                order_data AS (
	                SELECT d_order.year, d_order.month, sum(order_quantity) AS total_order
	                FROM fact_order fo
	                	JOIN dim_date d_order ON fo.order_date = d_order.date
					WHERE d_order.date BETWEEN $1 AND $2
	                       OR d_order.date BETWEEN $3 AND $4
	                GROUP BY d_order.year, d_order.month
	            )
                SELECT
                    ds.year,
                    ds.month,
                    COALESCE(ad.same_month_sales, 0) AS same_month_sales,
                    COALESCE(ad.diff_month_sales, 0) AS diff_month_sales,
                    COALESCE(ad.total_sales, 0) AS total_sales,
                    COALESCE(od.total_order, 0) AS total_order
                FROM date_series ds
                    LEFT JOIN aggregated_data ad ON ds.year = ad.year AND ds.month = ad.month
                    LEFT JOIN order_data od ON ds.year = od.year AND ds.month = od.month
                ORDER BY ds.year, ds.month
                """

        result = await execute_query(
            query=query,
            params=(
                date_range.date__gte,
                date_range.date__lte,
                date_range_target.date_target__gte,
                date_range_target.date_target__lte
            ),
            fetch_all=True
        )
        return result
    
    except Exception as e:
        logger.error(f"Error retrieving is-same-month: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve is-same-month: {str(e)}"
        )

@router.get("/sales-order-pct-diff", response_model=SalesOrderPctDiff)
async def get_sales_pivot(
    date_range: DateRangeParams = Depends(),
    date_range_target: DateRangeTargetParams = Depends(),
    exclude_factory: str = Query('30673', description="Factory code to exclude"),
    permitted = Depends(has_permission())
) -> SalesOrderPctDiff:
    try:
        
        query = """WITH sales_diff AS (
                        SELECT 
                            dd.year,
                            dd.month,
                            SUM(fs.sales_quantity) AS sales_quantity,
                            SUM(CASE WHEN fs.factory_code != $5 THEN fs.sales_quantity ELSE 0 END) AS remain_sales_quantity
                        FROM fact_sales fs JOIN dim_date dd 
                        ON fs.sales_date = dd.date
                        WHERE dd.date BETWEEN $1 AND $2
                        OR dd.date BETWEEN $3 AND $4
                        GROUP BY dd.year, dd.month
                    ),
                    sales_pct_diff AS (
                        SELECT 
                            year, 
                            month,
                            sd.sales_quantity,
                            sd.remain_sales_quantity,
                            (sd.sales_quantity / LAG(sd.sales_quantity, 1, sd.sales_quantity) OVER (ORDER BY year, month)) - 1 AS sales_pct_diff,
                            (sd.remain_sales_quantity  / LAG(sd.remain_sales_quantity, 1, sd.remain_sales_quantity) OVER (ORDER BY year, month)) -1 AS remain_sales_pct_diff
                        FROM sales_diff sd
                    ),
                    order_diff AS (
                        SELECT 
                            dd.year,
                            dd.month,
                            SUM(fo.order_quantity) AS order_quantity,
                            SUM(CASE WHEN fo.factory_code != $5 THEN fo.order_quantity ELSE 0 END) AS remain_order_quantity
                        FROM fact_order fo JOIN dim_date dd 
                        ON fo.order_date = dd.date
                        WHERE dd.date BETWEEN $1 AND $2
                        OR dd.date BETWEEN $3 AND $4
                        GROUP BY dd.year, dd.month
                    ),
                    order_pct_diff AS (
                        SELECT 
                            year, 
                            month,
                            od.order_quantity,
                            od.remain_order_quantity,
                            (od.order_quantity / LAG(od.order_quantity, 1, od.order_quantity) OVER (ORDER BY year, month)) - 1 AS order_pct_diff,
                            (od.remain_order_quantity / LAG(od.remain_order_quantity, 1, od.remain_order_quantity) OVER (ORDER BY year, month)) -1 AS remain_order_pct_diff
                        FROM order_diff od
                    )
                    SELECT
                        spd.year,
                        spd.month,
                        sales_quantity,
                        sales_pct_diff,
                        remain_sales_quantity,
                        remain_sales_pct_diff,
                        order_quantity,
                        order_pct_diff,
                        remain_order_quantity,
                        remain_order_pct_diff
                    FROM sales_pct_diff spd
                        JOIN order_pct_diff opd ON spd.year = opd.year AND spd.month = opd.month
                    ORDER BY spd.year DESC, spd.month DESC
                    LIMIT 1
                """

        result = await execute_query(
            query=query,
            params=(
                date_range.date__gte,
                date_range.date__lte,
                date_range_target.date_target__gte,
                date_range_target.date_target__lte,
                exclude_factory
            ),
            fetch_all=False,
            fetch_one=True
        )
        return result
    
    except Exception as e:
        logger.error(f"Error retrieving sales-order-pct-diff: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve sales-order-pct-diff: {str(e)}"
        )


@router.get("/thinner-paint-ratio", response_model=PivotThinnerPaintRatio)
async def get_sales_pivot(
    year: int = Query(datetime.now().year, ge=2020, le=datetime.now().year, description="Year"),
    thinner: str = Query(
        default="原料溶劑 NL DUNG MOI,成品溶劑DUNG MOI TP",
        description="Comma-separated thinner product types"
    ),
    paint: str = Query(
        default="烤調色PM HAP,木調色PM GO,底漆 LOT,面漆 BONG",
        description="Comma-separated paint product types"
    )
) -> PivotThinnerPaintRatio:
    try:
        # Split comma-separated strings into lists
        thinner_list = [t.strip() for t in thinner.split(',')]
        paint_list = [p.strip() for p in paint.split(',')]
        
        thinner_placeholders = ", ".join([f"${i+2}" for i in range(len(thinner_list))])
        paint_placeholders = ", ".join([f"${i+2+len(thinner_list)}" for i in range(len(paint_list))])
        
        query = f"""WITH thinner_paint_sales AS (
                    SELECT
                        df.factory_code,
                        df.factory_name,
                        dd.month,
                        SUM(CASE WHEN dp.product_type IN ({thinner_placeholders}) 
                            THEN fs.sales_quantity ELSE 0 END) as sales_thinner_quantity,
                        SUM(CASE WHEN dp.product_type IN ({paint_placeholders}) 
                            THEN fs.sales_quantity ELSE 0 END) as sales_paint_quantity
                    FROM fact_sales fs
                    JOIN dim_date dd ON fs.sales_date = dd.date
                    JOIN dim_factory df ON fs.factory_code = df.factory_code
                    JOIN dim_product dp ON fs.product_name = dp.product_name
                    WHERE dd.year = $1
                    GROUP BY df.factory_code, df.factory_name, dd.month
                    )
                    SELECT 
                        factory_code,
                        factory_name,
                        month,
                        sales_thinner_quantity,
                        sales_paint_quantity,
                        CASE 
                            WHEN sales_thinner_quantity = 0 AND sales_paint_quantity = 0 THEN '0'
                            WHEN sales_thinner_quantity = 0 THEN CONCAT('0:', sales_paint_quantity)
                            WHEN sales_paint_quantity = 0 THEN CONCAT(sales_thinner_quantity, ':0')
                            ELSE CONCAT(
                                ROUND((sales_thinner_quantity / NULLIF(sales_paint_quantity, 0))::NUMERIC, 1)::TEXT, 
                                ':1'
                            )
                        END AS ratio
                    FROM thinner_paint_sales
                    WHERE sales_thinner_quantity != 0
                    OR sales_paint_quantity != 0
                    ORDER BY factory_code, month
                """
        
        params = [year] + thinner_list + paint_list
        
        result = await execute_query(
            query=query,
            params=tuple(params),
            fetch_all=True
        )

        # Convert to DataFrame
        df = pd.DataFrame(result)
        
        if df.empty:
            return PivotThinnerPaintRatio(
                thinner_data=[],
                paint_data=[],
                ratio_data=[]
            )
        
        # Create pivot tables
        thinner_pivot = df.pivot_table(
            index=['factory_code', 'factory_name'],
            columns='month',
            values='sales_thinner_quantity',
            fill_value=0
        ).reset_index()
        
        paint_pivot = df.pivot_table(
            index=['factory_code', 'factory_name'],
            columns='month',
            values='sales_paint_quantity',
            fill_value=0
        ).reset_index()
        
        ratio_pivot = df.pivot(
            index=['factory_code', 'factory_name'],
            columns='month',
            values='ratio'
        ).fillna('0').reset_index()
        
        # Get the latest month column (highest month number)
        month_columns = [col for col in thinner_pivot.columns if col not in ['factory_code', 'factory_name']]
        if month_columns:
            latest_month = max(month_columns, key=int)
            
            # Calculate total sales for latest month for sorting
            sort_column = 'total_latest_month'
            thinner_pivot[sort_column] = thinner_pivot[latest_month] + paint_pivot[latest_month]
            
            # Sort all pivots by the total of latest month (descending)
            thinner_pivot = thinner_pivot.sort_values(sort_column, ascending=False).drop(columns=[sort_column])
            
            # Apply same sorting to paint and ratio pivots
            paint_pivot = paint_pivot.loc[thinner_pivot.index]
            ratio_pivot = ratio_pivot.loc[thinner_pivot.index]
            
            # Reset index after sorting
            thinner_pivot = thinner_pivot.reset_index(drop=True)
            paint_pivot = paint_pivot.reset_index(drop=True)
            ratio_pivot = ratio_pivot.reset_index(drop=True)
        
        for pivot_df in [thinner_pivot, paint_pivot, ratio_pivot]:
            pivot_df.columns = [str(col) for col in pivot_df.columns] #JSON key must be string
        
        # Convert to dict for JSON response
        thinner_data = thinner_pivot.to_dict('records')
        paint_data = paint_pivot.to_dict('records')
        ratio_data = ratio_pivot.to_dict('records')

        return PivotThinnerPaintRatio(
            thinner_data=thinner_data,
            paint_data=paint_data,
            ratio_data=ratio_data
        )

    except Exception as e:
        logger.error(f"Error retrieving thinner-paint-ratio: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve thinner-paint-ratio: {str(e)}"
        )
    

@router.get("/fact-order", response_model=List[FactOrder])
async def get_fact_order(
    date_range: DateRangeParams = Depends(),
    permitted = Depends(has_permission())
) -> List[FactOrder]:
    """
    All column from fact order
    """
    try:
        query = """SELECT 
                        fo.order_date,
                        fo.order_code,
                        fo.ct_date,
                        fo.factory_code,
                        fo.factory_order_code,
                        fo.tax_type,
                        fo.department,
                        fo.salesman,
                        fo.deposit_rate,
                        fo.payment_registration_code,
                        fo.payment_registration_name,
                        fo.delivery_address,
                        fo.product_code,
                        fo.product_name,
                        fo.qc,
                        fo.warehouse_type,
                        fo.order_quantity,
                        fo.delivered_quantity,
                        fo.package_order_quantity,
                        fo.delivered_package_order_quantity,
                        fo.unit,
                        fo.package_unit,
                        fo.estimated_delivery_date,
                        fo.original_estimated_delivery_date,
                        fo.pre_ct,
                        fo.finish_code,
                        fo.import_timestamp,
                        fo.import_wh_timestamp,
                        df.factory_name
                    FROM fact_order fo
                    JOIN dim_factory df 
                        ON fo.factory_code = df.factory_code
                    WHERE fo.order_date BETWEEN $1 AND $2
                """
        
        fact_order_result = await execute_query(
            query=query,
            params=(date_range.date__gte,
                    date_range.date__lte),
            fetch_all=True
        )

        if not fact_order_result:
            logger.warning("No data found for the specified criteria")
            return []

        return fact_order_result

    except Exception as e:
        logger.error(f"Error retrieving fact_order: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve fact_order: {str(e)}"
        )


@router.get("/fact-sales", response_model=List[FactSales])
async def get_fact_sales(
    date_range: DateRangeParams = Depends(),
    permitted = Depends(has_permission())
) -> List[FactSales]:
    """
    All column from fact sales
    """
    try:
        query = """SELECT
                        fs.product_code,
                        fs.product_name,
                        fs.qc,
                        fs.factory_code,
                        fs.sales_date,
                        fs.sales_code,
                        fs.order_code,
                        fs.sales_quantity,
                        fs.unit,
                        fs.package_sales_quantity,
                        fs.package_unit,
                        fs.department,
                        fs.salesman,
                        fs.warehouse_code,
                        fs.warehouse_type,
                        fs.import_code,
                        fs.factory_order_code,
                        fs.import_timestamp,
                        fs.import_wh_timestamp,
                        df.factory_name
                    FROM fact_sales fs
                    JOIN dim_factory df
                        ON fs.factory_code = df.factory_code
                    WHERE fs.sales_date BETWEEN $1 AND $2
                """
        
        fact_sales_result = await execute_query(
            query=query,
            params=(date_range.date__gte,
                    date_range.date__lte),
            fetch_all=True
        )

        if not fact_sales_result:
            logger.warning("No data found for the specified criteria")
            return []

        return fact_sales_result

    except Exception as e:
        logger.error(f"Error retrieving fact_sales: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve fact_sales: {str(e)}"
        )


@router.get("/sales-bom", response_model=List[SalesBOM])
async def get_sales_bom(
    date_range: DateRangeParams = Depends(),
    factory: str | None = None,
    group_by: str | None = None,
    permitted = Depends(has_permission())
) -> List[SalesBOM]:
    """
    Get sales quantity in a time period and calculate its BOM
    """
    try:
        factory_array = factory.split(',') if factory else None

        # Define allowed columns to prevent SQL injection
        allowed_columns = {"factory_code", "factory_name", "product_name", "material_name"}
        
        # Build group_by_columns: user selections + material_name (always included)
        group_by_columns = []
        if group_by:
            user_columns = [col.strip() for col in group_by.split(',')]
            # Validate user columns
            if not all(col in allowed_columns for col in user_columns):
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid group_by columns. Allowed: {', '.join(allowed_columns)}"
                )
            # Filter out material_name from user input (we'll add it at the end)
            group_by_columns = [col for col in user_columns if col != "material_name"]
        
        # Always include material_name at the end
        group_by_columns.append("material_name")

        group_by_clause = ", ".join(group_by_columns)
        select_columns = group_by_clause
        
        # Add sales_quantity to SELECT if product_name is in group_by
        if "product_name" in group_by_columns:
            select_columns += ", ROUND(SUM(sales_quantity)::decimal,2) AS sales_quantity, ROUND(MAX(ratio),4) as ratio" # use MAX(ratio) to pypass group by

        query = f"""
            WITH bom_data AS (
                SELECT
                    df.factory_code,
                    df.factory_name,
                    fs.product_name,
                    fs.sales_quantity,
                    bpm.material_name,
                    bpm.ratio,
                    (fs.sales_quantity * bpm.ratio) AS material_quantity
                FROM fact_sales fs
                    JOIN dim_factory df ON df.factory_code = fs.factory_code
                    JOIN bridge_product_material bpm ON fs.product_name = bpm.product_name
                WHERE fs.sales_date BETWEEN $1 AND $2
                    AND ($3::text[] IS NULL OR fs.factory_code = ANY($3))
                    AND bpm.is_current = TRUE
            )
            SELECT {select_columns}, ROUND(SUM(material_quantity)::decimal,2) AS material_quantity
            FROM bom_data
            GROUP BY {group_by_clause}
            ORDER BY {group_by_clause}
        """
        
        sales_bom_result = await execute_query(
            query=query,
            params=(
                date_range.date__gte,
                date_range.date__lte,
                factory_array,
            ),
            fetch_all=True
        )

        if not sales_bom_result:
            logger.warning("No data found for the specified criteria")
            return []

        return sales_bom_result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving sales_bom_result: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve sales_bom_result: {str(e)}"
        )
    

@router.get("/order-bom", response_model=List[OrderBOM])
async def get_order_bom(
    date_range: DateRangeParams = Depends(),
    factory: str | None = None,
    group_by: str | None = None,
    permitted = Depends(has_permission())
) -> List[OrderBOM]:
    """
    Get order quantity in a time period and calculate its BOM
    """
    try:
        factory_array = factory.split(',') if factory else None

        # Define allowed columns to prevent SQL injection
        allowed_columns = {"factory_code", "factory_name", "product_name", "material_name"}
        
        # Build group_by_columns: user selections + material_name (always included)
        group_by_columns = []
        if group_by:
            user_columns = [col.strip() for col in group_by.split(',')]
            # Validate user columns
            if not all(col in allowed_columns for col in user_columns):
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid group_by columns. Allowed: {', '.join(allowed_columns)}"
                )
            # Filter out material_name from user input (we'll add it at the end)
            group_by_columns = [col for col in user_columns if col != "material_name"]
        
        # Always include material_name at the end
        group_by_columns.append("material_name")

        group_by_clause = ", ".join(group_by_columns)
        select_columns = group_by_clause
        
        # Add order_quantity to SELECT if product_name is in group_by
        if "product_name" in group_by_columns:
            select_columns += ", ROUND(SUM(order_quantity)::decimal,2) AS order_quantity, ROUND(MAX(ratio),4) as ratio" # use MAX(ratio) to pypass group by

        query = f"""
            WITH bom_data AS (
                SELECT
                    df.factory_code,
                    df.factory_name,
                    fo.product_name,
                    fo.order_quantity,
                    bpm.material_name,
                    bpm.ratio,
                    (fo.order_quantity * bpm.ratio) AS material_quantity
                FROM fact_order fo
                    JOIN dim_factory df ON df.factory_code = fo.factory_code
                    JOIN bridge_product_material bpm ON fo.product_name = bpm.product_name
                WHERE fo.order_date BETWEEN $1 AND $2
                    AND ($3::text[] IS NULL OR fo.factory_code = ANY($3))
                    AND bpm.is_current = TRUE
            )
            SELECT {select_columns}, ROUND(SUM(material_quantity)::decimal,2) AS material_quantity
            FROM bom_data
            GROUP BY {group_by_clause}
            ORDER BY {group_by_clause}
        """
        
        order_bom_result = await execute_query(
            query=query,
            params=(
                date_range.date__gte,
                date_range.date__lte,
                factory_array,
            ),
            fetch_all=True
        )

        if not order_bom_result:
            logger.warning("No data found for the specified criteria")
            return []

        return order_bom_result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving order_bom_result: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve order_bom_result: {str(e)}"
        )