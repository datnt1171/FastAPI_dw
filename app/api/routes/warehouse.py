from fastapi import APIRouter, Depends, HTTPException, Query
import logging
from typing import List
from app.core.auth import has_permission
from app.core.database import execute_query
from app.schemas.warehouse import Overall
from app.schemas.schema_helpers import validate_sql_results
from datetime import datetime

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/warehouse", tags=["warehouse"])

@router.get("/overall", response_model=List[Overall])
async def get_overall_warehouse_data(
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

        overall_data = validate_sql_results(overall_result, Overall)
        return overall_data

    except Exception as e:
        logger.error(f"Error retrieving overall_data: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve overall_data: {str(e)}"
        )