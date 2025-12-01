import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict, Any
import asyncpg

logger = logging.getLogger(__name__)

async def process_sales_file(file_path: str, conn: asyncpg.Connection) -> Dict[str, Any]:
    """
    Process sales Excel file and load to staging and fact tables
    
    Args:
        file_path: Path to the uploaded Excel file
        conn: AsyncPG database connection
        
    Returns:
        Dictionary with processing results
    """
    file_name = Path(file_path).name
    logger.info(f"Processing sales file: {file_path}")
    
    stats = {
        "file_name": file_name,
        "started_at": datetime.now().isoformat(),
        "staging_rows": 0,
        "warehouse_rows": 0,
        "conflicts": 0,
        "errors": []
    }
    
    try:
        # Step 1: Read and prepare Excel data
        df = pd.read_excel(file_path)
        
        try:
            df.columns = [
                'sales_date', 'ct_date', 'sales_code', 'factory_code',
                'factory_name', 'salesman', 'product_code', 'product_name', 'qc',
                'warehouse_code', 'sales_quantity', 'order_code', 'import_code',
                'note', 'factory_order_code'
            ]
        except Exception as e:
            error_msg = f"Column mismatch in Excel file: {e}"
            logger.error(error_msg)
            stats["errors"].append(error_msg)
            raise ValueError(error_msg)
        
        # Drop rows with missing sales_code
        df.dropna(subset=['sales_code'], inplace=True)
        
        # Format date columns
        df['sales_date'] = pd.to_datetime(df['sales_date'], dayfirst=True, errors='coerce')
        df['ct_date'] = pd.to_datetime(df['ct_date'], dayfirst=True, errors='coerce')
        
        # Clean factory code
        df['factory_code'] = df['factory_code'].astype(str).str.replace('.0', '', regex=False)
        
        # Generate numerical order and combine with sales_code
        df["numerical_order"] = (df.groupby("sales_code").cumcount() + 1).astype(str).str.zfill(4)
        df["sales_code"] = df["sales_code"] + "-" + df["numerical_order"]
        
        # Replace NaN with None
        df = df.replace({np.nan: None})
        
        # Convert text columns to string (handle floats from Excel)
        text_columns = [
            'factory_code', 'factory_name', 'salesman', 'product_code', 'product_name',
            'qc', 'warehouse_code', 'order_code', 'import_code', 'note', 'factory_order_code',
            'numerical_order', 'sales_code'
        ]
        
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: str(x).replace('.0', '') if pd.notna(x) and x is not None else None
                )
        
        df['import_timestamp'] = datetime.now()
        
        # Add all missing columns expected in staging table
        all_columns = [
            'product_code', 'product_name', 'qc', 'factory_code', 'factory_name',
            'sales_date', 'sales_code', 'order_code', 'sales_quantity', 'gift_quantity',
            'unit', 'small_unit', 'package_sales_quantity', 'package_gift_quantity',
            'package_unit', 'priced_quantity', 'priced_unit', 'currency', 'exchange_rate',
            'price', 'unpaid_tw', 'tax_tw', 'unpaid_vn', 'tax_vn', 'capital',
            'gross_profit', 'gross_profit_rate', 'lot_code', 'tax_type', 'department',
            'salesman', 'export_factory_code', 'export_factory', 'warehouse_code',
            'warehouse_type', 'warehouse_loc', 'import_code', 'note', 'factory_order_code',
            'import_timestamp'
        ]
        
        for col in all_columns:
            if col not in df.columns:
                df[col] = None
        
        df = df[all_columns]
        
        # Step 2: Insert into staging table (copr23)
        staging_insert_query = """
            INSERT INTO copr23 (
                product_code, product_name, qc, factory_code, factory_name, sales_date, sales_code,
                order_code, sales_quantity, gift_quantity, unit, small_unit, package_sales_quantity,
                package_gift_quantity, package_unit, priced_quantity, priced_unit, currency, exchange_rate,
                price, unpaid_tw, tax_tw, unpaid_vn, tax_vn, capital, gross_profit, gross_profit_rate,
                lot_code, tax_type, department, salesman, export_factory_code, export_factory, warehouse_code,
                warehouse_type, warehouse_loc, import_code, note, factory_order_code, import_timestamp
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                $31, $32, $33, $34, $35, $36, $37, $38, $39, $40
            )
            ON CONFLICT (sales_code) DO NOTHING
        """
        
        successful_inserts = 0
        conflicts = 0
        
        async with conn.transaction():
            for _, row in df.iterrows():
                try:
                    await conn.execute(staging_insert_query, *tuple(row))
                    successful_inserts += 1
                except asyncpg.UniqueViolationError:
                    conflicts += 1
                except Exception as e:
                    logger.error(f"Error inserting sales {row['sales_code']}: {e}")
                    stats["errors"].append(f"Staging insert error for {row['sales_code']}: {str(e)}")
        
        stats["staging_rows"] = successful_inserts
        stats["conflicts"] = conflicts
        
        logger.info(f"Staging complete: {successful_inserts} rows, {conflicts} conflicts")
        
        # Step 3: Get latest import timestamp from warehouse
        latest_import_query = "SELECT COALESCE(MAX(import_timestamp), '1900-01-01'::timestamp) FROM fact_sales"
        latest_import = await conn.fetchval(latest_import_query)
        
        # Step 4: Get new data from staging
        staging_select_query = """
            SELECT 
                product_code, product_name, qc, factory_code,
                sales_date, sales_code, order_code, sales_quantity,
                unit, package_sales_quantity, package_unit,
                department, salesman, warehouse_code, warehouse_type, 
                import_code, factory_order_code, import_timestamp
            FROM copr23
            WHERE import_timestamp > $1
        """
        
        rows = await conn.fetch(staging_select_query, latest_import)
        
        if not rows:
            logger.info("No new sales data to process to warehouse")
            stats["finished_at"] = datetime.now().isoformat()
            return stats
        
        # Convert to DataFrame
        df_warehouse = pd.DataFrame([dict(row) for row in rows])
        
        # Step 5: Data transformations for warehouse
        df_warehouse['sales_date'] = pd.to_datetime(df_warehouse['sales_date'], dayfirst=True, errors='coerce')

        # Filter by sales code prefix
        df_warehouse['first_4_sales_code'] = df_warehouse['sales_code'].str.split("-").str[0]
        before_filter_count = len(df_warehouse)
        df_warehouse = df_warehouse[df_warehouse['first_4_sales_code'].isin(['2301', '2302'])]
        after_filter_count = len(df_warehouse)

        if before_filter_count > after_filter_count:
            filtered_out = before_filter_count - after_filter_count
            logger.info(f"Filtered out {filtered_out} rows due to sales_code prefix not in ['2301', '2302']")

        df_warehouse.drop(columns=['first_4_sales_code'], inplace=True)

        # Drop rows without qc
        before_qc_filter = len(df_warehouse)
        missing_qc_codes = df_warehouse[df_warehouse['qc'].isna()]['sales_code'].tolist()
        df_warehouse.dropna(subset=['qc'], inplace=True)
        after_qc_filter = len(df_warehouse)

        if before_qc_filter > after_qc_filter:
            filtered_out = before_qc_filter - after_qc_filter
            logger.info(f"Filtered out {filtered_out} rows due to missing qc: {missing_qc_codes}")
        
        # Convert text columns to string
        text_columns_wh = [
            'factory_code', 'product_code', 'product_name', 'qc', 'order_code',
            'unit', 'package_unit', 'department', 'salesman', 'warehouse_code',
            'warehouse_type', 'import_code', 'factory_order_code', 'sales_code'
        ]
        
        for col in text_columns_wh:
            if col in df_warehouse.columns:
                df_warehouse[col] = df_warehouse[col].apply(
                    lambda x: str(x).replace('.0', '') if pd.notna(x) and x is not None else None
                )
        
        # Factory code mapping for KDT (30895.2)
        df_KDT = df_warehouse[df_warehouse['factory_code'] == '30895.2'][
            ['sales_code', 'factory_code', 'factory_order_code']
        ].copy()
        
        if not df_KDT.empty:
            df_KDT['factory_order_code'] = df_KDT['factory_order_code'].fillna("temp")
            df_KDT.loc[df_KDT['factory_order_code'].str.contains('ST', case=False, na=False), 'factory_code'] = "30895.1"
            df_KDT.loc[df_KDT['factory_order_code'].str.contains('TN', case=False, na=False), 'factory_code'] = "30895"
            df_KDT.loc[df_KDT['factory_order_code'].str.contains('BP', case=False, na=False), 'factory_code'] = "30895.5"
            df_KDT.loc[df_KDT['factory_order_code'].str.contains('QT', case=False, na=False), 'factory_code'] = "30895.4"
            df_KDT.columns = ['sales_code', 'factory_code_fixed', 'factory_order_code']
            
            df_warehouse = df_warehouse.merge(
                df_KDT[['sales_code', 'factory_code_fixed']], 
                on='sales_code', 
                how='left'
            )
            df_warehouse['factory_code'] = df_warehouse['factory_code_fixed'].combine_first(df_warehouse['factory_code'])
            df_warehouse.drop(columns=['factory_code_fixed'], inplace=True)
        
        # Replace NaN with None
        df_warehouse = df_warehouse.replace({np.nan: None})
        df_warehouse['sales_date'] = df_warehouse['sales_date'].astype(object).where(
            df_warehouse['sales_date'].notnull(), None
        )
        
        df_warehouse['import_wh_timestamp'] = datetime.now()
        
        # Step 6: Insert into fact_sales
        warehouse_insert_query = """
            INSERT INTO fact_sales (
                product_code, product_name, qc, factory_code,
                sales_date, sales_code, order_code, sales_quantity,
                unit, package_sales_quantity, package_unit,
                department, salesman, warehouse_code, warehouse_type,
                import_code, factory_order_code, import_timestamp, import_wh_timestamp
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19
            )
            ON CONFLICT (sales_code) DO UPDATE SET
                sales_quantity = EXCLUDED.sales_quantity,
                import_wh_timestamp = EXCLUDED.import_wh_timestamp
        """
        
        warehouse_rows = 0
        async with conn.transaction():
            for _, row in df_warehouse.iterrows():
                try:
                    await conn.execute(warehouse_insert_query, *tuple(row))
                    warehouse_rows += 1
                except Exception as e:
                    logger.error(f"Error inserting to warehouse for sales {row['sales_code']}: {e}")
                    stats["errors"].append(f"Warehouse insert error for {row['sales_code']}: {str(e)}")
        
        stats["warehouse_rows"] = warehouse_rows
        stats["finished_at"] = datetime.now().isoformat()
        
        logger.info(f"Warehouse load complete: {warehouse_rows} rows")
        
        return stats
        
    except Exception as e:
        logger.error(f"Error processing sales file: {e}", exc_info=True)
        stats["errors"].append(f"Processing error: {str(e)}")
        stats["finished_at"] = datetime.now().isoformat()
        raise