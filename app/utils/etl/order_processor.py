import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import logging
from typing import Dict, Any
import asyncpg

logger = logging.getLogger(__name__)

async def process_order_file(file_path: str, conn: asyncpg.Connection) -> Dict[str, Any]:
    """
    Process order Excel file and load to staging and fact tables
    
    Args:
        file_path: Path to the uploaded Excel file
        conn: AsyncPG database connection
        
    Returns:
        Dictionary with processing results
    """
    file_name = Path(file_path).name
    logger.info(f"Processing order file: {file_path}")
    
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
        df_copr13 = pd.read_excel(file_path)
        df_copr13.columns = [
            'order_date', 'ct_date', 'original_estimated_delivery_date', 'estimated_delivery_date',
            'order_code', 'factory_code', 'factory_name', 'product_code',
            'product_name', 'qc', 'order_quantity', 'delivered_quantity',
            'factory_order_code', 'note', 'numerical_order', 'path', 'warehouse_type'
        ]
        
        # Drop rows with missing critical data
        df_copr13.dropna(subset=['order_code', 'numerical_order'], inplace=True)
        
        # Format date columns
        date_cols = ['order_date', 'ct_date', 'estimated_delivery_date', 'original_estimated_delivery_date']
        for col in date_cols:
            df_copr13[col] = pd.to_datetime(df_copr13[col], dayfirst=True, errors='coerce')
        
        # Format numerical order and combine with order_code
        df_copr13['numerical_order'] = df_copr13['numerical_order'].astype(int).apply(lambda x: f"{int(float(x)):04}")
        df_copr13['order_code'] = df_copr13['order_code'] + "-" + df_copr13['numerical_order']
        
        # Replace NaN with None for database insertion
        df_copr13 = df_copr13.replace({np.nan: None})
        for col in date_cols:
            df_copr13[col] = df_copr13[col].astype(object).where(df_copr13[col].notnull(), None)
        
        # Convert specific text columns that might be floats to strings
        text_columns = [
            'factory_code', 'factory_order_code', 'currency', 'tax_type', 'channel', 
            'type', 'area', 'nation', 'path', 'path_2', 'department', 'salesman',
            'export_factory', 'register_price', 'note', 'deposit', 'deposit_rate',
            'payment_registration_code', 'payment_registration_name', 'register_transaction',
            'delivery_address', 'delivery_address_2', 'volumn_unit', 'numerical_order',
            'product_code', 'product_name', 'qc', 'factory_product_code', 'warehouse_type',
            'predict_code', 'factory_product_name', 'factory_qc', 'unit', 'small_unit',
            'package_unit', 'priced_unit', 'pre_ct', 'note_1', 'finish_code',
            'package_pt', 'package_name', 'project_code', 'project_name'
        ]
        
        for col in text_columns:
            if col in df_copr13.columns:
                df_copr13[col] = df_copr13[col].apply(
                    lambda x: str(x).replace('.0', '') if pd.notna(x) and x is not None else None
                )
        
        df_copr13['import_timestamp'] = datetime.now()
        
        # Add all missing columns expected in staging table
        all_columns = [
            'order_date', 'order_code', 'ct_date', 'factory_code', 'factory_name',
            'factory_order_code', 'currency', 'exchange_rate', 'tax_type',
            'channel', 'type', 'area', 'nation', 'path', 'path_2', 'department',
            'salesman', 'export_factory', 'register_price', 'note', 'deposit',
            'deposit_rate', 'payment_registration_code', 'payment_registration_name',
            'register_transaction', 'delivery_address', 'delivery_address_2', 'volumn_unit',
            'money_order', 'tax', 'total_quantity', 'gw', 'total_volumn', 'total_package',
            'numerical_order', 'product_code', 'product_name', 'qc',
            'factory_product_code', 'warehouse_type', 'predict_code',
            'factory_product_name', 'factory_qc', 'order_quantity',
            'delivered_quantity', 'package_order_quantity',
            'delivered_package_order_quantity', 'gift_quantity',
            'delivered_gift_quantity', 'package_gift_quantity',
            'delivered_package_gift_quantity', 'reserve_quantity',
            'delivered_reserve_quantity', 'package_reserve_quantity',
            'delivered_package_reserve_quantity', 'temporary_export_quantity',
            'package_temporary_export_quantity', 'unit', 'small_unit',
            'package_unit', 'price', 'money', 'priced_quantity',
            'estimated_delivery_date', 'original_estimated_delivery_date',
            'priced_unit', 'pre_ct', 'note_1', 'finish_code', 'package_pt',
            'package_name', 'weight_with_package', 'volumn_with_package',
            'project_code', 'project_name', 'import_timestamp'
        ]
        
        for col in all_columns:
            if col not in df_copr13.columns:
                df_copr13[col] = None
        
        df_copr13 = df_copr13[all_columns]
        
        # Step 2: Insert into staging table (copr13)
        staging_insert_query = """
            INSERT INTO copr13 (
                order_date, order_code, ct_date, factory_code, factory_name,
                factory_order_code, currency, exchange_rate, tax_type,
                channel, type, area, nation, path, path_2, department,
                salesman, export_factory, register_price, note, deposit,
                deposit_rate, payment_registration_code,
                payment_registration_name, register_transaction, delivery_address,
                delivery_address_2, volumn_unit, money_order, tax,
                total_quantity, gw, total_volumn, total_package,
                numerical_order, product_code, product_name, qc,
                factory_product_code, warehouse_type, predict_code,
                factory_product_name, factory_qc, order_quantity,
                delivered_quantity, package_order_quantity,
                delivered_package_order_quantity, gift_quantity,
                delivered_gift_quantity, package_gift_quantity,
                delivered_package_gift_quantity, reserve_quantity,
                delivered_reserve_quantity, package_reserve_quantity,
                delivered_package_reserve_quantity, temporary_export_quantity,
                package_temporary_export_quantity, unit, small_unit,
                package_unit, price, money, priced_quantity,
                estimated_delivery_date, original_estimated_delivery_date,
                priced_unit, pre_ct, note_1, finish_code, package_pt,
                package_name, weight_with_package, volumn_with_package,
                project_code, project_name, import_timestamp
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
                $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
                $41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
                $51, $52, $53, $54, $55, $56, $57, $58, $59, $60,
                $61, $62, $63, $64, $65, $66, $67, $68, $69, $70,
                $71, $72, $73, $74, $75, $76
            )
            ON CONFLICT (order_code) DO UPDATE SET
                order_quantity = EXCLUDED.order_quantity,
                delivered_quantity = EXCLUDED.delivered_quantity,
                import_timestamp = EXCLUDED.import_timestamp
        """
        
        successful_inserts = 0
        conflicts = 0
        
        async with conn.transaction():
            for _, row in df_copr13.iterrows():
                try:
                    await conn.execute(staging_insert_query, *tuple(row))
                    successful_inserts += 1
                except asyncpg.UniqueViolationError:
                    conflicts += 1
                except Exception as e:
                    logger.error(f"Error inserting order {row['order_code']}: {e}")
                    stats["errors"].append(f"Staging insert error for {row['order_code']}: {str(e)}")
        
        stats["staging_rows"] = successful_inserts
        stats["conflicts"] = conflicts
        
        logger.info(f"Staging complete: {successful_inserts} rows, {conflicts} conflicts")
        
        # Step 3: Get latest import timestamp from warehouse
        latest_import_query = "SELECT COALESCE(MAX(import_timestamp), '1900-01-01'::timestamp) FROM fact_order"
        latest_import = await conn.fetchval(latest_import_query)
        
        # Step 4: Get new data from staging
        staging_select_query = """
            SELECT 
                order_date, order_code, ct_date, factory_code, factory_order_code,
                tax_type, department, salesman, deposit_rate, payment_registration_code, 
                payment_registration_name, delivery_address, product_code, product_name, 
                qc, warehouse_type, order_quantity, delivered_quantity,
                package_order_quantity, delivered_package_order_quantity, unit, package_unit, 
                estimated_delivery_date, original_estimated_delivery_date, pre_ct, 
                finish_code, import_timestamp
            FROM copr13
            WHERE import_timestamp > $1
        """
        
        rows = await conn.fetch(staging_select_query, latest_import)
        
        if not rows:
            logger.info("No new data to process to warehouse")
            stats["finished_at"] = datetime.now().isoformat()
            return stats
        
        # Convert to DataFrame
        df_warehouse = pd.DataFrame([dict(row) for row in rows])
        
        # Step 5: Data transformations for warehouse
        for col in date_cols:
            if col in df_warehouse.columns:
                df_warehouse[col] = pd.to_datetime(df_warehouse[col], dayfirst=True, errors='coerce')
        
        # Filter by order code prefix
        df_warehouse['first_4_order_code'] = df_warehouse['order_code'].str.split("-").str[0]
        df_warehouse = df_warehouse[df_warehouse['first_4_order_code'] == '2201']
        df_warehouse.drop(columns=['first_4_order_code'], inplace=True)
        
        # Drop rows without qc
        df_warehouse.dropna(subset=['qc'], inplace=True)
        
        # Clean factory code
        df_warehouse['factory_code'] = df_warehouse['factory_code'].astype(str).str.replace('.0', '', regex=False)
        
        # Factory code mapping for KDT (30895.2)
        df_KDT = df_warehouse[df_warehouse['factory_code'] == '30895.2'][
            ['order_code', 'factory_code', 'factory_order_code']
        ].copy()
        
        if not df_KDT.empty:
            df_KDT['factory_order_code'] = df_KDT['factory_order_code'].fillna("temp")
            df_KDT.loc[df_KDT['factory_order_code'].str.contains('ST', case=False, na=False), 'factory_code'] = "30895.1"
            df_KDT.loc[df_KDT['factory_order_code'].str.contains('TN', case=False, na=False), 'factory_code'] = "30895"
            df_KDT.loc[df_KDT['factory_order_code'].str.contains('BP', case=False, na=False), 'factory_code'] = "30895.5"
            df_KDT.loc[df_KDT['factory_order_code'].str.contains('QT', case=False, na=False), 'factory_code'] = "30895.4"
            df_KDT.columns = ['order_code', 'factory_code_fixed', 'factory_order_code']
            
            df_warehouse = df_warehouse.merge(
                df_KDT[['order_code', 'factory_code_fixed']], 
                on='order_code', 
                how='left'
            )
            df_warehouse['factory_code'] = df_warehouse['factory_code_fixed'].combine_first(df_warehouse['factory_code'])
            df_warehouse.drop(columns=['factory_code_fixed'], inplace=True)
        
        # Replace NaN with None
        df_warehouse = df_warehouse.replace({np.nan: None})
        for col in date_cols:
            if col in df_warehouse.columns:
                df_warehouse[col] = df_warehouse[col].astype(object).where(df_warehouse[col].notnull(), None)
        
        df_warehouse['import_wh_timestamp'] = datetime.now()
        
        # Step 6: Insert into fact_order
        warehouse_insert_query = """
            INSERT INTO fact_order (
                order_date, order_code, ct_date, factory_code, factory_order_code,
                tax_type, department, salesman, deposit_rate, payment_registration_code,
                payment_registration_name, delivery_address, product_code, product_name,
                qc, warehouse_type, order_quantity, delivered_quantity,
                package_order_quantity, delivered_package_order_quantity, unit, package_unit,
                estimated_delivery_date, original_estimated_delivery_date, pre_ct,
                finish_code, import_timestamp, import_wh_timestamp
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21, $22, $23, $24, $25, $26, $27, $28
            )
            ON CONFLICT (order_code) DO UPDATE SET
                order_quantity = EXCLUDED.order_quantity,
                delivered_quantity = EXCLUDED.delivered_quantity,
                import_wh_timestamp = EXCLUDED.import_wh_timestamp
        """
        
        warehouse_rows = 0
        async with conn.transaction():
            for _, row in df_warehouse.iterrows():
                try:
                    await conn.execute(warehouse_insert_query, *tuple(row))
                    warehouse_rows += 1
                except Exception as e:
                    logger.error(f"Error inserting to warehouse for order {row['order_code']}: {e}")
                    stats["errors"].append(f"Warehouse insert error for {row['order_code']}: {str(e)}")
        
        stats["warehouse_rows"] = warehouse_rows
        
        logger.info(f"Warehouse load complete: {warehouse_rows} rows")
        
        # Step 7: Update dimension tables
        await update_factory_list(conn)
        await update_product_list(conn)
        
        stats["finished_at"] = datetime.now().isoformat()
        
        return stats
        
    except Exception as e:
        logger.error(f"Error processing order file: {e}", exc_info=True)
        stats["errors"].append(f"Processing error: {str(e)}")
        stats["finished_at"] = datetime.now().isoformat()
        raise



async def update_factory_list(conn: asyncpg.Connection) -> int:
    """
    Update dim_factory table with distinct factories from staging
    
    Args:
        conn: AsyncPG database connection
        
    Returns:
        Number of factories upserted
    """
    try:
        upsert_query = """
            INSERT INTO dim_factory (factory_code, factory_name, is_active, has_onsite, salesman)
            SELECT DISTINCT ON (factory_code)
                REPLACE(factory_code, '.0', '') as factory_code,
                factory_name,
                TRUE as is_active,
                FALSE as has_onsite,
                salesman
            FROM copr13
            WHERE factory_code IS NOT NULL
            ORDER BY factory_code, order_date DESC NULLS LAST
            ON CONFLICT (factory_code) DO NOTHING
        """
        
        result = await conn.execute(upsert_query)
        
        # Extract number of rows affected from result
        rows_affected = int(result.split()[-1]) if result else 0
        
        logger.info(f"Dim factory updated: {rows_affected} factories")
        return rows_affected
        
    except Exception as e:
        logger.error(f"Error updating factory list: {e}", exc_info=True)
        raise


async def update_product_list(conn: asyncpg.Connection) -> int:
    """
    Update dim_product table with distinct products from staging
    
    Args:
        conn: AsyncPG database connection
        
    Returns:
        Number of products upserted
    """
    try:
        upsert_query = """
            INSERT INTO dim_product (product_name)
            SELECT DISTINCT ON (product_name)
                product_name
            FROM copr13
            WHERE product_name IS NOT NULL
            ORDER BY product_name, product_name
            ON CONFLICT (product_name) DO NOTHING
        """
        
        result = await conn.execute(upsert_query)
        
        # Extract number of rows affected from result
        rows_affected = int(result.split()[-1]) if result else 0
        
        logger.info(f"Dim product updated: {rows_affected} products")
        return rows_affected
        
    except Exception as e:
        logger.error(f"Error updating product list: {e}", exc_info=True)
        raise