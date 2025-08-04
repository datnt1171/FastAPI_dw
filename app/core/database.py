# app/core/database.py
import asyncpg
from typing import Dict, Any
import logging
from contextlib import asynccontextmanager

from app.core.config import settings
from app.core.sql_loader import sql_loader

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.pool: asyncpg.Pool = None
    
    async def init_pool(self):
        """Initialize connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                settings.get_database_url(),
                min_size=5,
                max_size=60,
                command_timeout=60
            )
            logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    async def close_pool(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")
    
    @asynccontextmanager
    async def get_connection(self):
        """Get database connection from pool"""
        if not self.pool:
            await self.init_pool()
        
        async with self.pool.acquire() as connection:
            yield connection

# Global instance
db_manager = DatabaseManager()

async def execute_sql_file(
    sql_file_path: str,
    params: Dict[str, Any] = None,
    fetch_one: bool = False,
    fetch_all: bool = True
) -> Any:
    """
    Execute SQL query from absolute file path with named parameters
    """
    query = sql_loader.load_query(sql_file_path)
    
    # Convert named parameters to positional for asyncpg
    if params:
        # Replace named parameters with $1, $2, etc.
        param_values = []
        modified_query = query
        
        for i, (key, value) in enumerate(params.items(), 1):
            modified_query = modified_query.replace(f":{key}", f"${i}")
            param_values.append(value)
        
        query = modified_query
        params_tuple = tuple(param_values)
    else:
        params_tuple = ()
    
    async with db_manager.get_connection() as conn:
        try:
            if settings.DEBUG:
                logger.debug(f"Executing SQL file: {sql_file_path}")
                logger.debug(f"Query: {query}")
                logger.debug(f"Params: {params}")
            
            if fetch_one:
                result = await conn.fetchrow(query, *params_tuple)
                return dict(result) if result else None
            elif fetch_all:
                results = await conn.fetch(query, *params_tuple)
                return [dict(row) for row in results]
            else:
                return await conn.execute(query, *params_tuple)
        except Exception as e:
            logger.error(f"Database query error in {sql_file_path}: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise