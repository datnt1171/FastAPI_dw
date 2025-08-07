# app/core/database.py
import asyncpg
from typing import Dict, Any
import logging
from contextlib import asynccontextmanager

from app.core.config import settings

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

async def get_db_connection():
    """Dependency to get database connection"""
    async with db_manager.get_connection() as conn:
        yield conn

async def execute_query(
    query: str, 
    params: tuple = None,
    fetch_one: bool = False,
    fetch_all: bool = True
) -> Any:
    """
    Execute a query and return results
    """
    async with db_manager.get_connection() as conn:
        try:
            if fetch_one:
                result = await conn.fetchrow(query, *(params or ()))
                return dict(result) if result else None
            elif fetch_all:
                results = await conn.fetch(query, *(params or ()))
                return [dict(row) for row in results]
            else:
                return await conn.execute(query, *(params or ()))
        except Exception as e:
            logger.error(f"Database query error: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            raise