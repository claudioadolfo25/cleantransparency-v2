"""
Database module for connecting to Neon PostgreSQL
"""
import os
import asyncpg
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class Database:
    """Database connection manager for Neon PostgreSQL"""
    
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self.connection_string = os.getenv(
            'DATABASE_URL',
            os.getenv('NEON_DATABASE_URL')
        )
    
    async def connect(self):
        """Establish connection pool to database"""
        if self.pool is None:
            try:
                self.pool = await asyncpg.create_pool(
                    self.connection_string,
                    min_size=2,
                    max_size=10,
                    command_timeout=60,
                    timeout=30
                )
                logger.info("✅ Connected to Neon PostgreSQL database")
                
                async with self.pool.acquire() as conn:
                    version = await conn.fetchval('SELECT version()')
                    logger.info(f"Database version: {version}")
                    
            except Exception as e:
                logger.error(f"❌ Failed to connect to database: {e}")
                raise
    
    async def disconnect(self):
        """Close database connection pool"""
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
            logger.info("Disconnected from database")
    
    async def execute(self, query: str, *args):
        """Execute a query without returning results"""
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)
    
    async def fetch(self, query: str, *args):
        """Fetch all rows from a query"""
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)
    
    async def fetchrow(self, query: str, *args):
        """Fetch single row from a query"""
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
    
    async def fetchval(self, query: str, *args):
        """Fetch single value from a query"""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args)

# Global database instance
db = Database()

# Dependency for FastAPI
async def get_db():
    """FastAPI dependency for database access"""
    if db.pool is None:
        await db.connect()
    return db
