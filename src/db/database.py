import databases
import logging
from typing import Optional
import os

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self._db: Optional[databases.Database] = None
        self._connected = False
    
    async def connect(self, database_url: str = None):
        """Conecta a PostgreSQL (Neon)"""
        try:
            url = database_url or os.getenv("DATABASE_URL")
            
            if not url:
                raise ValueError("❌ DATABASE_URL no configurada")
            
            logger.info(f"Conectando a PostgreSQL: {url[:30]}...")
            self._db = databases.Database(url)
            await self._db.connect()
            self._connected = True
            logger.info("✅ Conectado a PostgreSQL (Neon)")
        except Exception as e:
            logger.error(f"❌ Error conectando a PostgreSQL: {e}")
            self._connected = False
            raise
    
    async def disconnect(self):
        """Cierra la conexión"""
        if self._db:
            await self._db.disconnect()
            self._connected = False
            logger.info("Conexión a PostgreSQL cerrada")
    
    def is_connected(self) -> bool:
        return self._connected and self._db is not None
    
    @property
    def db(self) -> databases.Database:
        if not self.is_connected():
            raise RuntimeError("Base de datos no conectada")
        return self._db
    
    async def fetch_all(self, query: str, values: dict = None):
        return await self._db.fetch_all(query=query, values=values or {})
    
    async def fetch_one(self, query: str, values: dict = None):
        return await self._db.fetch_one(query=query, values=values or {})
    
    async def execute(self, query: str, values: dict = None):
        return await self._db.execute(query=query, values=values or {})

db = Database()
