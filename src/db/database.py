import os
import logging
import databases

logger = logging.getLogger(__name__)
DATABASE_URL = os.getenv("DATABASE_URL")

class Database:
    def __init__(self, url: str):
        self.url = url
        self.db = databases.Database(url) if url else None
    
    async def connect(self):
        if self.db:
            await self.db.connect()
            logger.info("✅ Conectado a PostgreSQL")
        else:
            logger.warning("⚠️ No hay DATABASE_URL definido")
    
    async def disconnect(self):
        if self.db:
            await self.db.disconnect()
            logger.info("🔌 Desconectado de PostgreSQL")
    
    def is_connected(self):
        # is_connected es una PROPIEDAD, no un método
        return self.db is not None and self.db.is_connected

# Instancia global que usa todo el proyecto
db = Database(DATABASE_URL)
database = db.db  # Alias for repositories that import 'database'

# Helper functions for main.py
async def connect_db():
    await db.connect()

async def disconnect_db():
    await db.disconnect()

def is_connected():
    return db.is_connected()
