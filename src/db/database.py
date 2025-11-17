import os
import logging
import databases

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

class Database:
    def __init__(self, url: str):
        self.db = databases.Database(url) if url else None

    async def connect(self):
        if self.db:
            await self.db.connect()
            logger.info("🔌 Conectado a PostgreSQL")

    async def disconnect(self):
        if self.db:
            await self.db.disconnect()
            logger.info("🔌 Desconectado de PostgreSQL")

    def is_connected(self):
        return self.db and self.db.is_connected

# Instancia global usada en TODO el proyecto
db = Database(DATABASE_URL)
