from databases import Database
import os
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.db = None
        self.connected = False
        
    async def connect(self):
        """Intenta conectar a la base de datos"""
        try:
            database_url = os.getenv("DATABASE_URL")
            if not database_url:
                logger.warning("⚠️ DATABASE_URL no configurada, modo sin BD")
                return
                
            self.db = Database(database_url)
            await self.db.connect()
            self.connected = True
            logger.info("✅ Base de datos conectada")
        except Exception as e:
            logger.error(f"❌ Failed to connect to database: {e}")
            logger.warning("⚠️ Continuando sin conexión a BD")
            self.connected = False
    
    async def disconnect(self):
        """Desconecta de la base de datos si está conectada"""
        if self.db and self.connected:
            await self.db.disconnect()
            logger.info("Base de datos desconectada")
    
    def is_connected(self):
        """Retorna True si la BD está conectada"""
        return self.connected

# Instancia global
db = DatabaseManager()
