from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self._db: Optional[firestore.AsyncClient] = None
        self._connected = False
    
    async def connect(self):
        """Conecta a Firestore"""
        try:
            # Usar AsyncClient para operaciones asíncronas
            self._db = firestore.AsyncClient(
                project="cleantransparency-prod2",
                database="cleantransparency-db"
            )
            self._connected = True
            logger.info("✅ Conectado a Firestore: cleantransparency-db")
        except Exception as e:
            logger.error(f"❌ Error conectando a Firestore: {e}")
            self._connected = False
            raise
    
    async def disconnect(self):
        """Cierra la conexión"""
        if self._db:
            self._db.close()
            self._connected = False
            logger.info("Conexión a Firestore cerrada")
    
    def is_connected(self) -> bool:
        """Verifica si está conectado"""
        return self._connected and self._db is not None
    
    @property
    def db(self) -> firestore.AsyncClient:
        """Retorna la instancia de Firestore"""
        if not self.is_connected():
            raise RuntimeError("Base de datos no conectada. Llama a connect() primero.")
        return self._db

# Instancia global
db = Database()
