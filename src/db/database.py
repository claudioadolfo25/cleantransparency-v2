import databases
import os
import logging

logger = logging.getLogger(__name__)

# Obtener DATABASE_URL del entorno
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL no está configurada")

# Crear instancia de base de datos
db = databases.Database(DATABASE_URL)

async def connect_db():
    """Conecta a PostgreSQL"""
    try:
        await db.connect()
        logger.info("✅ Conectado a PostgreSQL (Neon.tech)")
    except Exception as e:
        logger.error(f"❌ Error conectando a PostgreSQL: {e}")
        raise

async def disconnect_db():
    """Desconecta de PostgreSQL"""
    try:
        await db.disconnect()
        logger.info("Conexión a PostgreSQL cerrada")
    except Exception as e:
        logger.error(f"Error cerrando PostgreSQL: {e}")
