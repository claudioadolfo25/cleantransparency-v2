import databases
import os
import logging

logger = logging.getLogger(__name__)

# Obtener DATABASE_URL del entorno
DATABASE_URL = os.getenv("DATABASE_URL", "")

if not DATABASE_URL or DATABASE_URL.strip() == "":
    logger.warning("⚠️ DATABASE_URL está vacía o no configurada")
    db = None
else:
    logger.info(f"✅ DATABASE_URL configurada: {DATABASE_URL[:50]}...")
    db = databases.Database(DATABASE_URL)

async def connect_db():
    """Conecta a PostgreSQL"""
    if db is None:
        logger.warning("⚠️ No se puede conectar: DATABASE_URL no configurada")
        return
    
    try:
        await db.connect()
        logger.info("✅ Conectado a PostgreSQL (Neon.tech)")
    except Exception as e:
        logger.error(f"❌ Error conectando a PostgreSQL: {e}")
        raise

async def disconnect_db():
    """Desconecta de PostgreSQL"""
    if db is None:
        return
        
    try:
        await db.disconnect()
        logger.info("Conexión a PostgreSQL cerrada")
    except Exception as e:
        logger.error(f"Error cerrando PostgreSQL: {e}")
