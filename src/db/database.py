import os
import logging
import databases

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")

if not DATABASE_URL:
    logger.warning("⚠️ DATABASE_URL no configurada")
    database = None
else:
    logger.info(f"📦 DATABASE_URL: {DATABASE_URL[:50]}...")
    database = databases.Database(DATABASE_URL)


async def connect_db():
    if database is None:
        logger.warning("⚠️ No se puede conectar: database es None")
        return
    await database.connect()
    logger.info("✅ Conectado a PostgreSQL")


async def disconnect_db():
    if database is None:
        return
    await database.disconnect()
    logger.info("🔌 Desconectado de PostgreSQL")
