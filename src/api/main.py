from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.db.database import db, connect_db, disconnect_db
import logging
import sys
import os

# Importar routers
from src.api.routes import workflows, hitl, signing, query_routes, search_stats_routes

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="CLEANTRANSPARENCY v2 API",
    version="2.0-fase2a",
    description="API para certificacion Art. 17 Ley 21.595 - FASE 2A: Búsquedas y Estadísticas"
)

# CORS basico
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Registrar routers
app.include_router(workflows.router)
app.include_router(hitl.router)
app.include_router(signing.router)
app.include_router(query_routes.router)
app.include_router(search_stats_routes.router)

@app.on_event("startup")
async def startup():
    """Inicializar conexiones al arranque"""
    logger.info("=== CLEANTRANSPARENCY v2 API iniciando ===")
    logger.info(f"Puerto: {os.getenv('PORT', '8080')}")
    logger.info("Docs disponibles en: /docs")
    logger.info("🆕 FASE 2A: Búsquedas y Estadísticas activos")
    
    # Intentar conectar a la base de datos
    try:
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            logger.info("Conectando a PostgreSQL (Neon)...")
            await connect_db()
            logger.info("✅ Base de datos conectada")
        else:
            logger.warning("⚠️ DATABASE_URL no configurada, continuando sin BD")
    except Exception as e:
        logger.error(f"❌ Error conectando a base de datos: {e}")
        logger.warning("⚠️ Continuando sin conexión a BD")

@app.on_event("shutdown")
async def shutdown():
    """Cerrar conexiones al apagar"""
    logger.info("Cerrando conexión a base de datos")
    try:
        await disconnect_db()
    except Exception as e:
        logger.error(f"Error cerrando BD: {e}")

# Health check endpoint (CRITICO para Cloud Run)
@app.get("/health")
async def health_check():
    """Health check endpoint para Cloud Run"""
    return {
        "status": "healthy",
        "service": "cleantransparency-v2",
        "version": "2.0-fase2a",
        "database_connected": db.is_connected()
    }

@app.get("/")
async def root():
    """Endpoint raíz"""
    return {
        "status": "ok",
        "service": "workflows",
        "available_endpoints": [
            "POST /art17/run"
        ]
    }
