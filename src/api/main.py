from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.db.database import db
import logging
import sys

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
app.include_router(query_routes.router)  # FASE 1
app.include_router(search_stats_routes.router)  # 🆕 FASE 2A

# Health check endpoint (CRITICO para Cloud Run)
@app.get("/health")
async def health_check():
    """Health check endpoint para Cloud Run"""
    return {
        "status": "healthy",
        "service": "cleantransparency-v2",
        "version": "2.0-fase2a"
    }

# Root endpoint
@app.get("/")
async def root():
    return {
        "status": "ok",
        "message": "CLEANTRANSPARENCY v2 - FASE 2A: Búsquedas y Estadísticas",
        "version": "2.0-fase2a",
        "docs": "/docs",
        "fase_1_endpoints": {
            "workflow": "/api/v2/workflows/art17/{request_id}",
            "certificate": "/api/v2/certificates/{certificado_id}",
            "verify": "/api/v2/certificates/{certificado_id}/verify",
            "audit_trail": "/api/v2/audit/trail/{request_id}"
        },
        "fase_2a_endpoints": {
            "proveedor_profile": "/api/v2/proveedores/{rut}/profile",
            "search": "/api/v2/workflows/art17/search",
            "statistics": "/api/v2/workflows/art17/stats/summary"
        }
    }

# Eventos de lifecycle
@app.on_event("startup")
async def startup_event():
    logger.info("=== CLEANTRANSPARENCY v2 API iniciando ===")
    logger.info("Puerto: 8080")
    logger.info("Docs disponibles en: /docs")
    logger.info("🆕 FASE 2A: Búsquedas y Estadísticas activos")
    
    # Conectar a la base de datos
    try:
        await db.connect()
        logger.info("✅ Base de datos conectada")
    except Exception as e:
        logger.error(f"❌ Error conectando a BD: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Cerrando conexión a base de datos")
    await db.disconnect()
