from fastapi import FastAPI
from .routes import workflows, hitl, signing
from fastapi.middleware.cors import CORSMiddleware
from src.db.database import db
import logging
import sys

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="CLEANTRANSPARENCY v2 API",
    version="2.0",
    description="API para certificacion Art. 17 Ley 21.595"
)

# CORS basico (luego lo endurecemos)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Registrar routers
app.include_router(workflows.router, prefix="/api/v2/workflows", tags=["workflows"])
app.include_router(hitl.router, prefix="/api/v2/hitl", tags=["hitl"])
app.include_router(signing.router, prefix="/api/v2/sign", tags=["signing"])

# Health check endpoint (CRITICO para Cloud Run)
@app.get("/health")
async def health_check():
    """Health check endpoint para Cloud Run"""
    return {
        "status": "healthy",
        "service": "cleantransparency-v2",
        "version": "2.0"
    }

# Root endpoint
@app.get("/")
async def root():
    return {
        "status": "ok",
        "message": "CLEANTRANSPARENCY v2 activo",
        "version": "2.0",
        "docs": "/docs"
    }

# Eventos de lifecycle
@app.on_event("startup")
async def startup_event():
    logger.info("=== CLEANTRANSPARENCY v2 API iniciando ===")
    logger.info("Puerto: 8080")
    logger.info("Docs disponibles en: /docs")
#Conectar a la base de datos    
   try:
        await db.connect()
        logger.info("✅ Base de datos conectada")
    except Exception as e:
        logger.error(f"❌ Error conectando a BD: {e}"
