from fastapi import APIRouter, HTTPException, Query
from typing import Dict, Optional
import logging

from src.db.database import db, is_connected
from src.db.repositories.art17_repository import Art17Repository

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2", tags=["search-stats"])

# ==================== ENDPOINT 1: PERFIL DE PROVEEDOR ====================

@router.get("/proveedores/{rut}/profile")
async def get_proveedor_profile(rut: str) -> Dict:
    """
    Obtiene perfil completo de un proveedor incluyendo:
    - Estadísticas generales
    - Historial de riesgo
    - Historial de cumplimiento
    - Certificados recientes
    - Score de confianza
    """
    try:
        if not is_connected():
            raise HTTPException(
                status_code=503,
                detail="Base de datos no disponible"
            )
        
        repo = Art17Repository()
        profile = await repo.get_proveedor_profile(rut)
        
        if not profile['exists']:
            raise HTTPException(
                status_code=404,
                detail=f"Proveedor con RUT '{rut}' no encontrado"
            )
        
        logger.info(f"Perfil de proveedor {rut} consultado exitosamente")
        return profile
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al consultar perfil de proveedor {rut}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== ENDPOINT 2: BÚSQUEDA AVANZADA ====================

@router.get("/workflows/art17/search")
async def search_workflows(
    query: Optional[str] = Query(None, description="Texto a buscar en RUT, nombre, objeto"),
    status: Optional[str] = Query(None, description="Estado: completed, processing, failed"),
    riesgo: Optional[str] = Query(None, description="Nivel de riesgo: BAJO, MEDIO, ALTO"),
    monto_min: Optional[float] = Query(None, description="Monto mínimo del contrato"),
    monto_max: Optional[float] = Query(None, description="Monto máximo del contrato"),
    fecha_desde: Optional[str] = Query(None, description="Fecha desde (YYYY-MM-DD)"),
    fecha_hasta: Optional[str] = Query(None, description="Fecha hasta (YYYY-MM-DD)"),
    limit: int = Query(50, ge=1, le=100, description="Límite de resultados"),
    offset: int = Query(0, ge=0, description="Offset para paginación")
) -> Dict:
    """
    Búsqueda avanzada de workflows con múltiples filtros:
    
    - **query**: Búsqueda de texto libre en RUT, nombre proveedor, objeto contrato
    - **status**: Filtrar por estado (completed, processing, failed)
    - **riesgo**: Filtrar por nivel de riesgo (BAJO, MEDIO, ALTO)
    - **monto_min/monto_max**: Rango de montos
    - **fecha_desde/fecha_hasta**: Rango de fechas
    - **limit/offset**: Paginación
    
    Ejemplo:
    `/api/v2/workflows/art17/search?riesgo=BAJO&monto_min=10000000&limit=20`
    """
    try:
        if not is_connected():
            raise HTTPException(
                status_code=503,
                detail="Base de datos no disponible"
            )
        
        repo = Art17Repository()
        results = await repo.search_workflows(
            query=query,
            status=status,
            riesgo=riesgo,
            monto_min=monto_min,
            monto_max=monto_max,
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
            limit=limit,
            offset=offset
        )
        
        logger.info(f"Búsqueda ejecutada: {results['count']} resultados de {results['total']} totales")
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en búsqueda de workflows: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== ENDPOINT 3: ESTADÍSTICAS GENERALES ====================

@router.get("/workflows/art17/stats/summary")
async def get_statistics_summary() -> Dict:
    """
    Obtiene estadísticas generales del sistema:
    
    - Total de solicitudes (completadas, en proceso, fallidas)
    - Distribución de niveles de riesgo
    - Tasa de cumplimiento
    - Certificados emitidos
    - Tiempo promedio de procesamiento
    - Top 10 proveedores
    - Montos totales y promedios
    
    Ideal para dashboards y reportes ejecutivos.
    """
    try:
        if not is_connected():
            raise HTTPException(
                status_code=503,
                detail="Base de datos no disponible"
            )
        
        repo = Art17Repository()
        summary = await repo.get_statistics_summary()
        
        logger.info("Estadísticas generales consultadas exitosamente")
        return summary
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al obtener estadísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== ENDPOINT BONUS: HEALTH CHECK ====================

@router.get("/health/search-stats")
async def health_check() -> Dict:
    """Health check para endpoints de búsqueda y estadísticas"""
    return {
        "status": "ok",
        "service": "cleantransparency-search-stats",
        "database_connected": is_connected(),
        "endpoints": {
            "proveedor_profile": "/api/v2/proveedores/{rut}/profile",
            "search": "/api/v2/workflows/art17/search",
            "statistics": "/api/v2/workflows/art17/stats/summary"
        },
        "version": "2.0-fase2a"
    }
