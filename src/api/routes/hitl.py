from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Dict, Optional
import logging

from src.db.database import is_connected
from src.db.repositories.hitl_repository import HITLRepository

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2/hitl", tags=["hitl"])

# ==================== MODELOS ====================

class HITLDecisionRequest(BaseModel):
    decision: str  # 'approve', 'reject', 'escalate'
    reviewer: str
    notes: Optional[str] = None

# ==================== ENDPOINTS ====================

@router.get("/cases/pending")
async def get_pending_cases(
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0)
) -> Dict:
    """
    Obtiene casos que requieren revisión humana.
    Ordenados por prioridad (riesgo) y fecha.
    """
    try:
        if not is_connected():
            raise HTTPException(status_code=503, detail="Base de datos no disponible")
        
        repo = HITLRepository()
        result = await repo.get_pending_hitl_cases(limit=limit, offset=offset)
        
        logger.info(f"Casos HITL pendientes consultados: {result['count']} de {result['total']}")
        return result
        
    except Exception as e:
        logger.error(f"Error obteniendo casos HITL: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cases/{request_id}")
async def get_case_detail(request_id: str) -> Dict:
    """
    Obtiene detalle completo de un caso HITL incluyendo:
    - Información del proveedor
    - Razón de escalamiento
    - Nivel de riesgo
    - Audit trail
    """
    try:
        if not is_connected():
            raise HTTPException(status_code=503, detail="Base de datos no disponible")
        
        repo = HITLRepository()
        case = await repo.get_hitl_case_detail(request_id)
        
        if not case:
            raise HTTPException(
                status_code=404,
                detail=f"Caso HITL {request_id} no encontrado"
            )
        
        return case
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error obteniendo detalle de caso {request_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cases/{request_id}/decision")
async def submit_decision(request_id: str, decision_data: HITLDecisionRequest) -> Dict:
    """
    Registra la decisión del revisor humano.
    
    **Decisiones posibles:**
    - `approve`: Aprobar el workflow
    - `reject`: Rechazar el workflow
    - `escalate`: Escalar a nivel superior
    """
    try:
        if not is_connected():
            raise HTTPException(status_code=503, detail="Base de datos no disponible")
        
        repo = HITLRepository()
        result = await repo.submit_hitl_decision(
            request_id=request_id,
            decision=decision_data.decision,
            reviewer=decision_data.reviewer,
            notes=decision_data.notes
        )
        
        logger.info(f"Decisión HITL registrada: {request_id} - {decision_data.decision}")
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error registrando decisión HITL: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_statistics() -> Dict:
    """
    Estadísticas del sistema HITL:
    - Total de casos
    - Casos pendientes
    - Aprobados/Rechazados/Escalados
    - Tiempo promedio de revisión
    """
    try:
        if not is_connected():
            raise HTTPException(status_code=503, detail="Base de datos no disponible")
        
        repo = HITLRepository()
        stats = await repo.get_hitl_statistics()
        
        return stats
        
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas HITL: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health_check() -> Dict:
    """Health check del módulo HITL"""
    return {
        "status": "ok",
        "service": "cleantransparency-hitl",
        "database_connected": is_connected(),
        "endpoints": {
            "pending_cases": "/api/v2/hitl/cases/pending",
            "case_detail": "/api/v2/hitl/cases/{request_id}",
            "submit_decision": "/api/v2/hitl/cases/{request_id}/decision",
            "statistics": "/api/v2/hitl/statistics"
        },
        "version": "2.0-hitl"
    }
