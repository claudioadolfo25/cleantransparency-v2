from fastapi import APIRouter, HTTPException
from typing import Dict
import logging

from src.db.database import db, is_connected
from src.db.repositories.art17_repository import Art17Repository

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v2", tags=["queries"])

# ==================== ENDPOINT 1: CONSULTAR WORKFLOW COMPLETO ====================

@router.get("/workflows/art17/{request_id}")
async def get_workflow(request_id: str) -> Dict:
    """
    Obtiene información completa de un workflow Art17 por request_id
    """
    try:
        if not is_connected():
            raise HTTPException(
                status_code=503,
                detail="Base de datos no disponible"
            )
        
        repo = Art17Repository(db.db)
        workflow = await repo.get_workflow_by_request_id(request_id)
        
        if not workflow:
            raise HTTPException(
                status_code=404,
                detail=f"Workflow con request_id '{request_id}' no encontrado"
            )
        
        response = {
            "request_id": workflow['request_id'],
            "status": workflow['status'],
            "proveedor": {
                "rut": workflow['proveedor_rut'],
                "nombre": workflow['proveedor_nombre']
            },
            "contrato": {
                "monto": float(workflow['monto_contrato']) if workflow['monto_contrato'] else None,
                "objeto": workflow['objeto_contrato']
            },
            "created_at": str(workflow['request_created_at'])
        }
        
        if workflow['workflow_execution_id']:
            response['workflow_execution'] = {
                "id": workflow['workflow_execution_id'],
                "type": workflow['workflow_type'],
                "timestamps": {
                    "inicio": str(workflow['ingest_timestamp']),
                    "fin": str(workflow['timestamp_final'])
                },
                "hashes": {
                    "ingest": workflow['hash_ingest'],
                    "riesgo": workflow['hash_riesgo'],
                    "compliance": workflow['hash_compliance'],
                    "final": workflow['hash_final']
                },
                "resultados": {
                    "nivel_riesgo": workflow['riesgo'],
                    "cumplimiento": bool(workflow['cumplimiento'])
                },
                "metadata": workflow['metadata']
            }
        
        if workflow['certificado_id']:
            response['certificado'] = {
                "id": workflow['certificado_id'],
                "issued_at": str(workflow['certificado_issued_at']),
                "hash_final": workflow['hash_final']
            }
        
        logger.info(f"Workflow {request_id} consultado exitosamente")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al consultar workflow {request_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== ENDPOINT 2: VER CERTIFICADO ====================

@router.get("/certificates/{certificado_id}")
async def get_certificate(certificado_id: str) -> Dict:
    """
    Obtiene información completa de un certificado por su ID
    """
    try:
        if not is_connected():
            raise HTTPException(
                status_code=503,
                detail="Base de datos no disponible"
            )
        
        repo = Art17Repository(db.db)
        cert = await repo.get_certificate_by_id(certificado_id)
        
        if not cert:
            raise HTTPException(
                status_code=404,
                detail=f"Certificado '{certificado_id}' no encontrado"
            )
        
        response = {
            "certificado_id": cert['certificado_id'],
            "request_id": cert['request_id'],
            "issued_at": str(cert['issued_at']),
            "hash_final": cert['hash_final'],
            "proveedor": {
                "rut": cert['proveedor_rut'],
                "nombre": cert['proveedor_nombre']
            },
            "contrato": {
                "monto": float(cert['monto_contrato']) if cert['monto_contrato'] else None,
                "objeto": cert['objeto_contrato']
            },
            "evaluacion": {
                "nivel_riesgo": cert['riesgo'],
                "cumplimiento": bool(cert['cumplimiento']),
                "workflow_type": cert['workflow_type']
            },
            "verificacion": {
                "url": f"/api/v2/certificates/{certificado_id}/verify",
                "metodo": "GET"
            }
        }
        
        logger.info(f"Certificado {certificado_id} consultado exitosamente")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al consultar certificado {certificado_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== ENDPOINT 3: VERIFICAR INTEGRIDAD ====================

@router.get("/certificates/{certificado_id}/verify")
async def verify_certificate(certificado_id: str) -> Dict:
    """
    Verifica la integridad y validez de un certificado
    """
    try:
        if not is_connected():
            raise HTTPException(
                status_code=503,
                detail="Base de datos no disponible"
            )
        
        repo = Art17Repository(db.db)
        verification = await repo.verify_certificate_integrity(certificado_id)
        
        if not verification['exists']:
            raise HTTPException(
                status_code=404,
                detail=f"Certificado '{certificado_id}' no encontrado"
            )
        
        if verification['valid']:
            verification['confianza'] = "ALTA"
            verification['mensaje'] = "✅ Certificado válido e íntegro"
        else:
            verification['confianza'] = "BAJA"
            verification['mensaje'] = "⚠️ Certificado con problemas de integridad"
        
        if not verification.get('hash_match', False):
            verification['advertencia'] = "El hash del certificado no coincide con el hash final del workflow"
        
        if not verification.get('chain_complete', False):
            verification['advertencia'] = "La cadena de hashes está incompleta"
        
        logger.info(f"Verificación de certificado {certificado_id}: {'VÁLIDO' if verification['valid'] else 'INVÁLIDO'}")
        return verification
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al verificar certificado {certificado_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== ENDPOINT 4: TRAIL DE AUDITORÍA ====================

@router.get("/audit/trail/{request_id}")
async def get_audit_trail(request_id: str) -> Dict:
    """
    Obtiene el trail completo de auditoría para una solicitud
    """
    try:
        if not is_connected():
            raise HTTPException(
                status_code=503,
                detail="Base de datos no disponible"
            )
        
        repo = Art17Repository(db.db)
        trail = await repo.get_audit_trail(request_id)
        
        if not trail['exists']:
            raise HTTPException(
                status_code=404,
                detail=f"Request '{request_id}' no encontrado"
            )
        
        trail['resumen'] = {
            "total_etapas": len(trail['workflow_stages']),
            "total_eventos": len(trail['audit_events']),
            "estado_actual": trail['status'],
            "completado": trail['status'] == 'completed'
        }
        
        all_hashes = [stage.get('hash') for stage in trail['workflow_stages'] if stage.get('hash')]
        trail['integridad'] = {
            "hashes_completos": len(all_hashes) >= 4,
            "total_hashes": len(all_hashes),
            "cadena_intacta": len(all_hashes) > 0
        }
        
        logger.info(f"Audit trail de {request_id} generado exitosamente")
        return trail
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error al generar audit trail de {request_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== ENDPOINT BONUS: HEALTH CHECK ====================

@router.get("/health/queries")
async def health_check() -> Dict:
    """Health check para los endpoints de consulta"""
    return {
        "status": "ok",
        "service": "cleantransparency-queries",
        "database_connected": is_connected(),
        "endpoints": {
            "workflow": "/api/v2/workflows/art17/{request_id}",
            "certificate": "/api/v2/certificates/{certificado_id}",
            "verify": "/api/v2/certificates/{certificado_id}/verify",
            "audit_trail": "/api/v2/audit/trail/{request_id}"
        },
        "version": "2.0-fase1"
    }
