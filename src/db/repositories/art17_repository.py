import logging
from typing import Dict, List, Optional
from src.db.database import database

logger = logging.getLogger(__name__)

class Art17Repository:
    def __init__(self):
        self.db = database
    
    # ==================== MÉTODO 1: PERFIL DE PROVEEDOR ====================
    
    async def get_proveedor_profile(self, rut: str) -> Dict:
        """Obtiene perfil completo de un proveedor"""
        try:
            query = """
                SELECT 
                    request_id,
                    proveedor_nombre,
                    proveedor_rut,
                    status,
                    nivel_riesgo,
                    certificado_emitido,
                    created_at
                FROM workflow_executions
                WHERE proveedor_rut = :rut
                ORDER BY created_at DESC
            """
            results = await self.db.fetch_all(query=query, values={"rut": rut})
            
            if not results:
                return {
                    "exists": False,
                    "rut": rut,
                    "message": "Proveedor no encontrado"
                }
            
            return {
                "exists": True,
                "rut": rut,
                "nombre": results[0]["proveedor_nombre"],
                "total_solicitudes": len(results),
                "solicitudes": [dict(row) for row in results],
                "certificados_activos": sum(1 for r in results if r["certificado_emitido"]),
                "ultima_solicitud": str(results[0]["created_at"]) if results else None
            }
        except Exception as e:
            logger.error(f"Error al obtener perfil de proveedor {rut}: {e}")
            raise

    # ==================== MÉTODO 2: BÚSQUEDA DE WORKFLOWS ====================
    
    async def search_workflows(self, filters: Dict) -> List[Dict]:
        """Busca workflows con filtros"""
        try:
            conditions = ["1=1"]
            values = {}
            
            if filters.get("proveedor_rut"):
                conditions.append("proveedor_rut = :proveedor_rut")
                values["proveedor_rut"] = filters["proveedor_rut"]
            
            if filters.get("status"):
                conditions.append("status = :status")
                values["status"] = filters["status"]
            
            if filters.get("nivel_riesgo"):
                conditions.append("nivel_riesgo = :nivel_riesgo")
                values["nivel_riesgo"] = filters["nivel_riesgo"]
            
            query = f"""
                SELECT 
                    request_id,
                    proveedor_rut,
                    proveedor_nombre,
                    status,
                    nivel_riesgo,
                    certificado_emitido,
                    created_at
                FROM workflow_executions
                WHERE {" AND ".join(conditions)}
                ORDER BY created_at DESC
                LIMIT :limit OFFSET :offset
            """
            
            values["limit"] = filters.get("limit", 50)
            values["offset"] = filters.get("offset", 0)
            
            results = await self.db.fetch_all(query=query, values=values)
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error en búsqueda de workflows: {e}")
            raise

    # ==================== MÉTODO 3: ESTADÍSTICAS GENERALES ====================
    
    async def get_statistics_summary(self) -> Dict:
        """Obtiene estadísticas generales del sistema"""
        try:
            query = """
                SELECT 
                    COUNT(*) as total_solicitudes,
                    COUNT(DISTINCT proveedor_rut) as total_proveedores,
                    COUNT(*) FILTER (WHERE status = 'completed') as completadas,
                    COUNT(*) FILTER (WHERE status = 'in_progress') as en_proceso,
                    COUNT(*) FILTER (WHERE certificado_emitido = true) as certificados_emitidos,
                    COUNT(*) FILTER (WHERE nivel_riesgo = 'alto') as riesgo_alto,
                    COUNT(*) FILTER (WHERE nivel_riesgo = 'medio') as riesgo_medio,
                    COUNT(*) FILTER (WHERE nivel_riesgo = 'bajo') as riesgo_bajo
                FROM workflow_executions
                WHERE proveedor_rut IS NOT NULL
            """
            result = await self.db.fetch_one(query=query)
            return dict(result) if result else {}
        except Exception as e:
            logger.error(f"Error al obtener estadísticas: {e}")
            raise

    # ==================== MÉTODO 4: WORKFLOW POR ID ====================
    
    async def get_workflow_by_request_id(self, request_id: str) -> Optional[Dict]:
        """Obtiene un workflow completo por request_id"""
        try:
            query = """
                SELECT 
                    w.*,
                    r.proveedor_nombre as request_proveedor_nombre,
                    r.monto_contrato,
                    r.objeto_contrato,
                    r.created_at as request_created_at
                FROM workflow_executions w
                JOIN requests r ON w.request_id = r.request_id
                WHERE w.request_id = :request_id
            """
            result = await self.db.fetch_one(query=query, values={"request_id": request_id})
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Error al obtener workflow {request_id}: {e}")
            raise

    # ==================== MÉTODO 5: AUDIT TRAIL ====================
    
    async def get_audit_trail(self, request_id: str) -> List[Dict]:
        """Obtiene el trail de auditoría de una solicitud"""
        try:
            query = """
                SELECT 
                    id,
                    action,
                    user_id,
                    details,
                    timestamp
                FROM audit_log
                WHERE request_id = :request_id
                ORDER BY timestamp DESC
            """
            results = await self.db.fetch_all(query=query, values={"request_id": request_id})
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error al obtener audit trail de {request_id}: {e}")
            raise
