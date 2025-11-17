import logging
from typing import Dict, List, Optional
from src.db.database import database

logger = logging.getLogger(__name__)

class Art17Repository:
    def __init__(self):
        self.db = database
    
    async def get_proveedor_profile(self, rut: str) -> Dict:
        """Obtiene perfil completo de un proveedor"""
        try:
            query = """
                SELECT 
                    request_id, proveedor_nombre, proveedor_rut,
                    status, nivel_riesgo, certificado_emitido, created_at
                FROM workflow_executions
                WHERE proveedor_rut = :rut
                ORDER BY created_at DESC
            """
            results = await self.db.fetch_all(query=query, values={"rut": rut})
            
            if not results:
                return {"exists": False, "rut": rut, "message": "Proveedor no encontrado"}
            
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

    async def search_workflows(
        self,
        query: Optional[str] = None,
        status: Optional[str] = None,
        riesgo: Optional[str] = None,
        monto_min: Optional[float] = None,
        monto_max: Optional[float] = None,
        fecha_desde: Optional[str] = None,
        fecha_hasta: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Dict:
        """Busca workflows con múltiples filtros"""
        try:
            conditions = ["w.proveedor_rut IS NOT NULL"]
            values = {}
            
            # Búsqueda de texto libre
            if query:
                conditions.append("""(
                    w.proveedor_rut ILIKE :query OR 
                    w.proveedor_nombre ILIKE :query OR
                    r.objeto_contrato ILIKE :query
                )""")
                values["query"] = f"%{query}%"
            
            # Filtros específicos
            if status:
                conditions.append("w.status = :status")
                values["status"] = status
            
            if riesgo:
                conditions.append("LOWER(w.nivel_riesgo) = LOWER(:riesgo)")
                values["riesgo"] = riesgo
            
            if monto_min is not None:
                conditions.append("r.monto_contrato >= :monto_min")
                values["monto_min"] = monto_min
            
            if monto_max is not None:
                conditions.append("r.monto_contrato <= :monto_max")
                values["monto_max"] = monto_max
            
            if fecha_desde:
                conditions.append("w.created_at >= :fecha_desde::timestamp")
                values["fecha_desde"] = fecha_desde
            
            if fecha_hasta:
                conditions.append("w.created_at <= :fecha_hasta::timestamp")
                values["fecha_hasta"] = fecha_hasta
            
            # Query principal
            query_sql = f"""
                SELECT 
                    w.request_id, w.proveedor_rut, w.proveedor_nombre,
                    w.status, w.nivel_riesgo, w.certificado_emitido,
                    w.created_at, r.monto_contrato, r.objeto_contrato
                FROM workflow_executions w
                LEFT JOIN requests r ON w.request_id = r.request_id
                WHERE {" AND ".join(conditions)}
                ORDER BY w.created_at DESC
                LIMIT :limit OFFSET :offset
            """
            
            values["limit"] = limit
            values["offset"] = offset
            
            # Ejecutar búsqueda
            results = await self.db.fetch_all(query=query_sql, values=values)
            
            # Contar total
            count_sql = f"""
                SELECT COUNT(*) as total
                FROM workflow_executions w
                LEFT JOIN requests r ON w.request_id = r.request_id
                WHERE {" AND ".join(conditions)}
            """
            count_values = {k: v for k, v in values.items() if k not in ['limit', 'offset']}
            total_result = await self.db.fetch_one(query=count_sql, values=count_values)
            
            return {
                "count": len(results),
                "total": total_result["total"] if total_result else 0,
                "limit": limit,
                "offset": offset,
                "results": [dict(row) for row in results]
            }
        except Exception as e:
            logger.error(f"Error en búsqueda de workflows: {e}")
            raise

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

    async def get_workflow_by_request_id(self, request_id: str) -> Optional[Dict]:
        """Obtiene un workflow completo por request_id"""
        try:
            query = """
                SELECT 
                    w.*, r.proveedor_nombre as request_proveedor_nombre,
                    r.monto_contrato, r.objeto_contrato, r.created_at as request_created_at
                FROM workflow_executions w
                JOIN requests r ON w.request_id = r.request_id
                WHERE w.request_id = :request_id
            """
            result = await self.db.fetch_one(query=query, values={"request_id": request_id})
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Error al obtener workflow {request_id}: {e}")
            raise

    async def get_audit_trail(self, request_id: str) -> List[Dict]:
        """Obtiene el trail de auditoría de una solicitud"""
        try:
            query = """
                SELECT id, action, user_id, details, timestamp
                FROM audit_log
                WHERE request_id = :request_id
                ORDER BY timestamp DESC
            """
            results = await self.db.fetch_all(query=query, values={"request_id": request_id})
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error al obtener audit trail de {request_id}: {e}")
            raise
