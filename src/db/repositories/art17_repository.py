import databases
from typing import Dict, List, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class Art17Repository:
    def __init__(self, db: databases.Database):
        self.db = db
    
    # ==================== MÉTODO 1: PERFIL DE PROVEEDOR ====================
    
    async def get_proveedor_profile(self, rut: str) -> Dict:
        """Obtiene perfil completo de un proveedor"""
        try:
            # Consultar todos los workflows de este proveedor
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
            
            rows = await self.db.fetch_all(query=query, values={"rut": rut})
            
            if not rows:
                return {"exists": False, "rut": rut}
            
            # Calcular estadísticas
            total = len(rows)
            completadas = sum(1 for r in rows if r['status'] == 'completed')
            en_proceso = sum(1 for r in rows if r['status'] == 'processing')
            fallidas = sum(1 for r in rows if r['status'] == 'failed')
            
            # Distribución de riesgo
            bajo = sum(1 for r in rows if r.get('nivel_riesgo') == 'BAJO')
            medio = sum(1 for r in rows if r.get('nivel_riesgo') == 'MEDIO')
            alto = sum(1 for r in rows if r.get('nivel_riesgo') == 'ALTO')
            
            # Certificados
            certificados = sum(1 for r in rows if r.get('certificado_emitido') == True)
            
            return {
                "exists": True,
                "rut": rut,
                "nombre": rows[0]['proveedor_nombre'] if rows else "Desconocido",
                "estadisticas": {
                    "total_solicitudes": total,
                    "completadas": completadas,
                    "en_proceso": en_proceso,
                    "fallidas": fallidas
                },
                "distribucion_riesgo": {
                    "bajo": bajo,
                    "medio": medio,
                    "alto": alto
                },
                "certificados_emitidos": certificados,
                "historial_reciente": [
                    {
                        "request_id": r['request_id'],
                        "status": r['status'],
                        "nivel_riesgo": r.get('nivel_riesgo'),
                        "fecha": str(r['created_at'])
                    }
                    for r in rows[:10]  # Últimas 10
                ]
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo perfil de proveedor {rut}: {e}")
            raise
    
    # ==================== MÉTODO 2: BÚSQUEDA AVANZADA ====================
    
    async def search_workflows(
        self,
        status: Optional[str] = None,
        nivel_riesgo: Optional[str] = None,
        proveedor_rut: Optional[str] = None,
        fecha_desde: Optional[str] = None,
        fecha_hasta: Optional[str] = None,
        limit: int = 50,
        offset: int = 0
    ) -> Dict:
        """Búsqueda avanzada de workflows"""
        try:
            conditions = []
            values = {}
            
            if status:
                conditions.append("status = :status")
                values["status"] = status
            
            if nivel_riesgo:
                conditions.append("nivel_riesgo = :nivel_riesgo")
                values["nivel_riesgo"] = nivel_riesgo
            
            if proveedor_rut:
                conditions.append("proveedor_rut = :proveedor_rut")
                values["proveedor_rut"] = proveedor_rut
            
            if fecha_desde:
                conditions.append("created_at >= :fecha_desde")
                values["fecha_desde"] = fecha_desde
            
            if fecha_hasta:
                conditions.append("created_at <= :fecha_hasta")
                values["fecha_hasta"] = fecha_hasta
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            # Consulta con paginación
            query = f"""
                SELECT 
                    request_id,
                    proveedor_nombre,
                    proveedor_rut,
                    status,
                    nivel_riesgo,
                    monto_contrato,
                    certificado_emitido,
                    created_at
                FROM workflow_executions
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT :limit OFFSET :offset
            """
            
            values["limit"] = limit
            values["offset"] = offset
            
            rows = await self.db.fetch_all(query=query, values=values)
            
            # Contar total
            count_query = f"""
                SELECT COUNT(*) as total
                FROM workflow_executions
                WHERE {where_clause}
            """
            
            count_result = await self.db.fetch_one(query=count_query, values={k: v for k, v in values.items() if k not in ['limit', 'offset']})
            
            return {
                "total": count_result['total'] if count_result else 0,
                "limit": limit,
                "offset": offset,
                "results": [dict(row) for row in rows]
            }
            
        except Exception as e:
            logger.error(f"Error en búsqueda de workflows: {e}")
            raise
    
    # ==================== MÉTODO 3: ESTADÍSTICAS AGREGADAS ====================
    
    async def get_summary_statistics(self) -> Dict:
        """Obtiene estadísticas generales del sistema"""
        try:
            # Total de solicitudes
            total_query = """
                SELECT 
                    COUNT(*) as total_solicitudes,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completadas,
                    SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as en_proceso,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as fallidas
                FROM workflow_executions
            """
            
            totals = await self.db.fetch_one(total_query)
            
            if not totals or totals['total_solicitudes'] == 0:
                return {
                    "total_solicitudes": 0,
                    "message": "No hay datos disponibles"
                }
            
            # Distribución de riesgo
            riesgo_query = """
                SELECT 
                    nivel_riesgo,
                    COUNT(*) as cantidad
                FROM workflow_executions
                WHERE nivel_riesgo IS NOT NULL
                GROUP BY nivel_riesgo
            """
            
            riesgos = await self.db.fetch_all(riesgo_query)
            
            # Certificados emitidos
            cert_query = """
                SELECT COUNT(*) as total_certificados
                FROM workflow_executions
                WHERE certificado_emitido = true
            """
            
            certs = await self.db.fetch_one(cert_query)
            
            return {
                "resumen_general": {
                    "total_solicitudes": totals['total_solicitudes'],
                    "completadas": totals['completadas'],
                    "en_proceso": totals['en_proceso'],
                    "fallidas": totals['fallidas']
                },
                "distribucion_riesgo": {
                    r['nivel_riesgo']: r['cantidad']
                    for r in riesgos
                },
                "certificados_emitidos": certs['total_certificados'] if certs else 0
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            raise
    
    # ==================== MÉTODO 4: GET WORKFLOW BY ID ====================
    
    async def get_workflow_by_request_id(self, request_id: str) -> Optional[Dict]:
        """Obtiene un workflow por su request_id"""
        try:
            query = """
                SELECT *
                FROM workflow_executions
                WHERE request_id = :request_id
            """
            
            row = await self.db.fetch_one(query=query, values={"request_id": request_id})
            
            return dict(row) if row else None
            
        except Exception as e:
            logger.error(f"Error obteniendo workflow {request_id}: {e}")
            raise
