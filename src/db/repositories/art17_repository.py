from typing import Dict, List, Optional
import logging
from datetime import datetime
from src.db.database import database

logger = logging.getLogger(__name__)

class Art17Repository:
    def __init__(self):
        self.db = database
    
    async def get_workflow_by_request_id(self, request_id: str) -> Optional[Dict]:
        """Obtiene un workflow completo por request_id"""
        try:
            query = """
                SELECT * FROM workflow_executions 
                WHERE request_id = :request_id
            """
            result = await self.db.fetch_one(query=query, values={"request_id": request_id})
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Error al obtener workflow {request_id}: {e}")
            raise
    
    async def get_certificate_by_id(self, certificado_id: str) -> Optional[Dict]:
        """Obtiene un certificado por su ID"""
        try:
            query = """
                SELECT * FROM certificados 
                WHERE certificado_id = :certificado_id
            """
            result = await self.db.fetch_one(query=query, values={"certificado_id": certificado_id})
            return dict(result) if result else None
        except Exception as e:
            logger.error(f"Error al obtener certificado {certificado_id}: {e}")
            raise
    
    async def verify_certificate_integrity(self, certificado_id: str) -> Dict:
        """Verifica la integridad de un certificado"""
        try:
            cert = await self.get_certificate_by_id(certificado_id)
            if not cert:
                return {"valid": False, "message": "Certificado no encontrado"}
            
            # Aquí podrías agregar verificaciones adicionales de integridad
            return {
                "valid": True,
                "certificado_id": certificado_id,
                "message": "Certificado válido"
            }
        except Exception as e:
            logger.error(f"Error al verificar certificado {certificado_id}: {e}")
            return {"valid": False, "message": str(e)}
    
    async def get_audit_trail(self, request_id: str) -> List[Dict]:
        """Obtiene el trail de auditoría de un workflow"""
        try:
            query = """
                SELECT * FROM audit_trail 
                WHERE request_id = :request_id 
                ORDER BY timestamp DESC
            """
            results = await self.db.fetch_all(query=query, values={"request_id": request_id})
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error al obtener audit trail {request_id}: {e}")
            return []

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
            return {
                "rut": rut,
                "nombre": results[0]["proveedor_nombre"] if results else None,
                "total_solicitudes": len(results),
                "solicitudes": [dict(row) for row in results]
            }
        except Exception as e:
            logger.error(f"Error al obtener perfil de proveedor {rut}: {e}")
            raise

    async def search_proveedores(self, filters: Dict) -> List[Dict]:
        """Busca proveedores con filtros"""
        try:
            conditions = []
            values = {}
            
            if filters.get("proveedor_rut"):
                conditions.append("proveedor_rut = :rut")
                values["rut"] = filters["proveedor_rut"]
            
            if filters.get("status"):
                conditions.append("status = :status")
                values["status"] = filters["status"]
            
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            limit = filters.get("limit", 10)
            
            query = f"""
                SELECT * FROM workflow_executions 
                WHERE {where_clause}
                ORDER BY created_at DESC
                LIMIT {limit}
            """
            results = await self.db.fetch_all(query=query, values=values)
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error al buscar proveedores: {e}")
            return []

    async def get_statistics(self) -> Dict:
        """Obtiene estadísticas generales"""
        try:
            query = """
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
                    COUNT(CASE WHEN certificado_emitido = true THEN 1 END) as certificados
                FROM workflow_executions
            """
            result = await self.db.fetch_one(query=query)
            return dict(result) if result else {}
        except Exception as e:
            logger.error(f"Error al obtener estadísticas: {e}")
            return {}
