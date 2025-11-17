from typing import Dict, List
import logging
from datetime import datetime

from src.db.database import database  # <- IMPORT CORRECTO

logger = logging.getLogger(__name__)


class Art17Repository:
    def __init__(self):
        # Ahora la base de datos siempre vendrá desde database.py
        self.db = database

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
            conditions = ["1=1"]
            values = {}

            if filters.get("nombre"):
                conditions.append("proveedor_nombre ILIKE :nombre")
                values["nombre"] = f"%{filters['nombre']}%"

            if filters.get("rut"):
                conditions.append("proveedor_rut = :rut")
                values["rut"] = filters["rut"]

            if filters.get("status"):
                conditions.append("status = :status")
                values["status"] = filters["status"]

            query = f"""
                SELECT DISTINCT ON (proveedor_rut)
                    proveedor_rut,
                    proveedor_nombre,
                    status,
                    nivel_riesgo,
                    created_at
                FROM workflow_executions
                WHERE {" AND ".join(conditions)}
                ORDER BY proveedor_rut, created_at DESC
                LIMIT :limit OFFSET :offset
            """

            values["limit"] = filters.get("limit", 50)
            values["offset"] = filters.get("offset", 0)

            results = await self.db.fetch_all(query=query, values=values)
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error en búsqueda de proveedores: {e}")
            raise

    async def get_statistics(self) -> Dict:
        """Obtiene estadísticas generales"""
        try:
            query = """
                SELECT 
                    COUNT(DISTINCT proveedor_rut) as total_proveedores,
                    COUNT(*) as total_solicitudes,
                    COUNT(*) FILTER (WHERE status = 'completed') as completadas,
                    COUNT(*) FILTER (WHERE status = 'in_progress') as en_proceso,
                    COUNT(*) FILTER (WHERE certificado_emitido = true) as certificados_emitidos
                FROM workflow_executions
            """
            result = await self.db.fetch_one(query=query)
            return dict(result) if result else {}
        except Exception as e:
            logger.error(f"Error al obtener estadísticas: {e}")
            raise
