from google.cloud import firestore
from typing import Dict, List, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class Art17Repository:
    def __init__(self, db: firestore.AsyncClient):
        self.db = db
        self.collection = "art17_workflows"
    
    # ==================== MÉTODO 1: PERFIL DE PROVEEDOR ====================
    
    async def get_proveedor_profile(self, rut: str) -> Dict:
        """Obtiene perfil completo de un proveedor"""
        try:
            # Buscar todos los workflows de este proveedor
            query = self.db.collection(self.collection).where(
                filter=firestore.FieldFilter("proveedor_rut", "==", rut)
            )
            
            docs = [doc async for doc in query.stream()]
            
            if not docs:
                return {"exists": False, "rut": rut}
            
            # Calcular estadísticas
            total_solicitudes = len(docs)
            completadas = sum(1 for d in docs if d.get("status") == "completed")
            en_proceso = sum(1 for d in docs if d.get("status") == "processing")
            fallidas = sum(1 for d in docs if d.get("status") == "failed")
            
            # Distribución de riesgo
            riesgos = [d.get("nivel_riesgo", "DESCONOCIDO") for d in docs]
            bajo = riesgos.count("BAJO")
            medio = riesgos.count("MEDIO")
            alto = riesgos.count("ALTO")
            
            # Certificados emitidos
            certificados = sum(1 for d in docs if d.get("certificado_emitido") == True)
            
            # Nombre del proveedor (del primer documento)
            nombre_proveedor = docs[0].get("proveedor_nombre", "Desconocido")
            
            return {
                "exists": True,
                "rut": rut,
                "nombre": nombre_proveedor,
                "estadisticas": {
                    "total_solicitudes": total_solicitudes,
                    "completadas": completadas,
                    "en_proceso": en_proceso,
                    "fallidas": fallidas,
                    "certificados_emitidos": certificados
                },
                "distribucion_riesgo": {
                    "BAJO": bajo,
                    "MEDIO": medio,
                    "ALTO": alto
                },
                "tasa_cumplimiento": round((completadas / total_solicitudes * 100), 2) if total_solicitudes > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo perfil de proveedor {rut}: {e}")
            raise
    
    # ==================== MÉTODO 2: BÚSQUEDA AVANZADA ====================
    
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
        """Búsqueda avanzada de workflows"""
        try:
            # Iniciar query base
            ref = self.db.collection(self.collection)
            
            # Aplicar filtros
            if status:
                ref = ref.where(filter=firestore.FieldFilter("status", "==", status))
            
            if riesgo:
                ref = ref.where(filter=firestore.FieldFilter("nivel_riesgo", "==", riesgo))
            
            if monto_min is not None:
                ref = ref.where(filter=firestore.FieldFilter("monto_contrato", ">=", monto_min))
            
            if monto_max is not None:
                ref = ref.where(filter=firestore.FieldFilter("monto_contrato", "<=", monto_max))
            
            # Obtener documentos
            docs = [doc async for doc in ref.stream()]
            
            # Filtros adicionales en memoria (para búsqueda de texto)
            if query:
                query_lower = query.lower()
                docs = [
                    doc for doc in docs
                    if (query_lower in doc.get("proveedor_rut", "").lower() or
                        query_lower in doc.get("proveedor_nombre", "").lower() or
                        query_lower in doc.get("objeto_contrato", "").lower())
                ]
            
            # Total antes de paginación
            total = len(docs)
            
            # Paginación
            docs = docs[offset:offset + limit]
            
            # Formatear resultados
            results = []
            for doc in docs:
                data = doc.to_dict()
                results.append({
                    "request_id": doc.id,
                    "proveedor_rut": data.get("proveedor_rut"),
                    "proveedor_nombre": data.get("proveedor_nombre"),
                    "status": data.get("status"),
                    "nivel_riesgo": data.get("nivel_riesgo"),
                    "monto_contrato": data.get("monto_contrato"),
                    "created_at": data.get("created_at"),
                    "certificado_emitido": data.get("certificado_emitido", False)
                })
            
            return {
                "results": results,
                "count": len(results),
                "total": total,
                "limit": limit,
                "offset": offset
            }
            
        except Exception as e:
            logger.error(f"Error en búsqueda de workflows: {e}")
            raise
    
    # ==================== MÉTODO 3: ESTADÍSTICAS GENERALES ====================
    
    async def get_statistics_summary(self) -> Dict:
        """Obtiene estadísticas generales del sistema"""
        try:
            # Obtener todos los workflows
            docs = [doc async for doc in self.db.collection(self.collection).stream()]
            
            if not docs:
                return {
                    "total_solicitudes": 0,
                    "message": "No hay datos disponibles"
                }
            
            # Calcular estadísticas
            total = len(docs)
            completadas = sum(1 for d in docs if d.get("status") == "completed")
            en_proceso = sum(1 for d in docs if d.get("status") == "processing")
            fallidas = sum(1 for d in docs if d.get("status") == "failed")
            
            # Distribución de riesgo
            riesgos = [d.get("nivel_riesgo", "DESCONOCIDO") for d in docs]
            bajo = riesgos.count("BAJO")
            medio = riesgos.count("MEDIO")
            alto = riesgos.count("ALTO")
            
            # Certificados
            certificados = sum(1 for d in docs if d.get("certificado_emitido") == True)
            
            # Montos
            montos = [d.get("monto_contrato", 0) for d in docs if d.get("monto_contrato")]
            monto_total = sum(montos)
            monto_promedio = monto_total / len(montos) if montos else 0
            
            # Top 10 proveedores
            proveedores = {}
            for doc in docs:
                rut = doc.get("proveedor_rut")
                if rut:
                    proveedores[rut] = proveedores.get(rut, 0) + 1
            
            top_proveedores = sorted(proveedores.items(), key=lambda x: x[1], reverse=True)[:10]
            
            return {
                "total_solicitudes": total,
                "por_estado": {
                    "completadas": completadas,
                    "en_proceso": en_proceso,
                    "fallidas": fallidas
                },
                "distribucion_riesgo": {
                    "BAJO": bajo,
                    "MEDIO": medio,
                    "ALTO": alto
                },
                "certificados": {
                    "emitidos": certificados,
                    "tasa_emision": round((certificados / total * 100), 2) if total > 0 else 0
                },
                "montos": {
                    "total": monto_total,
                    "promedio": round(monto_promedio, 2)
                },
                "top_10_proveedores": [
                    {"rut": rut, "solicitudes": count}
                    for rut, count in top_proveedores
                ],
                "tasa_cumplimiento": round((completadas / total * 100), 2) if total > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            raise

    # ==================== MÉTODO ADICIONAL: GET WORKFLOW BY ID ====================
    
    async def get_workflow_by_request_id(self, request_id: str) -> Optional[Dict]:
        """Obtiene un workflow por su request_id"""
        try:
            doc = await self.db.collection(self.collection).document(request_id).get()
            
            if not doc.exists:
                return None
            
            data = doc.to_dict()
            data['request_id'] = doc.id
            return data
            
        except Exception as e:
            logger.error(f"Error obteniendo workflow {request_id}: {e}")
            raise
