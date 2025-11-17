from typing import Optional, Dict, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class Art17Repository:
    def __init__(self, db):
        self.db = db
    
    # ==================== MÉTODOS DE ESCRITURA (YA EXISTENTES) ====================
    
    async def save_request(self, data: Dict):
        query = """
            INSERT INTO requests (request_id, proveedor_rut, proveedor_nombre, monto_contrato, objeto_contrato, status)
            VALUES (:request_id, :proveedor_rut, :proveedor_nombre, :monto_contrato, :objeto_contrato, :status)
            ON CONFLICT (request_id) DO UPDATE SET status = :status
        """
        await self.db.execute(query, values=data)
        logger.info(f"Request {data['request_id']} guardado")
    
    async def save_workflow_execution(self, data: Dict):
        query = """
            INSERT INTO workflow_executions 
            (request_id, workflow_type, ingest_timestamp, hash_ingest, riesgo, hash_riesgo, 
             cumplimiento, hash_compliance, hash_final, timestamp_final, metadata)
            VALUES (:request_id, :workflow_type, :ingest_timestamp, :hash_ingest, :riesgo, 
                    :hash_riesgo, :cumplimiento, :hash_compliance, :hash_final, :timestamp_final, :metadata)
            RETURNING id
        """
        
        data_copy = data.copy()
        if 'ingest_timestamp' in data_copy and isinstance(data_copy['ingest_timestamp'], str):
            data_copy['ingest_timestamp'] = datetime.fromisoformat(data_copy['ingest_timestamp'])
        if 'timestamp_final' in data_copy and isinstance(data_copy['timestamp_final'], str):
            data_copy['timestamp_final'] = datetime.fromisoformat(data_copy['timestamp_final'])
        
        result = await self.db.fetch_one(query, values=data_copy)
        logger.info(f"Workflow execution guardado, ID: {result['id'] if result else None}")
        return result['id'] if result else None
    
    async def save_certificate(self, data: Dict):
        query = """
            INSERT INTO certificates (certificado_id, request_id, hash_final, issued_at)
            VALUES (:certificado_id, :request_id, :hash_final, :issued_at)
        """
        
        data_copy = data.copy()
        if 'issued_at' in data_copy and isinstance(data_copy['issued_at'], str):
            data_copy['issued_at'] = datetime.fromisoformat(data_copy['issued_at'])
        
        await self.db.execute(query, values=data_copy)
        logger.info(f"Certificado {data['certificado_id']} guardado")
    
    # ==================== MÉTODOS DE LECTURA (NUEVOS - FASE 1) ====================
    
    async def get_workflow_by_request_id(self, request_id: str) -> Optional[Dict]:
        """Obtiene workflow completo con todos los datos relacionados"""
        query = """
            SELECT 
                r.id as request_db_id,
                r.request_id,
                r.proveedor_rut,
                r.proveedor_nombre,
                r.monto_contrato,
                r.objeto_contrato,
                r.status,
                r.created_at as request_created_at,
                we.id as workflow_execution_id,
                we.workflow_type,
                we.ingest_timestamp,
                we.hash_ingest,
                we.riesgo,
                we.hash_riesgo,
                we.cumplimiento,
                we.hash_compliance,
                we.hash_final,
                we.timestamp_final,
                we.metadata,
                c.certificado_id,
                c.issued_at as certificado_issued_at
            FROM requests r
            LEFT JOIN workflow_executions we ON r.request_id = we.request_id
            LEFT JOIN certificates c ON r.request_id = c.request_id
            WHERE r.request_id = :request_id
        """
        result = await self.db.fetch_one(query, values={"request_id": request_id})
        
        if not result:
            return None
        
        return dict(result)
    
    async def get_certificate_by_id(self, certificado_id: str) -> Optional[Dict]:
        """Obtiene certificado con información del workflow asociado"""
        query = """
            SELECT 
                c.id as cert_db_id,
                c.certificado_id,
                c.request_id,
                c.hash_final,
                c.issued_at,
                r.proveedor_rut,
                r.proveedor_nombre,
                r.monto_contrato,
                r.objeto_contrato,
                we.riesgo,
                we.cumplimiento,
                we.workflow_type,
                we.timestamp_final
            FROM certificates c
            INNER JOIN requests r ON c.request_id = r.request_id
            LEFT JOIN workflow_executions we ON c.request_id = we.request_id
            WHERE c.certificado_id = :certificado_id
        """
        result = await self.db.fetch_one(query, values={"certificado_id": certificado_id})
        
        if not result:
            return None
        
        return dict(result)
    
    async def verify_certificate_integrity(self, certificado_id: str) -> Dict:
        """Verifica integridad del certificado validando la cadena de hashes"""
        query = """
            SELECT 
                c.certificado_id,
                c.hash_final as cert_hash_final,
                we.hash_ingest,
                we.hash_riesgo,
                we.hash_compliance,
                we.hash_final as workflow_hash_final,
                we.request_id,
                we.ingest_timestamp,
                we.timestamp_final
            FROM certificates c
            INNER JOIN workflow_executions we ON c.request_id = we.request_id
            WHERE c.certificado_id = :certificado_id
        """
        result = await self.db.fetch_one(query, values={"certificado_id": certificado_id})
        
        if not result:
            return {
                "certificado_id": certificado_id,
                "exists": False,
                "valid": False,
                "message": "Certificado no encontrado"
            }
        
        # Convertir a dict para acceso seguro
        data = dict(result)
        
        # Verificar que los hashes finales coinciden
        cert_hash = data.get('cert_hash_final')
        workflow_hash = data.get('workflow_hash_final')
        hash_match = cert_hash == workflow_hash if cert_hash and workflow_hash else False
        
        # Verificar que la cadena de hashes está completa
        chain_complete = all([
            data.get('hash_ingest'),
            data.get('hash_riesgo'),
            data.get('hash_compliance'),
            data.get('workflow_hash_final')
        ])
        
        is_valid = hash_match and chain_complete
        
        return {
            "certificado_id": certificado_id,
            "exists": True,
            "valid": is_valid,
            "hash_match": hash_match,
            "chain_complete": chain_complete,
            "verification_details": {
                "hash_ingest": data.get('hash_ingest'),
                "hash_riesgo": data.get('hash_riesgo'),
                "hash_compliance": data.get('hash_compliance'),
                "hash_final": data.get('workflow_hash_final'),
                "cert_hash_final": data.get('cert_hash_final')
            },
            "timestamps": {
                "workflow_start": str(data.get('ingest_timestamp', '')),
                "workflow_end": str(data.get('timestamp_final', ''))
            }
        }
        
        data = dict(result)
        
        # Verificar que los hashes finales coinciden
        hash_match = data['cert_hash_final'] == data['workflow_hash_final']
        
        # Verificar que la cadena de hashes está completa
        chain_complete = all([
            data['hash_ingest'],
            data['hash_riesgo'],
            data['hash_compliance'],
            data['hash_final']
        ])
        
        is_valid = hash_match and chain_complete
        
        return {
            "certificado_id": certificado_id,
            "exists": True,
            "valid": is_valid,
            "hash_match": hash_match,
            "chain_complete": chain_complete,
            "verification_details": {
                "hash_ingest": data['hash_ingest'],
                "hash_riesgo": data['hash_riesgo'],
                "hash_compliance": data['hash_compliance'],
                "hash_final": data['hash_final'],
                "cert_hash_final": data['cert_hash_final']
            },
            "timestamps": {
                "workflow_start": str(data['ingest_timestamp']),
                "workflow_end": str(data['timestamp_final'])
            }
        }
    
    async def get_audit_trail(self, request_id: str) -> Dict:
        """Obtiene el trail completo de auditoría para una solicitud"""
        # Obtener datos principales
        workflow_query = """
            SELECT 
                r.request_id,
                r.proveedor_rut,
                r.proveedor_nombre,
                r.monto_contrato,
                r.objeto_contrato,
                r.status,
                r.created_at,
                we.workflow_type,
                we.ingest_timestamp,
                we.hash_ingest,
                we.riesgo,
                we.hash_riesgo,
                we.cumplimiento,
                we.hash_compliance,
                we.hash_final,
                we.timestamp_final,
                c.certificado_id,
                c.issued_at as cert_issued_at
            FROM requests r
            LEFT JOIN workflow_executions we ON r.request_id = we.request_id
            LEFT JOIN certificates c ON r.request_id = c.request_id
            WHERE r.request_id = :request_id
        """
        
        workflow_data = await self.db.fetch_one(workflow_query, values={"request_id": request_id})
        
        if not workflow_data:
            return {
                "request_id": request_id,
                "exists": False,
                "message": "Request no encontrado"
            }
        
        data = dict(workflow_data)
        
        # Obtener logs de auditoría
        audit_query = """
            SELECT 
                event_type,
                event_data,
                timestamp
            FROM audit_log
            WHERE request_id = :request_id
            ORDER BY timestamp ASC
        """
        
        audit_logs = await self.db.fetch_all(audit_query, values={"request_id": request_id})
        
        # Construir el trail
        trail = {
            "request_id": request_id,
            "exists": True,
            "proveedor": {
                "rut": data['proveedor_rut'],
                "nombre": data['proveedor_nombre']
            },
            "contrato": {
                "monto": float(data['monto_contrato']) if data['monto_contrato'] else None,
                "objeto": data['objeto_contrato']
            },
            "status": data['status'],
            "workflow_stages": [],
            "audit_events": []
        }
        
        # Agregar etapas del workflow si existen
        if data['ingest_timestamp']:
            trail['workflow_stages'].append({
                "stage": "ingest",
                "timestamp": str(data['ingest_timestamp']),
                "hash": data['hash_ingest'],
                "status": "completed"
            })
        
        if data['hash_riesgo']:
            trail['workflow_stages'].append({
                "stage": "risk_assessment",
                "timestamp": str(data['ingest_timestamp']),
                "hash": data['hash_riesgo'],
                "result": data['riesgo'],
                "status": "completed"
            })
        
        if data['hash_compliance']:
            trail['workflow_stages'].append({
                "stage": "compliance_check",
                "timestamp": str(data['ingest_timestamp']),
                "hash": data['hash_compliance'],
                "result": bool(data['cumplimiento']),
                "status": "completed"
            })
        
        if data['hash_final']:
            trail['workflow_stages'].append({
                "stage": "finalization",
                "timestamp": str(data['timestamp_final']),
                "hash": data['hash_final'],
                "status": "completed"
            })
        
        if data['certificado_id']:
            trail['workflow_stages'].append({
                "stage": "certification",
                "timestamp": str(data['cert_issued_at']),
                "certificado_id": data['certificado_id'],
                "status": "issued"
            })
        
        # Agregar eventos de auditoría
        for log in audit_logs:
            trail['audit_events'].append({
                "event_type": log['event_type'],
                "timestamp": str(log['timestamp']),
                "data": log['event_data']
            })
        
        # Calcular duración total
        if data['ingest_timestamp'] and data['timestamp_final']:
            duration = data['timestamp_final'] - data['ingest_timestamp']
            trail['processing_time'] = {
                "seconds": duration.total_seconds(),
                "formatted": str(duration)
            }
        
        return trail

    # ==================== MÉTODOS DE BÚSQUEDA Y ESTADÍSTICAS (FASE 2A) ====================
    
    async def get_proveedor_profile(self, rut: str) -> Dict:
        """Obtiene perfil completo de un proveedor con historial"""
        # Datos básicos del proveedor
        proveedor_query = """
            SELECT 
                proveedor_rut,
                proveedor_nombre,
                COUNT(*) as total_solicitudes,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as solicitudes_completadas,
                SUM(monto_contrato) as monto_total_contratado,
                AVG(monto_contrato) as monto_promedio,
                MIN(created_at) as primera_solicitud,
                MAX(created_at) as ultima_solicitud
            FROM requests
            WHERE proveedor_rut = :rut
            GROUP BY proveedor_rut, proveedor_nombre
        """
        
        proveedor_data = await self.db.fetch_one(proveedor_query, values={"rut": rut})
        
        if not proveedor_data:
            return {
                "rut": rut,
                "exists": False,
                "message": "Proveedor no encontrado"
            }
        
        prov = dict(proveedor_data)
        
        # Historial de riesgos
        riesgo_query = """
            SELECT 
                we.riesgo,
                COUNT(*) as cantidad
            FROM workflow_executions we
            INNER JOIN requests r ON we.request_id = r.request_id
            WHERE r.proveedor_rut = :rut
            GROUP BY we.riesgo
        """
        
        riesgos = await self.db.fetch_all(riesgo_query, values={"rut": rut})
        
        # Historial de cumplimiento
        cumplimiento_query = """
            SELECT 
                we.cumplimiento,
                COUNT(*) as cantidad
            FROM workflow_executions we
            INNER JOIN requests r ON we.request_id = r.request_id
            WHERE r.proveedor_rut = :rut
            GROUP BY we.cumplimiento
        """
        
        cumplimientos = await self.db.fetch_all(cumplimiento_query, values={"rut": rut})
        
        # Certificados emitidos
        certificados_query = """
            SELECT 
                c.certificado_id,
                c.issued_at,
                r.request_id,
                r.monto_contrato,
                r.objeto_contrato,
                we.riesgo,
                we.cumplimiento
            FROM certificates c
            INNER JOIN requests r ON c.request_id = r.request_id
            LEFT JOIN workflow_executions we ON r.request_id = we.request_id
            WHERE r.proveedor_rut = :rut
            ORDER BY c.issued_at DESC
            LIMIT 10
        """
        
        certificados = await self.db.fetch_all(certificados_query, values={"rut": rut})
        
        # Construir perfil
        profile = {
            "rut": prov['proveedor_rut'],
            "nombre": prov['proveedor_nombre'],
            "exists": True,
            "estadisticas": {
                "total_solicitudes": prov['total_solicitudes'],
                "solicitudes_completadas": prov['solicitudes_completadas'],
                "tasa_completitud": round(prov['solicitudes_completadas'] / prov['total_solicitudes'] * 100, 2) if prov['total_solicitudes'] > 0 else 0,
                "monto_total_contratado": float(prov['monto_total_contratado']) if prov['monto_total_contratado'] else 0,
                "monto_promedio": float(prov['monto_promedio']) if prov['monto_promedio'] else 0,
                "primera_solicitud": str(prov['primera_solicitud']),
                "ultima_solicitud": str(prov['ultima_solicitud'])
            },
            "historial_riesgo": {
                r['riesgo']: r['cantidad'] for r in riesgos
            },
            "historial_cumplimiento": {
                "cumple": sum(c['cantidad'] for c in cumplimientos if c['cumplimiento']),
                "no_cumple": sum(c['cantidad'] for c in cumplimientos if not c['cumplimiento'])
            },
            "certificados_recientes": [
                {
                    "certificado_id": c['certificado_id'],
                    "request_id": c['request_id'],
                    "issued_at": str(c['issued_at']),
                    "monto": float(c['monto_contrato']) if c['monto_contrato'] else 0,
                    "objeto": c['objeto_contrato'],
                    "riesgo": c['riesgo'],
                    "cumplimiento": bool(c['cumplimiento'])
                }
                for c in certificados
            ]
        }
        
        # Calcular score de confianza
        total_evals = prov['total_solicitudes']
        if total_evals > 0:
            bajo_risk = profile['historial_riesgo'].get('BAJO', 0)
            cumple = profile['historial_cumplimiento']['cumple']
            
            risk_score = (bajo_risk / total_evals) * 50
            compliance_score = (cumple / total_evals) * 50
            
            profile['score_confianza'] = round(risk_score + compliance_score, 2)
            
            if profile['score_confianza'] >= 80:
                profile['nivel_confianza'] = "ALTO"
            elif profile['score_confianza'] >= 50:
                profile['nivel_confianza'] = "MEDIO"
            else:
                profile['nivel_confianza'] = "BAJO"
        else:
            profile['score_confianza'] = 0
            profile['nivel_confianza'] = "SIN_DATOS"
        
        return profile
    
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
        """Búsqueda avanzada de workflows con múltiples filtros"""
        
        # Construir query dinámica
        base_query = """
            SELECT 
                r.request_id,
                r.proveedor_rut,
                r.proveedor_nombre,
                r.monto_contrato,
                r.objeto_contrato,
                r.status,
                r.created_at,
                we.riesgo,
                we.cumplimiento,
                c.certificado_id
            FROM requests r
            LEFT JOIN workflow_executions we ON r.request_id = we.request_id
            LEFT JOIN certificates c ON r.request_id = c.request_id
            WHERE 1=1
        """
        
        conditions = []
        params = {}
        
        # Filtro de búsqueda de texto
        if query:
            conditions.append("""
                (LOWER(r.proveedor_nombre) LIKE :query 
                 OR LOWER(r.proveedor_rut) LIKE :query 
                 OR LOWER(r.objeto_contrato) LIKE :query
                 OR r.request_id LIKE :query)
            """)
            params['query'] = f"%{query.lower()}%"
        
        # Filtro de status
        if status:
            conditions.append("r.status = :status")
            params['status'] = status
        
        # Filtro de riesgo
        if riesgo:
            conditions.append("we.riesgo = :riesgo")
            params['riesgo'] = riesgo
        
        # Filtro de monto
        if monto_min is not None:
            conditions.append("r.monto_contrato >= :monto_min")
            params['monto_min'] = monto_min
        
        if monto_max is not None:
            conditions.append("r.monto_contrato <= :monto_max")
            params['monto_max'] = monto_max
        
        # Filtro de fechas
        if fecha_desde:
            conditions.append("r.created_at >= :fecha_desde")
            params['fecha_desde'] = fecha_desde
        
        if fecha_hasta:
            conditions.append("r.created_at <= :fecha_hasta")
            params['fecha_hasta'] = fecha_hasta
        
        # Agregar condiciones
        if conditions:
            base_query += " AND " + " AND ".join(conditions)
        
        # Contar total
        count_query = f"SELECT COUNT(*) as total FROM ({base_query}) as subq"
        total_result = await self.db.fetch_one(count_query, values=params)
        total = total_result['total'] if total_result else 0
        
        # Agregar paginación
        base_query += " ORDER BY r.created_at DESC LIMIT :limit OFFSET :offset"
        params['limit'] = limit
        params['offset'] = offset
        
        # Ejecutar búsqueda
        results = await self.db.fetch_all(base_query, values=params)
        
        workflows = [
            {
                "request_id": r['request_id'],
                "proveedor": {
                    "rut": r['proveedor_rut'],
                    "nombre": r['proveedor_nombre']
                },
                "contrato": {
                    "monto": float(r['monto_contrato']) if r['monto_contrato'] else 0,
                    "objeto": r['objeto_contrato']
                },
                "status": r['status'],
                "created_at": str(r['created_at']),
                "riesgo": r['riesgo'],
                "cumplimiento": bool(r['cumplimiento']) if r['cumplimiento'] is not None else None,
                "certificado_id": r['certificado_id']
            }
            for r in results
        ]
        
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "count": len(workflows),
            "workflows": workflows,
            "filtros_aplicados": {
                "query": query,
                "status": status,
                "riesgo": riesgo,
                "monto_min": monto_min,
                "monto_max": monto_max,
                "fecha_desde": fecha_desde,
                "fecha_hasta": fecha_hasta
            }
        }
    
    async def get_statistics_summary(self) -> Dict:
        """Obtiene estadísticas generales del sistema"""
        
        # Total de requests
        total_query = """
            SELECT 
                COUNT(*) as total_requests,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
                COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                SUM(monto_contrato) as monto_total,
                AVG(monto_contrato) as monto_promedio
            FROM requests
        """
        
        totals = await self.db.fetch_one(total_query)
        
        # Distribución de riesgos
        riesgo_query = """
            SELECT 
                riesgo,
                COUNT(*) as cantidad,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as porcentaje
            FROM workflow_executions
            GROUP BY riesgo
        """
        
        riesgos = await self.db.fetch_all(riesgo_query)
        
        # Cumplimiento
        cumplimiento_query = """
            SELECT 
                cumplimiento,
                COUNT(*) as cantidad
            FROM workflow_executions
            GROUP BY cumplimiento
        """
        
        cumplimientos = await self.db.fetch_all(cumplimiento_query)
        
        # Certificados emitidos
        cert_query = """
            SELECT 
                COUNT(*) as total_certificados,
                MIN(issued_at) as primer_certificado,
                MAX(issued_at) as ultimo_certificado
            FROM certificates
        """
        
        certs = await self.db.fetch_one(cert_query)
        
        # Top proveedores
        top_prov_query = """
            SELECT 
                proveedor_rut,
                proveedor_nombre,
                COUNT(*) as total_solicitudes,
                SUM(monto_contrato) as monto_total
            FROM requests
            GROUP BY proveedor_rut, proveedor_nombre
            ORDER BY total_solicitudes DESC
            LIMIT 10
        """
        
        top_proveedores = await self.db.fetch_all(top_prov_query)
        
        # Tiempo promedio de procesamiento
        time_query = """
            SELECT 
                AVG(EXTRACT(EPOCH FROM (timestamp_final - ingest_timestamp))) as avg_seconds
            FROM workflow_executions
            WHERE timestamp_final IS NOT NULL AND ingest_timestamp IS NOT NULL
        """
        
        time_result = await self.db.fetch_one(time_query)
        
        # Construir resumen
        total_cumple = sum(c['cantidad'] for c in cumplimientos if c['cumplimiento'])
        total_no_cumple = sum(c['cantidad'] for c in cumplimientos if not c['cumplimiento'])
        total_eval = total_cumple + total_no_cumple
        
        summary = {
            "resumen_general": {
                "total_solicitudes": totals['total_requests'],
                "completadas": totals['completed'],
                "en_proceso": totals['processing'],
                "fallidas": totals['failed'],
                "monto_total_contratado": float(totals['monto_total']) if totals['monto_total'] else 0,
                "monto_promedio": float(totals['monto_promedio']) if totals['monto_promedio'] else 0
            },
            "distribucion_riesgo": {
                r['riesgo']: {
                    "cantidad": r['cantidad'],
                    "porcentaje": float(r['porcentaje'])
                }
                for r in riesgos
            },
            "cumplimiento": {
                "total_evaluaciones": total_eval,
                "cumple": total_cumple,
                "no_cumple": total_no_cumple,
                "tasa_cumplimiento": round(total_cumple / total_eval * 100, 2) if total_eval > 0 else 0
            },
            "certificados": {
                "total_emitidos": certs['total_certificados'],
                "primer_certificado": str(certs['primer_certificado']) if certs['primer_certificado'] else None,
                "ultimo_certificado": str(certs['ultimo_certificado']) if certs['ultimo_certificado'] else None
            },
            "rendimiento": {
                "tiempo_promedio_procesamiento_segundos": round(float(time_result['avg_seconds']), 2) if time_result['avg_seconds'] else 0
            },
            "top_proveedores": [
                {
                    "rut": p['proveedor_rut'],
                    "nombre": p['proveedor_nombre'],
                    "total_solicitudes": p['total_solicitudes'],
                    "monto_total": float(p['monto_total']) if p['monto_total'] else 0
                }
                for p in top_proveedores
            ]
        }
        
        return summary

    # ==================== MÉTODOS DE BÚSQUEDA Y ESTADÍSTICAS (FASE 2A) ====================
    
    async def get_proveedor_profile(self, rut: str) -> Dict:
        """Obtiene perfil completo de un proveedor con historial"""
        # Datos básicos del proveedor
        proveedor_query = """
            SELECT 
                proveedor_rut,
                proveedor_nombre,
                COUNT(*) as total_solicitudes,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as solicitudes_completadas,
                SUM(monto_contrato) as monto_total_contratado,
                AVG(monto_contrato) as monto_promedio,
                MIN(created_at) as primera_solicitud,
                MAX(created_at) as ultima_solicitud
            FROM requests
            WHERE proveedor_rut = :rut
            GROUP BY proveedor_rut, proveedor_nombre
        """
        
        proveedor_data = await self.db.fetch_one(proveedor_query, values={"rut": rut})
        
        if not proveedor_data:
            return {
                "rut": rut,
                "exists": False,
                "message": "Proveedor no encontrado"
            }
        
        prov = dict(proveedor_data)
        
        # Historial de riesgos
        riesgo_query = """
            SELECT 
                we.riesgo,
                COUNT(*) as cantidad
            FROM workflow_executions we
            INNER JOIN requests r ON we.request_id = r.request_id
            WHERE r.proveedor_rut = :rut
            GROUP BY we.riesgo
        """
        
        riesgos = await self.db.fetch_all(riesgo_query, values={"rut": rut})
        
        # Historial de cumplimiento
        cumplimiento_query = """
            SELECT 
                we.cumplimiento,
                COUNT(*) as cantidad
            FROM workflow_executions we
            INNER JOIN requests r ON we.request_id = r.request_id
            WHERE r.proveedor_rut = :rut
            GROUP BY we.cumplimiento
        """
        
        cumplimientos = await self.db.fetch_all(cumplimiento_query, values={"rut": rut})
        
        # Certificados emitidos
        certificados_query = """
            SELECT 
                c.certificado_id,
                c.issued_at,
                r.request_id,
                r.monto_contrato,
                r.objeto_contrato,
                we.riesgo,
                we.cumplimiento
            FROM certificates c
            INNER JOIN requests r ON c.request_id = r.request_id
            LEFT JOIN workflow_executions we ON r.request_id = we.request_id
            WHERE r.proveedor_rut = :rut
            ORDER BY c.issued_at DESC
            LIMIT 10
        """
        
        certificados = await self.db.fetch_all(certificados_query, values={"rut": rut})
        
        # Construir perfil
        profile = {
            "rut": prov['proveedor_rut'],
            "nombre": prov['proveedor_nombre'],
            "exists": True,
            "estadisticas": {
                "total_solicitudes": prov['total_solicitudes'],
                "solicitudes_completadas": prov['solicitudes_completadas'],
                "tasa_completitud": round(prov['solicitudes_completadas'] / prov['total_solicitudes'] * 100, 2) if prov['total_solicitudes'] > 0 else 0,
                "monto_total_contratado": float(prov['monto_total_contratado']) if prov['monto_total_contratado'] else 0,
                "monto_promedio": float(prov['monto_promedio']) if prov['monto_promedio'] else 0,
                "primera_solicitud": str(prov['primera_solicitud']),
                "ultima_solicitud": str(prov['ultima_solicitud'])
            },
            "historial_riesgo": {
                r['riesgo']: r['cantidad'] for r in riesgos
            },
            "historial_cumplimiento": {
                "cumple": sum(c['cantidad'] for c in cumplimientos if c['cumplimiento']),
                "no_cumple": sum(c['cantidad'] for c in cumplimientos if not c['cumplimiento'])
            },
            "certificados_recientes": [
                {
                    "certificado_id": c['certificado_id'],
                    "request_id": c['request_id'],
                    "issued_at": str(c['issued_at']),
                    "monto": float(c['monto_contrato']) if c['monto_contrato'] else 0,
                    "objeto": c['objeto_contrato'],
                    "riesgo": c['riesgo'],
                    "cumplimiento": bool(c['cumplimiento'])
                }
                for c in certificados
            ]
        }
        
        # Calcular score de confianza
        total_evals = prov['total_solicitudes']
        if total_evals > 0:
            bajo_risk = profile['historial_riesgo'].get('BAJO', 0)
            cumple = profile['historial_cumplimiento']['cumple']
            
            risk_score = (bajo_risk / total_evals) * 50
            compliance_score = (cumple / total_evals) * 50
            
            profile['score_confianza'] = round(risk_score + compliance_score, 2)
            
            if profile['score_confianza'] >= 80:
                profile['nivel_confianza'] = "ALTO"
            elif profile['score_confianza'] >= 50:
                profile['nivel_confianza'] = "MEDIO"
            else:
                profile['nivel_confianza'] = "BAJO"
        else:
            profile['score_confianza'] = 0
            profile['nivel_confianza'] = "SIN_DATOS"
        
        return profile
    
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
        """Búsqueda avanzada de workflows con múltiples filtros"""
        
        # Construir query dinámica
        base_query = """
            SELECT 
                r.request_id,
                r.proveedor_rut,
                r.proveedor_nombre,
                r.monto_contrato,
                r.objeto_contrato,
                r.status,
                r.created_at,
                we.riesgo,
                we.cumplimiento,
                c.certificado_id
            FROM requests r
            LEFT JOIN workflow_executions we ON r.request_id = we.request_id
            LEFT JOIN certificates c ON r.request_id = c.request_id
            WHERE 1=1
        """
        
        conditions = []
        params = {}
        
        # Filtro de búsqueda de texto
        if query:
            conditions.append("""
                (LOWER(r.proveedor_nombre) LIKE :query 
                 OR LOWER(r.proveedor_rut) LIKE :query 
                 OR LOWER(r.objeto_contrato) LIKE :query
                 OR r.request_id LIKE :query)
            """)
            params['query'] = f"%{query.lower()}%"
        
        # Filtro de status
        if status:
            conditions.append("r.status = :status")
            params['status'] = status
        
        # Filtro de riesgo
        if riesgo:
            conditions.append("we.riesgo = :riesgo")
            params['riesgo'] = riesgo
        
        # Filtro de monto
        if monto_min is not None:
            conditions.append("r.monto_contrato >= :monto_min")
            params['monto_min'] = monto_min
        
        if monto_max is not None:
            conditions.append("r.monto_contrato <= :monto_max")
            params['monto_max'] = monto_max
        
        # Filtro de fechas
        if fecha_desde:
            conditions.append("r.created_at >= :fecha_desde")
            params['fecha_desde'] = fecha_desde
        
        if fecha_hasta:
            conditions.append("r.created_at <= :fecha_hasta")
            params['fecha_hasta'] = fecha_hasta
        
        # Agregar condiciones
        if conditions:
            base_query += " AND " + " AND ".join(conditions)
        
        # Contar total
        count_query = f"SELECT COUNT(*) as total FROM ({base_query}) as subq"
        total_result = await self.db.fetch_one(count_query, values=params)
        total = total_result['total'] if total_result else 0
        
        # Agregar paginación
        base_query += " ORDER BY r.created_at DESC LIMIT :limit OFFSET :offset"
        params['limit'] = limit
        params['offset'] = offset
        
        # Ejecutar búsqueda
        results = await self.db.fetch_all(base_query, values=params)
        
        workflows = [
            {
                "request_id": r['request_id'],
                "proveedor": {
                    "rut": r['proveedor_rut'],
                    "nombre": r['proveedor_nombre']
                },
                "contrato": {
                    "monto": float(r['monto_contrato']) if r['monto_contrato'] else 0,
                    "objeto": r['objeto_contrato']
                },
                "status": r['status'],
                "created_at": str(r['created_at']),
                "riesgo": r['riesgo'],
                "cumplimiento": bool(r['cumplimiento']) if r['cumplimiento'] is not None else None,
                "certificado_id": r['certificado_id']
            }
            for r in results
        ]
        
        return {
            "total": total,
            "limit": limit,
            "offset": offset,
            "count": len(workflows),
            "workflows": workflows,
            "filtros_aplicados": {
                "query": query,
                "status": status,
                "riesgo": riesgo,
                "monto_min": monto_min,
                "monto_max": monto_max,
                "fecha_desde": fecha_desde,
                "fecha_hasta": fecha_hasta
            }
        }
    
    async def get_statistics_summary(self) -> Dict:
        """Obtiene estadísticas generales del sistema"""
        
        # Total de requests
        total_query = """
            SELECT 
                COUNT(*) as total_requests,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
                COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                SUM(monto_contrato) as monto_total,
                AVG(monto_contrato) as monto_promedio
            FROM requests
        """
        
        totals = await self.db.fetch_one(total_query)
        
        # Distribución de riesgos
        riesgo_query = """
            SELECT 
                riesgo,
                COUNT(*) as cantidad,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as porcentaje
            FROM workflow_executions
            GROUP BY riesgo
        """
        
        riesgos = await self.db.fetch_all(riesgo_query)
        
        # Cumplimiento
        cumplimiento_query = """
            SELECT 
                cumplimiento,
                COUNT(*) as cantidad
            FROM workflow_executions
            GROUP BY cumplimiento
        """
        
        cumplimientos = await self.db.fetch_all(cumplimiento_query)
        
        # Certificados emitidos
        cert_query = """
            SELECT 
                COUNT(*) as total_certificados,
                MIN(issued_at) as primer_certificado,
                MAX(issued_at) as ultimo_certificado
            FROM certificates
        """
        
        certs = await self.db.fetch_one(cert_query)
        
        # Top proveedores
        top_prov_query = """
            SELECT 
                proveedor_rut,
                proveedor_nombre,
                COUNT(*) as total_solicitudes,
                SUM(monto_contrato) as monto_total
            FROM requests
            GROUP BY proveedor_rut, proveedor_nombre
            ORDER BY total_solicitudes DESC
            LIMIT 10
        """
        
        top_proveedores = await self.db.fetch_all(top_prov_query)
        
        # Tiempo promedio de procesamiento
        time_query = """
            SELECT 
                AVG(EXTRACT(EPOCH FROM (timestamp_final - ingest_timestamp))) as avg_seconds
            FROM workflow_executions
            WHERE timestamp_final IS NOT NULL AND ingest_timestamp IS NOT NULL
        """
        
        time_result = await self.db.fetch_one(time_query)
        
        # Construir resumen
        total_cumple = sum(c['cantidad'] for c in cumplimientos if c['cumplimiento'])
        total_no_cumple = sum(c['cantidad'] for c in cumplimientos if not c['cumplimiento'])
        total_eval = total_cumple + total_no_cumple
        
        summary = {
            "resumen_general": {
                "total_solicitudes": totals['total_requests'],
                "completadas": totals['completed'],
                "en_proceso": totals['processing'],
                "fallidas": totals['failed'],
                "monto_total_contratado": float(totals['monto_total']) if totals['monto_total'] else 0,
                "monto_promedio": float(totals['monto_promedio']) if totals['monto_promedio'] else 0
            },
            "distribucion_riesgo": {
                r['riesgo']: {
                    "cantidad": r['cantidad'],
                    "porcentaje": float(r['porcentaje'])
                }
                for r in riesgos
            },
            "cumplimiento": {
                "total_evaluaciones": total_eval,
                "cumple": total_cumple,
                "no_cumple": total_no_cumple,
                "tasa_cumplimiento": round(total_cumple / total_eval * 100, 2) if total_eval > 0 else 0
            },
            "certificados": {
                "total_emitidos": certs['total_certificados'],
                "primer_certificado": str(certs['primer_certificado']) if certs['primer_certificado'] else None,
                "ultimo_certificado": str(certs['ultimo_certificado']) if certs['ultimo_certificado'] else None
            },
            "rendimiento": {
                "tiempo_promedio_procesamiento_segundos": round(float(time_result['avg_seconds']), 2) if time_result['avg_seconds'] else 0
            },
            "top_proveedores": [
                {
                    "rut": p['proveedor_rut'],
                    "nombre": p['proveedor_nombre'],
                    "total_solicitudes": p['total_solicitudes'],
                    "monto_total": float(p['monto_total']) if p['monto_total'] else 0
                }
                for p in top_proveedores
            ]
        }
        
        return summary
