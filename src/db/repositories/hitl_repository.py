import logging
from typing import Dict, List, Optional
from datetime import datetime
from src.db.database import database

logger = logging.getLogger(__name__)

class HITLRepository:
    def __init__(self):
        self.db = database
    
    # ==================== LISTAR CASOS PENDIENTES ====================
    
    async def get_pending_hitl_cases(self, limit: int = 50, offset: int = 0) -> Dict:
        """Obtiene todos los casos que requieren revisión humana"""
        try:
            query = """
                SELECT 
                    w.request_id,
                    w.proveedor_rut,
                    w.proveedor_nombre,
                    w.nivel_riesgo,
                    w.riesgo_score,
                    w.hitl_reason,
                    w.created_at,
                    w.updated_at,
                    r.monto_contrato,
                    r.objeto_contrato
                FROM workflow_executions w
                LEFT JOIN requests r ON w.request_id = r.request_id
                WHERE w.hitl_required = true 
                  AND w.hitl_decision IS NULL
                  AND w.status = 'hitl_required'
                ORDER BY 
                    CASE w.nivel_riesgo
                        WHEN 'alto' THEN 1
                        WHEN 'medio' THEN 2
                        WHEN 'bajo' THEN 3
                    END,
                    w.created_at ASC
                LIMIT :limit OFFSET :offset
            """
            
            results = await self.db.fetch_all(
                query=query, 
                values={"limit": limit, "offset": offset}
            )
            
            # Contar total
            count_query = """
                SELECT COUNT(*) as total
                FROM workflow_executions
                WHERE hitl_required = true 
                  AND hitl_decision IS NULL
                  AND status = 'hitl_required'
            """
            total_result = await self.db.fetch_one(query=count_query)
            
            return {
                "count": len(results),
                "total": total_result["total"] if total_result else 0,
                "pending_cases": [dict(row) for row in results]
            }
        except Exception as e:
            logger.error(f"Error obteniendo casos HITL pendientes: {e}")
            raise
    
    # ==================== OBTENER CASO ESPECÍFICO ====================
    
    async def get_hitl_case_detail(self, request_id: str) -> Optional[Dict]:
        """Obtiene detalle completo de un caso HITL"""
        try:
            query = """
                SELECT 
                    w.*,
                    r.proveedor_nombre as request_proveedor_nombre,
                    r.monto_contrato,
                    r.objeto_contrato,
                    r.status as request_status
                FROM workflow_executions w
                LEFT JOIN requests r ON w.request_id = r.request_id
                WHERE w.request_id = :request_id
                  AND w.hitl_required = true
            """
            
            result = await self.db.fetch_one(
                query=query,
                values={"request_id": request_id}
            )
            
            if not result:
                return None
            
            case = dict(result)
            
            # Obtener audit trail
            audit_query = """
                SELECT action, user_id, details, timestamp
                FROM audit_log
                WHERE request_id = :request_id
                ORDER BY timestamp DESC
            """
            audit_results = await self.db.fetch_all(
                query=audit_query,
                values={"request_id": request_id}
            )
            
            case["audit_trail"] = [dict(row) for row in audit_results]
            
            return case
        except Exception as e:
            logger.error(f"Error obteniendo detalle de caso HITL {request_id}: {e}")
            raise
    
    # ==================== TOMAR DECISIÓN HITL ====================
    
    async def submit_hitl_decision(
        self,
        request_id: str,
        decision: str,  # 'approve', 'reject', 'escalate'
        reviewer: str,
        notes: Optional[str] = None
    ) -> Dict:
        """Registra la decisión del revisor humano"""
        try:
            # Validar decisión
            valid_decisions = ['approve', 'reject', 'escalate']
            if decision not in valid_decisions:
                raise ValueError(f"Decisión inválida. Debe ser: {valid_decisions}")
            
            # Determinar nuevo status
            new_status = {
                'approve': 'completed',
                'reject': 'failed',
                'escalate': 'hitl_required'
            }[decision]
            
            # Actualizar workflow
            update_query = """
                UPDATE workflow_executions
                SET 
                    hitl_decision = :decision,
                    hitl_reviewer = :reviewer,
                    hitl_reviewed_at = NOW(),
                    hitl_notes = :notes,
                    status = :new_status,
                    updated_at = NOW(),
                    completed_at = CASE 
                        WHEN :decision = 'approve' THEN NOW()
                        WHEN :decision = 'reject' THEN NOW()
                        ELSE completed_at
                    END
                WHERE request_id = :request_id
                  AND hitl_required = true
                  AND hitl_decision IS NULL
                RETURNING *
            """
            
            result = await self.db.fetch_one(
                query=update_query,
                values={
                    "request_id": request_id,
                    "decision": decision,
                    "reviewer": reviewer,
                    "notes": notes,
                    "new_status": new_status
                }
            )
            
            if not result:
                raise ValueError(f"Caso {request_id} no encontrado o ya fue revisado")
            
            # Registrar en audit log
            audit_query = """
                INSERT INTO audit_log (request_id, action, user_id, details, timestamp)
                VALUES (:request_id, :action, :user_id, :details, NOW())
            """
            
            await self.db.execute(
                query=audit_query,
                values={
                    "request_id": request_id,
                    "action": f"HITL_DECISION_{decision.upper()}",
                    "user_id": reviewer,
                    "details": {"decision": decision, "notes": notes}
                }
            )
            
            logger.info(f"Decisión HITL registrada: {request_id} - {decision} por {reviewer}")
            
            return {
                "success": True,
                "request_id": request_id,
                "decision": decision,
                "new_status": new_status,
                "reviewed_by": reviewer,
                "reviewed_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error registrando decisión HITL para {request_id}: {e}")
            raise
    
    # ==================== ESTADÍSTICAS HITL ====================
    
    async def get_hitl_statistics(self) -> Dict:
        """Obtiene estadísticas del sistema HITL"""
        try:
            query = """
                SELECT 
                    COUNT(*) FILTER (WHERE hitl_required = true) as total_hitl_cases,
                    COUNT(*) FILTER (WHERE hitl_required = true AND hitl_decision IS NULL) as pending,
                    COUNT(*) FILTER (WHERE hitl_decision = 'approve') as approved,
                    COUNT(*) FILTER (WHERE hitl_decision = 'reject') as rejected,
                    COUNT(*) FILTER (WHERE hitl_decision = 'escalate') as escalated,
                    AVG(EXTRACT(EPOCH FROM (hitl_reviewed_at - created_at))/3600) 
                        FILTER (WHERE hitl_reviewed_at IS NOT NULL) as avg_review_time_hours
                FROM workflow_executions
                WHERE created_at > NOW() - INTERVAL '30 days'
            """
            
            result = await self.db.fetch_one(query=query)
            
            return dict(result) if result else {}
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas HITL: {e}")
            raise
