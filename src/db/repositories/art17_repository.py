"""
Repository for Art17 Workflow database operations
"""
from typing import Optional, Dict, List
from datetime import datetime
import uuid
import logging

logger = logging.getLogger(__name__)

class Art17Repository:
    """Repository for managing Art17 workflow data"""
    
    def __init__(self, db):
        self.db = db
    
    async def create_workflow(self, data: Dict) -> str:
        """Create a new Art17 workflow record"""
        workflow_id = str(uuid.uuid4())
        
        query = """
            INSERT INTO art17_workflows (
                id, request_id, proveedor_rut, proveedor_nombre,
                monto_contrato, objeto_contrato, ingest_timestamp,
                hash_ingest, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
        """
        
        await self.db.execute(
            query,
            workflow_id,
            data.get('request_id'),
            data.get('proveedor_rut'),
            data.get('proveedor_nombre'),
            data.get('monto_contrato'),
            data.get('objeto_contrato'),
            datetime.fromisoformat(data.get('ingest_timestamp')),
            data.get('hash_ingest'),
            'processing'
        )
        
        logger.info(f"Created workflow {workflow_id} for request {data.get('request_id')}")
        return workflow_id
    
    async def update_risk_check(self, request_id: str, data: Dict):
        """Update workflow with risk check results"""
        query = """
            UPDATE art17_workflows
            SET risk_check_timestamp = $1,
                riesgo = $2,
                hash_riesgo = $3
            WHERE request_id = $4
        """
        
        await self.db.execute(
            query,
            datetime.utcnow(),
            data.get('riesgo'),
            data.get('hash_riesgo'),
            request_id
        )
    
    async def update_compliance_check(self, request_id: str, data: Dict):
        """Update workflow with compliance check results"""
        query = """
            UPDATE art17_workflows
            SET compliance_check_timestamp = $1,
                cumplimiento = $2,
                hash_compliance = $3
            WHERE request_id = $4
        """
        
        await self.db.execute(
            query,
            datetime.utcnow(),
            data.get('cumplimiento'),
            data.get('hash_compliance'),
            request_id
        )
    
    async def finalize_workflow(self, request_id: str, data: Dict):
        """Finalize workflow with certificate"""
        query = """
            UPDATE art17_workflows
            SET final_timestamp = $1,
                certificado_id = $2,
                hash_final = $3,
                status = $4
            WHERE request_id = $5
        """
        
        await self.db.execute(
            query,
            datetime.fromisoformat(data.get('timestamp_final')),
            data.get('certificado_id'),
            data.get('hash_final'),
            'completed',
            request_id
        )
    
    async def get_workflow_by_request_id(self, request_id: str) -> Optional[Dict]:
        """Get workflow by request ID"""
        query = "SELECT * FROM art17_workflows WHERE request_id = $1"
        row = await self.db.fetchrow(query, request_id)
        return dict(row) if row else None
    
    async def list_workflows(self, status: Optional[str] = None, limit: int = 50) -> List[Dict]:
        """List workflows with optional filters"""
        query = """
            SELECT * FROM art17_workflows
            WHERE ($1::VARCHAR IS NULL OR status = $1)
            ORDER BY created_at DESC
            LIMIT $2
        """
        rows = await self.db.fetch(query, status, limit)
        return [dict(row) for row in rows]
