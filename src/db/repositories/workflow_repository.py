from databases import Database
from datetime import datetime
import json

class WorkflowRepository:
    def __init__(self, db: Database):
        self.db = db
    
    async def save_request(self, request_data: dict):
        query = """
            INSERT INTO requests (request_id, proveedor_rut, proveedor_nombre, 
                                 monto_contrato, objeto_contrato, status)
            VALUES (:request_id, :proveedor_rut, :proveedor_nombre, 
                    :monto_contrato, :objeto_contrato, :status)
            ON CONFLICT (request_id) DO UPDATE 
            SET status = :status
            RETURNING id
        """
        return await self.db.fetch_one(query, values=request_data)
    
    async def save_workflow_execution(self, workflow_data: dict):
        query = """
            INSERT INTO workflow_executions 
            (request_id, workflow_type, ingest_timestamp, hash_ingest,
             riesgo, hash_riesgo, cumplimiento, hash_compliance, 
             hash_final, timestamp_final, metadata)
            VALUES (:request_id, :workflow_type, :ingest_timestamp, :hash_ingest,
                    :riesgo, :hash_riesgo, :cumplimiento, :hash_compliance,
                    :hash_final, :timestamp_final, :metadata)
            RETURNING id
        """
        return await self.db.fetch_one(query, values=workflow_data)
    
    async def save_certificate(self, cert_data: dict):
        query = """
            INSERT INTO certificates 
            (certificado_id, request_id, workflow_execution_id, hash_final, 
             firma_digital, issued_at)
            VALUES (:certificado_id, :request_id, :workflow_execution_id, 
                    :hash_final, :firma_digital, :issued_at)
            RETURNING id
        """
        return await self.db.fetch_one(query, values=cert_data)
