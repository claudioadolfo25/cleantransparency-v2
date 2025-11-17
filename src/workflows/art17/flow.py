from langgraph.graph import StateGraph, END
from datetime import datetime
import uuid
import hashlib
from typing import TypedDict, Optional
import logging

logger = logging.getLogger(__name__)

class Art17State(TypedDict, total=False):
    request_id: Optional[str]
    proveedor_rut: Optional[str]
    proveedor_nombre: Optional[str]
    monto_contrato: Optional[float]
    objeto_contrato: Optional[str]
    ingest_timestamp: Optional[str]
    hash_ingest: Optional[str]
    riesgo: Optional[str]
    hash_riesgo: Optional[str]
    cumplimiento: Optional[bool]
    hash_compliance: Optional[str]
    certificado_id: Optional[str]
    timestamp_final: Optional[str]
    hash_final: Optional[str]
    workflow_id: Optional[str]

def compute_hash(state: Art17State):
    serialized = str(state).encode()
    return hashlib.sha256(serialized).hexdigest()

async def ingest(state: Art17State):
    from src.db.database import db
    from src.db.repositories.art17_repository import Art17Repository
    
    state["ingest_timestamp"] = datetime.utcnow().isoformat()
    state["hash_ingest"] = compute_hash(state)
    
    try:
        if db.db and db.is_connected():
            repo = Art17Repository(db.db)
            await repo.save_request({
                "request_id": state["request_id"],
                "proveedor_rut": state["proveedor_rut"],
                "proveedor_nombre": state.get("proveedor_nombre"),
                "monto_contrato": state.get("monto_contrato"),
                "objeto_contrato": state.get("objeto_contrato"),
                "status": "processing"
            })
            logger.info(f"✅ Request {state['request_id']} guardado")
        else:
            logger.warning("⚠️ BD no disponible")
    except Exception as e:
        logger.error(f"❌ Error guardando en BD: {e}")
    
    return state

async def risk_check(state: Art17State):
    rut = state.get("proveedor_rut", "")
    state["riesgo"] = "BAJO" if rut.endswith("0") else "MEDIO"
    state["hash_riesgo"] = compute_hash(state)
    logger.info(f"✅ Risk check: {state['riesgo']}")
    return state

async def compliance_check(state: Art17State):
    state["cumplimiento"] = True
    state["hash_compliance"] = compute_hash(state)
    logger.info(f"✅ Compliance: {state['cumplimiento']}")
    return state

async def final_report(state: Art17State):
    from src.db.database import db
    from src.db.repositories.art17_repository import Art17Repository
    
    state["certificado_id"] = f"CERT-{uuid.uuid4().hex[:10]}"
    state["timestamp_final"] = datetime.utcnow().isoformat()
    state["hash_final"] = compute_hash(state)
    
    try:
        if db.db and db.is_connected():
            repo = Art17Repository(db.db)
            
            workflow_id = await repo.save_workflow_execution({
                "request_id": state["request_id"],
                "workflow_type": "art17",
                "ingest_timestamp": state["ingest_timestamp"],
                "hash_ingest": state["hash_ingest"],
                "riesgo": state["riesgo"],
                "hash_riesgo": state["hash_riesgo"],
                "cumplimiento": state["cumplimiento"],
                "hash_compliance": state["hash_compliance"],
                "hash_final": state["hash_final"],
                "timestamp_final": state["timestamp_final"],
                "metadata": "{}"
            })
            
            state["workflow_id"] = workflow_id
            
            await repo.save_certificate({
                "certificado_id": state["certificado_id"],
                "request_id": state["request_id"],
                "hash_final": state["hash_final"],
                "issued_at": state["timestamp_final"]
            })
            
            await repo.save_request({
                "request_id": state["request_id"],
                "proveedor_rut": state["proveedor_rut"],
                "proveedor_nombre": state.get("proveedor_nombre"),
                "monto_contrato": state.get("monto_contrato"),
                "objeto_contrato": state.get("objeto_contrato"),
                "status": "completed"
            })
            
            logger.info(f"✅ Certificado: {state['certificado_id']}")
    except Exception as e:
        logger.error(f"❌ Error finalizando: {e}")
    
    return state

def build():
    g = StateGraph(Art17State)
    g.add_node("ingest", ingest)
    g.add_node("risk", risk_check)
    g.add_node("compliance", compliance_check)
    g.add_node("final", final_report)
    g.set_entry_point("ingest")
    g.add_edge("ingest", "risk")
    g.add_edge("risk", "compliance")
    g.add_edge("compliance", "final")
    g.add_edge("final", END)
    return g.compile()

workflow = build()

async def run_art17_workflow(input_data: dict):
    initial = Art17State(**input_data)
    result = await workflow.ainvoke(initial)
    return result
