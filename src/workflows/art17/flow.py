from langgraph.graph import StateGraph, END
from datetime import datetime
import uuid
import hashlib
from typing import TypedDict, Optional


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


def compute_hash(state: Art17State):
    serialized = str(state).encode()
    return hashlib.sha256(serialized).hexdigest()

async def ingest(state: Art17State):
    state["ingest_timestamp"] = datetime.utcnow().isoformat()
    state["hash_ingest"] = compute_hash(state)
    return state

async def risk_check(state: Art17State):
    rut = state.get("proveedor_rut", "")
    state["riesgo"] = "BAJO" if rut.endswith("0") else "MEDIO"
    state["hash_riesgo"] = compute_hash(state)
    return state

async def compliance_check(state: Art17State):
    state["cumplimiento"] = True
    state["hash_compliance"] = compute_hash(state)
    return state

async def final_report(state: Art17State):
    state["certificado_id"] = f"CERT-{uuid.uuid4().hex[:10]}"
    state["timestamp_final"] = datetime.utcnow().isoformat()
    state["hash_final"] = compute_hash(state)
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
