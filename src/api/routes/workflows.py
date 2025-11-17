from fastapi import APIRouter
from src.workflows.art17.flow import run_art17_workflow
from pydantic import BaseModel
from typing import Optional

router = APIRouter()

class Art17Input(BaseModel):
    request_id: str
    proveedor_rut: str
    proveedor_nombre: Optional[str] = None
    monto_contrato: Optional[float] = None
    objeto_contrato: Optional[str] = None

@router.get("/")
async def workflows_root():
    return {
        "status": "ok",
        "service": "workflows",
        "available_endpoints": [
            "POST /art17/run"
        ]
    }

@router.post("/art17/run")
async def run_art17(payload: Art17Input):
    result = await run_art17_workflow(payload.dict())
    return {
        "status": "ok",
        "workflow": "art17",
        "result": result
    }
