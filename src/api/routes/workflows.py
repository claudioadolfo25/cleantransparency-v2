from fastapi import APIRouter, Depends, HTTPException
from src.workflows.art17.flow import run_art17_workflow
from src.db.database import get_db
from src.db.repositories.art17_repository import Art17Repository
from pydantic import BaseModel
from typing import Optional

router = APIRouter()

class Art17Input(BaseModel):
    request_id: str
    proveedor_rut: str
    proveedor_nombre: Optional[str] = None
    monto_contrato: Optional[float] = None
    objeto_contrato: Optional[str] = None

@router.post("/art17/run")
async def run_art17(payload: Art17Input):
    """Ejecutar workflow Art17"""
    result = await run_art17_workflow(payload.dict())
    return {
        "status": "ok",
        "workflow": "art17",
        "result": result
    }

@router.get("/art17/{request_id}")
async def get_workflow(request_id: str, db = Depends(get_db)):
    """Obtener workflow por request_id desde BD"""
    repo = Art17Repository(db)
    workflow = await repo.get_workflow_by_request_id(request_id)
    
    if not workflow:
        raise HTTPException(status_code=404, detail="Workflow not found")
    
    return {
        "status": "ok",
        "workflow": workflow
    }

@router.get("/art17/certificate/{certificado_id}")
async def get_by_certificate(certificado_id: str, db = Depends(get_db)):
    """Obtener workflow por certificado_id"""
    repo = Art17Repository(db)
    workflow = await repo.get_workflow_by_certificate_id(certificado_id)
    
    if not workflow:
        raise HTTPException(status_code=404, detail="Certificate not found")
    
    return {
        "status": "ok",
        "workflow": workflow
    }

@router.get("/art17")
async def list_workflows(
    status: Optional[str] = None,
    limit: int = 50,
    db = Depends(get_db)
):
    """Listar workflows con filtros opcionales"""
    repo = Art17Repository(db)
    workflows = await repo.list_workflows(status=status, limit=limit)
    
    return {
        "status": "ok",
        "total": len(workflows),
        "workflows": workflows
    }

@router.get("/art17/stats/summary")
async def get_statistics(db = Depends(get_db)):
    """Obtener estadísticas de workflows"""
    repo = Art17Repository(db)
    stats = await repo.get_statistics()
    
    return {
        "status": "ok",
        "statistics": stats
    }
