from fastapi import APIRouter
from workflows.art17.flow import run_art17_workflow
from pydantic import BaseModel

router = APIRouter()

class Art17Input(BaseModel):
    request_id: str
    proveedor_rut: str

@router.post("/art17/run")
async def run_art17(payload: Art17Input):
    result = await run_art17_workflow(payload.dict())
    return {
        "status": "ok",
        "workflow": "art17",
        "result": result
    }
