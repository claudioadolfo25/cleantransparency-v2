from fastapi import APIRouter
from pydantic import BaseModel
from src.services.hitl import resolve_hitl_request

router = APIRouter()

class HitlDecision(BaseModel):
    request_id: str
    decision: str
    reviewer: str
    notes: str = ""

@router.post("/resolve")
async def resolve_hitl(decision: HitlDecision):
    await resolve_hitl_request(decision.dict())
    return {"status": "ok", "message": "decision registrada"}
