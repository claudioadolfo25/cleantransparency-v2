import json
import os
import asyncio

HITL_QUEUE = {}

async def resolve_hitl_request(payload: dict):
    request_id = payload["request_id"]
    HITL_QUEUE[request_id] = {
        "decision": payload["decision"],
        "reviewer": payload["reviewer"],
        "notes": payload.get("notes", ""),
    }
    await asyncio.sleep(0.1)
