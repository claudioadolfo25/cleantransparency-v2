from fastapi import FastAPI
from .routes import workflows, hitl, signing
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="CLEANTRANSPARENCY v2 API", version="2.0")

# CORS básico (luego lo endurecemos)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Registrar routers
app.include_router(workflows.router, prefix="/api/v2/workflows", tags=["workflows"])
app.include_router(hitl.router, prefix="/api/v2/hitl", tags=["hitl"])
app.include_router(signing.router, prefix="/api/v2/sign", tags=["signing"])

@app.get("/")
async def root():
    return {
        "status": "ok",
        "message": "CLEANTRANSPARENCY v2 activo",
        "version": "2.0"
    }
