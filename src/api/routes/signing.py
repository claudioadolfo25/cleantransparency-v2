from fastapi import APIRouter
from pydantic import BaseModel
from src.signing.p12_signer import P12HashSigner

router = APIRouter()

class SignHashRequest(BaseModel):
    hash_hex: str

@router.get("/")
def signing_root():
    return {
        "status": "ok",
        "service": "signing",
        "available_endpoints": [
            "POST /hash",
            "GET /test"
        ]
    }

@router.post("/hash")
def sign_hash(payload: SignHashRequest):
    # Nota: en produccion usa /secrets/p12_certificado_v2
    signer = P12HashSigner(
        p12_path="/secrets/p12_certificado_v2",
        password_env="P12_PASSWORD"
    )
    firma = signer.sign_hash(payload.hash_hex)
    return {"firma_base64": firma}

@router.get("/test")
def test_sign():
    return {"status": "ok", "message": "signing endpoint disponible"}
