from fastapi import APIRouter
from pydantic import BaseModel
from signing.p12_signer import P12HashSigner

router = APIRouter()

class SignHashRequest(BaseModel):
    hash_hex: str

@router.post("/hash")
def sign_hash(payload: SignHashRequest):
    # Nota: en desarrollo no existe /secrets/certificado.p12
    signer = P12HashSigner(
        p12_path="tests/certificado_test.p12",
        password_env="P12_PASSWORD"
    )
    firma = signer.sign_hash(payload.hash_hex)
    return {"firma_base64": firma}
@router.get("/test")
def test_sign():
    import hashlib
    signer = P12HashSigner()
    test_hash = hashlib.sha256(b"hola clean transparency").hexdigest()
    firma = signer.sign_hash(test_hash)
    return {
        "hash": test_hash,
        "firma_base64": firma
    }
