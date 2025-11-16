class P12HashSigner:
    def __init__(self, p12_path: str = "/secrets/p12_certificado_v2", password_env: str = "P12_PASSWORD"):
        self.p12_path = p12_path
        self.password = os.getenv(password_env, "").encode()

        if not os.path.exists(p12_path):
            raise FileNotFoundError(f"No se encontró el archivo P12 en: {p12_path}")

        with open(p12_path, "rb") as f:
            p12_data = f.read()

        key, cert, _ = load_key_and_certificates(p12_data, self.password)
        self.private_key = key
        self.cert = cert

