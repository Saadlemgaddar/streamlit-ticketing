import json, hmac, hashlib, base64, os
SECRET = os.environ.get("APP_SECRET", "change-me-in-env").encode()

def sign_token(payload: dict) -> str:
    raw = json.dumps(payload, separators=(',', ':'), sort_keys=True).encode()
    sig = hmac.new(SECRET, raw, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(raw + b"." + sig).decode()

def verify_token(token: str):
    try:
        blob = base64.urlsafe_b64decode(token.encode())
        raw, sig = blob.rsplit(b".", 1)
        good = hmac.new(SECRET, raw, hashlib.sha256).digest()
        if not hmac.compare_digest(sig, good):
            return None
        return json.loads(raw.decode())
    except Exception:
        return None
