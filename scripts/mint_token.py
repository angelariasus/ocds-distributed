"""Mints an ES256 JWT for the query API: python mint_token.py <role> [ttl_seconds] [subject]."""

import base64
import json
import sys
import time
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def main() -> None:
    role = sys.argv[1] if len(sys.argv) > 1 else "reader"
    ttl = int(sys.argv[2]) if len(sys.argv) > 2 else 3600
    subject = sys.argv[3] if len(sys.argv) > 3 else f"{role}@ocds"
    key = serialization.load_pem_private_key(Path("/pki/jwt-signing.key").read_bytes(), password=None)
    now = int(time.time())
    header = {"alg": "ES256", "typ": "JWT", "kid": "ocds-1"}
    claims = {"iss": "ocds-distributed", "aud": "ocds-api", "sub": subject, "roles": [role], "iat": now, "exp": now + ttl}
    signing_input = f"{b64url(json.dumps(header, separators=(',', ':')).encode())}.{b64url(json.dumps(claims, separators=(',', ':')).encode())}"
    r, s = decode_dss_signature(key.sign(signing_input.encode(), ec.ECDSA(hashes.SHA256())))
    print(f"{signing_input}.{b64url(r.to_bytes(32, 'big') + s.to_bytes(32, 'big'))}")


if __name__ == "__main__":
    main()
