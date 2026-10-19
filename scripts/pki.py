"""Generates the project's internal PKI into deploy/pki (never committed).

- ca.{crt,key}: the internal certificate authority every service trusts.
- <service>.{crt,key}: one identity per service; SANs are the compose DNS names, and every
  certificate is valid for both TLS server and client use, which is what mutual TLS needs.
- rogue-ca.crt, intruder.{crt,key}: a certificate from a *different* CA, for negative tests.
- jwt-signing.key, jwks.json: ES256 key that signs API tokens and the JWKS Envoy verifies them with.
- ledger-signing.{key,pub}: Ed25519 key that signs the audit ledger's tree heads.

Run through scripts/gen_pki.sh (needs only Docker). Existing files are kept unless --force.
"""

import base64
import datetime as dt
import json
import sys
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

OUT = Path(sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("-") else "deploy/pki")
FORCE = "--force" in sys.argv

SERVICES = {
    "catalog": ["catalog", *[f"catalog{i}" for i in range(1, 6)], "localhost"],
    "ml": ["ml", "localhost"],
    "api": ["api", "localhost"],
    "envoy": ["envoy"],
    "extractor": ["extractor"],
}


def pem_key(key) -> bytes:
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())


def write(name: str, data: bytes) -> None:
    (OUT / name).write_bytes(data)
    print("  wrote", name)


def ca(common_name: str):
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name), x509.NameAttribute(NameOID.ORGANIZATION_NAME, "ocds-distributed")])
    now = dt.datetime.now(dt.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(x509.KeyUsage(False, False, False, False, False, True, True, False, False), critical=True)
        .sign(key, hashes.SHA256())
    )
    return key, cert


def leaf(ca_key, ca_cert, common_name: str, sans: list[str]):
    key = ec.generate_private_key(ec.SECP256R1())
    now = dt.datetime.now(dt.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(days=825))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(s) for s in sans]), critical=False)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    return key, cert


def b64url(n: int, length: int) -> str:
    return base64.urlsafe_b64encode(n.to_bytes(length, "big")).rstrip(b"=").decode()


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    if (OUT / "ca.crt").exists() and not FORCE:
        print(f"{OUT}/ca.crt exists; keeping the current PKI (use --force to regenerate)")
        return

    ca_key, ca_cert = ca("ocds-distributed internal CA")
    write("ca.key", pem_key(ca_key))
    write("ca.crt", ca_cert.public_bytes(serialization.Encoding.PEM))
    for service, sans in SERVICES.items():
        key, cert = leaf(ca_key, ca_cert, service, sans)
        write(f"{service}.key", pem_key(key))
        write(f"{service}.crt", cert.public_bytes(serialization.Encoding.PEM))

    rogue_key, rogue_cert = ca("rogue CA")
    write("rogue-ca.crt", rogue_cert.public_bytes(serialization.Encoding.PEM))
    intruder_key, intruder_cert = leaf(rogue_key, rogue_cert, "catalog", SERVICES["catalog"])
    write("intruder.key", pem_key(intruder_key))
    write("intruder.crt", intruder_cert.public_bytes(serialization.Encoding.PEM))

    jwt_key = ec.generate_private_key(ec.SECP256R1())
    write("jwt-signing.key", pem_key(jwt_key))
    numbers = jwt_key.public_key().public_numbers()
    jwks = {"keys": [{"kty": "EC", "crv": "P-256", "alg": "ES256", "use": "sig", "kid": "ocds-1", "x": b64url(numbers.x, 32), "y": b64url(numbers.y, 32)}]}
    write("jwks.json", json.dumps(jwks, indent=2).encode())

    ledger_key = ed25519.Ed25519PrivateKey.generate()
    write("ledger-signing.key", pem_key(ledger_key))
    write("ledger-signing.pub", ledger_key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo))


if __name__ == "__main__":
    main()
