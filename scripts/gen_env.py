"""Print a fresh .env with random cluster identifiers and credentials."""

import base64
import secrets
import uuid

cluster_id = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("=")

print(f"""KAFKA_CLUSTER_ID={cluster_id}

GARAGE_RPC_SECRET={secrets.token_hex(32)}
GARAGE_ADMIN_TOKEN={secrets.token_hex(24)}

S3_ACCESS_KEY=GK{secrets.token_hex(12)}
S3_SECRET_KEY={secrets.token_hex(32)}
S3_BUCKET=bronze

PLAN_SOURCES=seace_v3
PLAN_FROM=2026-08
PLAN_TO=2026-09
PLAN_INTERVAL=6h
EXTRACTOR_REPLICAS=3""")
