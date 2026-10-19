import json
import logging
import random
from dataclasses import dataclass

import grpc
from prometheus_client import Counter

from catalog.v1 import catalog_pb2, catalog_pb2_grpc

log = logging.getLogger(__name__)

CATALOG_ERRORS = Counter("extractor_catalog_errors_total", "Catalog calls that failed.", ["op"])


@dataclass(frozen=True)
class Entry:
    value: dict | None
    context: bytes
    conflicts: int = 0


class Catalog:
    """Client of the Dynamo-style catalog; any node can coordinate, so one is picked at random per call."""

    def __init__(
        self, addresses: list[str], timeout: float = 5.0, stubs=None, credentials: grpc.ChannelCredentials | None = None
    ):
        self.timeout = timeout

        def channel(addr: str) -> grpc.Channel:
            return grpc.secure_channel(addr, credentials) if credentials else grpc.insecure_channel(addr)

        self.stubs = stubs or [catalog_pb2_grpc.CatalogStub(channel(addr)) for addr in addresses]

    def _stub(self):
        return random.choice(self.stubs)

    def get(self, key: str) -> Entry | None:
        try:
            resp = self._stub().Get(catalog_pb2.GetRequest(key=key), timeout=self.timeout)
        except grpc.RpcError as err:
            CATALOG_ERRORS.labels("get").inc()
            log.warning("catalog get %s failed: %s", key, err.code())
            return None
        if not resp.versions:
            return None
        # Siblings mean two writers raced; the caller only needs the most advanced checkpoint.
        values = [json.loads(v.value) for v in resp.versions]
        best = max(values, key=lambda v: v.get("pages", 0))
        return Entry(value=best, context=resp.context, conflicts=len(values) - 1)

    def put(self, key: str, value: dict, context: bytes = b"") -> bytes | None:
        try:
            resp = self._stub().Put(
                catalog_pb2.PutRequest(key=key, value=json.dumps(value).encode(), context=context),
                timeout=self.timeout,
            )
        except grpc.RpcError as err:
            CATALOG_ERRORS.labels("put").inc()
            log.warning("catalog put %s failed: %s", key, err.code())
            return None
        return resp.context


def mtls_credentials(ca: str, cert: str, key: str) -> grpc.ChannelCredentials:
    """Client identity for mutual TLS: the catalog rejects callers without a certificate from the internal CA."""

    def read(path: str) -> bytes:
        with open(path, "rb") as fh:
            return fh.read()

    return grpc.ssl_channel_credentials(root_certificates=read(ca), private_key=read(key), certificate_chain=read(cert))
