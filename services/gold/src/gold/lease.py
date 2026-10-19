"""Distributed lease on etcd with a fencing token.

etcd is a Raft-replicated, linearizable store, which is what a lock needs: the catalog's
Dynamo-style store cannot say which of two concurrent acquirers won, etcd can. The token is the
revision at which the key was created: it only grows, so a holder whose lease expired while it was
paused carries a token smaller than its successor's and gets rejected downstream.
"""

import base64
import logging
import threading
import time
from dataclasses import dataclass

import requests

log = logging.getLogger(__name__)


class LeaseError(Exception):
    pass


@dataclass(frozen=True)
class Lease:
    key: str
    holder: str
    lease_id: int
    token: int  # fencing token: create revision of the key


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


class EtcdLease:
    """Minimal client over etcd's gRPC gateway; good enough for one key and a keep-alive."""

    def __init__(self, endpoints: list[str], ttl_seconds: int = 20, timeout: float = 5.0):
        self.endpoints = endpoints
        self.ttl = ttl_seconds
        self.timeout = timeout
        self._stop = threading.Event()
        self._keepalive: threading.Thread | None = None

    def _call(self, path: str, body: dict) -> dict:
        last: Exception | None = None
        for endpoint in self.endpoints:
            try:
                resp = requests.post(f"{endpoint}/v3/{path}", json=body, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as err:
                last = err
        raise LeaseError(f"etcd unreachable: {last}")

    def acquire(self, key: str, holder: str, wait_seconds: float = 0.0, poll: float = 1.0) -> Lease:
        """Creates key only if absent (compare-and-swap), attached to a TTL lease."""
        deadline = time.monotonic() + wait_seconds
        while True:
            lease_id = int(self._call("lease/grant", {"TTL": self.ttl})["ID"])
            txn = self._call(
                "kv/txn",
                {
                    "compare": [{"key": _b64(key), "target": "CREATE", "create_revision": "0"}],
                    "success": [{"requestPut": {"key": _b64(key), "value": _b64(holder), "lease": str(lease_id)}}],
                    "failure": [{"requestRange": {"key": _b64(key)}}],
                },
            )
            if txn.get("succeeded"):
                token = int(txn["header"]["revision"])
                log.info("lease %s acquired by %s token=%d", key, holder, token)
                return Lease(key=key, holder=holder, lease_id=lease_id, token=token)
            self._call("lease/revoke", {"ID": str(lease_id)})
            first = txn.get("responses", [{}])[0]
            kvs = (first.get("response_range") or first.get("responseRange") or {}).get("kvs") or []
            owner = base64.b64decode(kvs[0].get("value", "")).decode() if kvs else "?"
            if time.monotonic() >= deadline:
                raise LeaseError(f"{key} is held by {owner}")
            log.info("lease %s held by %s, waiting", key, owner)
            time.sleep(poll)

    def keep_alive(self, lease: Lease) -> None:
        """Refreshes the TTL from a background thread; a paused process stops refreshing and loses the key."""

        def loop():
            while not self._stop.wait(self.ttl / 3):
                try:
                    self._call("lease/keepalive", {"ID": str(lease.lease_id)})
                except LeaseError as err:
                    log.warning("keep-alive failed: %s", err)

        self._keepalive = threading.Thread(target=loop, name="lease-keepalive", daemon=True)
        self._keepalive.start()

    def release(self, lease: Lease) -> None:
        self._stop.set()
        try:
            self._call("lease/revoke", {"ID": str(lease.lease_id)})
            log.info("lease %s released by %s", lease.key, lease.holder)
        except LeaseError as err:
            log.warning("release failed (lease will expire on its own): %s", err)

    def holder(self, key: str) -> str | None:
        resp = self._call("kv/range", {"key": _b64(key)})
        kvs = resp.get("kvs") or []
        return base64.b64decode(kvs[0]["value"]).decode() if kvs else None
