import logging
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager

import grpc
from prometheus_client import Counter, Histogram

from limiter.v1 import limiter_pb2, limiter_pb2_grpc

log = logging.getLogger(__name__)

OK = limiter_pb2.OUTCOME_OK
THROTTLED = limiter_pb2.OUTCOME_THROTTLED
ERROR = limiter_pb2.OUTCOME_ERROR
# The upstream never answered, so there is no load signal to feed the controller.
UNREPORTED = -1

PERMIT_WAIT = Histogram(
    "extractor_permit_wait_seconds",
    "Time spent waiting for a permit from the limiter.",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)
FALLBACKS = Counter("extractor_limiter_fallbacks_total", "Permits granted locally because the limiter was unreachable.")


class Permit:
    def __init__(self) -> None:
        self.outcome = OK


class RateLimiter:
    """Client of the global limiter. Falls back to a conservative local rate when it is unreachable."""

    def __init__(
        self,
        address: str,
        resource: str,
        client_id: str,
        fallback_rate: float = 0.5,
        rpc_timeout: float = 120.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        stub: limiter_pb2_grpc.LimiterStub | None = None,
    ):
        self.resource = resource
        self.client_id = client_id
        self.fallback_interval = 1.0 / fallback_rate
        self.rpc_timeout = rpc_timeout
        self.clock = clock
        self.sleep = sleep
        self.stub = stub or limiter_pb2_grpc.LimiterStub(grpc.insecure_channel(address))
        self._lock = threading.Lock()
        self._last_local: float | None = None

    @contextmanager
    def permit(self) -> Iterator[Permit]:
        self._acquire()
        permit = Permit()
        start = self.clock()
        try:
            yield permit
        except Exception:
            if permit.outcome == OK:
                permit.outcome = ERROR
            raise
        finally:
            self._report(permit.outcome, self.clock() - start)

    def _acquire(self) -> None:
        start = self.clock()
        try:
            self.stub.Acquire(
                limiter_pb2.AcquireRequest(resource=self.resource, client_id=self.client_id),
                timeout=self.rpc_timeout,
            )
        except grpc.RpcError as err:
            log.warning("limiter unavailable (%s), using local fallback rate", err.code())
            FALLBACKS.inc()
            self._local_wait()
        PERMIT_WAIT.observe(self.clock() - start)

    def _local_wait(self) -> None:
        with self._lock:
            if self._last_local is not None:
                wait = self._last_local + self.fallback_interval - self.clock()
                if wait > 0:
                    self.sleep(wait)
            self._last_local = self.clock()

    def _report(self, outcome: int, latency: float) -> None:
        if outcome == UNREPORTED:
            return
        try:
            self.stub.Report(
                limiter_pb2.ReportRequest(
                    resource=self.resource,
                    client_id=self.client_id,
                    latency_ms=int(latency * 1000),
                    outcome=outcome,
                ),
                timeout=5.0,
            )
        except grpc.RpcError as err:
            log.debug("limiter report dropped: %s", err.code())
