import grpc
import pytest

from extractor import limiter as lim
from limiter.v1 import limiter_pb2


class FakeRpcError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


class FakeStub:
    def __init__(self, available=True):
        self.available = available
        self.acquires = 0
        self.reports: list[limiter_pb2.ReportRequest] = []

    def Acquire(self, request, timeout=None):
        if not self.available:
            raise FakeRpcError()
        self.acquires += 1
        return limiter_pb2.AcquireResponse(waited_ms=0, rate=2.0)

    def Report(self, request, timeout=None):
        if not self.available:
            raise FakeRpcError()
        self.reports.append(request)
        return limiter_pb2.ReportResponse()


class FakeClock:
    def __init__(self):
        self.t = 0.0
        self.slept: list[float] = []

    def now(self):
        return self.t

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.t += seconds


def make(available=True, fallback_rate=0.5):
    stub = FakeStub(available)
    clock = FakeClock()
    rl = lim.RateLimiter("ignored", "portal", "w1", fallback_rate, clock=clock.now, sleep=clock.sleep, stub=stub)
    return rl, stub, clock


def test_reports_latency_and_ok_outcome():
    rl, stub, clock = make()
    with rl.permit():
        clock.t += 0.25
    assert stub.acquires == 1
    assert stub.reports[0].outcome == limiter_pb2.OUTCOME_OK
    assert stub.reports[0].latency_ms == 250


def test_exception_reports_error_unless_marked_throttled():
    rl, stub, _ = make()
    with pytest.raises(RuntimeError), rl.permit():
        raise RuntimeError("boom")
    assert stub.reports[0].outcome == limiter_pb2.OUTCOME_ERROR

    with pytest.raises(RuntimeError), rl.permit() as permit:
        permit.outcome = lim.THROTTLED
        raise RuntimeError("429")
    assert stub.reports[1].outcome == limiter_pb2.OUTCOME_THROTTLED


def test_falls_back_to_local_rate_when_limiter_is_down():
    rl, _, clock = make(available=False, fallback_rate=0.5)
    with rl.permit():
        pass
    with rl.permit():
        pass
    assert clock.slept == [2.0]


def test_unreported_outcome_sends_nothing():
    rl, stub, _ = make()
    with pytest.raises(OSError), rl.permit() as permit:
        permit.outcome = lim.UNREPORTED
        raise OSError("connection refused")
    assert stub.reports == []
