from extractor.breaker import CLOSED, HALF_OPEN, OPEN, CircuitBreaker


class FakeClock:
    def __init__(self):
        self.t = 100.0
        self.slept: list[float] = []

    def now(self):
        return self.t

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.t += seconds


def make(threshold=3, recovery=10.0):
    clock = FakeClock()
    return CircuitBreaker("test", threshold, recovery, clock=clock.now, sleep=clock.sleep), clock


def test_opens_after_threshold_failures():
    breaker, _ = make()
    for _ in range(2):
        breaker.record_failure()
    assert breaker.state == CLOSED
    breaker.record_failure()
    assert breaker.state == OPEN


def test_waits_for_recovery_then_probes():
    breaker, clock = make(threshold=1, recovery=10.0)
    breaker.record_failure()
    clock.t += 4
    breaker.wait_until_allowed()
    assert clock.slept == [6.0]
    assert breaker.state == HALF_OPEN


def test_probe_failure_reopens_immediately():
    breaker, clock = make(threshold=3, recovery=10.0)
    for _ in range(3):
        breaker.record_failure()
    clock.t += 10
    breaker.wait_until_allowed()
    breaker.record_failure()
    assert breaker.state == OPEN
    assert breaker.opened_at == clock.t


def test_probe_success_closes_and_resets():
    breaker, clock = make(threshold=2, recovery=10.0)
    breaker.record_failure()
    breaker.record_failure()
    clock.t += 10
    breaker.wait_until_allowed()
    breaker.record_success()
    assert breaker.state == CLOSED
    assert breaker.failures == 0
    breaker.record_failure()
    assert breaker.state == CLOSED
