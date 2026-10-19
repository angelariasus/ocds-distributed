import logging
import time
from collections.abc import Callable

from prometheus_client import Counter, Gauge

log = logging.getLogger(__name__)

CLOSED, OPEN, HALF_OPEN = "closed", "open", "half_open"
STATE_CODES = {CLOSED: 0, OPEN: 1, HALF_OPEN: 2}

STATE = Gauge("extractor_breaker_state", "Circuit breaker state (0 closed, 1 open, 2 half-open).", ["name"])
TRANSITIONS = Counter("extractor_breaker_transitions_total", "Breaker state changes.", ["name", "to"])


class CircuitBreaker:
    """Stops hammering an upstream that keeps failing; lets a single probe through after a cooldown."""

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_seconds: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds
        self.clock = clock
        self.sleep = sleep
        self.state = CLOSED
        self.failures = 0
        self.opened_at = 0.0
        STATE.labels(name).set(0)

    def wait_until_allowed(self) -> None:
        if self.state == OPEN:
            remaining = self.opened_at + self.recovery_seconds - self.clock()
            if remaining > 0:
                log.info("breaker %s open, pausing %.1fs", self.name, remaining)
                self.sleep(remaining)
            self._transition(HALF_OPEN)

    def record_success(self) -> None:
        self.failures = 0
        if self.state != CLOSED:
            self._transition(CLOSED)

    def record_failure(self) -> None:
        self.failures += 1
        if self.state == HALF_OPEN or self.failures >= self.failure_threshold:
            self.opened_at = self.clock()
            self._transition(OPEN)

    def _transition(self, state: str) -> None:
        if state == self.state:
            return
        log.warning("breaker %s: %s -> %s", self.name, self.state, state)
        self.state = state
        STATE.labels(self.name).set(STATE_CODES[state])
        TRANSITIONS.labels(self.name, state).inc()
