"""The Gold load as a saga: a sequence of steps with compensations.

    acquire lease ─▶ read Silver ─▶ load staging ─▶ swap (one transaction: fence check,
    dimension upserts, fact replace per year, outbox event, load record) ─▶ cleanup ─▶ release

A failure after staging drops the staging tables and records the failed load; the swap itself is
atomic in PostgreSQL so production is either untouched or fully replaced for the loaded years.
Everything downstream (Kafka consumers of gold.events) sees the load only after the outbox relay
publishes the event the swap wrote, which is what makes "load committed" and "event published"
agree without a two-phase commit between PostgreSQL and Kafka.
"""

import logging
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from .lease import Lease
from .star import StarFrames

log = logging.getLogger(__name__)

LOCK_KEY = "/locks/gold-loader"


class LeaseStore(Protocol):
    def acquire(self, key: str, holder: str, wait_seconds: float = 0.0, poll: float = 1.0) -> Lease: ...
    def keep_alive(self, lease: Lease) -> None: ...
    def release(self, lease: Lease) -> None: ...


class Warehouse(Protocol):
    def ensure_schema(self) -> None: ...
    def load_staging(self, token: int, frames: StarFrames) -> None: ...
    def swap(self, token: int, holder: str, years: list[int]) -> int: ...
    def drop_staging(self, token: int) -> None: ...
    def record_load(self, token: int, holder: str, status: str, years: list[int], rows: int | None) -> None: ...


class StepFailed(Exception):
    def __init__(self, step: str, cause: Exception):
        super().__init__(f"step {step} failed: {cause}")
        self.step = step
        self.cause = cause


@dataclass
class Outcome:
    token: int
    status: str
    years: list[int]
    rows_fact: int = 0
    steps: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0


class GoldSaga:
    def __init__(
        self,
        leases: LeaseStore,
        warehouse: Warehouse,
        read_silver: Callable[[], StarFrames],
        holder: str | None = None,
        lease_wait_seconds: float = 0.0,
        fail_at: str | None = None,
        pause_after_lease: Callable[[], None] | None = None,
    ):
        self.leases = leases
        self.warehouse = warehouse
        self.read_silver = read_silver
        self.holder = holder or socket.gethostname()
        self.lease_wait_seconds = lease_wait_seconds
        self.fail_at = fail_at  # test hook: name of the step that must blow up
        self.pause_after_lease = pause_after_lease  # test hook: simulate a long GC pause / SIGSTOP

    def _step(self, name: str, outcome: Outcome, fn: Callable[[], object]) -> object:
        if self.fail_at == name:
            raise StepFailed(name, RuntimeError("injected failure"))
        try:
            result = fn()
        except Exception as err:
            raise StepFailed(name, err) from err
        outcome.steps.append(name)
        return result

    def run(self) -> Outcome:
        started = time.monotonic()
        lease = self.leases.acquire(LOCK_KEY, self.holder, wait_seconds=self.lease_wait_seconds)
        outcome = Outcome(token=lease.token, status="running", years=[], steps=["acquire"])
        self.leases.keep_alive(lease)
        staged = False
        try:
            if self.pause_after_lease:
                self.pause_after_lease()
            self._step("schema", outcome, self.warehouse.ensure_schema)
            frames = self._step("read_silver", outcome, self.read_silver)
            outcome.years = sorted(int(y) for y in frames.fact["anio_fiscal"].dropna().unique())
            self._step("staging", outcome, lambda: self.warehouse.load_staging(lease.token, frames))
            staged = True
            outcome.rows_fact = self._step("swap", outcome, lambda: self.warehouse.swap(lease.token, self.holder, outcome.years))
            outcome.status = "committed"
            self.warehouse.record_load(lease.token, self.holder, "committed", outcome.years, outcome.rows_fact)
        except StepFailed as err:
            outcome.status = f"failed:{err.step}"
            log.error("saga aborted at %s: %s", err.step, err.cause)
            try:
                self.warehouse.record_load(lease.token, self.holder, outcome.status, outcome.years, None)
            except Exception as rec_err:
                log.warning("could not record failed load: %s", rec_err)
        finally:
            if staged:
                try:
                    self.warehouse.drop_staging(lease.token)
                    outcome.steps.append("cleanup")
                except Exception as err:
                    log.warning("staging cleanup failed for token %d: %s", lease.token, err)
            self.leases.release(lease)
            outcome.steps.append("release")
            outcome.duration_seconds = time.monotonic() - started
        return outcome
