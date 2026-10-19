import pandas as pd
import pytest

from gold.lease import Lease, LeaseError
from gold.saga import GoldSaga
from gold.star import StarFrames


class FakeLeases:
    """In-memory etcd: one key, create-if-absent, tokens strictly increasing."""

    def __init__(self):
        self.revision = 10
        self.held: dict[str, str] = {}
        self.log: list[str] = []

    def acquire(self, key, holder, wait_seconds=0.0, poll=1.0):
        if key in self.held:
            raise LeaseError(f"{key} is held by {self.held[key]}")
        self.revision += 1
        self.held[key] = holder
        self.log.append(f"acquire:{holder}:{self.revision}")
        return Lease(key=key, holder=holder, lease_id=self.revision, token=self.revision)

    def keep_alive(self, lease):
        pass

    def release(self, lease):
        self.held.pop(lease.key, None)
        self.log.append(f"release:{lease.holder}")

    def expire(self, key):
        self.held.pop(key, None)


class FakeWarehouse:
    def __init__(self):
        self.calls: list[str] = []
        self.fence = 0
        self.loads: dict[int, str] = {}
        self.staged: set[int] = set()

    def ensure_schema(self):
        self.calls.append("schema")

    def load_staging(self, token, frames):
        self.calls.append(f"staging:{token}")
        self.staged.add(token)

    def swap(self, token, holder, years):
        if token < self.fence:
            raise RuntimeError(f"stale fencing token {token}")
        self.fence = token
        self.calls.append(f"swap:{token}:{years}")
        return 42

    def drop_staging(self, token):
        self.calls.append(f"drop:{token}")
        self.staged.discard(token)

    def record_load(self, token, holder, status, years, rows):
        self.loads[token] = status


def frames():
    fact = pd.DataFrame({"anio_fiscal": pd.Series([2025, 2026, 2026], dtype="Int64")})
    empty = pd.DataFrame()
    return StarFrames(tiempo=empty, entidad=empty, proveedor=empty, item=empty, tipo_proceso=empty, fact=fact)


def test_happy_path_runs_every_step_in_order():
    leases, wh = FakeLeases(), FakeWarehouse()
    outcome = GoldSaga(leases, wh, frames, holder="loader-a").run()

    assert outcome.status == "committed" and outcome.rows_fact == 42 and outcome.years == [2025, 2026]
    assert outcome.steps == ["acquire", "schema", "read_silver", "staging", "swap", "cleanup", "release"]
    assert wh.calls == ["schema", "staging:11", "swap:11:[2025, 2026]", "drop:11"]
    assert wh.loads == {11: "committed"} and leases.held == {}


def test_failure_at_swap_compensates_and_leaves_production_untouched():
    leases, wh = FakeLeases(), FakeWarehouse()
    outcome = GoldSaga(leases, wh, frames, holder="loader-a", fail_at="swap").run()

    assert outcome.status == "failed:swap"
    assert "swap:11:[2025, 2026]" not in wh.calls and "drop:11" in wh.calls
    assert wh.staged == set() and wh.fence == 0
    assert wh.loads == {11: "failed:swap"} and leases.held == {}


def test_second_loader_cannot_acquire_while_first_holds_the_lease():
    leases, wh = FakeLeases(), FakeWarehouse()
    leases.acquire("/locks/gold-loader", "loader-a")
    with pytest.raises(LeaseError):
        GoldSaga(leases, wh, frames, holder="loader-b").run()


def test_stalled_loader_is_fenced_out_by_its_successor():
    leases, wh = FakeLeases(), FakeWarehouse()

    def stall():
        # Loader A's lease expires while it is paused; loader B acquires and completes a load.
        leases.expire("/locks/gold-loader")
        GoldSaga(leases, wh, frames, holder="loader-b").run()

    outcome = GoldSaga(leases, wh, frames, holder="loader-a", pause_after_lease=stall).run()

    assert outcome.status == "failed:swap"
    assert wh.fence == 12  # loader B's token stays the highest accepted write
    assert wh.loads == {12: "committed", 11: "failed:swap"}
    assert [c for c in wh.calls if c.startswith("swap")] == ["swap:12:[2025, 2026]"]
