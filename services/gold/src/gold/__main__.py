import argparse
import logging
import os
import signal
import time

from . import relay
from .lease import EtcdLease
from .saga import GoldSaga
from .silver import read_items
from .star import build
from .warehouse import PostgresWarehouse

log = logging.getLogger("gold")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    env = os.environ.get
    parser = argparse.ArgumentParser(prog="gold")
    sub = parser.add_subparsers(dest="command", required=True)
    load = sub.add_parser("load", help="run the Gold load saga once")
    load.add_argument("--fail-at", default=env("FAIL_AT"), help="inject a failure at a step (staging, swap)")
    load.add_argument("--pause-after-lease", type=float, default=float(env("PAUSE_AFTER_LEASE", "0")), help="seconds to sleep after acquiring the lease")
    load.add_argument("--wait", type=float, default=float(env("LEASE_WAIT_SECONDS", "0")), help="seconds to wait for the lease")
    sub.add_parser("relay", help="publish outbox events to Kafka forever")
    args = parser.parse_args()

    dsn = env("GOLD_DSN", "postgresql://postgres:postgres@haproxy:5000/postgres")
    if args.command == "relay":
        relay.run(dsn, env("KAFKA_BROKERS", "kafka1:9092"), env("GOLD_EVENTS_TOPIC", "gold.events"), float(env("RELAY_INTERVAL", "2")))
        return

    def pause():
        if args.pause_after_lease > 0:
            log.warning("pausing %.0fs after acquiring the lease (simulating a stalled loader)", args.pause_after_lease)
            time.sleep(args.pause_after_lease)

    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt))
    saga = GoldSaga(
        leases=EtcdLease(env("ETCD_ENDPOINTS", "http://etcd1:2379").split(","), ttl_seconds=int(env("LEASE_TTL_SECONDS", "20"))),
        warehouse=PostgresWarehouse(dsn),
        read_silver=lambda: build(read_items(env("SILVER_ITEMS_PATH", "items"))),
        holder=env("HOLDER", None),
        lease_wait_seconds=args.wait,
        fail_at=args.fail_at,
        pause_after_lease=pause,
    )
    outcome = saga.run()
    log.info(
        "load %s token=%d years=%s rows_fact=%d steps=%s took=%.1fs",
        outcome.status,
        outcome.token,
        outcome.years,
        outcome.rows_fact,
        outcome.steps,
        outcome.duration_seconds,
    )
    raise SystemExit(0 if outcome.status == "committed" else 1)


if __name__ == "__main__":
    main()
