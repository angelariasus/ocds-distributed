import json
import logging
import threading
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import ijson
from prometheus_client import Counter

log = logging.getLogger(__name__)

EMITTED = Counter("extractor_observations_total", "Record observations published to bronze.records.", ["kind"])


class Producer(Protocol):
    def produce(self, topic: str, key: bytes, value: bytes, headers: list[tuple[str, bytes]]) -> None: ...
    def flush(self, timeout: float) -> int: ...
    def poll(self, timeout: float) -> int: ...


class LamportClock:
    """Per-worker logical clock stamped on every observation so the compiler can order them causally."""

    def __init__(self) -> None:
        self._time = 0
        self._lock = threading.Lock()

    def tick(self) -> int:
        with self._lock:
            self._time += 1
            return self._time


class Emitter:
    """Publishes one observation per OCDS record seen, keyed by OCID so every sighting of a process lands
    on the same partition."""

    def __init__(self, producer: Producer, topic: str, source_worker: str, clock: LamportClock | None = None):
        self.producer = producer
        self.topic = topic
        self.worker_id = source_worker
        self.clock = clock or LamportClock()

    def emit(self, record: dict, source: str, origin: str, kind: str) -> None:
        ocid = record.get("ocid")
        if not ocid:
            return
        lamport = self.clock.tick()
        observation = {
            "ocid": ocid,
            "source": source,
            "origin": origin,
            "worker_id": self.worker_id,
            "observed_at": datetime.now(UTC).isoformat(),
            "lamport": lamport,
            "record": record,
        }
        self.producer.produce(
            self.topic,
            key=ocid.encode(),
            value=json.dumps(observation, ensure_ascii=False).encode(),
            headers=[("lamport", str(lamport).encode())],
        )
        self.producer.poll(0)
        EMITTED.labels(kind).inc()

    def emit_page(self, records: list[dict], source: str, object_key: str) -> int:
        for record in records:
            self.emit(record, source, f"api:{object_key}", "api")
        return len(records)

    def emit_archive(self, archive: Path, source: str, file_id: str) -> int:
        """Streams the records array of the zipped monthly file without loading the 70 MB JSON at once."""
        count = 0
        with zipfile.ZipFile(archive) as zf:
            member = next(m for m in zf.infolist() if m.filename.endswith(".json"))
            with zf.open(member) as fh:
                # use_float: ijson yields Decimal by default, which json.dumps rejects.
                for record in ijson.items(fh, "records.item", use_float=True):
                    self.emit(record, source, f"bulk:{file_id}", "bulk")
                    count += 1
                    if count % 1000 == 0:
                        self.producer.flush(30)
        self.producer.flush(30)
        return count
