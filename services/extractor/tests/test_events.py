import io
import json
import zipfile

from extractor.events import Emitter, LamportClock


class FakeProducer:
    def __init__(self):
        self.messages = []

    def produce(self, topic, key, value, headers):
        self.messages.append((topic, key, json.loads(value), dict(headers)))

    def flush(self, timeout):
        return 0

    def poll(self, timeout):
        return 0


def test_emit_page_keys_by_ocid_and_stamps_lamport():
    producer = FakeProducer()
    emitter = Emitter(producer, "bronze.records", "w1")
    records = [{"ocid": "ocds-a", "compiledRelease": {}}, {"ocid": "ocds-b"}, {"no": "ocid"}]

    emitter.emit_page(records, "seace_v3", "ocds/api/source=seace_v3/prefix=12/120001.json")

    assert [m[1] for m in producer.messages] == [b"ocds-a", b"ocds-b"]
    first, second = producer.messages[0][2], producer.messages[1][2]
    assert first["origin"] == "api:ocds/api/source=seace_v3/prefix=12/120001.json"
    assert first["worker_id"] == "w1" and first["record"] == records[0]
    assert (first["lamport"], second["lamport"]) == (1, 2)
    assert producer.messages[1][3]["lamport"] == b"2"


def test_emit_archive_streams_every_record(tmp_path):
    records = [
        {
            "ocid": f"ocds-{i}",
            "compiledRelease": {"date": "2026-09-01", "tender": {"value": {"amount": 1234.5, "n": i}}},
        }
        for i in range(2500)
    ]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("2026-09_seace_v3.json", json.dumps({"version": "1.1", "records": records}))
    archive = tmp_path / "m.zip"
    archive.write_bytes(buf.getvalue())
    producer = FakeProducer()

    n = Emitter(producer, "bronze.records", "w1", LamportClock()).emit_archive(archive, "seace_v3", "seace_v3-2026-09")

    assert n == 2500 and len(producer.messages) == 2500
    assert producer.messages[-1][2]["origin"] == "bulk:seace_v3-2026-09"
    assert producer.messages[-1][2]["lamport"] == 2500
    assert producer.messages[0][2]["record"]["compiledRelease"]["tender"]["value"] == {"amount": 1234.5, "n": 0}
