import hashlib
import io
import zipfile
from pathlib import Path

import pytest

from extractor.extractor import ExhaustedError, Extractor
from extractor.integrity import IntegrityError, inner_json_sha256
from extractor.models import Task

PAYLOAD = b'{"records": []}'
PAYLOAD_SHA = hashlib.sha256(PAYLOAD).hexdigest()

TASK = Task(
    task_id="seace_v3-2026-09",
    source="seace_v3",
    year=2026,
    month=9,
    url="https://example/json",
    sha_url="https://example/sha",
    published_at="2026-10-17T12:30:14-05:00",
    planned_at="2026-10-18T00:00:00Z",
)


def make_zip(payload: bytes, name: str = "2026-09_seace_v3.json") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(name, payload)
    return buf.getvalue()


class FakeSource:
    def __init__(self, archives: list[bytes], sha: str = PAYLOAD_SHA):
        self.archives = archives
        self.sha = sha
        self.downloads = 0

    def fetch_sha(self, url: str) -> str:
        return self.sha

    def download(self, url: str, dest: Path) -> int:
        data = self.archives[min(self.downloads, len(self.archives) - 1)]
        self.downloads += 1
        dest.write_bytes(data)
        return len(data)


class FakeStore:
    def __init__(self):
        self.objects: dict[str, dict[str, str]] = {}
        self.saved: dict[str, bytes] = {}

    def stored_sha(self, key: str) -> str | None:
        return self.objects.get(key, {}).get("sha256")

    def put(self, key: str, path: Path, metadata: dict[str, str]) -> None:
        self.objects[key] = metadata
        self.saved[key] = path.read_bytes()

    def get_file(self, key: str, dest: Path) -> None:
        dest.write_bytes(self.saved[key])


def extractor(source, store, attempts=3):
    return Extractor(source, store, "w1", max_attempts=attempts, sleep=lambda _: None)


def test_stores_new_archive_with_checksum_metadata():
    store = FakeStore()
    result = extractor(FakeSource([make_zip(PAYLOAD)]), store).handle(TASK)

    assert result.status == "stored"
    assert result.attempts == 1
    assert store.objects[TASK.object_key]["sha256"] == PAYLOAD_SHA


def test_skips_when_stored_checksum_matches():
    store = FakeStore()
    store.objects[TASK.object_key] = {"sha256": PAYLOAD_SHA}
    source = FakeSource([make_zip(PAYLOAD)])

    result = extractor(source, store).handle(TASK)

    assert result.status == "skipped"
    assert source.downloads == 0


def test_refetches_when_portal_republished_file():
    store = FakeStore()
    store.objects[TASK.object_key] = {"sha256": "stale"}

    result = extractor(FakeSource([make_zip(PAYLOAD)]), store).handle(TASK)

    assert result.status == "stored"
    assert store.objects[TASK.object_key]["sha256"] == PAYLOAD_SHA


def test_retries_after_checksum_mismatch():
    source = FakeSource([make_zip(b"regenerating"), make_zip(PAYLOAD)])

    result = extractor(source, FakeStore()).handle(TASK)

    assert result.status == "stored"
    assert result.attempts == 2


def test_gives_up_after_max_attempts():
    source = FakeSource([make_zip(b"corrupted")])
    store = FakeStore()

    with pytest.raises(ExhaustedError) as exc:
        extractor(source, store, attempts=3).handle(TASK)

    assert exc.value.attempts == 3
    assert isinstance(exc.value.cause, IntegrityError)
    assert source.downloads == 3
    assert store.objects == {}


def test_inner_sha_rejects_unexpected_archive_layout(tmp_path):
    archive = tmp_path / "a.zip"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("a.json", PAYLOAD)
        zf.writestr("b.json", PAYLOAD)
    archive.write_bytes(buf.getvalue())

    with pytest.raises(IntegrityError):
        inner_json_sha256(archive)


class FakeEmitter:
    def __init__(self):
        self.archives = 0

    def emit_archive(self, archive, source, file_id):
        self.archives += 1
        return 3


class FakeCheckpoints:
    def __init__(self):
        self.data = {}

    def get(self, key):
        from extractor.catalog import Entry

        return Entry(value=self.data[key], context=b"c") if key in self.data else None

    def put(self, key, value, context=b""):
        self.data[key] = value
        return b"c"


def test_stored_but_never_emitted_archive_is_replayed_from_the_store():
    store, emitter, cps = FakeStore(), FakeEmitter(), FakeCheckpoints()
    store.objects[TASK.object_key] = {"sha256": PAYLOAD_SHA}
    store.saved[TASK.object_key] = make_zip(PAYLOAD)
    source = FakeSource([make_zip(PAYLOAD)])
    ex = Extractor(source, store, "w1", sleep=lambda _: None, emitter=emitter, checkpoints=cps)

    first = ex.handle(TASK)
    second = ex.handle(TASK)

    assert (first.status, first.records) == ("reemitted", 3)
    assert second.status == "skipped"
    assert source.downloads == 0 and emitter.archives == 1
    assert cps.data[f"emitted/{TASK.object_key}"]["sha256"] == PAYLOAD_SHA


def test_new_download_is_marked_as_emitted():
    store, emitter, cps = FakeStore(), FakeEmitter(), FakeCheckpoints()
    ex = Extractor(FakeSource([make_zip(PAYLOAD)]), store, "w1", sleep=lambda _: None, emitter=emitter, checkpoints=cps)
    assert ex.handle(TASK).status == "stored"
    assert ex.handle(TASK).status == "skipped"
    assert emitter.archives == 1
