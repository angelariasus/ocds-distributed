import logging
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from .events import Emitter
from .integrity import IntegrityError, inner_json_sha256
from .models import RECORDS_RANGE, Result, Task

log = logging.getLogger(__name__)


class Source(Protocol):
    def fetch_sha(self, url: str) -> str: ...
    def download(self, url: str, dest: Path) -> int: ...


class Store(Protocol):
    def stored_sha(self, key: str) -> str | None: ...
    def put(self, key: str, path: Path, metadata: dict[str, str]) -> None: ...


class Crawler(Protocol):
    def crawl(self, task: Task) -> tuple[int, int]: ...


class ExhaustedError(Exception):
    def __init__(self, task: Task, attempts: int, cause: Exception):
        super().__init__(f"{task.task_id} failed after {attempts} attempts: {cause}")
        self.attempts = attempts
        self.cause = cause


class Extractor:
    def __init__(
        self,
        source: Source,
        store: Store,
        worker_id: str,
        max_attempts: int = 4,
        backoff_seconds: float = 2.0,
        sleep: Callable[[float], None] = time.sleep,
        crawler: Crawler | None = None,
        emitter: Emitter | None = None,
    ):
        self.source = source
        self.store = store
        self.crawler = crawler
        self.emitter = emitter
        self.worker_id = worker_id
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self.sleep = sleep

    def handle(self, task: Task) -> Result:
        for attempt in range(1, self.max_attempts + 1):
            try:
                return self._process(task, attempt)
            except Exception as err:
                log.warning("task %s attempt %d failed: %s", task.task_id, attempt, err)
                if attempt == self.max_attempts:
                    raise ExhaustedError(task, attempt, err) from err
                self.sleep(self.backoff_seconds * 2 ** (attempt - 1))
        raise AssertionError("unreachable")

    def _process(self, task: Task, attempt: int) -> Result:
        if task.kind == RECORDS_RANGE:
            if self.crawler is None:
                raise RuntimeError("records_range tasks need a crawler")
            pages, records = self.crawler.crawl(task)
            return self._result(task, "crawled", "", 0, attempt, pages=pages, records=records)

        expected = self.source.fetch_sha(task.sha_url)
        if self.store.stored_sha(task.object_key) == expected:
            return self._result(task, "skipped", expected, 0, attempt)

        with tempfile.TemporaryDirectory() as tmp:
            archive = Path(tmp) / "archive.zip"
            size = self.source.download(task.url, archive)
            # A mismatch usually means the portal regenerated the file between both requests.
            actual = inner_json_sha256(archive)
            if actual != expected:
                raise IntegrityError(f"sha256 mismatch: expected {expected}, got {actual}")
            self.store.put(
                task.object_key,
                archive,
                {
                    "sha256": actual,
                    "task-id": task.task_id,
                    "published-at": task.published_at,
                },
            )
            records = self.emitter.emit_archive(archive, task.source, task.task_id) if self.emitter else 0
        return self._result(task, "stored", expected, size, attempt, records=records)

    def _result(self, task: Task, status: str, sha: str, size: int, attempts: int, **counts: int) -> Result:
        prefix_dir = f"ocds/api/source={task.source}/prefix={task.prefix}/"
        return Result(
            task_id=task.task_id,
            status=status,
            object_key=prefix_dir if task.kind == RECORDS_RANGE else task.object_key,
            sha256=sha,
            size_bytes=size,
            attempts=attempts,
            worker_id=self.worker_id,
            finished_at=datetime.now(UTC).isoformat(),
            **counts,
        )
