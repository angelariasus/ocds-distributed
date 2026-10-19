import hashlib
import json
import logging
import time
from collections.abc import Callable
from typing import Protocol
from urllib.parse import urlencode

from prometheus_client import Counter

from . import limiter as lim
from .breaker import CircuitBreaker
from .catalog import Catalog
from .events import Emitter
from .models import Task

log = logging.getLogger(__name__)

PAGES = Counter("extractor_pages_total", "recordsAfter pages fetched by outcome.", ["outcome"])
RECORDS = Counter("extractor_records_total", "OCDS records stored from the cursor API.")
RESUMED = Counter("extractor_crawls_resumed_total", "Crawls resumed from a catalog checkpoint.")


class ThrottledError(Exception):
    pass


class UpstreamError(Exception):
    pass


class PageSource(Protocol):
    def get_json(self, url: str) -> tuple[int, dict]: ...


class PageStore(Protocol):
    def put_bytes(self, key: str, data: bytes, metadata: dict[str, str], content_type: str) -> None: ...


def tender_id(record: dict) -> str:
    return str(record.get("compiledRelease", {}).get("tender", {}).get("id", ""))


def belongs(record: dict, task: Task) -> bool:
    # ocids embed the source system ("ocds-dgv273-seacev3-..."); pages near a
    # boundary can mix sources, so the prefix alone is not enough.
    source_tag = f"-{task.source.replace('_', '')}-"
    return tender_id(record).startswith(task.prefix or "") and source_tag in record.get("ocid", "")


class RangeCrawler:
    """Walks the cursor API over one tender-id prefix, storing every page under a deterministic key."""

    def __init__(
        self,
        source: PageSource,
        store: PageStore,
        limiter: lim.RateLimiter,
        breaker: CircuitBreaker,
        base_url: str,
        page_size: int = 100,
        max_page_attempts: int = 8,
        backoff_seconds: float = 1.0,
        max_backoff_seconds: float = 8.0,
        sleep: Callable[[float], None] = time.sleep,
        catalog: Catalog | None = None,
        emitter: Emitter | None = None,
    ):
        self.source = source
        self.store = store
        self.limiter = limiter
        self.breaker = breaker
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self.max_page_attempts = max_page_attempts
        self.backoff_seconds = backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.sleep = sleep
        self.catalog = catalog
        self.emitter = emitter

    def crawl(self, task: Task) -> tuple[int, int]:
        prefix = task.prefix or ""
        # "z" sorts after every digit, so this cursor sits just above the whole prefix range.
        start = {"size": self.page_size, "searchAfter": f"{task.source},{prefix}z,0,0,x"}
        url: str | None = f"{self.base_url}/recordsAfter?{urlencode(start)}"
        pages = records = 0
        context = b""
        # Scoped to the planning round: a redelivered message resumes, a new round re-crawls.
        checkpoint_key = f"crawl/{task.task_id}/{task.planned_at}"
        if self.catalog and (entry := self.catalog.get(checkpoint_key)):
            if entry.value.get("done"):
                return entry.value["pages"], entry.value["records"]
            url, pages, records, context = (
                entry.value["next"],
                entry.value["pages"],
                entry.value["records"],
                entry.context,
            )
            RESUMED.inc()
        log.info("task %s crawl starting at page %d", task.task_id, pages + 1)
        while url:
            page = self.fetch_page(url)
            batch = [r for r in page.get("records", []) if belongs(r, task)]
            if batch:
                self.store_page(task, batch)
                pages += 1
                records += len(batch)
            if len(batch) < len(page.get("records", [])) or not batch:
                url = None
            else:
                url = page.get("links", {}).get("next")
            context = self.checkpoint(checkpoint_key, url, pages, records, context)
        return pages, records

    def checkpoint(self, key: str, next_url: str | None, pages: int, records: int, context: bytes) -> bytes:
        if not self.catalog:
            return context
        state = {"next": next_url, "pages": pages, "records": records, "done": next_url is None}
        # Written after the page is durably stored, so a crash loses at most one page of work.
        return self.catalog.put(key, state, context) or context

    def fetch_page(self, url: str) -> dict:
        for attempt in range(1, self.max_page_attempts + 1):
            self.breaker.wait_until_allowed()
            try:
                page = self._fetch_once(url)
            except (ThrottledError, UpstreamError, OSError) as err:
                self.breaker.record_failure()
                PAGES.labels("retry").inc()
                log.warning("page attempt %d failed: %s", attempt, err)
                if attempt == self.max_page_attempts:
                    raise
                self.sleep(min(self.backoff_seconds * 2 ** (attempt - 1), self.max_backoff_seconds))
                continue
            self.breaker.record_success()
            PAGES.labels("ok").inc()
            return page
        raise AssertionError("unreachable")

    def _fetch_once(self, url: str) -> dict:
        with self.limiter.permit() as permit:
            try:
                status, body = self.source.get_json(url)
            except OSError:
                permit.outcome = lim.UNREPORTED
                raise
            if status == 429:
                permit.outcome = lim.THROTTLED
                raise ThrottledError(url)
            if status >= 500:
                permit.outcome = lim.ERROR
                raise UpstreamError(f"{status} from {url}")
            if status != 200:
                raise RuntimeError(f"unexpected {status} from {url}")
            return body

    def store_page(self, task: Task, batch: list[dict]) -> None:
        data = json.dumps({"records": batch}, ensure_ascii=False).encode()
        key = task.page_key(tender_id(batch[0]))
        self.store.put_bytes(
            key,
            data,
            {"sha256": hashlib.sha256(data).hexdigest(), "task-id": task.task_id, "records": str(len(batch))},
            "application/json",
        )
        RECORDS.inc(len(batch))
        if self.emitter:
            self.emitter.emit_page(batch, task.source, key)
