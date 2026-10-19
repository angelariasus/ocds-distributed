import json
from urllib.parse import parse_qs, urlparse

import pytest

from extractor.breaker import OPEN, CircuitBreaker
from extractor.crawler import RangeCrawler, ThrottledError
from extractor.limiter import RateLimiter
from extractor.models import RECORDS_RANGE, Task
from limiter.v1 import limiter_pb2

BASE = "http://mock/api/v1"
TASK = Task(task_id="api-seace_v3-12", source="seace_v3", planned_at="t", kind=RECORDS_RANGE, prefix="12")


def record(source, tid):
    return {"ocid": f"ocds-dgv273-{source.replace('_', '')}-{tid}", "compiledRelease": {"tender": {"id": tid}}}


class FakePortal:
    """Serves records in descending tender-id order with the portal's cursor shape."""

    def __init__(self, records_by_source: dict[str, list[str]], page_size=2, fail_statuses=()):
        self.rows = [
            (s, t) for s in sorted(records_by_source, reverse=True) for t in sorted(records_by_source[s], reverse=True)
        ]
        self.page_size = page_size
        self.fail_statuses = list(fail_statuses)
        self.calls = 0

    def get_json(self, url):
        self.calls += 1
        if self.fail_statuses:
            status = self.fail_statuses.pop(0)
            if status == "down":
                raise OSError("connection refused")
            return status, {}
        src, tid = parse_qs(urlparse(url).query)["searchAfter"][0].split(",")[:2]
        after = [(s, t) for s, t in self.rows if (s, t) < (src, tid)]
        page = after[: self.page_size]
        body = {"records": [record(s, t) for s, t in page], "links": {"next": None}}
        if len(page) == self.page_size:
            s, t = page[-1]
            body["links"]["next"] = f"{BASE}/recordsAfter?size={self.page_size}&searchAfter={s},{t},0,0,x"
        return 200, body


class FakeStore:
    def __init__(self):
        self.objects: dict[str, dict] = {}

    def put_bytes(self, key, data, metadata, content_type):
        self.objects[key] = {"body": json.loads(data), "meta": metadata}


class FakeStub:
    def __init__(self):
        self.outcomes = []

    def Acquire(self, request, timeout=None):
        return limiter_pb2.AcquireResponse()

    def Report(self, request, timeout=None):
        self.outcomes.append(request.outcome)
        return limiter_pb2.ReportResponse()


def make(portal, **kw):
    store = FakeStore()
    stub = FakeStub()
    limiter = RateLimiter("x", "portal", "w1", stub=stub)
    breaker = CircuitBreaker(
        "portal", failure_threshold=kw.pop("threshold", 5), recovery_seconds=0, sleep=lambda _: None
    )
    crawler = RangeCrawler(
        portal, store, limiter, breaker, BASE, page_size=2, backoff_seconds=0, sleep=lambda _: None, **kw
    )
    return crawler, store, stub, breaker


def test_crawls_only_the_prefix_and_stops_at_boundary():
    portal = FakePortal({"seace_v3": ["130001", "125000", "124000", "123000", "119999"]})
    crawler, store, _, _ = make(portal)

    pages, records = crawler.crawl(TASK)

    assert (pages, records) == (2, 3)
    assert sorted(store.objects) == [
        "ocds/api/source=seace_v3/prefix=12/123000.json",
        "ocds/api/source=seace_v3/prefix=12/125000.json",
    ]
    assert store.objects["ocds/api/source=seace_v3/prefix=12/125000.json"]["meta"]["records"] == "2"


def test_stops_when_cursor_crosses_into_another_source():
    portal = FakePortal({"seace_v3": ["120002", "120001"], "seace_v2": ["129999", "129998"]})
    crawler, store, _, _ = make(portal)

    pages, records = crawler.crawl(TASK)

    assert records == 2
    assert all("source=seace_v3" in key for key in store.objects)


def test_throttled_page_is_retried_and_reported():
    portal = FakePortal({"seace_v3": ["120001"]}, fail_statuses=[429, 503])
    crawler, _, stub, _ = make(portal)

    _, records = crawler.crawl(TASK)

    assert records == 1
    assert stub.outcomes[:2] == [limiter_pb2.OUTCOME_THROTTLED, limiter_pb2.OUTCOME_ERROR]
    assert stub.outcomes[2] == limiter_pb2.OUTCOME_OK


def test_persistent_failures_open_breaker_and_exhaust():
    portal = FakePortal({"seace_v3": ["120001"]}, fail_statuses=[429] * 10)
    crawler, _, _, breaker = make(portal, threshold=3, max_page_attempts=4)

    with pytest.raises(ThrottledError):
        crawler.crawl(TASK)

    assert breaker.state == OPEN
    assert portal.calls == 4


def test_connection_failures_trip_breaker_without_feeding_limiter():
    portal = FakePortal({"seace_v3": ["120001"]}, fail_statuses=["down"] * 3)
    crawler, _, stub, breaker = make(portal, threshold=3)
    transitions = []
    breaker._transition = lambda state, _orig=breaker._transition: (transitions.append(state), _orig(state))

    _, records = crawler.crawl(TASK)

    assert records == 1
    assert transitions[:2] == ["open", "half_open"]
    assert stub.outcomes == [limiter_pb2.OUTCOME_OK]
