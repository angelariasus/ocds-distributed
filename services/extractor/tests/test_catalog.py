import json

import grpc

from catalog.v1 import catalog_pb2
from extractor.catalog import Catalog


class FakeRpcError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE


class FakeStub:
    def __init__(self, versions=(), available=True):
        self.versions = list(versions)
        self.available = available
        self.puts = []

    def Get(self, request, timeout=None):
        if not self.available:
            raise FakeRpcError()
        return catalog_pb2.GetResponse(
            versions=[catalog_pb2.Version(value=json.dumps(v).encode()) for v in self.versions],
            context=b"ctx",
        )

    def Put(self, request, timeout=None):
        if not self.available:
            raise FakeRpcError()
        self.puts.append((request.key, json.loads(request.value), request.context))
        return catalog_pb2.PutResponse(context=b"ctx2")


def test_get_returns_none_when_missing():
    assert Catalog([], stubs=[FakeStub()]).get("crawl/x") is None


def test_get_picks_most_advanced_sibling():
    stub = FakeStub(versions=[{"pages": 3, "next": "a"}, {"pages": 5, "next": "b"}])
    entry = Catalog([], stubs=[stub]).get("crawl/x")
    assert entry.value["pages"] == 5
    assert entry.conflicts == 1
    assert entry.context == b"ctx"


def test_put_passes_context_and_returns_new_one():
    stub = FakeStub()
    ctx = Catalog([], stubs=[stub]).put("crawl/x", {"pages": 1}, b"ctx")
    assert ctx == b"ctx2"
    assert stub.puts == [("crawl/x", {"pages": 1}, b"ctx")]


def test_unavailable_catalog_degrades_to_none():
    stub = FakeStub(available=False)
    c = Catalog([], stubs=[stub])
    assert c.get("k") is None
    assert c.put("k", {}) is None
