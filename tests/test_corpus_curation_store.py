"""Tests for ``tools.corpus_curation_store.CorpusCurationStore`` (issue #611).

Uses an in-memory fake Blob Storage backend (list_blobs / download_blob /
upload_blob with ETag semantics) so these tests never touch the real Azure
SDK or network, and exercise the exact concurrency/idempotency contract the
module documents: ETag-based optimistic concurrency with bounded retry, and
structural idempotency (replay of an identical decision is a no-op; a
conflicting second decision raises ``CurationDecisionConflict``).
"""

from __future__ import annotations

import json

import pytest
from azure.core.exceptions import ResourceModifiedError, ResourceNotFoundError

from tools.corpus_curation_store import (
    CorpusCurationStore,
    CurationConcurrencyExhausted,
    CurationDecisionConflict,
    CurationItemNotFound,
    item_id_for_blob,
)


class _FakeProperties:
    def __init__(self, etag: str):
        self.etag = etag


class _FakeDownload:
    def __init__(self, data: bytes, etag: str):
        self._data = data
        self.properties = _FakeProperties(etag)

    async def readall(self) -> bytes:
        return self._data


class _FakeBlobItem:
    def __init__(self, name: str):
        self.name = name


class _FakeBlobClient:
    def __init__(self, backend: "_FakeBackend", name: str):
        self._backend = backend
        self._name = name

    async def download_blob(self):
        if self._name not in self._backend.blobs:
            raise ResourceNotFoundError("not found")
        data, etag = self._backend.blobs[self._name]
        return _FakeDownload(data, etag)

    async def upload_blob(self, data, *, overwrite=True, etag=None, match_condition=None, content_settings=None):
        current = self._backend.blobs.get(self._name)
        if etag is not None:
            if current is None or current[1] != etag:
                raise ResourceModifiedError("etag mismatch")
        self._backend.counter += 1
        new_etag = f"etag-{self._backend.counter}"
        payload = data.encode("utf-8") if isinstance(data, str) else data
        self._backend.blobs[self._name] = (payload, new_etag)


class _FakeContainerClient:
    def __init__(self, backend: "_FakeBackend"):
        self._backend = backend

    def get_blob_client(self, name: str) -> _FakeBlobClient:
        return _FakeBlobClient(self._backend, name)

    async def list_blobs(self, name_starts_with: str = ""):
        for name in list(self._backend.blobs.keys()):
            yield _FakeBlobItem(name)


class _FakeBlobServiceClient:
    def __init__(self, backend: "_FakeBackend"):
        self._backend = backend

    def get_container_client(self, _name: str) -> _FakeContainerClient:
        return _FakeContainerClient(self._backend)


class _FakeBackend:
    def __init__(self):
        self.blobs: dict[str, tuple[bytes, str]] = {}
        self.counter = 0

    def seed(self, name: str, data: dict) -> None:
        self.counter += 1
        self.blobs[name] = (json.dumps(data).encode("utf-8"), f"etag-{self.counter}")


class _FakeConfig:
    def get(self, key, default=None, allow_none=True):
        return {"JOBS_LOG_CONTAINER": "jobs"}.get(key, default)


def _make_store(backend: _FakeBackend) -> CorpusCurationStore:
    store = object.__new__(CorpusCurationStore)
    store._cfg = _FakeConfig()
    store._blob_service = _FakeBlobServiceClient(backend)
    return store


@pytest.mark.asyncio
async def test_list_pending_items_only_returns_blocked_undecided_documents():
    backend = _FakeBackend()
    backend.seed(
        "blob-indexer/files/doc-a.pdf.json",
        {"blocked": True, "blockedReason": "processing_blocked", "blockedAt": "2026-01-01T00:00:00Z", "fileName": "doc-a.pdf"},
    )
    backend.seed(
        "blob-indexer/files/doc-b.pdf.json",
        {"blocked": False, "fileName": "doc-b.pdf"},
    )
    backend.seed(
        "blob-indexer/files/doc-c.pdf.json",
        {
            "blocked": True,
            "fileName": "doc-c.pdf",
            "curationDecision": "approve",
            "curationDecidedAt": "2026-01-02T00:00:00Z",
        },
    )
    store = _make_store(backend)

    items, total = await store.list_pending_items(limit=10, offset=0)

    assert total == 1
    assert len(items) == 1
    assert items[0].document_id == "doc-a.pdf"
    assert items[0].reason_code == "processing_blocked"
    # Never exposes internal blob paths as the wire item_id.
    assert not items[0].item_id.startswith("blob-indexer")
    assert items[0].item_id == item_id_for_blob("blob-indexer/files/doc-a.pdf.json")


@pytest.mark.asyncio
async def test_list_pending_items_never_carries_document_content_fields():
    backend = _FakeBackend()
    backend.seed(
        "blob-indexer/files/doc-a.pdf.json",
        {
            "blocked": True,
            "fileName": "doc-a.pdf",
            "blockedReason": "checksum_mismatch",
            "blockedAt": "2026-01-01T00:00:00Z",
            # Fields that must never leak into the curation item shape:
            "content": "the actual extracted document text",
            "chunks": ["chunk one", "chunk two"],
        },
    )
    store = _make_store(backend)

    items, _total = await store.list_pending_items(limit=10, offset=0)

    item = items[0]
    assert set(vars(item)) == {
        "item_id",
        "document_id",
        "title",
        "reason_code",
        "submitted_at",
        "blob_name",
    }
    assert "content" not in vars(item).values()


@pytest.mark.asyncio
async def test_count_pending_and_decided():
    backend = _FakeBackend()
    backend.seed("x/files/1.json", {"blocked": True, "fileName": "a"})
    backend.seed("x/files/2.json", {"blocked": True, "fileName": "b", "curationDecision": "reject"})
    backend.seed("x/files/3.json", {"blocked": False, "fileName": "c"})
    store = _make_store(backend)

    pending, decided = await store.count_pending_and_decided()

    assert pending == 1
    assert decided == 1


@pytest.mark.asyncio
async def test_record_decision_first_write_succeeds():
    backend = _FakeBackend()
    backend.seed(
        "x/files/a.json", {"blocked": True, "fileName": "a", "blockedAt": "2026-01-01T00:00:00Z"}
    )
    store = _make_store(backend)
    item_id = item_id_for_blob("x/files/a.json")

    decision, decided_at, was_replay = await store.record_decision(
        item_id, decision="approve", note="looks fine", decided_by="operator-oid"
    )

    assert decision == "approve"
    assert decided_at
    assert was_replay is False
    stored = json.loads(backend.blobs["x/files/a.json"][0])
    assert stored["curationDecision"] == "approve"
    assert stored["curationDecisionNote"] == "looks fine"
    assert stored["curationDecidedBy"] == "operator-oid"


@pytest.mark.asyncio
async def test_record_decision_is_idempotent_on_replay():
    backend = _FakeBackend()
    backend.seed("x/files/a.json", {"blocked": True, "fileName": "a"})
    store = _make_store(backend)
    item_id = item_id_for_blob("x/files/a.json")

    first = await store.record_decision(item_id, decision="approve", note=None, decided_by="op-1")
    second = await store.record_decision(item_id, decision="approve", note=None, decided_by="op-1")

    assert first[0] == second[0] == "approve"
    assert second[2] is True  # was_replay
    assert first[1] == second[1]  # decided_at unchanged


@pytest.mark.asyncio
async def test_record_decision_conflicts_on_different_decision():
    backend = _FakeBackend()
    backend.seed("x/files/a.json", {"blocked": True, "fileName": "a"})
    store = _make_store(backend)
    item_id = item_id_for_blob("x/files/a.json")

    await store.record_decision(item_id, decision="approve", note=None, decided_by="op-1")

    with pytest.raises(CurationDecisionConflict):
        await store.record_decision(item_id, decision="reject", note=None, decided_by="op-2")


@pytest.mark.asyncio
async def test_record_decision_raises_not_found_for_unknown_item():
    backend = _FakeBackend()
    store = _make_store(backend)

    with pytest.raises(CurationItemNotFound):
        await store.record_decision("cur_" + "0" * 32, decision="approve", note=None, decided_by="op-1")


@pytest.mark.asyncio
async def test_record_decision_raises_not_found_for_unblocked_item():
    backend = _FakeBackend()
    backend.seed("x/files/a.json", {"blocked": False, "fileName": "a"})
    store = _make_store(backend)
    item_id = item_id_for_blob("x/files/a.json")

    with pytest.raises(CurationItemNotFound):
        await store.record_decision(item_id, decision="approve", note=None, decided_by="op-1")


@pytest.mark.asyncio
async def test_record_decision_retries_on_concurrent_writer_then_succeeds():
    backend = _FakeBackend()
    backend.seed("x/files/a.json", {"blocked": True, "fileName": "a"})
    store = _make_store(backend)
    item_id = item_id_for_blob("x/files/a.json")

    blob_client = store  # not used directly; simulate a racing writer by
    # bumping the etag out from under the first read via a direct backend
    # mutation between download and upload. We monkeypatch upload_blob to
    # fail exactly once before delegating to the real fake implementation.
    container = _FakeContainerClient(backend)
    real_get_blob_client = container.get_blob_client
    attempts = {"count": 0}

    class _FlakyBlobClient(_FakeBlobClient):
        async def upload_blob(self, data, *, overwrite=True, etag=None, match_condition=None, content_settings=None):
            attempts["count"] += 1
            if attempts["count"] == 1:
                # Simulate a concurrent writer changing the blob first.
                backend.counter += 1
                backend.blobs[self._name] = (
                    backend.blobs[self._name][0],
                    f"etag-{backend.counter}",
                )
                raise ResourceModifiedError("etag mismatch")
            return await super().upload_blob(
                data,
                overwrite=overwrite,
                etag=backend.blobs[self._name][1],
                match_condition=match_condition,
                content_settings=content_settings,
            )

    def _flaky_get_blob_client(name):
        return _FlakyBlobClient(backend, name)

    container.get_blob_client = _flaky_get_blob_client

    class _FlakyContainerBackend(_FakeBlobServiceClient):
        def get_container_client(self, _name):
            return container

    store._blob_service = _FlakyContainerBackend(backend)

    decision, decided_at, was_replay = await store.record_decision(
        item_id, decision="approve", note=None, decided_by="op-1"
    )

    assert decision == "approve"
    assert was_replay is False
    assert attempts["count"] == 2


@pytest.mark.asyncio
async def test_record_decision_raises_after_exhausting_retries():
    backend = _FakeBackend()
    backend.seed("x/files/a.json", {"blocked": True, "fileName": "a"})
    store = _make_store(backend)
    item_id = item_id_for_blob("x/files/a.json")

    class _AlwaysConflictBlobClient(_FakeBlobClient):
        async def upload_blob(self, *args, **kwargs):
            raise ResourceModifiedError("etag mismatch")

    class _AlwaysConflictContainer(_FakeContainerClient):
        def get_blob_client(self, name):
            return _AlwaysConflictBlobClient(self._backend, name)

    class _AlwaysConflictService(_FakeBlobServiceClient):
        def get_container_client(self, _name):
            return _AlwaysConflictContainer(self._backend)

    store._blob_service = _AlwaysConflictService(backend)

    with pytest.raises(CurationConcurrencyExhausted):
        await store.record_decision(item_id, decision="approve", note=None, decided_by="op-1")


def test_item_id_never_reveals_blob_path():
    item_id = item_id_for_blob("blob-indexer/files/very-sensitive-name.pdf.json")

    assert "very-sensitive-name" not in item_id
    assert item_id.startswith("cur_")
    assert len(item_id) <= 128


@pytest.mark.asyncio
async def test_record_decision_rejects_unsupported_decision_value():
    backend = _FakeBackend()
    backend.seed("x/files/a.json", {"blocked": True, "fileName": "a"})
    store = _make_store(backend)
    item_id = item_id_for_blob("x/files/a.json")

    with pytest.raises(ValueError):
        await store.record_decision(item_id, decision="delete", note=None, decided_by="op-1")
