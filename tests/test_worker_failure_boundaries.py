"""Actual worker boundaries must not infer ACLs or Search success from failures."""

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock

from azure.core.exceptions import AzureError, HttpResponseError, ServiceRequestError
from azure.search.documents.models import IndexingResult
import pytest


@pytest.fixture
def worker_module(monkeypatch):
    config = SimpleNamespace(get=lambda key, default=None, **kwargs: default)
    dependencies = ModuleType("dependencies")
    dependencies.get_config = lambda: config
    monkeypatch.setitem(sys.modules, "dependencies", dependencies)

    def load(name, package="jobs"):
        path = Path(__file__).resolve().parents[1] / package / f"{name}.py"
        spec = importlib.util.spec_from_file_location(f"{package}.{name}_failure_under_test", path)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, spec.name, module)
        spec.loader.exec_module(module)
        return module

    return load


@pytest.mark.asyncio
@pytest.mark.parametrize(("module_name", "class_name"), [
    ("blob_storage_indexer", "BlobStorageDocumentIndexer"),
    ("blob_storage_indexer", "BlobStorageDeletedItemsCleaner"),
    ("sharepoint_indexer", "SharePointIndexer"),
])
@pytest.mark.parametrize("operation", ["upload_documents", "delete_documents"])
@pytest.mark.parametrize("case", ["confirmed", "missing", "failed", "duplicate", "unrelated", "exhausted"])
async def test_worker_search_requires_actual_confirmation(
    worker_module, monkeypatch, caplog, module_name, class_name, operation, case,
):
    module = worker_module(module_name)
    worker = object.__new__(getattr(module, class_name))
    worker.cfg = SimpleNamespace(indexer_name="worker")
    sleep = AsyncMock()
    monkeypatch.setattr(module.asyncio, "sleep", sleep)
    calls = []
    failure = HttpResponseError(message="private-search-canary", response=SimpleNamespace(
        headers={"Retry-After": "2"}, status_code=503, reason="unavailable",
    ))

    def result(key, succeeded=True):
        return IndexingResult.deserialize({"key": key, "status": succeeded, "statusCode": 200})

    async def send(**kwargs):
        calls.append(kwargs)
        if case == "exhausted":
            raise failure
        return {
            "confirmed": [result("key")], "missing": [], "failed": [result("key", False)],
            "duplicate": [result("key"), result("key")], "unrelated": [result("other")],
        }[case]

    send.__name__ = operation
    if case == "confirmed":
        response = await worker._with_backoff(send, documents=[{"id": "key"}])
        assert response[0].succeeded is True
    elif case == "exhausted":
        with pytest.raises(HttpResponseError) as raised:
            await worker._with_backoff(send, documents=[{"id": "key"}])
        assert raised.value is failure
        assert len(calls) == 8
        assert sleep.await_count == 7
        assert sleep.await_args_list[0].args == (2.0,)
    else:
        with pytest.raises(AzureError, match="confirm"):
            await worker._with_backoff(send, documents=[{"id": "key"}])
        assert len(calls) == 1
        sleep.assert_not_awaited()
    assert "private-search-canary" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ServiceRequestError("private-metadata"), RuntimeError("private-metadata")])
async def test_blob_metadata_failure_cannot_become_empty_acl(worker_module, monkeypatch, caplog, failure):
    module = worker_module("blob_storage_indexer")
    worker = object.__new__(module.BlobStorageDocumentIndexer)
    worker.cfg = module.BlobIndexerConfig(storage_account_name="storage", source_container="source")
    worker._ensure_clients = AsyncMock()
    worker._read_file_log = AsyncMock(return_value=None)
    worker._write_file_log = AsyncMock()
    worker._get_container_rbac_scope = lambda: ""
    properties = SimpleNamespace(size=0, metadata={})
    blob = SimpleNamespace(get_blob_properties=AsyncMock(side_effect=[failure, properties]),
                           download_blob=AsyncMock(side_effect=RuntimeError("must not download")))
    worker._blob_service = SimpleNamespace(get_container_client=lambda name: SimpleNamespace(
        get_blob_client=lambda name: blob,
    ))
    result = await worker._process_one("document.txt", datetime.now(timezone.utc), "text/plain", "run")
    assert result["status"] == "error"
    assert blob.get_blob_properties.await_count == 1
    blob.download_blob.assert_not_awaited()
    assert worker._write_file_log.await_args.args[2]["status"] == "error"
    assert "private-metadata" not in caplog.text + str(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ServiceRequestError("private-permission"), RuntimeError("defect")])
async def test_sharepoint_permission_failure_is_not_an_empty_acl(worker_module, failure):
    module = worker_module("sharepoint_indexer")
    worker = object.__new__(module.SharePointIndexer)
    worker.cfg = SimpleNamespace(indexer_name="worker")
    worker._graph_client = SimpleNamespace(get_item_permission_principal_ids=AsyncMock(side_effect=failure))
    with pytest.raises(type(failure)) as raised:
        await worker._get_security_principals_for_item(
            session=object(), site_id="site", collection_id="list", item_id="item",
        )
    assert raised.value is failure


@pytest.mark.parametrize("operation", ["json", "literal"])
def test_acl_parser_defect_does_not_become_fallback_identifiers(worker_module, monkeypatch, operation):
    module = worker_module("blob_storage_indexer")
    worker = object.__new__(module.BlobStorageDocumentIndexer)

    def fail(value):
        raise RuntimeError("parser defect")

    if operation == "json":
        monkeypatch.setattr(module.json, "loads", fail)
    else:
        monkeypatch.setattr(module.ast, "literal_eval", fail)
    with pytest.raises(RuntimeError, match="parser defect"):
        worker._parse_security_ids("user-a; user-b")


@pytest.mark.parametrize("raw", ['["a", "b"]', "['a', 'b']", "a; b", "a,b"])
def test_acl_parser_retains_supported_legacy_encodings(worker_module, raw):
    module = worker_module("blob_storage_indexer")
    worker = object.__new__(module.BlobStorageDocumentIndexer)
    assert worker._parse_security_ids(raw) == ["a", "b"]


def test_spreadsheet_configuration_failure_is_not_disabled_mode(worker_module):
    module = worker_module("blob_storage_indexer")
    worker = object.__new__(module.BlobStorageDocumentIndexer)

    def fail(*args):
        raise RuntimeError("configuration failed")

    worker._app = SimpleNamespace(get=fail)
    with pytest.raises(RuntimeError, match="configuration failed"):
        worker._should_stage_excel_rowwise({"fileName": "document.xlsx"})


@pytest.mark.parametrize("key", [
    "metadata-security-user-ids", "metadata-security-group-ids",
    "metadata-security-id", "metadata-security-rbac-scope",
])
def test_hyphenated_acl_keys_never_enter_searchable_metadata(worker_module, key):
    module = worker_module("blob_storage_indexer")
    assert module._extract_custom_metadata({key: "private-acl", "category": "public"}) == [
        {"key": "category", "value": "public"},
    ]


@pytest.mark.parametrize(("headers", "expected"), [
    ({}, 1.0), ({"Retry-After": "3"}, 3.0), ({"retry-after-ms": "2500"}, 2.5),
    ({"Retry-After": "100", "retry-after-ms": "2500"}, 2.5),
    ({"Retry-After": "not-a-number"}, 1.0), ({"Retry-After": "NaN"}, 1.0),
    ({"Retry-After": "Infinity"}, 1.0), ({"Retry-After": "-3"}, 1.0),
])
def test_sdk_retry_header_units_and_invalid_values(worker_module, headers, expected):
    from tools.aisearch import search_retry_delay

    error = HttpResponseError(message="unavailable", response=SimpleNamespace(
        headers=headers, status_code=503, reason="unavailable",
    ))
    assert search_retry_delay(error, 1.0) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["index-scan", "late-source-page"])
async def test_blob_run_failure_propagates_and_closes_without_finished_summary(
    worker_module, stage, caplog,
):
    module = worker_module("blob_storage_indexer")
    worker = object.__new__(module.BlobStorageDocumentIndexer)
    worker.cfg = module.BlobIndexerConfig(storage_account_name="storage", source_container="source")
    worker._ensure_clients = AsyncMock()
    worker._ensure_container = AsyncMock()
    worker._close_clients_safely = AsyncMock()
    failure = RuntimeError("private-scan-canary")
    summaries = []

    async def write(container, summary, run_id):
        summaries.append(dict(summary))

    async def blobs(**kwargs):
        yield SimpleNamespace(name="a.txt", last_modified=datetime.now(timezone.utc))
        raise failure

    worker._write_run_summary = write
    worker._load_latest_index_state = AsyncMock(
        return_value={}, side_effect=failure if stage == "index-scan" else None,
    )
    worker._blob_service = SimpleNamespace(get_container_client=lambda name: SimpleNamespace(
        list_blobs=blobs,
    ))
    with pytest.raises(RuntimeError) as raised:
        await worker.run()
    assert raised.value is failure
    assert summaries[-1]["status"] == "failed"
    assert all(summary["status"] != "finished" for summary in summaries)
    worker._close_clients_safely.assert_awaited_once()
    assert "private-scan-canary" not in caplog.text + str(summaries)


@pytest.mark.asyncio
@pytest.mark.parametrize("class_name", ["BlobStorageDocumentIndexer", "BlobStorageDeletedItemsCleaner"])
async def test_partial_blob_initialization_closes_every_owned_resource(worker_module, class_name):
    module = worker_module("blob_storage_indexer")
    worker = object.__new__(getattr(module, class_name))
    worker.cfg = module.BlobIndexerConfig(storage_account_name="storage", source_container="source")
    worker._ensure_clients = AsyncMock(side_effect=RuntimeError("initialization failed"))
    worker._close_clients_safely = AsyncMock()
    with pytest.raises(RuntimeError, match="initialization failed"):
        await worker.run()
    worker._close_clients_safely.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["initial-summary", "final-summary"])
async def test_blob_summary_defect_cannot_skip_resource_cleanup(worker_module, stage):
    module = worker_module("blob_storage_indexer")
    worker = object.__new__(module.BlobStorageDocumentIndexer)
    worker.cfg = module.BlobIndexerConfig(storage_account_name="storage", source_container="source")
    worker._ensure_clients = AsyncMock()
    worker._ensure_container = AsyncMock()
    worker._close_clients_safely = AsyncMock()
    worker._load_latest_index_state = AsyncMock(return_value={})

    async def blobs(**kwargs):
        for item in ():
            yield item

    async def write(container, summary, run_id):
        if stage == "initial-summary" or summary["status"] == "finished":
            raise RuntimeError("summary defect")

    worker._write_run_summary = write
    worker._blob_service = SimpleNamespace(get_container_client=lambda name: SimpleNamespace(list_blobs=blobs))
    with pytest.raises(RuntimeError, match="summary defect"):
        await worker.run()
    worker._close_clients_safely.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("class_name", ["BlobStorageDocumentIndexer", "BlobStorageDeletedItemsCleaner"])
@pytest.mark.parametrize("failed_client", ["_search_client", "_blob_service", "_credential"])
async def test_blob_cleanup_attempts_all_clients_without_masking_primary(
    worker_module, class_name, failed_client, caplog,
):
    module = worker_module("blob_storage_indexer")
    worker = object.__new__(getattr(module, class_name))
    calls = []
    for attribute in ("_search_client", "_blob_service", "_credential"):
        async def close(attribute=attribute):
            calls.append(attribute)
            if attribute == failed_client:
                raise RuntimeError("private-close-canary")
        setattr(worker, attribute, SimpleNamespace(close=close))
    primary = ValueError("primary failure")
    with pytest.raises(ValueError) as raised:
        try:
            raise primary
        finally:
            await worker._close_clients_safely()
    assert raised.value is primary
    assert calls == ["_search_client", "_blob_service", "_credential"]
    assert "cleanup failed" in caplog.text
    assert "private-close-canary" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(("class_name", "method", "args"), [
    ("BlobStorageDocumentIndexer", "_read_file_log", ("key",)),
    ("BlobStorageDocumentIndexer", "_ensure_container", ("jobs",)),
    ("BlobStorageDocumentIndexer", "_write_file_log", ("jobs", "key", {})),
    ("BlobStorageDocumentIndexer", "_write_run_summary", ("jobs", {}, "run")),
    ("BlobStorageDeletedItemsCleaner", "_ensure_container", ("jobs",)),
    ("BlobStorageDeletedItemsCleaner", "_write_file_log", ("jobs", "key", {})),
    ("BlobStorageDeletedItemsCleaner", "_write_run_summary", ("jobs", {})),
])
@pytest.mark.parametrize("failure", [AzureError("private-log"), RuntimeError("defect")])
async def test_blob_optional_log_helpers_only_recover_sdk_failures(
    worker_module, class_name, method, args, failure, caplog,
):
    module = worker_module("blob_storage_indexer")
    worker = object.__new__(getattr(module, class_name))
    worker.cfg = module.BlobIndexerConfig(storage_account_name="storage", source_container="source")
    worker._ensure_clients = AsyncMock()
    blob = SimpleNamespace(download_blob=AsyncMock(side_effect=failure))
    container = SimpleNamespace(
        create_container=AsyncMock(side_effect=failure), upload_blob=AsyncMock(side_effect=failure),
        get_blob_client=lambda name: blob,
    )
    worker._blob_service = SimpleNamespace(get_container_client=lambda name: container)
    if isinstance(failure, RuntimeError):
        with pytest.raises(RuntimeError) as raised:
            await getattr(worker, method)(*args)
        assert raised.value is failure
    else:
        await getattr(worker, method)(*args)
        assert "AzureError" in caplog.text
        assert "private-log" not in caplog.text


@pytest.mark.asyncio
async def test_required_staging_container_failure_is_not_optional_logging(worker_module):
    module = worker_module("blob_storage_indexer")
    worker = object.__new__(module.BlobStorageDocumentIndexer)
    worker.cfg = module.BlobIndexerConfig(storage_account_name="storage", source_container="source")
    failure = AzureError("staging unavailable")
    worker._blob_service = SimpleNamespace(get_container_client=lambda name: SimpleNamespace(
        create_container=AsyncMock(side_effect=failure),
    ))
    with pytest.raises(AzureError) as raised:
        await worker._ensure_container(worker.cfg.staging_container)
    assert raised.value is failure


@pytest.mark.asyncio
async def test_corrupt_staged_document_is_not_silently_skipped(worker_module):
    module = worker_module("blob_storage_indexer")
    worker = object.__new__(module.BlobStorageDocumentIndexer)
    worker.cfg = SimpleNamespace(indexer_name="worker", batch_size=10)
    worker._ensure_clients = AsyncMock()
    worker._with_backoff = AsyncMock()

    async def blobs(**kwargs):
        yield SimpleNamespace(name="stage/document.json")

    blob = SimpleNamespace(download_blob=AsyncMock(return_value=SimpleNamespace(
        readall=AsyncMock(return_value=b"invalid-json"),
    )))
    worker._blob_service = SimpleNamespace(get_container_client=lambda name: SimpleNamespace(
        list_blobs=blobs, get_blob_client=lambda name: blob,
    ))
    with pytest.raises(module.json.JSONDecodeError):
        await worker._upload_staged_docs_in_batches("staging", "stage/")
    worker._with_backoff.assert_not_awaited()
