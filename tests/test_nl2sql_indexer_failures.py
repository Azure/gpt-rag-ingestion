"""NL2SQL retries and optional job logs cannot fabricate a confirmed upload."""

import asyncio
import importlib.util
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
import sys
import types
from unittest.mock import AsyncMock

from azure.core.exceptions import AzureError, ResourceExistsError, ResourceNotFoundError
import pytest


@pytest.fixture
def indexer(monkeypatch):
    dependencies = types.ModuleType("dependencies")
    dependencies.get_config = lambda: {}
    monkeypatch.setitem(sys.modules, "dependencies", dependencies)
    tools = types.ModuleType("tools")
    tools.__path__ = []
    tools.AISearchClient = object
    tools.AzureOpenAIClient = lambda **kwargs: types.SimpleNamespace(get_embeddings=lambda text: [0.1, 0.2])
    monkeypatch.setitem(sys.modules, "tools", tools)
    credentials = types.ModuleType("tools.credentials")
    credentials.get_azure_client_id = lambda config: None
    monkeypatch.setitem(sys.modules, "tools.credentials", credentials)
    path = Path(__file__).resolve().parents[1] / "jobs" / "nl2sql_indexer.py"
    spec = importlib.util.spec_from_file_location("nl2sql_indexer_under_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)

    modified = datetime(2026, 1, 1, tzinfo=timezone.utc)
    blob = types.SimpleNamespace(
        get_blob_properties=AsyncMock(return_value=types.SimpleNamespace(last_modified=modified, etag="etag")),
        download_blob=AsyncMock(return_value=types.SimpleNamespace(
            readall=AsyncMock(return_value=b'{"datasource":"source","question":"question","query":"SELECT 1"}'),
        )),
    )
    previous = {"lastModified": modified.isoformat(), "etag": "etag"}
    log_blob = types.SimpleNamespace(download_blob=AsyncMock(return_value=types.SimpleNamespace(
        readall=AsyncMock(return_value=json.dumps(previous).encode()),
    )))

    class Container:
        def __init__(self, contents):
            self.get_blob_client = lambda name: contents
            self.create_container = AsyncMock()
            self.upload_blob = AsyncMock()
            self.page_failure = None

        async def list_blobs(self):
            yield types.SimpleNamespace(name="queries/example.json")
            if self.page_failure:
                raise self.page_failure

    source = Container(blob)
    logs = Container(log_blob)
    service = types.SimpleNamespace(
        get_container_client=lambda name: logs if name == "jobs" else source,
        close=AsyncMock(),
    )
    search = types.SimpleNamespace(
        search_documents=AsyncMock(return_value={"count": 0, "documents": []}),
        index_document=AsyncMock(return_value=True),
        close=AsyncMock(),
    )
    instance = object.__new__(module.NL2SQLIndexer)
    instance.cfg = module.NL2SQLIndexerConfig(
        storage_account_name="fixture", queries_index_name="queries-index",
        tables_index_name="tables-index", measures_index_name="measures-index",
    )
    instance._blob_service = service
    instance._credential = types.SimpleNamespace(close=AsyncMock())
    instance._ai_search = search
    return types.SimpleNamespace(
        instance=instance, source=source, logs=logs, blob=blob, log_blob=log_blob,
        search=search, module=module,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["ensure", "read", "file-log", "summary", "exists"])
async def test_unexpected_optional_boundary_failure_propagates(indexer, operation, caplog):
    failure = RuntimeError("private-optional-canary")
    if operation == "ensure":
        indexer.logs.create_container.side_effect = failure
        call = indexer.instance._ensure_container("jobs")
    elif operation == "read":
        indexer.log_blob.download_blob.side_effect = failure
        call = indexer.instance._read_previous_log("jobs", "previous.json")
    elif operation == "exists":
        indexer.search.search_documents.side_effect = failure
        call = indexer.instance._exists_in_index("index", "key")
    else:
        indexer.logs.upload_blob.side_effect = failure
        call = (
            indexer.instance._write_file_log("jobs", "file.json", {})
            if operation == "file-log" else indexer.instance._write_run_summary("jobs", {}, "run")
        )
    with pytest.raises(RuntimeError) as caught:
        await call
    assert caught.value is failure
    assert "private-optional-canary" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["ensure", "read", "file-log", "summary", "exists"])
async def test_expected_optional_sdk_failure_is_diagnostic_not_primary_success(indexer, operation, caplog):
    failure = AzureError("private-optional-canary")
    if operation == "ensure":
        indexer.logs.create_container.side_effect = failure
        result = await indexer.instance._ensure_container("jobs")
    elif operation == "read":
        indexer.log_blob.download_blob.side_effect = failure
        result = await indexer.instance._read_previous_log("jobs", "previous.json")
    elif operation == "exists":
        indexer.search.search_documents.side_effect = failure
        result = await indexer.instance._exists_in_index("index", "key")
    else:
        indexer.logs.upload_blob.side_effect = failure
        result = (
            await indexer.instance._write_file_log("jobs", "file.json", {})
            if operation == "file-log" else await indexer.instance._write_run_summary("jobs", {}, "run")
        )
    assert result is (False if operation == "exists" else None)
    assert caplog.records
    assert "private-optional-canary" not in caplog.text
    indexer.search.index_document.assert_not_awaited()


@pytest.mark.asyncio
async def test_existing_log_container_and_missing_or_corrupt_previous_log_keep_reindex_default(indexer, caplog):
    indexer.logs.create_container.side_effect = ResourceExistsError("exists")
    await indexer.instance._ensure_container("jobs")
    indexer.log_blob.download_blob.side_effect = ResourceNotFoundError("missing")
    assert await indexer.instance._read_previous_log("jobs", "missing.json") is None
    indexer.log_blob.download_blob.side_effect = None
    indexer.log_blob.download_blob.return_value.readall.return_value = b"{"
    assert await indexer.instance._read_previous_log("jobs", "corrupt.json") is None
    assert caplog.records


@pytest.mark.asyncio
@pytest.mark.parametrize("confirmed", [True, False])
async def test_expected_existence_failure_reindexes_and_requires_upload_confirmation(indexer, confirmed):
    indexer.search.search_documents.side_effect = AzureError("unavailable")
    indexer.search.index_document.return_value = confirmed
    result = await indexer.instance._process_one("queries/example.json", "run")
    assert result["status"] == ("success" if confirmed else "error")
    indexer.search.index_document.assert_awaited_once()
    document = indexer.search.index_document.call_args.kwargs["document"]
    assert document == {
        "id": "queries-example-json", "datasource": "source", "question": "question",
        "query": "SELECT 1", "reasoning": "", "contentVector": [0.1, 0.2],
    }
    assert indexer.search.index_document.call_args.kwargs["index_name"] == "queries-index"


@pytest.mark.asyncio
async def test_confirmed_unchanged_document_skips_download_and_upload(indexer):
    indexer.search.search_documents.return_value = {"count": 1, "documents": [{"id": "queries-example-json"}]}
    result = await indexer.instance._process_one("queries/example.json", "run")
    assert result == {"status": "skipped", "kind": "queries"}
    indexer.blob.download_blob.assert_not_awaited()
    indexer.search.index_document.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["tables", "measures"])
async def test_existing_kind_fields_and_stable_id_are_preserved(indexer, kind):
    payload = (
        {"table": "products", "description": "description", "datasource": "source", "columns": ["id"]}
        if kind == "tables" else {
            "datasource": "source", "name": "total", "description": "description",
            "type": "measure", "source_table": "products", "data_type": "number", "source_model": "model",
        }
    )
    indexer.blob.download_blob.return_value.readall.return_value = json.dumps(payload).encode()
    result = await indexer.instance._process_one(f"{kind}/example.json", "run")
    assert result == {"status": "success", "kind": kind, "vectorDims": 2}
    kwargs = indexer.search.index_document.call_args.kwargs
    assert kwargs["index_name"] == f"{kind}-index"
    assert kwargs["document"] == {**payload, "id": f"{kind}-example-json", "contentVector": [0.1, 0.2]}


@pytest.mark.asyncio
@pytest.mark.parametrize("confirmed", [True, False])
async def test_optional_log_write_failure_cannot_change_confirmed_document_result(indexer, confirmed, caplog):
    indexer.logs.upload_blob.side_effect = AzureError("private-log-write-canary")
    indexer.search.index_document.return_value = confirmed
    result = await indexer.instance._process_one("queries/example.json", "run")
    assert result["status"] == ("success" if confirmed else "error")
    indexer.search.index_document.assert_awaited_once()
    assert "File-log upload failed" in caplog.text
    assert "private-log-write-canary" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["properties", "exists", "embedding", "upload"])
async def test_document_failure_is_explicit_and_does_not_expose_dependency_payload(indexer, stage, monkeypatch, caplog):
    failure = RuntimeError("private-document-canary")
    if stage == "properties":
        indexer.blob.get_blob_properties.side_effect = failure
    elif stage == "exists":
        indexer.search.search_documents.side_effect = failure
    elif stage == "upload":
        indexer.search.index_document.side_effect = failure
    else:
        def fail(**kwargs):
            raise failure
        monkeypatch.setattr(indexer.module, "AzureOpenAIClient", fail)
    result = await indexer.instance._process_one("queries/example.json", "run")
    assert result["status"] == "error"
    assert "RuntimeError" in result["error"]
    logs = [json.loads(call.kwargs["data"]) for call in indexer.logs.upload_blob.call_args_list]
    assert logs[-1]["status"] == "error"
    assert "private-document-canary" not in json.dumps([result, logs]) + caplog.text
    if stage != "upload":
        indexer.search.index_document.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("resource", ["_ai_search", "_blob_service", "_credential"])
@pytest.mark.parametrize("primary_failed", [True, False])
async def test_cleanup_preserves_primary_outcome_and_attempts_all_clients(indexer, resource, primary_failed, caplog):
    primary = AzureError("listing failed")
    if primary_failed:
        indexer.source.page_failure = primary
    getattr(indexer.instance, resource).close.side_effect = RuntimeError("private-cleanup-canary")
    if primary_failed:
        with pytest.raises(AzureError) as caught:
            await indexer.instance.run()
        assert caught.value is primary
        indexer.logs.upload_blob.assert_not_awaited()
    else:
        await indexer.instance.run()
        summary = json.loads(indexer.logs.upload_blob.call_args.kwargs["data"])
        assert summary["success"] == 1
        assert summary["failed"] == 0
    for name in ("_ai_search", "_blob_service", "_credential"):
        getattr(indexer.instance, name).close.assert_awaited_once()
    assert "cleanup" in caplog.text.lower()
    assert "private-cleanup-canary" not in caplog.text


@pytest.mark.asyncio
async def test_late_page_failure_is_not_a_finished_run_and_cleans_up(indexer, caplog):
    caplog.set_level(logging.INFO)
    indexer.source.page_failure = AzureError("listing failed")
    with pytest.raises(AzureError):
        await indexer.instance.run()
    assert "RUN-COMPLETE" not in caplog.text
    indexer.search.index_document.assert_not_awaited()
    indexer.logs.upload_blob.assert_not_awaited()
    for name in ("_ai_search", "_blob_service", "_credential"):
        getattr(indexer.instance, name).close.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_counts_unconfirmed_document_as_failed_not_indexed(indexer, caplog):
    caplog.set_level(logging.INFO)
    indexer.search.index_document.return_value = False
    await indexer.instance.run()
    summary = json.loads(indexer.logs.upload_blob.call_args.kwargs["data"])
    assert summary["success"] == 0
    assert summary["failed"] == 1
    assert summary["byKind"]["queries"]["vectorsGenerated"] == 0
    assert '"itemsIndexed": 0' in caplog.text
    assert '"itemsFailed": 1' in caplog.text


@pytest.mark.asyncio
async def test_document_cancellation_is_not_a_failed_record(indexer):
    indexer.blob.download_blob.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await indexer.instance._process_one("queries/example.json", "run")
    indexer.search.index_document.assert_not_awaited()
