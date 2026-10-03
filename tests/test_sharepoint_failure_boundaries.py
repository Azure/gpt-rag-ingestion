"""Separate SharePoint source configuration from optional store diagnostics."""

import asyncio
import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiohttp
from azure.core.exceptions import AzureError
import httpx
import openai
import pytest

from tests.test_worker_failure_boundaries import worker_module


@pytest.fixture
def sharepoint(worker_module):
    module = worker_module("sharepoint_indexer")
    worker = object.__new__(module.SharePointIndexer)
    worker.cfg = SimpleNamespace(indexer_name="worker", jobs_log_container="jobs",
                                 storage_account_name="storage")
    worker._storage_writable = True
    worker._blob_op_timeout_s = 5
    worker._run_summary_total_timeout_s = 5
    worker._app = SimpleNamespace(get=lambda key, default=None, **kwargs: default)
    worker._cosmos_sites_loaded = False
    worker._cosmos_datasource_container = "datasources"
    worker._cosmos_site_configs = None
    worker._lookup_columns_cache = {}
    worker._list_nav_url_cache = {}
    return module, worker


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [AzureError("private-config"), RuntimeError("defect")])
async def test_datasource_config_failure_is_not_an_empty_loaded_configuration(sharepoint, failure):
    _, worker = sharepoint
    worker._cosmos_client = SimpleNamespace(list_documents=AsyncMock(side_effect=failure))
    with pytest.raises(type(failure)) as raised:
        await worker._hydrate_site_configs_from_cosmos()
    assert raised.value is failure
    assert worker._cosmos_sites_loaded is False


@pytest.mark.asyncio
async def test_empty_datasource_configuration_is_a_valid_loaded_result(sharepoint):
    _, worker = sharepoint
    worker._cosmos_client = SimpleNamespace(list_documents=AsyncMock(return_value=[]))
    await worker._hydrate_site_configs_from_cosmos()
    assert worker._cosmos_sites_loaded is True
    assert worker._cosmos_site_configs is None


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["_get_lookup_columns_map", "_get_list_navigation_base_url"])
@pytest.mark.parametrize("failure", [aiohttp.ClientConnectionError("private-graph"), RuntimeError("defect")])
async def test_optional_graph_metadata_does_not_hide_defects(sharepoint, method, failure, caplog):
    _, worker = sharepoint
    worker._graph_client = SimpleNamespace(
        get_lookup_columns=AsyncMock(side_effect=failure),
        get_list_metadata=AsyncMock(side_effect=failure),
    )
    args = (object(), "site", "list") if method == "_get_lookup_columns_map" else (
        object(), "site", "example.org", "site", "list", "Documents",
    )
    if isinstance(failure, RuntimeError):
        with pytest.raises(RuntimeError) as raised:
            await getattr(worker, method)(*args)
        assert raised.value is failure
    else:
        result = await getattr(worker, method)(*args)
        assert result == ({} if method == "_get_lookup_columns_map" else "https://example.org/sites/site/Lists/Documents")
        assert "unavailable" in caplog.text
        assert "private-graph" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["_get_body_lastmod_by_id", "_get_latest_mod_for_parent"])
@pytest.mark.parametrize("failure", [AzureError("private-search"), RuntimeError("defect")])
async def test_freshness_failure_never_hides_programming_defects(sharepoint, method, failure, caplog):
    _, worker = sharepoint
    worker._search_client = SimpleNamespace(
        get_document=AsyncMock(side_effect=failure), search=AsyncMock(side_effect=failure),
    )
    if isinstance(failure, RuntimeError):
        with pytest.raises(RuntimeError) as raised:
            await getattr(worker, method)("parent")
        assert raised.value is failure
    else:
        assert await getattr(worker, method)("parent") is None
        assert "unavailable" in caplog.text
        assert "private-search" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(("method", "args"), [
    ("_ensure_log_container", ()), ("_read_file_log", ("file.json",)),
    ("_write_file_log", ("file.json", {})),
])
@pytest.mark.parametrize("failure", [AzureError("private-storage"), RuntimeError("defect")])
async def test_sharepoint_log_helpers_only_recover_sdk_failures(sharepoint, method, args, failure, caplog):
    _, worker = sharepoint
    blob = SimpleNamespace(download_blob=AsyncMock(side_effect=failure))
    worker._blob_service = SimpleNamespace(get_container_client=lambda name: SimpleNamespace(
        create_container=AsyncMock(side_effect=failure), upload_blob=AsyncMock(side_effect=failure),
        get_blob_client=lambda name: blob,
    ))
    if isinstance(failure, RuntimeError):
        with pytest.raises(RuntimeError) as raised:
            await getattr(worker, method)(*args)
        assert raised.value is failure
    else:
        await getattr(worker, method)(*args)
        assert "AzureError" in caplog.text
        assert "private-storage" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["exists", "upload", "delete"])
@pytest.mark.parametrize("failure", [AzureError("private-probe"), RuntimeError("defect")])
async def test_optional_storage_probe_only_recovers_sdk_failures(sharepoint, stage, failure, caplog):
    _, worker = sharepoint
    container = SimpleNamespace(
        exists=AsyncMock(return_value=True), upload_blob=AsyncMock(), delete_blob=AsyncMock(),
    )
    getattr(container, {"exists": "exists", "upload": "upload_blob", "delete": "delete_blob"}[stage]).side_effect = failure
    worker._blob_service = SimpleNamespace(get_container_client=lambda name: container)
    if isinstance(failure, RuntimeError):
        with pytest.raises(RuntimeError) as raised:
            await worker._init_storage_logging_guard()
        assert raised.value is failure
    else:
        await worker._init_storage_logging_guard()
        assert worker._storage_writable is (stage == "delete")
        assert "AzureError" in caplog.text
        assert "private-probe" not in caplog.text


@pytest.mark.asyncio
async def test_optional_summary_failure_preserves_primary_exception(sharepoint, caplog):
    _, worker = sharepoint
    worker._write_run_summary = AsyncMock(side_effect=RuntimeError("private-summary"))
    primary = ValueError("primary")
    with pytest.raises(ValueError) as raised:
        try:
            raise primary
        finally:
            await worker._write_run_summary_safely("run", {"status": "failed"})
    assert raised.value is primary
    assert "summary" in caplog.text and "private-summary" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["configuration", "collection"])
async def test_sharepoint_run_propagates_failures_and_always_closes(sharepoint, stage, caplog):
    _, worker = sharepoint
    failure = RuntimeError("private-run-canary")
    worker._ensure_clients = AsyncMock()
    worker._close_clients = AsyncMock()
    worker._hydrate_site_configs_from_cosmos = AsyncMock(
        side_effect=failure if stage == "configuration" else None,
    )
    worker._parse_collections = lambda: [{"siteDomain": "example.org", "siteName": "site"}]
    worker._process_collection = AsyncMock(side_effect=failure)
    worker.cfg.max_concurrency = 1
    worker._http_total_timeout_s = 1
    summaries = []

    async def write(run_id, summary):
        summaries.append(dict(summary))

    worker._write_run_summary_safely = write
    with pytest.raises(RuntimeError) as raised:
        await worker.run()
    assert raised.value is failure
    worker._close_clients.assert_awaited_once()
    assert all(summary["status"] != "finished" for summary in summaries)
    if stage == "collection":
        assert summaries[-1]["status"] == "failed"
    assert "private-run-canary" not in caplog.text + str(summaries)


@pytest.mark.asyncio
@pytest.mark.parametrize("attribute", ["_search_client", "_blob_service", "_kv", "_credential"])
async def test_sharepoint_cleanup_does_not_mask_primary(sharepoint, attribute, caplog):
    _, worker = sharepoint
    calls = []
    for name in ("_search_client", "_blob_service", "_kv", "_credential"):
        async def close(name=name):
            calls.append(name)
            if name == attribute:
                raise RuntimeError("private-close")
        setattr(worker, name, SimpleNamespace(close=close))
    primary = ValueError("primary")
    with pytest.raises(ValueError) as raised:
        try:
            raise primary
        finally:
            await worker._close_clients()
    assert raised.value is primary
    assert calls == ["_search_client", "_blob_service", "_kv", "_credential"]
    assert "cleanup failed" in caplog.text
    assert "private-close" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["upload", "parse", "pointer"])
async def test_summary_inner_retries_do_not_hide_programming_errors(sharepoint, monkeypatch, stage):
    module, worker = sharepoint
    failure = RuntimeError("summary defect")
    summary = {"runId": "run", "status": "failed"}
    worker._ensure_log_container = AsyncMock()
    blob = SimpleNamespace(
        upload_blob=AsyncMock(side_effect=failure if stage == "upload" else None),
        download_blob=AsyncMock(return_value=SimpleNamespace(
            readall=AsyncMock(return_value=json.dumps(summary).encode()),
        )),
    )
    container = SimpleNamespace(
        get_blob_client=lambda name: blob,
        upload_blob=AsyncMock(side_effect=failure if stage == "pointer" else None),
    )
    worker._blob_service = SimpleNamespace(get_container_client=lambda name: container)
    sleep = AsyncMock()
    monkeypatch.setattr(module.asyncio, "sleep", sleep)
    if stage == "parse":
        monkeypatch.setattr(module.json, "loads", Mock(side_effect=failure))
    with pytest.raises(RuntimeError) as raised:
        await worker._write_run_summary("run", summary)
    assert raised.value is failure
    sleep.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["defect", "connection", "server", "rate-limit"])
async def test_embedding_preserves_real_sdk_retries_without_payloads(sharepoint, monkeypatch, caplog, case):
    module, worker = sharepoint
    worker._aoai_sem = asyncio.Semaphore(1)
    worker._aoai_backoff_cap = 4
    worker._aoai_rate_limit_tries = 2
    worker._aoai_transient_tries = 2
    request = httpx.Request("POST", "https://model.example")
    response = httpx.Response(429 if case == "rate-limit" else 503, request=request,
                              headers={"Retry-After": "invalid"})
    failure = {
        "defect": RuntimeError("private-embedding"),
        "connection": openai.APIConnectionError(message="private-embedding", request=request),
        "server": openai.InternalServerError("private-embedding", response=response, body=None),
        "rate-limit": openai.RateLimitError("private-embedding", response=response, body=None),
    }[case]
    worker._aoai = SimpleNamespace(get_embeddings=Mock(side_effect=[failure, [0.1]]))
    sleep = AsyncMock()
    monkeypatch.setattr(module.asyncio, "sleep", sleep)
    monkeypatch.setattr(module.random, "uniform", lambda *args: 0)
    with caplog.at_level(logging.DEBUG):
        if case == "defect":
            with pytest.raises(RuntimeError) as raised:
                await worker._embed("private-document-content")
            assert raised.value is failure
            sleep.assert_not_awaited()
        else:
            assert await worker._embed("private-document-content") == [0.1]
            sleep.assert_awaited_once_with(1.0)
    assert "private-embedding" not in caplog.text
    assert "private-document-content" not in caplog.text


def prepare_collection(worker):
    async def items(**kwargs):
        yield {"id": "item", "lastModifiedDateTime": "2026-01-01T00:00:00Z", "fields": {}}

    worker._graph_client = SimpleNamespace(get_site_id=AsyncMock(return_value="site"), iter_items=items)
    worker._get_list_navigation_base_url = AsyncMock(return_value="")
    worker._get_lookup_columns_map = AsyncMock(return_value={})
    worker._resolve_lookup_fields_for_item = AsyncMock(return_value={})
    worker._get_security_principals_for_item = AsyncMock(return_value=([], []))
    worker._read_file_log = AsyncMock(return_value=None)
    worker._write_file_log = AsyncMock()
    worker._get_body_lastmod_by_id = AsyncMock(return_value=None)
    worker.cfg.max_file_processing_attempts = 3
    worker._item_timeout_s = 1
    worker._collection_gather_timeout_s = 1
    return {"siteDomain": "example.org", "siteName": "site", "listId": "list"}


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["acl", "lookup"])
async def test_collection_observes_item_failures_instead_of_finishing_empty(sharepoint, stage, caplog):
    module, worker = sharepoint
    spec = prepare_collection(worker)
    failure = RuntimeError("private-item")
    method = "_get_security_principals_for_item" if stage == "acl" else "_resolve_lookup_fields_for_item"
    getattr(worker, method).side_effect = failure
    stats = module.RunStats()
    if stage == "lookup":
        with pytest.raises(RuntimeError) as raised:
            await worker._process_collection(object(), spec, "run", asyncio.Semaphore(1), stats, asyncio.Lock())
        assert raised.value is failure
    else:
        result = await worker._process_collection(object(), spec, "run", asyncio.Semaphore(1), stats, asyncio.Lock())
        assert result["failed"] == 1 and result["success"] == 0
        assert stats.items_failed == 1
        assert worker._write_file_log.await_args.args[1]["status"] == "error"
    worker._get_body_lastmod_by_id.assert_not_awaited()
    assert "private-item" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["late-page", "timeout"])
async def test_collection_failure_cancels_and_observes_its_children(sharepoint, stage):
    module, worker = sharepoint
    spec = prepare_collection(worker)
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    failure = RuntimeError("late page failed")

    async def block(**kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async def items(**kwargs):
        yield {"id": "item", "lastModifiedDateTime": "2026-01-01T00:00:00Z", "fields": {}}
        await entered.wait()
        if stage == "late-page":
            raise failure

    worker._resolve_lookup_fields_for_item = block
    worker._graph_client.iter_items = items
    worker._collection_gather_timeout_s = 0
    with pytest.raises(RuntimeError if stage == "late-page" else TimeoutError):
        await worker._process_collection(
            object(), spec, "run", asyncio.Semaphore(1), module.RunStats(), asyncio.Lock(),
        )
    assert cancelled.is_set()
