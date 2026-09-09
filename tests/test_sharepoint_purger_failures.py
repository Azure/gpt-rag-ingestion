"""SharePoint purge outcomes require a complete scan and matching SDK confirmations."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

from azure.core.exceptions import AzureError
from azure.search.documents.models import IndexingResult
import pytest

from tests.test_worker_failure_boundaries import worker_module
from tests import test_sharepoint_failure_boundaries as shared


@pytest.fixture
def purger(worker_module):
    module = worker_module("sharepoint_purger")
    worker = object.__new__(module.SharePointPurger)
    worker.cfg = SimpleNamespace(indexer_name="purger", jobs_log_container="jobs",
                                 storage_account_name="storage", batch_size=10)
    worker._storage_writable = True
    worker._blob_op_timeout_s = 5
    worker._run_summary_total_timeout_s = 5
    worker._app = SimpleNamespace(get=lambda key, default=None, **kwargs: default)
    return module, worker


def result(key, succeeded=True):
    return IndexingResult.deserialize({
        "key": key, "status": succeeded, "statusCode": 200 if succeeded else 400,
        "errorMessage": "private-search",
    })


@pytest.mark.asyncio
@pytest.mark.parametrize(("case", "expected"), [
    ("confirmed", (2, 0)), ("partial", (1, 1)), ("missing", (1, 1)),
    ("duplicate", (0, 2)), ("unrelated", (0, 2)), ("malformed", (0, 2)),
    ("error", (0, 2)),
])
async def test_purge_counts_only_matching_positive_sdk_results(purger, caplog, case, expected):
    _, worker = purger
    worker._search_client = object()
    responses = {
        "confirmed": [result("one"), result("two")],
        "partial": [result("one"), result("two", False)],
        "missing": [result("one")],
        "duplicate": [result("one"), result("one")],
        "unrelated": [result("other")],
        "malformed": [object()],
    }
    worker._with_backoff = AsyncMock(
        return_value=responses.get(case), side_effect=RuntimeError("private-search") if case == "error" else None,
    )
    worker._search_client = SimpleNamespace(delete_documents=AsyncMock())
    assert await worker._delete_docs_by_id("run", [{"id": "one"}, {"id": "two"}]) == expected
    assert "private-search" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["success", "count", "late-page", "delete"])
async def test_purge_run_requires_complete_scan_and_deletion(purger, stage, caplog):
    module, worker = purger
    worker._ensure_clients = AsyncMock()
    worker._close_clients = AsyncMock()
    worker._graph_client = object()
    worker._http_total_timeout_s = 1
    worker._search_page_size = 1000
    worker._allowed_collection_keys = None
    worker._collection_items_cache = {"example.org/site/list": {"keep"}}
    summaries = []

    async def write(run_id, summary):
        summaries.append(dict(summary))

    async def page():
        yield {"id": "one", "metadata_storage_path": "/example.org/site/list/gone"}

    async def pages():
        yield page()
        if stage == "late-page":
            raise AzureError("private-page")

    results = SimpleNamespace(
        get_count=AsyncMock(return_value=1, side_effect=AzureError("private-count") if stage == "count" else None),
        by_page=pages,
    )
    worker._search_client = SimpleNamespace(
        search=AsyncMock(return_value=results),
        delete_documents=AsyncMock(return_value=[result("one", stage != "delete")]),
    )
    worker._write_run_summary_safely = write
    if stage == "success":
        await worker.run()
        assert summaries[-1]["docsDeleted"] == 1
        assert summaries[-1]["status"] == "finished"
    else:
        with pytest.raises(AzureError):
            await worker.run()
        assert summaries[-1]["status"] == "failed"
        assert all(summary["status"] != "finished" for summary in summaries)
        if stage == "count":
            worker._search_client.delete_documents.assert_not_awaited()
        elif stage == "late-page":
            assert summaries[-1]["docsDeleted"] == 1
        else:
            assert summaries[-1]["docsDeleted"] == 0
            assert summaries[-1]["docsFailedDelete"] == 1
    worker._close_clients.assert_awaited_once()
    assert "private-" not in caplog.text + str(summaries)


@pytest.mark.asyncio
@pytest.mark.parametrize("attribute", ["_search_client", "_blob_service", "_kv", "_credential"])
async def test_purger_cleanup_preserves_primary_and_attempts_every_resource(purger, attribute, caplog):
    await shared.test_sharepoint_cleanup_does_not_mask_primary(purger, attribute, caplog)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["exists", "upload", "delete"])
@pytest.mark.parametrize("failure", [AzureError("private-probe"), RuntimeError("defect")])
async def test_purger_probe_only_recovers_sdk_failures(purger, stage, failure, caplog):
    await shared.test_optional_storage_probe_only_recovers_sdk_failures(purger, stage, failure, caplog)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["upload", "parse", "pointer"])
async def test_purger_summary_inner_retries_do_not_hide_defects(purger, monkeypatch, stage):
    await shared.test_summary_inner_retries_do_not_hide_programming_errors(purger, monkeypatch, stage)


@pytest.mark.asyncio
async def test_purger_summary_failure_is_secondary(purger, caplog):
    await shared.test_optional_summary_failure_preserves_primary_exception(purger, caplog)
