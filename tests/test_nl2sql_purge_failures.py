"""Purge totals reflect confirmed deletions and never hide a failed scan."""

import importlib.util
import asyncio
import json
import logging
from pathlib import Path
import sys
import types

from azure.core.exceptions import AzureError
import pytest


@pytest.fixture
def purger(monkeypatch):
    dependencies = types.ModuleType("dependencies")
    dependencies.get_config = lambda: None
    monkeypatch.setitem(sys.modules, "dependencies", dependencies)
    tools = types.ModuleType("tools")
    tools.__path__ = []
    tools.AISearchClient = object
    monkeypatch.setitem(sys.modules, "tools", tools)
    credentials = types.ModuleType("tools.credentials")
    credentials.get_azure_client_id = lambda config: None
    monkeypatch.setitem(sys.modules, "tools.credentials", credentials)
    path = Path(__file__).resolve().parents[1] / "jobs" / "nl2sql_purger.py"
    spec = importlib.util.spec_from_file_location("nl2sql_purge_under_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)

    class Pages:
        def __init__(self, late_failure=False, empty=False):
            self.late_failure = late_failure
            self.empty = empty

        async def by_page(self):
            async def page():
                if self.empty:
                    return
                yield {"id": "a"}
                yield {"id": "b"}
            yield page()
            if self.late_failure:
                raise AzureError("late page failed")

    class Search:
        failure = None
        outcome = {"deleted": 2, "failed": 0}
        failure_at = None
        late_page_at = None
        delete_failure = None
        close_failure = None
        search_calls = 0
        close_calls = 0
        delete_calls = 0
        count_error = AzureError("count failed")

        async def get_search_client(self, index):
            return self

        async def search(self, **kwargs):
            self.search_calls += 1
            if self.failure:
                raise self.failure
            if self.failure_at == self.search_calls:
                raise self.count_error
            assert kwargs["headers"] == {"x-ms-enable-elevated-read": "true"}
            return Pages(self.late_page_at == self.search_calls, self.delete_calls > 0 and self.search_calls > 2)

        async def delete_documents(self, **kwargs):
            self.delete_calls += 1
            if self.delete_failure:
                raise self.delete_failure
            return self.outcome

        async def close(self):
            self.close_calls += 1
            if self.close_failure:
                raise self.close_failure

    class Container:
        def __init__(self):
            self.summaries = []

        async def create_container(self):
            pass

        async def list_blobs(self):
            for item in ():
                yield item

        async def upload_blob(self, *, data, **kwargs):
            self.summaries.append(json.loads(data))

    class Service:
        close_calls = 0
        close_failure = None

        def __init__(self):
            self.container = Container()

        def get_container_client(self, name):
            return self.container

        async def close(self):
            self.close_calls += 1
            if self.close_failure:
                raise self.close_failure

    instance = object.__new__(module.NL2SQLPurger)
    instance._ai_search = Search()
    instance._blob_service = Service()
    instance._credential = Service()
    instance.cfg = module.NL2SQLPurgerConfig(storage_account_name="fixture", queries_index_name="index")
    return instance


@pytest.mark.asyncio
async def test_purge_counts_only_confirmed_deletions(purger):
    assert await purger._purge_one_index("index", set()) == 2


@pytest.mark.asyncio
async def test_partial_delete_is_not_a_finished_success(purger):
    purger._ai_search.outcome = {"deleted": 1, "failed": 1}
    with pytest.raises(RuntimeError, match="confirm"):
        await purger._purge_one_index("index", set())


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["purge", "count"])
async def test_scan_failure_is_not_an_empty_index(purger, operation):
    purger._ai_search.failure = AzureError("search unavailable")
    with pytest.raises(AzureError):
        if operation == "purge":
            await purger._purge_one_index("index", set())
        else:
            await purger._count_index_docs("index")


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["before-count", "purge-page", "after-count", "delete", "partial", "cancelled"])
async def test_run_failure_never_publishes_finished_and_closes_owned_clients(purger, caplog, stage):
    if stage in {"before-count", "after-count"}:
        purger._ai_search.failure_at = 1 if stage == "before-count" else 3
    elif stage == "purge-page":
        purger._ai_search.late_page_at = 2
    elif stage == "partial":
        purger._ai_search.outcome = {"deleted": 1, "failed": 1}
    else:
        purger._ai_search.delete_failure = asyncio.CancelledError() if stage == "cancelled" else AzureError("delete failed")
    expected = asyncio.CancelledError if stage == "cancelled" else (RuntimeError if stage == "partial" else AzureError)
    with caplog.at_level(logging.INFO), pytest.raises(expected):
        await purger.run()
    assert not purger._blob_service.container.summaries
    assert "RUN-COMPLETE" not in caplog.text
    assert '"status": "finished"' not in caplog.text
    assert purger._blob_service.close_calls == 1
    assert purger._credential.close_calls == 1
    assert purger._ai_search.close_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("resource", ["_ai_search", "_blob_service", "_credential"])
async def test_cleanup_failure_cannot_replace_primary_failure_or_skip_other_clients(purger, caplog, resource):
    purger._ai_search.failure_at = 3
    getattr(purger, resource).close_failure = RuntimeError("private cleanup detail")
    with pytest.raises(AzureError, match="count failed") as failure:
        await purger.run()
    assert failure.value is purger._ai_search.count_error
    assert purger._ai_search.close_calls == 1
    assert purger._blob_service.close_calls == 1
    assert purger._credential.close_calls == 1
    assert "cleanup" in caplog.text.lower()
    assert "private cleanup detail" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("resource", ["_ai_search", "_blob_service", "_credential"])
async def test_cleanup_failure_preserves_confirmed_primary_summary(purger, caplog, resource):
    getattr(purger, resource).close_failure = RuntimeError("private cleanup detail")
    await purger.run()
    assert purger._blob_service.container.summaries[0]["status"] == "finished"
    assert purger._ai_search.close_calls == 1
    assert purger._blob_service.close_calls == 1
    assert purger._credential.close_calls == 1
    assert "cleanup" in caplog.text.lower()
    assert "private cleanup detail" not in caplog.text


@pytest.mark.asyncio
async def test_run_success_publishes_confirmed_totals_and_closes_owned_clients(purger):
    await purger.run()
    summary = purger._blob_service.container.summaries[0]
    assert summary["status"] == "finished"
    assert summary["results"] == [{"kind": "queries", "deleted": 2, "before": 2, "after": 0}]
    assert purger._ai_search.close_calls == 1
