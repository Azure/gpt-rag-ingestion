"""Purge totals reflect confirmed deletions and never hide a failed scan."""

import importlib.util
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
        async def by_page(self):
            async def page():
                yield {"id": "a"}
                yield {"id": "b"}
            yield page()

    class Search:
        failure = None
        outcome = {"deleted": 2, "failed": 0}

        async def get_search_client(self, index):
            return self

        async def search(self, **kwargs):
            if self.failure:
                raise self.failure
            return Pages()

        async def delete_documents(self, **kwargs):
            return self.outcome

    instance = object.__new__(module.NL2SQLPurger)
    instance._ai_search = Search()
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
