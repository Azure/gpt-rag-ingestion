"""Primary Search outcomes must follow confirmed SDK results, not submission."""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
import sys
import types
from unittest.mock import AsyncMock

from azure.core.exceptions import AzureError
from azure.search.documents.models import IndexingResult
import pytest

from telemetry import audit as real_audit


@pytest.fixture
def search_boundary(monkeypatch):
    config = types.SimpleNamespace(get=lambda key, default=None, **kwargs: default)
    dependencies = types.ModuleType("dependencies")
    dependencies.get_config = lambda: config
    monkeypatch.setitem(sys.modules, "dependencies", dependencies)
    credentials = types.ModuleType("tools.credentials")
    credentials.get_azure_client_id = lambda config: None
    monkeypatch.setitem(sys.modules, "tools.credentials", credentials)
    audits = []
    telemetry = types.ModuleType("telemetry")
    telemetry.audit = types.SimpleNamespace(record_search_batch_result=lambda **kwargs: audits.append(kwargs))
    monkeypatch.setitem(sys.modules, "telemetry", telemetry)

    path = Path(__file__).resolve().parents[1] / "tools" / "aisearch.py"
    spec = importlib.util.spec_from_file_location("search_write_under_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)

    class Client:
        result = []
        failure = None

        def __init__(self):
            self.requests = []

        async def upload_documents(self, *, documents):
            self.requests.append(("upload", documents))
            if self.failure:
                raise self.failure
            return self.result

        async def delete_documents(self, *, documents):
            self.requests.append(("delete", documents))
            if self.failure:
                raise self.failure
            return self.result

    client = Client()
    boundary = object.__new__(module.AISearchClient)

    async def get_client(index):
        return client

    boundary.get_search_client = get_client
    return boundary, client, audits


def result(key, succeeded):
    return IndexingResult.deserialize({
        "key": key, "status": succeeded, "statusCode": 200 if succeeded else 400,
        "errorMessage": "private downstream detail",
    })


@pytest.mark.asyncio
async def test_single_delete_uses_sdk_delete_and_requires_confirmation(search_boundary):
    boundary, client, audits = search_boundary
    client.result = [result("a", True)]
    await boundary.delete_document("index", "id", "a")
    assert client.requests == [("delete", [{"id": "a"}])]
    assert audits[0]["operation"] == "delete_documents"


@pytest.mark.asyncio
@pytest.mark.parametrize("response", [[], [result("a", False)], [result("wrong", True)]])
async def test_single_delete_cannot_report_success_without_matching_result(search_boundary, response, caplog):
    boundary, client, _ = search_boundary
    client.result = response
    with caplog.at_level(logging.INFO), pytest.raises(AzureError):
        await boundary.delete_document("index", "id", "a")
    assert "Successfully deleted" not in caplog.text
    assert "private downstream detail" not in caplog.text


@pytest.mark.asyncio
async def test_single_delete_sdk_failure_propagates_without_payload_logging(search_boundary, caplog):
    boundary, client, _ = search_boundary
    client.failure = AzureError("private downstream detail")
    with pytest.raises(AzureError):
        await boundary.delete_document("index", "id", "a")
    assert "private downstream detail" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "expected"),
    [
        ([result("a", True), result("b", True)], {"deleted": 2, "failed": 0}),
        ([result("a", True), result("b", False)], {"deleted": 1, "failed": 1}),
        ([result("a", True)], {"deleted": 1, "failed": 1}),
        ([], {"deleted": 0, "failed": 2}),
        ([result("wrong", True)], {"deleted": 0, "failed": 2}),
        ([result("a", True), result("a", True)], {"deleted": 0, "failed": 2}),
    ],
)
async def test_batch_delete_counts_only_matching_confirmed_keys(search_boundary, response, expected, caplog):
    boundary, client, audits = search_boundary
    client.result = response
    assert await boundary.delete_documents("index", "id", ["a", "b"]) == expected
    assert client.requests == [("delete", [{"id": "a"}, {"id": "b"}])]
    assert audits[0]["operation"] == "delete_documents"
    assert "private downstream detail" not in caplog.text


@pytest.mark.asyncio
async def test_batch_delete_sdk_error_reports_every_requested_key_failed(search_boundary, caplog):
    boundary, client, _ = search_boundary
    client.failure = AzureError("private downstream detail")
    assert await boundary.delete_documents("index", "id", ["a", "b"]) == {"deleted": 0, "failed": 2}
    assert "private downstream detail" not in caplog.text


@pytest.mark.asyncio
async def test_index_rejection_is_false_without_payload_logging(search_boundary, caplog):
    boundary, client, audits = search_boundary
    client.result = [result("a", False)]
    assert await boundary.index_document("index", {"id": "a"}) is False
    assert client.requests == [("upload", [{"id": "a"}])]
    assert audits[0]["operation"] == "upload_documents"
    assert "private downstream detail" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "expected"),
    [([result("a", True)], True), ([result("wrong", True)], False), ([], False)],
)
async def test_index_success_requires_one_matching_confirmation(search_boundary, response, expected):
    boundary, client, _ = search_boundary
    client.result = response
    assert await boundary.index_document("index", {"id": "a"}) is expected


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["index", "delete", "batch"])
async def test_unexpected_write_failure_cannot_be_swallowed(search_boundary, operation):
    boundary, client, _ = search_boundary
    client.failure = RuntimeError("unexpected write failure")
    with pytest.raises(RuntimeError, match="unexpected write failure"):
        if operation == "index":
            await boundary.index_document("index", {"id": "a"})
        elif operation == "delete":
            await boundary.delete_document("index", "id", "a")
        else:
            await boundary.delete_documents("index", "id", ["a"])


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["index", "delete", "batch"])
async def test_audit_export_failure_cannot_change_a_confirmed_sdk_outcome(
    search_boundary, monkeypatch, caplog, operation,
):
    boundary, client, _ = search_boundary
    client.result = [result("a", True)]
    monkeypatch.setattr(sys.modules["search_write_under_test"], "audit", real_audit)

    def fail_export(*args, **kwargs):
        raise RuntimeError("private exporter detail")

    monkeypatch.setattr(real_audit._logger, "info", fail_export)
    if operation == "index":
        assert await boundary.index_document("index", {"id": "a"}) is True
    elif operation == "delete":
        assert await boundary.delete_document("index", "id", "a") is None
    else:
        assert await boundary.delete_documents("index", "id", ["a"]) == {"deleted": 1, "failed": 0}
    assert "Audit event export failed" in caplog.text
    assert "private exporter detail" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("late_failure", [False, True])
async def test_service_reference_iterator_has_no_top_cap_and_propagates_pages(search_boundary, late_failure):
    boundary, client, _ = search_boundary

    async def documents():
        for number in range(1001):
            yield {"relatedImages": [str(number)]}
        if late_failure:
            raise AzureError("page failed")

    client.search = AsyncMock(return_value=documents())
    observed = []
    if late_failure:
        with pytest.raises(AzureError, match="page failed"):
            async for document in boundary.iter_documents("index", ["relatedImages"]):
                observed.append(document)
    else:
        async for document in boundary.iter_documents("index", ["relatedImages"]):
            observed.append(document)
    assert len(observed) == 1001
    kwargs = client.search.await_args.kwargs
    assert kwargs["select"] == ["relatedImages"]
    assert kwargs["headers"] == {"x-ms-enable-elevated-read": "true"}
    assert "top" not in kwargs and "skip" not in kwargs


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [AzureError("private-search-query"), RuntimeError("private-search-query")])
async def test_query_translation_is_explicit_failure_without_payloads(search_boundary, failure, caplog):
    boundary, client, _ = search_boundary
    client.search = AsyncMock(side_effect=failure)
    outcome = await boundary.search_documents("index")
    assert outcome["documents"] == [] and outcome["count"] == 0
    assert outcome["error"]
    assert "private-search-query" not in caplog.text + str(outcome)


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_resource", ["first", "second", "credential"])
async def test_search_close_attempts_every_resource_and_propagates_failure(search_boundary, failed_resource):
    boundary, _, _ = search_boundary
    calls = []
    failure = AzureError("cleanup failed")

    def resource(name):
        async def close():
            calls.append(name)
            if name == failed_resource:
                raise failure
        return types.SimpleNamespace(close=close)

    boundary.clients = {"first": resource("first"), "second": resource("second")}
    boundary.credential = resource("credential")
    with pytest.raises(AzureError) as raised:
        await boundary.close()
    assert raised.value is failure
    assert calls == ["first", "second", "credential"]
    assert boundary.clients == {}
