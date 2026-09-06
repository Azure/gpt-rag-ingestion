"""Primary Search outcomes must follow confirmed SDK results, not submission."""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
import sys
import types

from azure.core.exceptions import AzureError
import pytest


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
    return types.SimpleNamespace(key=key, succeeded=succeeded, error_message="private downstream detail")


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
