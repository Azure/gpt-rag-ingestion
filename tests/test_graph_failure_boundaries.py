"""Graph retries are limited to actual transport failures, not arbitrary defects."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest

from tests.test_worker_failure_boundaries import worker_module


@pytest.fixture
def graph_class(worker_module):
    return worker_module("sharepoint_graph_client").SharePointGraphClient


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["_gget", "_gbytes", "download_drive_item"])
@pytest.mark.parametrize("failure", [
    RuntimeError("private-graph-canary"), aiohttp.ClientConnectionError("private-graph-canary"),
])
async def test_graph_retries_transport_only(graph_class, monkeypatch, caplog, method, failure):
    client = object.__new__(graph_class)
    client._graph_token = "not-a-real-token"
    client._graph_base = "https://graph.example"
    session = SimpleNamespace(get=Mock(side_effect=failure))
    sleep = AsyncMock()
    monkeypatch.setattr("asyncio.sleep", sleep)
    argument = {"@microsoft.graph.downloadUrl": "https://download.example"} if method == "download_drive_item" else "https://graph.example/items"
    with pytest.raises(RuntimeError) as raised:
        await getattr(client, method)(session, argument)
    if isinstance(failure, aiohttp.ClientError):
        assert session.get.call_count == 6
        assert sleep.await_count == 6
    else:
        assert raised.value is failure
        assert session.get.call_count == 1
        sleep.assert_not_awaited()
    assert "private-graph-canary" not in caplog.text


def test_guid_conversion_defect_is_not_an_absent_principal(graph_class):
    class Broken:
        def __str__(self):
            raise RuntimeError("conversion defect")

    with pytest.raises(RuntimeError, match="conversion defect"):
        graph_class._is_guid(Broken())


@pytest.mark.parametrize(("value", "expected"), [
    ("9dd20ac9-cf53-49b6-9880-4b734ea03804", True), ("invalid", False), ("", False),
])
def test_guid_validation_preserves_real_and_invalid_values(graph_class, value, expected):
    assert graph_class._is_guid(value) is expected
