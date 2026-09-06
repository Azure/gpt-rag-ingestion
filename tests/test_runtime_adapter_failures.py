"""SDK and parser failures retain the adapter's explicit public result contract."""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
import sys
import types
from unittest.mock import AsyncMock

from azure.core.exceptions import AzureError, ResourceNotFoundError
import pytest
import requests


@pytest.fixture
def adapter(monkeypatch):
    config = types.SimpleNamespace(
        get=lambda key, default=None, **kwargs: default or "configured",
        aiocredential=object(),
    )
    dependencies = types.ModuleType("dependencies")
    dependencies.get_config = lambda: config
    monkeypatch.setitem(sys.modules, "dependencies", dependencies)

    def load(name):
        path = Path(__file__).resolve().parents[1] / "tools" / f"{name}.py"
        spec = importlib.util.spec_from_file_location(f"{name}_failure_under_test", path)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, spec.name, module)
        spec.loader.exec_module(module)
        return module

    return load


@pytest.mark.parametrize(("name", "class_name"), [
    ("keyvault", "KeyVaultClient"), ("aisearch", "AISearchClient"),
])
def test_credential_construction_failure_propagates_without_payload(
    adapter, monkeypatch, caplog, name, class_name,
):
    module = adapter(name)
    failure = RuntimeError("private-constructor-canary")

    def fail(**kwargs):
        raise failure

    monkeypatch.setattr(module, "ManagedIdentityCredential", fail)
    with pytest.raises(RuntimeError) as caught:
        getattr(module, class_name)()
    assert caught.value is failure
    assert "private-constructor-canary" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [AzureError("private-secret-canary"), RuntimeError("private-secret-canary")])
async def test_keyvault_expected_service_failure_is_optional_but_programming_failure_propagates(
    adapter, monkeypatch, caplog, failure,
):
    module = adapter("keyvault")
    boundary = object.__new__(module.KeyVaultClient)
    boundary.kv_uri = "https://example.vault.azure.net"
    boundary.credential = object()
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.get_secret.side_effect = failure
    monkeypatch.setattr(module, "AsyncSecretClient", lambda **kwargs: client)
    if isinstance(failure, AzureError):
        assert await boundary.get_secret("secret") is None
    else:
        with pytest.raises(RuntimeError) as caught:
            await boundary.get_secret("secret")
        assert caught.value is failure
    client.__aexit__.assert_awaited_once()
    assert "private-secret-canary" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [
    ResourceNotFoundError("absent"), AzureError("private-cosmos-canary"), RuntimeError("private-cosmos-canary"),
])
async def test_cosmos_read_only_not_found_means_absent(adapter, monkeypatch, caplog, failure):
    caplog.set_level(logging.INFO)
    module = adapter("cosmosdb")
    container = types.SimpleNamespace(read_item=AsyncMock(side_effect=failure))
    database = types.SimpleNamespace(get_container_client=lambda name: container)
    client = AsyncMock()
    client.__aenter__.return_value = types.SimpleNamespace(get_database_client=lambda **kwargs: database)
    monkeypatch.setattr(module, "CosmosClient", lambda *args, **kwargs: client)
    boundary = module.CosmosDBClient()
    if isinstance(failure, ResourceNotFoundError):
        assert await boundary.get_document("container", "key") is None
    else:
        with pytest.raises(type(failure)) as caught:
            await boundary.get_document("container", "key")
        assert caught.value is failure
        assert "does not exist" not in caplog.text
    client.__aexit__.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["create_document", "update_document"])
@pytest.mark.parametrize("failure", [AzureError("private-write-canary"), RuntimeError("private-write-canary")])
async def test_cosmos_write_sdk_failure_retains_explicit_none(adapter, monkeypatch, caplog, operation, failure):
    caplog.set_level(logging.INFO)
    module = adapter("cosmosdb")
    container = types.SimpleNamespace(
        create_item=AsyncMock(side_effect=failure),
        replace_item=AsyncMock(side_effect=failure),
    )
    database = types.SimpleNamespace(get_container_client=lambda name: container)
    client = AsyncMock()
    client.__aenter__.return_value = types.SimpleNamespace(get_database_client=lambda **kwargs: database)
    monkeypatch.setattr(module, "CosmosClient", lambda *args, **kwargs: client)
    boundary = module.CosmosDBClient()
    args = ("container", "key") if operation == "create_document" else ("container", {"id": "key"})
    if isinstance(failure, AzureError):
        assert await getattr(boundary, operation)(*args) is None
    else:
        with pytest.raises(RuntimeError) as caught:
            await getattr(boundary, operation)(*args)
        assert caught.value is failure
    client.__aexit__.assert_awaited_once()
    assert "private-write-canary" not in caplog.text


@pytest.mark.parametrize("failure", [AzureError("private-blob-canary"), RuntimeError("private-blob-canary")])
def test_blob_download_retries_only_sdk_failures(adapter, monkeypatch, caplog, failure):
    module = adapter("blob")
    waits = []
    monkeypatch.setattr(module.time, "sleep", waits.append)
    calls = []

    def download():
        calls.append(True)
        raise failure

    boundary = object.__new__(module.BlobClient)
    boundary.container_name, boundary.blob_name = "container", "blob"
    boundary.blob_service_client = types.SimpleNamespace(
        get_blob_client=lambda **kwargs: types.SimpleNamespace(download_blob=download),
    )
    with pytest.raises(type(failure)) as caught:
        boundary.download_blob()
    assert caught.value is failure
    assert len(calls) == (2 if isinstance(failure, AzureError) else 1)
    assert waits == ([10] if isinstance(failure, AzureError) else [])
    assert "private-blob-canary" not in caplog.text


@pytest.mark.parametrize("name", ["doc_intelligence", "content_understanding"])
@pytest.mark.parametrize("stage", ["auth", "submit", "poll", "json"])
def test_analysis_sdk_failure_returns_errors_without_payload(adapter, monkeypatch, caplog, name, stage):
    module = adapter(name)
    failure = AzureError("private-analysis-canary") if stage == "auth" else requests.RequestException("private-analysis-canary")

    def get_token(*args):
        if stage == "auth":
            raise failure
        return types.SimpleNamespace(token="test-only-token")

    monkeypatch.setattr(module, "ChainedTokenCredential", lambda *args: types.SimpleNamespace(get_token=get_token))
    client_class = module.DocumentIntelligenceClient if name == "doc_intelligence" else module.ContentUnderstandingClient
    boundary = client_class()
    monkeypatch.setattr(module.time, "sleep", lambda seconds: None)

    def post(*args, **kwargs):
        if stage == "submit":
            raise failure
        return types.SimpleNamespace(status_code=202, headers={"Operation-Location": "https://example.test/result"}, text="")

    def parse():
        raise requests.exceptions.JSONDecodeError("private-analysis-canary", "", 0)

    def get(*args, **kwargs):
        if stage == "poll":
            raise failure
        return types.SimpleNamespace(status_code=200, text="", json=parse)

    monkeypatch.setattr(module.requests, "post", post)
    monkeypatch.setattr(module.requests, "get", get)
    result, errors = boundary.analyze_document_from_bytes(b"document", "document.pdf")
    assert errors
    assert not result.get("content")
    assert "private-analysis-canary" not in str(errors) + caplog.text


@pytest.mark.parametrize("name", ["doc_intelligence", "content_understanding"])
def test_analysis_programming_failure_propagates(adapter, monkeypatch, name):
    module = adapter(name)
    failure = RuntimeError("unexpected token implementation")

    def get_token(*args):
        raise failure

    monkeypatch.setattr(module, "ChainedTokenCredential", lambda *args: types.SimpleNamespace(get_token=get_token))
    client_class = module.DocumentIntelligenceClient if name == "doc_intelligence" else module.ContentUnderstandingClient
    boundary = client_class()
    with pytest.raises(RuntimeError) as caught:
        boundary.analyze_document_from_bytes(b"document", "document.pdf")
    assert caught.value is failure


@pytest.mark.parametrize("name", ["doc_intelligence", "content_understanding"])
def test_analysis_confirmed_success_keeps_content(adapter, monkeypatch, name):
    module = adapter(name)
    monkeypatch.setattr(module, "ChainedTokenCredential", lambda *args: types.SimpleNamespace(
        get_token=lambda *args: types.SimpleNamespace(token="test-only-token"),
    ))
    client_class = module.DocumentIntelligenceClient if name == "doc_intelligence" else module.ContentUnderstandingClient
    boundary = client_class()
    monkeypatch.setattr(module.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(module.requests, "post", lambda *args, **kwargs: types.SimpleNamespace(
        status_code=202, headers={"Operation-Location": "https://example.test/result"}, text="",
    ))
    monkeypatch.setattr(module.requests, "get", lambda *args, **kwargs: types.SimpleNamespace(
        status_code=200, text="", json=lambda: {
            "status": "succeeded", "analyzeResult": {"content": "markdown"},
            "result": {"contents": [{"markdown": "markdown"}]},
        },
    ))
    result, errors = boundary.analyze_document_from_bytes(b"document", "document.pdf")
    assert errors == []
    assert result["content"] == "markdown"
