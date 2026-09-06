"""SDK and parser failures retain the adapter's explicit public result contract."""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
import sys
import types
from unittest.mock import AsyncMock

from azure.core.exceptions import AzureError, ResourceNotFoundError
import httpx
import openai
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


@pytest.mark.parametrize(("name", "method"), [
    ("doc_intelligence", "bytes"),
    ("doc_intelligence", "blob"),
    ("content_understanding", "bytes"),
])
@pytest.mark.parametrize("stage", ["auth", "submit", "poll", "json", "submit-status", "poll-status", "poll-http"])
def test_analysis_sdk_failure_returns_errors_without_payload(adapter, monkeypatch, caplog, name, method, stage):
    module = adapter(name)
    failure = AzureError("private-analysis-canary") if stage == "auth" else requests.RequestException("private-analysis-canary")

    def get_token(*args):
        if stage == "auth":
            raise failure
        return types.SimpleNamespace(token="test-only-token")

    monkeypatch.setattr(module, "ChainedTokenCredential", lambda *args: types.SimpleNamespace(get_token=get_token))
    client_class = module.DocumentIntelligenceClient if name == "doc_intelligence" else module.ContentUnderstandingClient
    boundary = client_class()
    if method == "blob":
        monkeypatch.setattr(module, "BlobServiceClient", lambda **kwargs: types.SimpleNamespace(
            get_blob_client=lambda **kwargs: types.SimpleNamespace(
                download_blob=lambda: types.SimpleNamespace(readall=lambda: b"document"),
            ),
        ))
    monkeypatch.setattr(module.time, "sleep", lambda seconds: None)

    def post(*args, **kwargs):
        if stage == "submit":
            raise failure
        return types.SimpleNamespace(
            status_code=400 if stage == "submit-status" else 202,
            headers={"Operation-Location": "https://example.test/result"},
            text="private-analysis-canary" if stage == "submit-status" else "",
        )

    def parse():
        if stage == "poll-status":
            return {"status": "failed"}
        if stage == "poll-http":
            return {"status": "succeeded", "result": {"contents": [{"markdown": "not-confirmed"}]}}
        raise requests.exceptions.JSONDecodeError("private-analysis-canary", "", 0)

    def get(*args, **kwargs):
        if stage == "poll":
            raise failure
        return types.SimpleNamespace(
            status_code=500 if stage == "poll-http" else 200,
            text="private-analysis-canary" if stage in {"poll-status", "poll-http"} else "", json=parse,
        )

    monkeypatch.setattr(module.requests, "post", post)
    monkeypatch.setattr(module.requests, "get", get)
    result, errors = (
        boundary.analyze_document_from_blob_url("https://example.test/container/document.pdf")
        if method == "blob" else boundary.analyze_document_from_bytes(b"document", "document.pdf")
    )
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


@pytest.mark.parametrize(("name", "method"), [
    ("doc_intelligence", "bytes"),
    ("doc_intelligence", "blob"),
    ("content_understanding", "bytes"),
])
@pytest.mark.parametrize("filename", ["document.pdf", "document.jpg"])
def test_analysis_confirmed_success_keeps_content(adapter, monkeypatch, caplog, name, method, filename):
    caplog.set_level(logging.DEBUG)
    module = adapter(name)
    monkeypatch.setattr(module, "ChainedTokenCredential", lambda *args: types.SimpleNamespace(
        get_token=lambda *args: types.SimpleNamespace(token="test-only-token"),
    ))
    client_class = module.DocumentIntelligenceClient if name == "doc_intelligence" else module.ContentUnderstandingClient
    boundary = client_class()
    if method == "blob":
        monkeypatch.setattr(module, "BlobServiceClient", lambda **kwargs: types.SimpleNamespace(
            get_blob_client=lambda **kwargs: types.SimpleNamespace(
                download_blob=lambda: types.SimpleNamespace(readall=lambda: b"document"),
            ),
        ))
    monkeypatch.setattr(module.time, "sleep", lambda seconds: None)
    def post(endpoint, **kwargs):
        assert kwargs["headers"]["Authorization"] == "Bearer test-only-token"
        if name == "doc_intelligence":
            assert ("features=ocrHighResolution" in endpoint) == filename.endswith(".pdf")
        return types.SimpleNamespace(
            status_code=202, headers={"Operation-Location": "https://example.test/result"}, text="",
        )

    monkeypatch.setattr(module.requests, "post", post)
    monkeypatch.setattr(module.requests, "get", lambda *args, **kwargs: types.SimpleNamespace(
        status_code=200, text="", json=lambda: {
            "status": "succeeded", "analyzeResult": {"content": "markdown"},
            "result": {"contents": [{"markdown": "markdown"}]},
        },
    ))
    result, errors = (
        boundary.analyze_document_from_blob_url(f"https://example.test/container/{filename}")
        if method == "blob" else boundary.analyze_document_from_bytes(b"document", filename)
    )
    assert errors == []
    assert result["content"] == "markdown"
    assert "test-only-token" not in caplog.text


@pytest.mark.parametrize("stage", ["auth", "blob"])
@pytest.mark.parametrize("failure", [AzureError("private-blob-canary"), RuntimeError("private-blob-canary")])
def test_analysis_blob_sdk_failure_is_explicit_and_programming_errors_propagate(
    adapter, monkeypatch, caplog, stage, failure,
):
    module = adapter("doc_intelligence")

    def fail(*args, **kwargs):
        raise failure

    get_token = fail if stage == "auth" else lambda *args: types.SimpleNamespace(token="test-only-token")
    monkeypatch.setattr(module, "ChainedTokenCredential", lambda *args: types.SimpleNamespace(get_token=get_token))
    monkeypatch.setattr(module, "BlobServiceClient", fail)
    boundary = module.DocumentIntelligenceClient()
    errors = []
    if isinstance(failure, AzureError):
        result, errors = boundary.analyze_document_from_blob_url("https://example.test/container/document.pdf")
        assert result == {}
        assert errors
    else:
        with pytest.raises(RuntimeError) as caught:
            boundary.analyze_document_from_blob_url("https://example.test/container/document.pdf")
        assert caught.value is failure
    assert "private-blob-canary" not in str(errors) + caplog.text


@pytest.mark.parametrize("status", [200, 202, 404])
def test_analysis_figure_requires_200_without_response_payload(adapter, monkeypatch, caplog, status):
    module = adapter("doc_intelligence")
    monkeypatch.setattr(module, "ChainedTokenCredential", lambda *args: types.SimpleNamespace(
        get_token=lambda *args: types.SimpleNamespace(token="test-only-token"),
    ))
    boundary = module.DocumentIntelligenceClient()
    response = requests.Response()
    response.status_code = status
    response._content = b"private-figure-canary"
    monkeypatch.setattr(module.requests, "get", lambda *args, **kwargs: response)
    if status == 200:
        assert boundary.get_figure("model", "result", "figure") == b"private-figure-canary"
    else:
        with pytest.raises(requests.HTTPError) as caught:
            boundary.get_figure("model", "result", "figure")
        assert caught.value.response is response
        assert "private-figure-canary" not in str(caught.value)
    assert "private-figure-canary" not in caplog.text


@pytest.fixture
def openai_boundary(adapter):
    module = adapter("aoai")
    boundary = object.__new__(module.AzureOpenAIClient)
    boundary.document_filename = ""
    boundary.chat_deployment = boundary.vision_deployment = "chat"
    boundary.embedding_deployment = "embedding"
    boundary.max_gpt_tokens = boundary.max_embed_tokens = 100
    boundary.retry_max_attempts = 1
    boundary.retry_base_seconds = 1
    boundary.retry_max_seconds = 60
    boundary.retry_jitter_seconds = 0
    for name in (
        "_retry_wait_total_sec", "_retry_count", "_embedding_calls",
        "_embedding_tokens_total", "_completion_calls",
        "_completion_input_tokens", "_completion_output_tokens",
    ):
        setattr(boundary, name, 0)
    boundary._truncate_input = lambda text, limit: text
    return module, boundary


def sdk_status_error(status, retry_header=None):
    response = httpx.Response(
        status, request=httpx.Request("POST", "https://example.test"),
        headers={} if retry_header is None else {"Retry-After": retry_header},
    )
    kind = openai.RateLimitError if status == 429 else openai.APIStatusError
    return kind("private-openai-canary", response=response, body=None)


@pytest.mark.parametrize("operation", ["get_completion", "get_embeddings"])
@pytest.mark.parametrize("failure", [
    RuntimeError("private-openai-canary"),
    openai.OpenAIError("private-openai-canary"),
    sdk_status_error(403),
    sdk_status_error(429),
])
def test_openai_terminal_failure_preserves_identity_without_payload(
    openai_boundary, monkeypatch, caplog, operation, failure,
):
    module, boundary = openai_boundary
    calls, waits = [], []

    def create(**kwargs):
        calls.append(kwargs)
        raise failure

    endpoint = types.SimpleNamespace(create=create)
    boundary.client = types.SimpleNamespace(
        embeddings=endpoint, chat=types.SimpleNamespace(completions=endpoint),
    )
    monkeypatch.setattr(module.time, "sleep", waits.append)
    with pytest.raises(type(failure)) as caught:
        getattr(boundary, operation)("input", retry_after=False)
    assert caught.value is failure
    assert len(calls) == 1
    assert waits == []
    assert "private-openai-canary" not in caplog.text


@pytest.mark.parametrize("operation", ["get_completion", "get_embeddings"])
@pytest.mark.parametrize("header", ["2.5", "invalid"])
def test_openai_sdk_retry_then_success_preserves_usage(openai_boundary, monkeypatch, operation, header):
    module, boundary = openai_boundary
    calls, waits = [], []
    response = types.SimpleNamespace(
        usage=types.SimpleNamespace(prompt_tokens=3, completion_tokens=2, total_tokens=5),
        choices=[types.SimpleNamespace(message=types.SimpleNamespace(content="answer"))],
        data=[types.SimpleNamespace(embedding=[0.1])],
    )

    def create(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise sdk_status_error(429, header)
        return response

    endpoint = types.SimpleNamespace(create=create)
    boundary.client = types.SimpleNamespace(
        embeddings=endpoint, chat=types.SimpleNamespace(completions=endpoint),
    )
    monkeypatch.setattr(module.time, "sleep", waits.append)
    result = getattr(boundary, operation)("input")
    assert result == ("answer" if operation == "get_completion" else [0.1])
    assert waits == ([2.5] if header == "2.5" else [1])
    assert boundary._retry_count == 1
    assert boundary._retry_wait_total_sec == sum(waits)
    if operation == "get_completion":
        assert (boundary._completion_calls, boundary._completion_input_tokens, boundary._completion_output_tokens) == (1, 3, 2)
    else:
        assert (boundary._embedding_calls, boundary._embedding_tokens_total) == (1, 5)


@pytest.mark.parametrize(("message", "expected"), [
    ("retry after 3.5 seconds", 3.5), ("no retry hint", None),
])
def test_openai_retry_message_control(openai_boundary, message, expected):
    _, boundary = openai_boundary
    assert boundary._extract_retry_after_seconds(Exception(message)) == expected
