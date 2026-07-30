"""Security contract tests for the fail-closed hosted retrieval endpoint."""

from __future__ import annotations

import importlib
import importlib.util
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

import dependencies as real_dependencies


@pytest.mark.asyncio
async def test_delegated_user_bearer_requires_oid(monkeypatch):
    async def _claims(_request, expected_audiences=None):
        assert expected_audiences == ["https://search.azure.com"]
        return {"scp": "access_as_user"}

    monkeypatch.setattr(real_dependencies, "validate_bearer_jwt", _claims)
    monkeypatch.setattr(
        real_dependencies,
        "get_config",
        lambda: types.SimpleNamespace(
            get=lambda key, **_: (
                "https://search.azure.com"
                if key == "HOSTED_RETRIEVAL_TOKEN_AUDIENCE"
                else None
            )
        ),
    )
    request = Request(
        {
            "type": "http",
            "headers": [(b"authorization", b"Bearer opaque-token")],
        }
    )

    with pytest.raises(HTTPException) as exc:
        await real_dependencies.validate_delegated_user_bearer(request)

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_delegated_user_bearer_rejects_app_only_token(monkeypatch):
    async def _claims(_request, expected_audiences=None):
        assert expected_audiences == ["https://search.azure.com"]
        return {"oid": "service-principal", "roles": ["retrieve"], "idtyp": "app"}

    monkeypatch.setattr(real_dependencies, "validate_bearer_jwt", _claims)
    monkeypatch.setattr(
        real_dependencies,
        "get_config",
        lambda: types.SimpleNamespace(
            get=lambda key, **_: (
                "https://search.azure.com"
                if key == "HOSTED_RETRIEVAL_TOKEN_AUDIENCE"
                else None
            )
        ),
    )
    request = Request(
        {
            "type": "http",
            "headers": [(b"authorization", b"Bearer opaque-token")],
        }
    )

    with pytest.raises(HTTPException) as exc:
        await real_dependencies.validate_delegated_user_bearer(request)

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_delegated_user_bearer_preserves_validated_token(monkeypatch):
    async def _claims(_request, expected_audiences=None):
        assert expected_audiences == ["https://search.azure.com"]
        return {"oid": "user-a", "scp": "access_as_user", "idtyp": "user"}

    monkeypatch.setattr(real_dependencies, "validate_bearer_jwt", _claims)
    monkeypatch.setattr(
        real_dependencies,
        "get_config",
        lambda: types.SimpleNamespace(
            get=lambda key, **_: (
                "https://search.azure.com"
                if key == "HOSTED_RETRIEVAL_TOKEN_AUDIENCE"
                else None
            )
        ),
    )
    request = Request(
        {
            "type": "http",
            "headers": [(b"authorization", b"Bearer opaque-token")],
        }
    )

    bearer = await real_dependencies.validate_delegated_user_bearer(request)

    assert bearer.access_token == "opaque-token"
    assert bearer.claims["oid"] == "user-a"
    assert "opaque-token" not in repr(bearer)


@pytest.mark.asyncio
async def test_delegated_user_bearer_requires_configured_audience(monkeypatch):
    monkeypatch.setattr(
        real_dependencies,
        "get_config",
        lambda: types.SimpleNamespace(get=lambda _key, **_: None),
    )
    request = Request(
        {
            "type": "http",
            "headers": [(b"authorization", b"Bearer opaque-token")],
        }
    )

    with pytest.raises(HTTPException) as exc:
        await real_dependencies.validate_delegated_user_bearer(request)

    assert exc.value.status_code == 500
    assert "HOSTED_RETRIEVAL_TOKEN_AUDIENCE" in exc.value.detail


@dataclass(frozen=True)
class _ValidatedUserBearer:
    access_token: str
    claims: Dict[str, Any]


def _install_stubs(
    monkeypatch,
    *,
    config_values: Optional[Dict[str, str]] = None,
    token_claims: Optional[Dict[str, Dict[str, Any]]] = None,
    search_result: Optional[Dict[str, Any]] = None,
    search_behavior: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
):
    config = dict(config_values or {})
    claims_by_token = token_claims or {
        "user-a-token": {
            "oid": "user-a",
            "scp": "access_as_user",
            "idtyp": "user",
        }
    }

    class _FakeConfig:
        def get(self, key, default=None, allow_none=True, **_):
            return config.get(key, default)

    async def _validate_delegated_user_bearer(request: Request):
        authorization = request.headers.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Missing Authorization header.")
        token = authorization.removeprefix("Bearer ").strip()
        claims = claims_by_token.get(token)
        if claims is None:
            raise HTTPException(status_code=401, detail="Invalid token.")
        if not claims.get("oid") or not claims.get("scp"):
            raise HTTPException(status_code=403, detail="Delegated user token required.")
        return _ValidatedUserBearer(access_token=token, claims=claims)

    dependencies_stub = types.ModuleType("dependencies")
    dependencies_stub.ValidatedUserBearer = _ValidatedUserBearer
    dependencies_stub.get_config = lambda action=None: _FakeConfig()
    dependencies_stub.validate_delegated_user_bearer = (
        _validate_delegated_user_bearer
    )
    monkeypatch.setitem(sys.modules, "dependencies", dependencies_stub)

    tools_pkg = types.ModuleType("tools")
    tools_pkg.__path__ = []
    monkeypatch.setitem(sys.modules, "tools", tools_pkg)

    search_calls = []
    close_calls = []
    default_result = (
        search_result
        if search_result is not None
        else {"documents": [], "count": 0}
    )

    class _FakeAISearchClient:
        async def search_documents(self, **kwargs):
            search_calls.append(kwargs)
            if search_behavior is not None:
                return search_behavior(kwargs)
            return default_result

        async def close(self):
            close_calls.append(True)

    aisearch_stub = types.ModuleType("tools.aisearch")
    aisearch_stub.AISearchClient = _FakeAISearchClient
    monkeypatch.setitem(sys.modules, "tools.aisearch", aisearch_stub)
    return {"search_calls": search_calls, "close_calls": close_calls}


def _build_client(
    monkeypatch,
    *,
    enabled: bool = True,
    inv_002_validated: bool = True,
    config_values: Optional[Dict[str, str]] = None,
    token_claims: Optional[Dict[str, Dict[str, Any]]] = None,
    search_result: Optional[Dict[str, Any]] = None,
    search_behavior: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
):
    config = {
        "SEARCH_RAG_INDEX_NAME": "rag-index",
        "HOSTED_RETRIEVAL_TOKEN_AUDIENCE": "https://search.azure.com",
        "HOSTED_RETRIEVAL_ENABLED": str(enabled).lower(),
        "HOSTED_RETRIEVAL_INV_002_VALIDATED": str(inv_002_validated).lower(),
    }
    if config_values:
        config.update(config_values)

    state = _install_stubs(
        monkeypatch,
        config_values=config,
        token_claims=token_claims,
        search_result=search_result,
        search_behavior=search_behavior,
    )

    sys.modules.pop("api.retrieval", None)
    api_pkg = types.ModuleType("api")
    api_pkg.__path__ = [str(Path(__file__).resolve().parents[1] / "api")]
    monkeypatch.setitem(sys.modules, "api", api_pkg)
    retrieval = importlib.import_module("api.retrieval")

    app = FastAPI()
    app.include_router(retrieval.router)
    return TestClient(app, raise_server_exceptions=True), state


def _post(
    client: TestClient,
    body: Optional[Dict[str, Any]] = None,
    token: Optional[str] = "user-a-token",
):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return client.post(
        "/retrieve",
        json=body or {"query": "hello"},
        headers=headers,
    )


def test_missing_identity_fails_closed(monkeypatch):
    client, state = _build_client(monkeypatch)

    response = _post(client, token=None)

    assert response.status_code == 401
    assert state["search_calls"] == []


def test_openapi_contract_exposes_bearer_schema_and_statuses(monkeypatch):
    client, _ = _build_client(monkeypatch)

    schema = client.get("/openapi.json").json()
    operation = schema["paths"]["/retrieve"]["post"]
    request_schema = schema["components"]["schemas"]["RetrieveRequest"]

    assert set(request_schema["properties"]) == {"query", "top"}
    assert operation["security"] == [{"HTTPBearer": []}]
    assert set(operation["responses"]) >= {
        "200",
        "401",
        "403",
        "422",
        "500",
        "502",
        "503",
    }
    assert schema["components"]["securitySchemes"]["HTTPBearer"] == {
        "type": "http",
        "description": (
            "Delegated UserEntraToken validated and forwarded to Azure AI Search."
        ),
        "scheme": "bearer",
    }


def test_disabled_mode_does_not_require_hosted_identity_configuration(monkeypatch):
    client, state = _build_client(
        monkeypatch,
        enabled=False,
        inv_002_validated=False,
        config_values={"HOSTED_RETRIEVAL_TOKEN_AUDIENCE": ""},
    )

    response = _post(client, token=None)

    assert response.status_code == 503
    assert state["search_calls"] == []


def test_spoofed_user_context_is_rejected(monkeypatch):
    client, state = _build_client(monkeypatch)

    response = _post(
        client,
        {"query": "hello", "userContext": {"oid": "victim-user"}},
    )

    assert response.status_code == 422
    assert state["search_calls"] == []


def test_spoofed_group_context_is_rejected(monkeypatch):
    client, state = _build_client(monkeypatch)

    response = _post(
        client,
        {"query": "hello", "groupIds": ["victim-group"]},
    )

    assert response.status_code == 422
    assert state["search_calls"] == []


def test_caller_selected_index_is_rejected(monkeypatch):
    client, state = _build_client(monkeypatch)

    response = _post(
        client,
        {"query": "hello", "indexName": "other-index"},
    )

    assert response.status_code == 422
    assert state["search_calls"] == []


@pytest.mark.parametrize(
    ("enabled", "validated"),
    [(False, False), (True, False), (False, True)],
)
def test_native_retrieval_gate_defaults_fail_closed(
    monkeypatch, enabled, validated
):
    client, state = _build_client(
        monkeypatch,
        enabled=enabled,
        inv_002_validated=validated,
    )

    response = _post(client)

    assert response.status_code == 503
    assert "INV-002" in response.json()["detail"]
    assert state["search_calls"] == []


def test_missing_server_owned_index_is_explicit_failure(monkeypatch):
    client, state = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": ""},
    )

    response = _post(client)

    assert response.status_code == 500
    assert state["search_calls"] == []


def test_native_search_receives_only_validated_bearer(monkeypatch):
    client, state = _build_client(monkeypatch)

    response = _post(client, {"query": "  hello  ", "top": 3})

    assert response.status_code == 200
    assert len(state["search_calls"]) == 1
    call = state["search_calls"][0]
    assert call["index_name"] == "rag-index"
    assert call["search_text"] == "hello"
    assert call["top"] == 3
    assert call["filter_str"] is None
    assert call["use_elevated_read"] is False
    assert call["query_source_authorization"] == "user-a-token"
    assert state["close_calls"] == [True]


def test_search_failure_is_not_success_shaped(monkeypatch):
    client, state = _build_client(
        monkeypatch,
        search_result={
            "documents": [],
            "count": 0,
            "error": "sensitive downstream detail",
        },
    )

    response = _post(client)

    assert response.status_code == 502
    assert response.json()["detail"] == "Azure AI Search query failed."
    assert "sensitive downstream detail" not in response.text
    assert state["close_calls"] == [True]


def test_search_exception_is_sanitized_and_client_is_closed(monkeypatch):
    def _raise(_kwargs):
        raise RuntimeError("secret-bearing downstream failure")

    client, state = _build_client(
        monkeypatch,
        search_behavior=_raise,
    )

    response = _post(client)

    assert response.status_code == 502
    assert response.json()["detail"] == "Azure AI Search query failed."
    assert "secret-bearing downstream failure" not in response.text
    assert state["close_calls"] == [True]


@pytest.mark.asyncio
async def test_search_client_builds_native_authorization_header(monkeypatch):
    config_stub = types.SimpleNamespace(
        get=lambda key, default=None, **_: (
            "search-service" if key == "SEARCH_SERVICE_NAME" else default
        )
    )
    dependencies_stub = types.ModuleType("dependencies")
    dependencies_stub.get_config = lambda: config_stub
    monkeypatch.setitem(sys.modules, "dependencies", dependencies_stub)

    credentials_stub = types.ModuleType("tools.credentials")
    credentials_stub.get_azure_client_id = lambda _config: None
    monkeypatch.setitem(sys.modules, "tools.credentials", credentials_stub)

    telemetry_stub = types.ModuleType("telemetry")
    telemetry_stub.audit = types.SimpleNamespace()
    monkeypatch.setitem(sys.modules, "telemetry", telemetry_stub)

    module_path = Path(__file__).resolve().parents[1] / "tools" / "aisearch.py"
    spec = importlib.util.spec_from_file_location(
        "aisearch_native_header_under_test",
        module_path,
    )
    assert spec and spec.loader
    aisearch = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(aisearch)

    captured = {}

    class _Results:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

    class _SearchClient:
        async def search(self, **kwargs):
            captured.update(kwargs)
            return _Results()

    instance = object.__new__(aisearch.AISearchClient)

    async def _get_search_client(_index_name):
        return _SearchClient()

    instance.get_search_client = _get_search_client

    result = await instance.search_documents(
        index_name="rag-index",
        search_text="hello",
        use_elevated_read=False,
        query_source_authorization="validated-token",
    )

    assert result == {"count": 0, "documents": []}
    assert captured["headers"] == {
        "x-ms-query-source-authorization": "Bearer validated-token"
    }
    assert "x-ms-enable-elevated-read" not in captured["headers"]
    assert captured["filter"] is None


def test_group_only_document_does_not_cross_to_nonmember(monkeypatch):
    documents_by_token = {
        "group-a-token": [
            {"id": "group-a-doc", "content": "allowed for group A"}
        ],
        "group-b-token": [],
    }

    def _native_trim(kwargs):
        token = kwargs["query_source_authorization"]
        docs = documents_by_token[token]
        return {"documents": docs, "count": len(docs)}

    claims = {
        "group-a-token": {
            "oid": "user-a",
            "scp": "access_as_user",
            "idtyp": "user",
        },
        "group-b-token": {
            "oid": "user-b",
            "scp": "access_as_user",
            "idtyp": "user",
        },
    }
    client, state = _build_client(
        monkeypatch,
        token_claims=claims,
        search_behavior=_native_trim,
    )

    allowed = _post(client, token="group-a-token")
    denied = _post(client, token="group-b-token")

    assert [item["id"] for item in allowed.json()["results"]] == ["group-a-doc"]
    assert denied.json() == {"results": [], "count": 0}
    assert [
        call["query_source_authorization"] for call in state["search_calls"]
    ] == ["group-a-token", "group-b-token"]


def test_user_only_document_does_not_cross_to_other_user(monkeypatch):
    documents_by_token = {
        "user-a-token": [{"id": "user-a-doc", "content": "private A"}],
        "user-b-token": [],
    }

    def _native_trim(kwargs):
        docs = documents_by_token[kwargs["query_source_authorization"]]
        return {"documents": docs, "count": len(docs)}

    claims = {
        "user-a-token": {
            "oid": "user-a",
            "scp": "access_as_user",
            "idtyp": "user",
        },
        "user-b-token": {
            "oid": "user-b",
            "scp": "access_as_user",
            "idtyp": "user",
        },
    }
    client, _ = _build_client(
        monkeypatch,
        token_claims=claims,
        search_behavior=_native_trim,
    )

    allowed = _post(client, token="user-a-token")
    denied = _post(client, token="user-b-token")

    assert [item["id"] for item in allowed.json()["results"]] == ["user-a-doc"]
    assert denied.json() == {"results": [], "count": 0}


def test_response_fields_and_strings_are_bounded(monkeypatch):
    oversized = {
        "id": "i" * 300,
        "content": "c" * 9_000,
        "title": "t" * 700,
        "url": "u" * 3_000,
        "category": "g" * 300,
        "source": "s" * 300,
        "contentVector": [1.0],
        "metadata_security_user_ids": ["secret"],
        "metadata_security_group_ids": ["secret-group"],
        "@search.score": 0.75,
    }
    client, _ = _build_client(
        monkeypatch,
        search_result={"documents": [oversized], "count": 1},
    )

    response = _post(client)

    assert response.status_code == 200
    result = response.json()["results"][0]
    assert set(result) == {
        "id",
        "content",
        "title",
        "url",
        "category",
        "source",
        "score",
    }
    assert len(result["id"]) == 256
    assert len(result["content"]) == 8_000
    assert len(result["title"]) == 512
    assert len(result["url"]) == 2_048
    assert len(result["category"]) == 256
    assert len(result["source"]) == 256


@pytest.mark.parametrize(
    "body",
    [
        {"query": ""},
        {"query": "   "},
        {"query": "x" * 1_001},
        {"query": "hello", "top": 0},
        {"query": "hello", "top": 11},
    ],
)
def test_request_limits_are_enforced_before_search(monkeypatch, body):
    client, state = _build_client(monkeypatch)

    response = _post(client, body)

    assert response.status_code == 422
    assert state["search_calls"] == []
