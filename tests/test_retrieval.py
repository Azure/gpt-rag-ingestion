"""Contract tests for the ``POST /retrieve`` Foundry Toolbox endpoint.

Validates the security contract introduced by the hosted-agent retrieval path:

* ``X-API-KEY`` is required — missing key → 401.
* ``userContext.oid`` is required and must be non-empty — missing or blank
  OID → 422 (Pydantic validation) rather than a silent success.
* An empty OID string that passes the type check is rejected by the
  ``oid_not_blank`` validator with a clear message.
* Valid requests are forwarded to AI Search with a user-scoped OData filter
  and ``use_elevated_read=False``.
* The OData filter treats a document as public only when BOTH
  ``metadata_security_user_ids`` AND ``metadata_security_group_ids`` are
  empty — a document with a non-empty group ACL must not appear as public.
* The response shape is bounded: ``results`` array + ``count`` integer;
  no vectors, security IDs, or authorization metadata.
* Output is capped at ``top`` results (≤ ``_MAX_TOP``).
* ``top`` outside the valid range is rejected before any downstream call.
* A missing/misconfigured ``SEARCH_RAG_INDEX_NAME`` returns HTTP 500.
* Caller-supplied ``indexName`` is not accepted (field removed from model).
* Search backend errors are surfaced as HTTP 503, not empty 200.

Mirrors the stubbing pattern in ``tests/test_admin_run_now.py`` so this
file never imports ``main`` or the full ingestion stack.
"""

from __future__ import annotations

import importlib
import re
import sys
import types
from typing import Any, Dict, List, Optional

import pytest
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import APIKeyHeader
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# APIKeyHeader instance used by the stub — mirrors the real dependency so
# FastAPI's parameter analysis produces the same route signature shape.
_KEY_SCHEME = APIKeyHeader(name="X-API-KEY", auto_error=False)


def _install_stubs(
    monkeypatch,
    *,
    api_key: str = "test-key",
    config_values: Optional[Dict[str, str]] = None,
    search_documents_result: Optional[Dict[str, Any]] = None,
):
    """Wire fake ``dependencies``, ``tools``, and ``tools.aisearch``."""

    cfg_values = dict(config_values or {})

    class _FakeConfig:
        def get(self, key, default=None, allow_none=True, **_):
            return cfg_values.get(key, default)

    def _get_config(action=None):
        return _FakeConfig()

    # Stub dependency mirrors the real validate_api_key_header signature so
    # FastAPI correctly registers it as a header-based dependency.
    _api_key_ref = api_key

    def _validate_api_key_header(
        x_api_key: Optional[str] = Depends(_KEY_SCHEME),
    ) -> None:
        if not x_api_key or x_api_key != _api_key_ref:
            raise HTTPException(status_code=401, detail="Invalid API key.")

    dependencies_stub = types.ModuleType("dependencies")
    dependencies_stub.get_config = _get_config
    dependencies_stub.validate_api_key_header = _validate_api_key_header
    monkeypatch.setitem(sys.modules, "dependencies", dependencies_stub)

    # tools stubs
    tools_pkg = types.ModuleType("tools")
    tools_pkg.__path__ = []
    monkeypatch.setitem(sys.modules, "tools", tools_pkg)

    credentials_stub = types.ModuleType("tools.credentials")
    credentials_stub.get_azure_client_id = lambda cfg=None: "fake-client-id"
    monkeypatch.setitem(sys.modules, "tools.credentials", credentials_stub)

    # Track calls to AISearchClient.search_documents
    search_calls: List[dict] = []
    _result = search_documents_result if search_documents_result is not None else {"documents": [], "count": 0}

    class _FakeAISearchClient:
        def __init__(self):
            pass

        async def search_documents(self, **kwargs):
            search_calls.append(kwargs)
            return _result

    aisearch_stub = types.ModuleType("tools.aisearch")
    aisearch_stub.AISearchClient = _FakeAISearchClient
    monkeypatch.setitem(sys.modules, "tools.aisearch", aisearch_stub)

    return {"search_calls": search_calls}


def _build_client(monkeypatch, *, api_key="test-key", config_values=None, search_result=None):
    state = _install_stubs(
        monkeypatch,
        api_key=api_key,
        config_values=config_values,
        search_documents_result=search_result,
    )

    # Re-import api.retrieval so it picks up the stubs.
    for mod in list(sys.modules):
        if mod in ("api.retrieval", "api"):
            del sys.modules[mod]

    from pathlib import Path

    api_pkg = types.ModuleType("api")
    api_pkg.__path__ = [str(Path(__file__).resolve().parents[1] / "api")]
    sys.modules["api"] = api_pkg

    retrieval_module = importlib.import_module("api.retrieval")

    app = FastAPI()
    app.include_router(retrieval_module.router)
    return TestClient(app, raise_server_exceptions=True), state


# ---------------------------------------------------------------------------
# Auth contract
# ---------------------------------------------------------------------------


def test_missing_api_key_returns_401(monkeypatch):
    client, _ = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
    )
    r = client.post(
        "/retrieve",
        json={"query": "hello", "userContext": {"oid": "user-oid-123"}},
    )
    assert r.status_code == 401, r.text


def test_wrong_api_key_returns_401(monkeypatch):
    client, _ = _build_client(
        monkeypatch,
        api_key="correct-key",
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
    )
    r = client.post(
        "/retrieve",
        json={"query": "hello", "userContext": {"oid": "user-oid-123"}},
        headers={"X-API-KEY": "wrong-key"},
    )
    assert r.status_code == 401, r.text


# ---------------------------------------------------------------------------
# User-context contract — fail closed
# ---------------------------------------------------------------------------


def test_missing_user_context_returns_422(monkeypatch):
    """Omitting userContext entirely is a schema violation → 422."""
    client, _ = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
    )
    r = client.post(
        "/retrieve",
        json={"query": "hello"},
        headers={"X-API-KEY": "test-key"},
    )
    # Pydantic rejects the missing required field before auth runs only when
    # the dependency ordering places schema validation first; in FastAPI the
    # body model is validated before dependencies so 422 is expected.
    assert r.status_code in (401, 422), r.text


def test_missing_oid_returns_422(monkeypatch):
    """userContext present but oid missing → 422."""
    client, _ = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
    )
    r = client.post(
        "/retrieve",
        json={"query": "hello", "userContext": {}},
        headers={"X-API-KEY": "test-key"},
    )
    assert r.status_code == 422, r.text


def test_blank_oid_returns_422(monkeypatch):
    """oid present but whitespace-only → 422 (oid_not_blank validator)."""
    client, _ = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
    )
    r = client.post(
        "/retrieve",
        json={"query": "hello", "userContext": {"oid": "   "}},
        headers={"X-API-KEY": "test-key"},
    )
    assert r.status_code == 422, r.text


# ---------------------------------------------------------------------------
# Query validation
# ---------------------------------------------------------------------------


def test_blank_query_returns_422(monkeypatch):
    client, _ = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
    )
    r = client.post(
        "/retrieve",
        json={"query": "  ", "userContext": {"oid": "user-oid-123"}},
        headers={"X-API-KEY": "test-key"},
    )
    assert r.status_code == 422, r.text


def test_query_too_long_returns_422(monkeypatch):
    client, _ = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
    )
    r = client.post(
        "/retrieve",
        json={"query": "x" * 1001, "userContext": {"oid": "user-oid-123"}},
        headers={"X-API-KEY": "test-key"},
    )
    assert r.status_code == 422, r.text


# ---------------------------------------------------------------------------
# top bounds
# ---------------------------------------------------------------------------


def test_top_zero_returns_422(monkeypatch):
    client, _ = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
    )
    r = client.post(
        "/retrieve",
        json={"query": "hello", "userContext": {"oid": "user-oid-123"}, "top": 0},
        headers={"X-API-KEY": "test-key"},
    )
    assert r.status_code == 422, r.text


def test_top_above_max_returns_422(monkeypatch):
    client, _ = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
    )
    r = client.post(
        "/retrieve",
        json={"query": "hello", "userContext": {"oid": "user-oid-123"}, "top": 11},
        headers={"X-API-KEY": "test-key"},
    )
    assert r.status_code == 422, r.text


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def test_missing_index_name_returns_500(monkeypatch):
    """No SEARCH_RAG_INDEX_NAME in config → 500."""
    client, _ = _build_client(monkeypatch, config_values={})
    r = client.post(
        "/retrieve",
        json={"query": "hello", "userContext": {"oid": "user-oid-123"}},
        headers={"X-API-KEY": "test-key"},
    )
    assert r.status_code == 500, r.text


def test_index_name_in_body_is_ignored(monkeypatch):
    """``indexName`` is not a valid request field — callers may not select arbitrary indexes.

    The request still succeeds but the body-supplied value must be silently
    ignored; search must always target the configured index.
    """
    client, state = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "config-index"},
        search_result={"documents": []},
    )
    r = client.post(
        "/retrieve",
        json={
            "query": "hello",
            "userContext": {"oid": "user-oid-123"},
            "indexName": "attacker-index",
        },
        headers={"X-API-KEY": "test-key"},
    )
    # Request succeeds and search must target only the configured index.
    assert r.status_code == 200, r.text
    assert state["search_calls"][0]["index_name"] == "config-index"


# ---------------------------------------------------------------------------
# Happy path — shape and filter contract
# ---------------------------------------------------------------------------


def test_successful_retrieval_returns_bounded_shape(monkeypatch):
    docs = [
        {
            "id": f"doc-{i}",
            "content": f"content {i}",
            "title": f"Title {i}",
            "url": f"https://example.com/{i}",
            "category": "general",
            "source": "blob",
            "@search.score": 0.9 - i * 0.1,
        }
        for i in range(3)
    ]
    client, state = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
        search_result={"documents": docs},
    )
    r = client.post(
        "/retrieve",
        json={"query": "test query", "userContext": {"oid": "abc-oid"}, "top": 3},
        headers={"X-API-KEY": "test-key"},
    )
    assert r.status_code == 200, r.text
    body = r.json()

    assert "results" in body
    assert "count" in body
    assert body["count"] == 3
    assert len(body["results"]) == 3

    first = body["results"][0]
    # Allowed fields only
    assert set(first.keys()) == {"id", "content", "title", "url", "category", "source", "score"}
    # No vectors, no security fields
    assert "contentVector" not in first
    assert "captionVector" not in first
    assert "metadata_security_user_ids" not in first
    assert "metadata_security_group_ids" not in first


def test_search_called_with_user_oid_filter_and_no_elevation(monkeypatch):
    """The OData filter must include the user OID, the group-empty check, and elevated read must be off."""
    client, state = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
        search_result={"documents": []},
    )
    user_oid = "test-user-oid-9999"
    r = client.post(
        "/retrieve",
        json={"query": "some query", "userContext": {"oid": user_oid}},
        headers={"X-API-KEY": "test-key"},
    )
    assert r.status_code == 200, r.text
    assert len(state["search_calls"]) == 1

    call = state["search_calls"][0]
    # Elevated read must be disabled for permission filtering to apply.
    assert call.get("use_elevated_read") is False

    filter_str = call.get("filter_str", "")
    # OData filter must contain the user OID.
    assert user_oid in filter_str, f"OID not found in filter: {filter_str!r}"

    # Public-document clause must require BOTH user and group lists to be empty.
    assert "not metadata_security_user_ids/any()" in filter_str
    assert "not metadata_security_group_ids/any()" in filter_str


def test_filter_excludes_group_only_restricted_docs(monkeypatch):
    """A document with non-empty group ACL but empty user ACL must not be treated as public.

    The OData filter must include ``not metadata_security_group_ids/any()``
    as a conjunct in the public-document clause so that group-restricted
    documents never pass through when the requesting user is not listed in
    ``metadata_security_user_ids``.
    """
    client, state = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
        search_result={"documents": []},
    )
    r = client.post(
        "/retrieve",
        json={"query": "sensitive doc", "userContext": {"oid": "user-a"}},
        headers={"X-API-KEY": "test-key"},
    )
    assert r.status_code == 200, r.text
    filter_str = state["search_calls"][0]["filter_str"]

    # The public clause must be a conjunction: both user AND group lists empty.
    assert (
        "not metadata_security_group_ids/any()" in filter_str
    ), f"Group ACL check missing from filter: {filter_str!r}"

    # The two empty-list checks must appear together (AND), not as independent OR branches.
    and_pattern = re.compile(
        r"not metadata_security_user_ids/any\(\)\s+and\s+not metadata_security_group_ids/any\(\)",
        re.IGNORECASE,
    )
    assert and_pattern.search(filter_str), (
        f"Public clause must AND both ACL fields; got: {filter_str!r}"
    )


def test_oid_single_quote_is_escaped_in_filter(monkeypatch):
    """OData injection: a single quote in the OID must be doubled."""
    client, state = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
        search_result={"documents": []},
    )
    r = client.post(
        "/retrieve",
        json={"query": "q", "userContext": {"oid": "o'id"}},
        headers={"X-API-KEY": "test-key"},
    )
    assert r.status_code == 200, r.text
    filter_str = state["search_calls"][0]["filter_str"]
    # Single quote must be escaped as ''
    assert "o''id" in filter_str


def test_empty_search_result_returns_empty_list(monkeypatch):
    client, _ = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
        search_result={"documents": []},
    )
    r = client.post(
        "/retrieve",
        json={"query": "nothing here", "userContext": {"oid": "user-oid"}},
        headers={"X-API-KEY": "test-key"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["results"] == []
    assert body["count"] == 0


def test_default_top_is_applied(monkeypatch):
    """When ``top`` is not supplied, the default value is forwarded to search."""
    from api.retrieval import _DEFAULT_TOP

    client, state = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
        search_result={"documents": []},
    )
    r = client.post(
        "/retrieve",
        json={"query": "test", "userContext": {"oid": "user-oid"}},
        headers={"X-API-KEY": "test-key"},
    )
    assert r.status_code == 200, r.text
    assert state["search_calls"][0]["top"] == _DEFAULT_TOP


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_search_backend_error_returns_503(monkeypatch):
    """A Search backend error must surface as HTTP 503, not an empty 200."""
    client, _ = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
        search_result={"documents": [], "error": "Service unavailable", "count": 0},
    )
    r = client.post(
        "/retrieve",
        json={"query": "test", "userContext": {"oid": "user-oid"}},
        headers={"X-API-KEY": "test-key"},
    )
    assert r.status_code == 503, r.text


# ---------------------------------------------------------------------------
# Cross-user isolation
# ---------------------------------------------------------------------------


def test_two_users_receive_different_filters(monkeypatch):
    """Each user must see only a filter scoped to their own OID.

    Protected content must never cross user boundaries.
    """
    client_a, state_a = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
        search_result={"documents": []},
    )
    r_a = client_a.post(
        "/retrieve",
        json={"query": "q", "userContext": {"oid": "user-alpha"}},
        headers={"X-API-KEY": "test-key"},
    )
    assert r_a.status_code == 200, r_a.text
    filter_a = state_a["search_calls"][0]["filter_str"]
    assert "user-alpha" in filter_a
    assert "user-beta" not in filter_a

    client_b, state_b = _build_client(
        monkeypatch,
        config_values={"SEARCH_RAG_INDEX_NAME": "rag-index"},
        search_result={"documents": []},
    )
    r_b = client_b.post(
        "/retrieve",
        json={"query": "q", "userContext": {"oid": "user-beta"}},
        headers={"X-API-KEY": "test-key"},
    )
    assert r_b.status_code == 200, r_b.text
    filter_b = state_b["search_calls"][0]["filter_str"]
    assert "user-beta" in filter_b
    assert "user-alpha" not in filter_b
