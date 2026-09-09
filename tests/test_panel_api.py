"""Tests for `api/panel.py` — the ADR-0001 hosted/panel administrative API
(Azure/GPT-RAG#592).

Mirrors the stubbing pattern used by `tests/test_admin_run_now.py` /
`tests/test_admin_config.py`: fake `dependencies`, `tools.cosmosdb`, and
`main` modules are installed via `monkeypatch.setitem(sys.modules, ...)` so
`api.panel` (and, transitively via `api.admin`, the overview aggregation)
can be imported and exercised on a bare `FastAPI()` app without importing
the real `main.py` or touching any Azure SDK.

Covers:
* `GET /api/panel/status` — open, reflects mode/auth without requiring a
  token; 404 when the resolved mode does not have the panel enabled.
* `require_panel_admin` — 500 (hard fail, no dev bypass) without an Entra
  tenant, 403 without the ``Admin`` role, success with it.
* `GET/POST /api/panel/feedback` — Cosmos-backed contract, bounded/strict
  request validation, 502 on Cosmos failure.
* `GET /api/panel/overview` — retains jobs/files on feedback failure, but
  returns zero or partial feedback counts without an availability signal.
  These characterization tests do not approve that unresolved contract.
* `GET /api/panel/conversations/{id}/history` — explicit 501 blocker,
  still gated by admin auth.
"""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

REPO_ROOT = Path(__file__).resolve().parents[1]


class _FakeConfig:
    """Minimal App Configuration stand-in used by `dependencies.get_config()`."""

    def __init__(self, values: dict | None = None) -> None:
        self._values = values or {}

    def get(self, key, default=None, allow_none=True):
        return self._values.get(key, default)


class _FakeCosmosDBClient:
    """Stand-in for `tools.cosmosdb.CosmosDBClient` used by the panel router."""

    list_error: Exception | None = None
    create_returns_none: bool = False
    create_error: Exception | None = None
    documents: list[dict] = []
    created_calls: list[tuple] = []

    def __init__(self, *args, **kwargs) -> None:
        pass

    async def list_documents(self, container_name):
        if _FakeCosmosDBClient.list_error is not None:
            raise _FakeCosmosDBClient.list_error
        return _FakeCosmosDBClient.documents

    async def create_document(self, container_name, key, body):
        _FakeCosmosDBClient.created_calls.append((container_name, key, body))
        if _FakeCosmosDBClient.create_error is not None:
            raise _FakeCosmosDBClient.create_error
        if _FakeCosmosDBClient.create_returns_none:
            return None
        return body


def _install_stubs(
    monkeypatch,
    *,
    tenant_id: str | None,
    claims: dict | Exception | None,
    config_values: dict | None = None,
    available_jobs: list[str] | None = None,
    running_jobs: list[str] | None = None,
):
    """Wire fake `dependencies`, `tools.cosmosdb`, and `main` modules."""

    cfg_values = dict(config_values or {})
    cfg_values.setdefault("OAUTH_AZURE_AD_TENANT_ID", tenant_id)

    async def _validate_bearer_jwt(_request, expected_audiences=None):
        if isinstance(claims, Exception):
            raise claims
        if claims is None:
            raise HTTPException(status_code=401, detail="Missing token")
        return claims

    dependencies_stub = types.ModuleType("dependencies")
    dependencies_stub.get_config = lambda: _FakeConfig(cfg_values)
    dependencies_stub.validate_bearer_jwt = _validate_bearer_jwt
    monkeypatch.setitem(sys.modules, "dependencies", dependencies_stub)

    tools_pkg = types.ModuleType("tools")
    tools_pkg.__path__ = [str(REPO_ROOT / "tools")]
    cosmosdb_stub = types.ModuleType("tools.cosmosdb")
    cosmosdb_stub.CosmosDBClient = _FakeCosmosDBClient
    monkeypatch.setitem(sys.modules, "tools", tools_pkg)
    monkeypatch.setitem(sys.modules, "tools.cosmosdb", cosmosdb_stub)

    # `api.admin._available_job_types` / `_running_job_types` (used by the
    # overview endpoint) read jobs.runtime state; stub the canonical owner
    # minimally, matching tests/test_admin_run_now.py's convention.
    main_stub = types.ModuleType("jobs.runtime")
    main_stub.JOB_REGISTRY = {jt: None for jt in (available_jobs or [])}
    main_stub.running_jobs = {jt: {} for jt in (running_jobs or [])}
    monkeypatch.setitem(sys.modules, "jobs.runtime", main_stub)

    # Reset the fake Cosmos client's class-level state for test isolation.
    _FakeCosmosDBClient.list_error = None
    _FakeCosmosDBClient.create_returns_none = False
    _FakeCosmosDBClient.create_error = None
    _FakeCosmosDBClient.documents = []
    _FakeCosmosDBClient.created_calls = []


def _build_client(monkeypatch, **stub_kwargs) -> TestClient:
    _install_stubs(monkeypatch, **stub_kwargs)

    if "api" not in sys.modules:
        api_pkg = types.ModuleType("api")
        api_pkg.__path__ = [str(REPO_ROOT / "api")]
        sys.modules["api"] = api_pkg

    for mod in ("api.panel", "api.admin"):
        sys.modules.pop(mod, None)

    panel_module = importlib.import_module("api.panel")
    app = FastAPI()
    app.include_router(panel_module.router)
    return TestClient(app)


_ADMIN_CLAIMS = {"oid": "user-123", "roles": ["Admin"]}
_NON_ADMIN_CLAIMS = {"oid": "user-456", "roles": ["Reader"]}


# ---------------------------------------------------------------------------
# GET /api/panel/status
# ---------------------------------------------------------------------------


def test_status_open_reports_mode_and_auth_enabled(monkeypatch):
    client = _build_client(
        monkeypatch,
        tenant_id="tenant-1",
        claims=_ADMIN_CLAIMS,
        config_values={
            "DEPLOY_HOSTED_AGENT_ORCHESTRATION": "true",
            "DEPLOY_ADMINISTRATIVE_PANEL": "true",
        },
    )
    r = client.get("/api/panel/status")
    assert r.status_code == 200
    body = r.json()
    assert body == {"mode": "hosted_panel", "panelEnabled": True, "authEnabled": True}


def test_status_reports_auth_disabled_without_tenant(monkeypatch):
    client = _build_client(
        monkeypatch,
        tenant_id=None,
        claims=None,
        config_values={
            "DEPLOY_HOSTED_AGENT_ORCHESTRATION": "true",
            "DEPLOY_ADMINISTRATIVE_PANEL": "true",
        },
    )
    r = client.get("/api/panel/status")
    assert r.status_code == 200
    assert r.json()["authEnabled"] is False


def test_status_404_when_mode_no_longer_panel(monkeypatch):
    """Even if the router is mounted, `/status` must self-check the *current*
    resolved mode and hide itself (404) rather than trust stale mount state."""
    client = _build_client(
        monkeypatch,
        tenant_id="tenant-1",
        claims=_ADMIN_CLAIMS,
        config_values={
            "DEPLOY_HOSTED_AGENT_ORCHESTRATION": "true",
            "DEPLOY_ADMINISTRATIVE_PANEL": "false",
        },
    )
    r = client.get("/api/panel/status")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# require_panel_admin — auth gate
# ---------------------------------------------------------------------------


def test_feedback_get_returns_500_without_entra_tenant(monkeypatch):
    """No development-mode auth bypass: panel routes hard-fail (500) rather
    than silently opening up, unlike `api.admin.require_admin`."""
    client = _build_client(monkeypatch, tenant_id=None, claims=None)
    r = client.get("/api/panel/feedback")
    assert r.status_code == 500
    assert "OAUTH_AZURE_AD_TENANT_ID" in r.json()["detail"]


def test_feedback_get_returns_403_without_admin_role(monkeypatch):
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_NON_ADMIN_CLAIMS)
    r = client.get("/api/panel/feedback")
    assert r.status_code == 403


def test_feedback_get_returns_401_without_token(monkeypatch):
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=None)
    r = client.get("/api/panel/feedback")
    assert r.status_code == 401


# ---------------------------------------------------------------------------
# GET /api/panel/feedback
# ---------------------------------------------------------------------------


def test_feedback_list_success_and_filters_by_conversation_id(monkeypatch):
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_ADMIN_CLAIMS)
    _FakeCosmosDBClient.documents = [
        {
            "id": "f1",
            "conversationId": "conv-a",
            "messageId": "m1",
            "rating": "up",
            "comment": None,
            "tags": [],
            "createdBy": "u1",
            "createdAt": "2026-01-01T00:00:00.000000Z",
        },
        {
            "id": "f2",
            "conversationId": "conv-b",
            "rating": "down",
            "createdAt": "2026-01-01T00:00:00.000000Z",
        },
    ]

    r = client.get("/api/panel/feedback")
    assert r.status_code == 200
    assert {rec["id"] for rec in r.json()} == {"f1", "f2"}

    r = client.get("/api/panel/feedback", params={"conversationId": "conv-a"})
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 1
    assert body[0]["id"] == "f1"


def test_feedback_list_returns_502_on_cosmos_failure(monkeypatch):
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_ADMIN_CLAIMS)
    _FakeCosmosDBClient.list_error = RuntimeError("cosmos down")
    r = client.get("/api/panel/feedback")
    assert r.status_code == 502


def test_feedback_list_returns_502_on_malformed_document(monkeypatch, caplog):
    """A stored document with `"rating"` present but an invalid value (or
    missing a required field) must not raise an unhandled ValidationError
    (which would 500) and must not be silently dropped from the response
    either. It must surface as an explicit, sanitized 502 data-integrity
    error, and the document content must never be logged."""
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_ADMIN_CLAIMS)
    _FakeCosmosDBClient.documents = [
        {
            "id": "f1",
            "conversationId": "conv-a",
            "rating": "up",
            "createdAt": "2026-01-01T00:00:00.000000Z",
        },
        {
            "id": "corrupt",
            "conversationId": "conv-b",
            "rating": "sideways",  # invalid Literal value -> ValidationError
            "createdAt": "2026-01-01T00:00:00.000000Z",
            "comment": "super secret user complaint text",
        },
    ]

    with caplog.at_level("ERROR"):
        r = client.get("/api/panel/feedback")

    assert r.status_code == 502
    body = r.json()
    assert "1" in body["detail"]
    assert "data integrity" in body["detail"].lower()
    # Never leak document content (comment/conversationId) in the response
    # or in logs — only a count is surfaced.
    assert "super secret user complaint text" not in r.text
    assert "super secret user complaint text" not in caplog.text
    assert "conv-b" not in r.text


def test_feedback_list_returns_502_on_malformed_document_missing_required_field(monkeypatch):
    """Missing a required field (`createdAt`) is also a ValidationError from
    `FeedbackRecord(**doc)` and must be handled the same way as an invalid
    rating value — explicit 502, not an unhandled 500."""
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_ADMIN_CLAIMS)
    _FakeCosmosDBClient.documents = [
        {"id": "no-created-at", "conversationId": "conv-c", "rating": "down"},
    ]
    r = client.get("/api/panel/feedback")
    assert r.status_code == 502
    assert "data integrity" in r.json()["detail"].lower()


def test_feedback_list_returns_502_on_document_missing_rating_entirely(monkeypatch):
    """A stored document that has no `"rating"` key at all must be counted
    as a data-integrity failure — the same as an invalid rating value or a
    missing `createdAt` — and must never be silently excluded from the
    count/response without surfacing a 502. (Regression: an earlier
    prefilter special-cased "rating" absence and skipped such documents
    without incrementing `invalid_count`, contradicting the
    no-silent-corruption contract.)"""
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_ADMIN_CLAIMS)
    _FakeCosmosDBClient.documents = [
        {
            "id": "good",
            "conversationId": "conv-a",
            "rating": "up",
            "createdAt": "2026-01-01T00:00:00.000000Z",
        },
        {"id": "no-rating-at-all", "conversationId": "conv-z", "createdAt": "2026-01-01T00:00:00.000000Z"},
    ]
    r = client.get("/api/panel/feedback")
    assert r.status_code == 502
    body = r.json()
    assert "1" in body["detail"]
    assert "data integrity" in body["detail"].lower()


# ---------------------------------------------------------------------------
# POST /api/panel/feedback
# ---------------------------------------------------------------------------


def test_feedback_create_success(monkeypatch):
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_ADMIN_CLAIMS)
    r = client.post(
        "/api/panel/feedback",
        json={
            "conversationId": "conv-a",
            "messageId": "m1",
            "rating": "up",
            "comment": "great answer",
            "tags": ["accurate", "fast"],
        },
    )
    assert r.status_code == 201
    body = r.json()
    assert body["conversationId"] == "conv-a"
    assert body["rating"] == "up"
    assert body["createdBy"] == "user-123"
    assert body["tags"] == ["accurate", "fast"]
    assert len(_FakeCosmosDBClient.created_calls) == 1


def test_feedback_create_rejects_unknown_field(monkeypatch):
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_ADMIN_CLAIMS)
    r = client.post(
        "/api/panel/feedback",
        json={"conversationId": "conv-a", "rating": "up", "unexpectedField": "nope"},
    )
    assert r.status_code == 422


def test_feedback_create_rejects_invalid_rating(monkeypatch):
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_ADMIN_CLAIMS)
    r = client.post(
        "/api/panel/feedback",
        json={"conversationId": "conv-a", "rating": "sideways"},
    )
    assert r.status_code == 422


def test_feedback_create_rejects_oversized_tag(monkeypatch):
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_ADMIN_CLAIMS)
    r = client.post(
        "/api/panel/feedback",
        json={"conversationId": "conv-a", "rating": "up", "tags": ["x" * 65]},
    )
    assert r.status_code == 422


def test_feedback_create_rejects_too_many_tags(monkeypatch):
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_ADMIN_CLAIMS)
    r = client.post(
        "/api/panel/feedback",
        json={"conversationId": "conv-a", "rating": "down", "tags": [f"t{i}" for i in range(17)]},
    )
    assert r.status_code == 422


def test_feedback_create_returns_502_on_cosmos_failure(monkeypatch):
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_ADMIN_CLAIMS)
    _FakeCosmosDBClient.create_returns_none = True
    r = client.post(
        "/api/panel/feedback",
        json={"conversationId": "conv-a", "rating": "up"},
    )
    assert r.status_code == 502


def test_feedback_create_returns_502_on_cosmos_exception(monkeypatch, caplog):
    """`CosmosDBClient.create_document` raising (e.g. a transient Cosmos
    outage or SDK error), not just returning `None`, must not propagate as
    an unhandled 500 — it must be caught and surfaced as the same
    documented 502 write-failure contract, without logging request body
    content (comment/tags)."""
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_ADMIN_CLAIMS)
    _FakeCosmosDBClient.create_error = RuntimeError("cosmos write exploded")
    with caplog.at_level("ERROR"):
        r = client.post(
            "/api/panel/feedback",
            json={
                "conversationId": "conv-a",
                "rating": "up",
                "comment": "super secret user complaint text",
            },
        )
    assert r.status_code == 502
    assert "super secret user complaint text" not in r.text
    assert "super secret user complaint text" not in caplog.text


def test_feedback_create_requires_admin_role(monkeypatch):
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_NON_ADMIN_CLAIMS)
    r = client.post(
        "/api/panel/feedback",
        json={"conversationId": "conv-a", "rating": "up"},
    )
    assert r.status_code == 403


# ---------------------------------------------------------------------------
# GET /api/panel/overview
# ---------------------------------------------------------------------------


def test_overview_aggregates_jobs_files_and_feedback(monkeypatch):
    client = _build_client(
        monkeypatch,
        tenant_id="tenant-1",
        claims=_ADMIN_CLAIMS,
        config_values={
            "DEPLOY_HOSTED_AGENT_ORCHESTRATION": "true",
            "DEPLOY_ADMINISTRATIVE_PANEL": "true",
        },
        available_jobs=["blob_index", "sharepoint_index"],
        running_jobs=["blob_index"],
    )
    _FakeCosmosDBClient.documents = [
        {"rating": "up"},
        {"rating": "up"},
        {"rating": "down"},
    ]

    # The overview endpoint late-imports `api.admin` on first call; import it
    # explicitly here so we can seed its cache before that happens.
    admin_module = importlib.import_module("api.admin")
    # Seed the admin cache directly so the overview endpoint never attempts a
    # real blob call (matching `_cached_load`'s cache-hit shape).
    admin_module._cache["runs"] = (
        __import__("time").monotonic(),
        ([{"runId": "r1"}, {"runId": "r2"}], ["blob"]),
    )
    admin_module._cache["files"] = (
        __import__("time").monotonic(),
        ([{"name": "f1"}], ["blob"]),
    )

    r = client.get("/api/panel/overview")
    assert r.status_code == 200
    body = r.json()
    assert body["mode"] == "hosted_panel"
    assert body["historyAvailable"] is False
    assert sorted(body["jobs"]["availableJobTypes"]) == ["blob_index", "sharepoint_index"]
    assert body["jobs"]["runningJobTypes"] == ["blob_index"]
    assert body["jobs"]["totalRuns"] == 2
    assert body["files"]["totalFiles"] == 1
    assert body["feedback"] == {"available": True, "totalRecords": 3, "upCount": 2, "downCount": 1}

    admin_module._cache.pop("runs", None)
    admin_module._cache.pop("files", None)


def test_overview_degrades_gracefully_when_cosmos_unavailable(monkeypatch):
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_ADMIN_CLAIMS)
    _FakeCosmosDBClient.list_error = RuntimeError("cosmos down")

    admin_module = importlib.import_module("api.admin")
    admin_module._cache["runs"] = (__import__("time").monotonic(), ([], []))
    admin_module._cache["files"] = (__import__("time").monotonic(), ([], []))

    r = client.get("/api/panel/overview")
    assert r.status_code == 200
    body = r.json()
    assert body["feedback"] == {"available": False, "totalRecords": 0, "upCount": 0, "downCount": 0}

    admin_module._cache.pop("runs", None)
    admin_module._cache.pop("files", None)


def test_overview_requires_admin_role(monkeypatch):
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_NON_ADMIN_CLAIMS)
    r = client.get("/api/panel/overview")
    assert r.status_code == 403


def test_overview_genuine_empty_feedback_is_available(monkeypatch):
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_ADMIN_CLAIMS)
    _FakeCosmosDBClient.documents = []
    admin = importlib.import_module("api.admin")
    for key in ("runs", "files"):
        monkeypatch.setitem(admin._cache, key, (__import__("time").monotonic(), ([], [])))
    response = client.get("/api/panel/overview")
    assert response.status_code == 200
    assert response.json()["feedback"] == {
        "available": True, "totalRecords": 0, "upCount": 0, "downCount": 0,
    }


@pytest.mark.parametrize("stage", ["constructor", "read", "partial-read", "partial-aggregation"])
def test_overview_failure_marks_feedback_unavailable(monkeypatch, caplog, stage):
    client = _build_client(
        monkeypatch, tenant_id="tenant-1", claims=_ADMIN_CLAIMS,
        config_values={
            "DEPLOY_HOSTED_AGENT_ORCHESTRATION": "true",
            "DEPLOY_ADMINISTRATIVE_PANEL": "true",
        },
        available_jobs=["blob_index"], running_jobs=["blob_index"],
    )
    # Exercise the real router AND materializing Cosmos adapter, replacing
    # only configuration and the SDK boundary. A failed SDK iteration never
    # returns its partially collected list to the overview aggregator.
    spec = importlib.util.spec_from_file_location("tools.cosmosdb", REPO_ROOT / "tools/cosmosdb.py")
    assert spec and spec.loader
    cosmos = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, cosmos)
    spec.loader.exec_module(cosmos)
    cfg = types.SimpleNamespace(
        get=lambda key: "configured",
        aiocredential=object(),
    )
    failure = RuntimeError("private-feedback-failure-canary")
    monkeypatch.setattr(cosmos, "get_config", Mock(
        return_value=cfg, side_effect=failure if stage == "constructor" else None,
    ))
    visited = []

    class BrokenRating(dict):
        def get(self, key, default=None):
            # Inject an aggregation defect after valid rows, not an SDK
            # pagination failure or a claim about ordinary JSON dictionaries.
            visited.append("aggregation-failure")
            raise failure

    async def documents():
        if stage == "read":
            visited.append("read-failure")
            raise failure
        for rating in ("up", "down"):
            visited.append(rating)
            yield {"rating": rating}
        if stage == "partial-read":
            visited.append("read-failure")
            raise failure
        yield BrokenRating()

    container = types.SimpleNamespace(query_items=Mock(side_effect=lambda **kwargs: documents()))
    database = types.SimpleNamespace(get_container_client=Mock(return_value=container))
    sdk = AsyncMock()
    sdk.__aenter__.return_value = types.SimpleNamespace(
        get_database_client=Mock(return_value=database),
    )
    monkeypatch.setattr(cosmos, "CosmosClient", Mock(return_value=sdk))
    admin = importlib.import_module("api.admin")
    monkeypatch.setitem(admin._cache, "runs", (
        __import__("time").monotonic(), ([{"runId": "r1"}, {"runId": "r2"}], ["blob"]),
    ))
    monkeypatch.setitem(admin._cache, "files", (
        __import__("time").monotonic(), ([{"name": "f1"}], ["blob"]),
    ))

    response = client.get("/api/panel/overview")
    assert response.status_code == 200
    assert response.json() == {
        "mode": "hosted_panel",
        "jobs": {"availableJobTypes": ["blob_index"], "runningJobTypes": ["blob_index"], "totalRuns": 2},
        "files": {"totalFiles": 1},
        "feedback": {"available": False, "totalRecords": 0, "upCount": 0, "downCount": 0},
        "historyAvailable": False,
    }
    assert visited == {
        "constructor": [],
        "read": ["read-failure"],
        "partial-read": ["up", "down", "read-failure"],
        "partial-aggregation": ["up", "down", "aggregation-failure"],
    }[stage]
    if stage == "constructor":
        cosmos.CosmosClient.assert_not_called()
    else:
        container.query_items.assert_called_once_with(query="SELECT * FROM c", partition_key=None)
        sdk.__aexit__.assert_awaited_once()
    assert "[panel] Failed to load feedback summary for overview." in caplog.text
    assert "private-feedback-failure-canary" not in response.text + caplog.text


# ---------------------------------------------------------------------------
# GET /api/panel/conversations/{id}/history — explicit cross-repo blocker
# ---------------------------------------------------------------------------


def test_history_stub_returns_501_for_admin(monkeypatch):
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_ADMIN_CLAIMS)
    r = client.get("/api/panel/conversations/conv-a/history")
    assert r.status_code == 501
    assert "Azure/GPT-RAG#592" in r.json()["detail"]


def test_history_stub_still_requires_admin_role(monkeypatch):
    client = _build_client(monkeypatch, tenant_id="tenant-1", claims=_NON_ADMIN_CLAIMS)
    r = client.get("/api/panel/conversations/conv-a/history")
    assert r.status_code == 403
