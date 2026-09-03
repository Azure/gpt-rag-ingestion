"""Tests for the panel operator API surfaces (issue #611, ADR-0004):

``GET /panel/overview/metrics``, ``GET /panel/corpus-curation/queue``, and
``POST /panel/corpus-curation/{item_id}/decision``.

Mirrors the stubbing pattern in ``tests/test_retrieval.py`` so this file
never imports ``main`` (which would pull in the full ingestion stack) and
never touches the real Azure SDKs.
"""

from __future__ import annotations

import base64
import importlib
import json
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient


@dataclass(frozen=True)
class _ValidatedUserBearer:
    access_token: str
    claims: Dict[str, Any]


_DEFAULT_CONFIG = {
    "DEPLOY_ADMINISTRATIVE_PANEL": "true",
    "PANEL_OPERATOR_SURFACES_ENABLED": "true",
    "PANEL_OPERATOR_APP_ROLE": "PanelOperator",
    "DATA_INGEST_APP_APIKEY": "test-signing-secret",
    "PANEL_OVERVIEW_MIN_CARDINALITY": "5",
    "PANEL_CURSOR_TTL_SECONDS": "600",
    "PANEL_OWNER_INDEX_DATABASE_CONTAINER": "panel-conversation-owner-index",
    "PANEL_FEEDBACK_DATABASE_CONTAINER": "panel-feedback",
}


class _CurationConcurrencyExhausted(Exception):
    def __init__(self, item_id):
        self.item_id = item_id


class _CurationDecisionConflict(Exception):
    def __init__(self, item_id, existing_decision, requested_decision):
        self.item_id = item_id
        self.existing_decision = existing_decision
        self.requested_decision = requested_decision


class _CurationItemNotFound(Exception):
    def __init__(self, item_id):
        self.item_id = item_id


def _install_stubs(
    monkeypatch,
    *,
    config_values: Optional[Dict[str, str]] = None,
    auth_behavior=None,
    cosmos_counts: Optional[Dict[str, int]] = None,
    cosmos_raises: bool = False,
    store_pending_items=None,
    store_total: int = 0,
    store_raises_on_list: bool = False,
    store_decision_behavior=None,
):
    config = dict(_DEFAULT_CONFIG)
    if config_values:
        config.update(config_values)

    class _FakeConfig:
        def get(self, key, default=None, allow_none=True, **_):
            return config.get(key, default)

    async def _default_auth(_request: Request) -> _ValidatedUserBearer:
        auth = _request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Missing Authorization header.")
        return _ValidatedUserBearer(access_token=auth[7:], claims={"oid": "operator-oid-1"})

    dependencies_stub = types.ModuleType("dependencies")
    dependencies_stub.get_config = lambda action=None: _FakeConfig()
    dependencies_stub.validate_delegated_operator_bearer = auth_behavior or _default_auth
    dependencies_stub.operator_role_or_group_configured = lambda: bool(
        config.get("PANEL_OPERATOR_APP_ROLE") or config.get("PANEL_OPERATOR_GROUP_ID")
    )
    monkeypatch.setitem(sys.modules, "dependencies", dependencies_stub)

    # Note: the real `tools` package (`tools/__init__.py`) is intentionally
    # left in place -- it lazily re-exports submodules via `__getattr__` and
    # never eagerly imports anything, so it is safe to import for real here.
    # Only the two submodules this router touches are stubbed below.

    counts = cosmos_counts or {}

    class _FakeCosmosDBClient:
        async def count_documents(self, container_name):
            if cosmos_raises:
                raise RuntimeError("cosmos down")
            return counts.get(container_name, 0)

    cosmosdb_stub = types.ModuleType("tools.cosmosdb")
    cosmosdb_stub.CosmosDBClient = _FakeCosmosDBClient
    monkeypatch.setitem(sys.modules, "tools.cosmosdb", cosmosdb_stub)

    @dataclass(frozen=True)
    class _Item:
        item_id: str
        document_id: str
        title: str
        reason_code: str
        submitted_at: str

    pending_items = [
        _Item(**d) if isinstance(d, dict) else d
        for d in (store_pending_items if store_pending_items is not None else [])
    ]

    class _FakeCorpusCurationStore:
        async def count_pending_and_decided(self):
            if cosmos_raises:
                raise RuntimeError("store down")
            return (len(pending_items), 3)

        async def list_pending_items(self, *, limit, offset):
            if store_raises_on_list:
                raise RuntimeError("store down")
            page = pending_items[offset : offset + limit]
            return page, store_total or len(pending_items)

        async def record_decision(self, item_id, *, decision, note, decided_by):
            if store_decision_behavior is not None:
                return store_decision_behavior(item_id, decision, note, decided_by)
            raise _CurationItemNotFound(item_id)

    corpus_stub = types.ModuleType("tools.corpus_curation_store")
    corpus_stub.CorpusCurationStore = _FakeCorpusCurationStore
    corpus_stub.CurationItemNotFound = _CurationItemNotFound
    corpus_stub.CurationDecisionConflict = _CurationDecisionConflict
    corpus_stub.CurationConcurrencyExhausted = _CurationConcurrencyExhausted
    corpus_stub.CurationItem = _Item
    monkeypatch.setitem(sys.modules, "tools.corpus_curation_store", corpus_stub)

    return {"item_cls": _Item}


def _build_client(monkeypatch, **kwargs):
    stubs = _install_stubs(monkeypatch, **kwargs)

    sys.modules.pop("api.panel_operator", None)
    api_pkg = types.ModuleType("api")
    api_pkg.__path__ = [str(Path(__file__).resolve().parents[1] / "api")]
    monkeypatch.setitem(sys.modules, "api", api_pkg)
    panel_operator = importlib.import_module("api.panel_operator")

    app = FastAPI()
    app.include_router(panel_operator.router)
    return TestClient(app, raise_server_exceptions=True), panel_operator, stubs


def _auth_header(token: str = "user-token") -> Dict[str, str]:
    return {"Authorization": "Bearer " + token}


# ---------------------------------------------------------------------------
# Gate
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "overrides",
    [
        {"DEPLOY_ADMINISTRATIVE_PANEL": "false"},
        {"PANEL_OPERATOR_SURFACES_ENABLED": "false"},
        {"PANEL_OPERATOR_APP_ROLE": "", "PANEL_OPERATOR_GROUP_ID": ""},
        {"DATA_INGEST_APP_APIKEY": ""},
    ],
)
def test_gate_defaults_fail_closed_for_every_endpoint(monkeypatch, overrides):
    client, _mod, _ = _build_client(monkeypatch, config_values=overrides)

    r1 = client.get("/panel/overview/metrics", headers=_auth_header())
    r2 = client.get("/panel/corpus-curation/queue", headers=_auth_header())
    r3 = client.post(
        "/panel/corpus-curation/cur_" + "0" * 32 + "/decision",
        json={"decision": "approve"},
        headers=_auth_header(),
    )

    assert r1.status_code == 503
    assert r2.status_code == 503
    assert r3.status_code == 503


def test_gate_enabled_allows_overview(monkeypatch):
    client, _mod, _ = _build_client(monkeypatch, cosmos_counts={"panel-conversation-owner-index": 10})

    response = client.get("/panel/overview/metrics", headers=_auth_header())

    assert response.status_code == 200


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


def test_missing_bearer_is_401(monkeypatch):
    client, _mod, _ = _build_client(monkeypatch)

    response = client.get("/panel/overview/metrics")

    assert response.status_code == 401


def test_app_only_token_is_403(monkeypatch):
    async def _reject_app_only(_request):
        raise HTTPException(status_code=403, detail="Delegated user token required.")

    client, _mod, _ = _build_client(monkeypatch, auth_behavior=_reject_app_only)

    response = client.get("/panel/overview/metrics", headers=_auth_header())

    assert response.status_code == 403


def test_missing_operator_role_is_403(monkeypatch):
    async def _reject_role(_request):
        raise HTTPException(status_code=403, detail="Operator role or group required.")

    client, _mod, _ = _build_client(monkeypatch, auth_behavior=_reject_role)

    response = client.get("/panel/corpus-curation/queue", headers=_auth_header())

    assert response.status_code == 403


# ---------------------------------------------------------------------------
# Overview metrics: suppression + correlation id + downstream failures
# ---------------------------------------------------------------------------


def test_overview_metrics_suppresses_low_cardinality_buckets(monkeypatch):
    client, _mod, _ = _build_client(
        monkeypatch,
        cosmos_counts={"panel-conversation-owner-index": 2, "panel-feedback": 1},
        store_pending_items=[],
    )

    response = client.get("/panel/overview/metrics", headers=_auth_header())

    assert response.status_code == 200
    body = response.json()
    assert body["counts"]["conversation_count"] is None
    assert body["counts"]["feedback_count"] is None
    assert body["counts"]["corpus_pending_count"] is None
    import re

    assert re.match(r"^req_[0-9a-f]{32}$", body["correlation_id"])


def test_overview_metrics_returns_counts_at_or_above_threshold(monkeypatch):
    item_cls = None

    def _mk(n):
        return [
            {"item_id": f"cur_{i:032d}", "document_id": f"doc-{i}", "title": f"doc-{i}", "reason_code": "x", "submitted_at": "t"}
            for i in range(n)
        ]

    client, mod, stubs = _build_client(
        monkeypatch,
        cosmos_counts={"panel-conversation-owner-index": 7, "panel-feedback": 8},
    )

    response = client.get("/panel/overview/metrics", headers=_auth_header())

    assert response.status_code == 200
    body = response.json()
    assert body["counts"]["conversation_count"] == 7
    assert body["counts"]["feedback_count"] == 8


def test_overview_metrics_never_exposes_content_fields(monkeypatch):
    client, _mod, _ = _build_client(
        monkeypatch, cosmos_counts={"panel-conversation-owner-index": 9, "panel-feedback": 9}
    )

    response = client.get("/panel/overview/metrics", headers=_auth_header())

    body = response.json()
    assert set(body) == {"schema_version", "generated_at", "correlation_id", "counts"}
    assert set(body["counts"]) == {
        "conversation_count",
        "feedback_count",
        "corpus_pending_count",
        "corpus_decided_count",
    }


def test_overview_metrics_cosmos_failure_is_502(monkeypatch):
    client, _mod, _ = _build_client(monkeypatch, cosmos_raises=True)

    response = client.get("/panel/overview/metrics", headers=_auth_header())

    assert response.status_code == 502


# ---------------------------------------------------------------------------
# Corpus curation queue: metadata-only shape, cursor round-trip and tamper
# ---------------------------------------------------------------------------


def _items(n):
    return [
        {
            "item_id": f"cur_{i:032d}",
            "document_id": f"doc-{i}.pdf",
            "title": f"doc-{i}.pdf",
            "reason_code": "processing_blocked",
            "submitted_at": "2026-01-01T00:00:00Z",
        }
        for i in range(n)
    ]


def test_curation_queue_returns_metadata_only_fields(monkeypatch):
    client, mod, stubs = _build_client(monkeypatch, store_pending_items=_items(1), store_total=1)

    response = client.get("/panel/corpus-curation/queue", headers=_auth_header())

    assert response.status_code == 200
    body = response.json()
    assert len(body["items"]) == 1
    assert set(body["items"][0]) == {
        "item_id",
        "document_id",
        "title",
        "reason_code",
        "submitted_at",
    }
    assert body["next_cursor"] is None


def test_curation_queue_issues_cursor_when_more_pages_remain(monkeypatch):
    client, mod, stubs = _build_client(monkeypatch, store_pending_items=_items(30), store_total=30)

    response = client.get("/panel/corpus-curation/queue", headers=_auth_header())

    body = response.json()
    assert len(body["items"]) == 25
    assert body["next_cursor"] is not None


def test_curation_queue_cursor_round_trip_returns_next_page(monkeypatch):
    client, mod, stubs = _build_client(monkeypatch, store_pending_items=_items(30), store_total=30)

    first = client.get("/panel/corpus-curation/queue", headers=_auth_header())
    cursor = first.json()["next_cursor"]

    second = client.get(
        "/panel/corpus-curation/queue",
        params={"cursor": cursor},
        headers=_auth_header(),
    )

    assert second.status_code == 200
    body = second.json()
    assert len(body["items"]) == 5
    assert body["next_cursor"] is None


def test_curation_queue_rejects_tampered_cursor(monkeypatch):
    client, mod, stubs = _build_client(monkeypatch, store_pending_items=_items(30), store_total=30)

    first = client.get("/panel/corpus-curation/queue", headers=_auth_header())
    cursor = first.json()["next_cursor"]
    mid = len(cursor) // 2
    flipped_char = "A" if cursor[mid] != "A" else "B"
    tampered = cursor[:mid] + flipped_char + cursor[mid + 1 :]

    response = client.get(
        "/panel/corpus-curation/queue",
        params={"cursor": tampered},
        headers=_auth_header(),
    )

    assert response.status_code == 422


def test_curation_queue_rejects_expired_cursor(monkeypatch):
    client, mod, stubs = _build_client(
        monkeypatch,
        store_pending_items=_items(30),
        store_total=30,
        config_values={"PANEL_CURSOR_TTL_SECONDS": "-1"},
    )

    first = client.get("/panel/corpus-curation/queue", headers=_auth_header())
    cursor = first.json()["next_cursor"]

    response = client.get(
        "/panel/corpus-curation/queue",
        params={"cursor": cursor},
        headers=_auth_header(),
    )

    assert response.status_code == 422


def test_curation_queue_rejects_cross_principal_cursor(monkeypatch):
    client, mod, stubs = _build_client(monkeypatch, store_pending_items=_items(30), store_total=30)

    first = client.get("/panel/corpus-curation/queue", headers=_auth_header())
    cursor = first.json()["next_cursor"]

    async def _other_operator(_request):
        return _ValidatedUserBearer(access_token="tok", claims={"oid": "a-different-operator"})

    client2, _mod2, _ = _build_client(
        monkeypatch,
        store_pending_items=_items(30),
        store_total=30,
        auth_behavior=_other_operator,
    )

    response = client2.get(
        "/panel/corpus-curation/queue",
        params={"cursor": cursor},
        headers=_auth_header(),
    )

    assert response.status_code == 422


def test_curation_queue_store_failure_is_502(monkeypatch):
    client, _mod, _ = _build_client(monkeypatch, store_raises_on_list=True)

    response = client.get("/panel/corpus-curation/queue", headers=_auth_header())

    assert response.status_code == 502


# ---------------------------------------------------------------------------
# Corpus curation decision: malformed id, not found, conflict, concurrency,
# success.
# ---------------------------------------------------------------------------

_VALID_ITEM_ID = "cur_" + "a" * 32


def test_decision_rejects_malformed_item_id(monkeypatch):
    client, _mod, _ = _build_client(monkeypatch)

    response = client.post(
        "/panel/corpus-curation/not-a-valid-id/decision",
        json={"decision": "approve"},
        headers=_auth_header(),
    )

    assert response.status_code == 422


def test_decision_rejects_unknown_extra_field(monkeypatch):
    client, _mod, _ = _build_client(monkeypatch)

    response = client.post(
        f"/panel/corpus-curation/{_VALID_ITEM_ID}/decision",
        json={"decision": "approve", "unexpected": "value"},
        headers=_auth_header(),
    )

    assert response.status_code == 422


def test_decision_not_found_is_404(monkeypatch):
    def _behavior(item_id, decision, note, decided_by):
        raise _CurationItemNotFound(item_id)

    client, _mod, _ = _build_client(monkeypatch, store_decision_behavior=_behavior)

    response = client.post(
        f"/panel/corpus-curation/{_VALID_ITEM_ID}/decision",
        json={"decision": "approve"},
        headers=_auth_header(),
    )

    assert response.status_code == 404


def test_decision_conflict_is_422(monkeypatch):
    def _behavior(item_id, decision, note, decided_by):
        raise _CurationDecisionConflict(item_id, "approve", decision)

    client, _mod, _ = _build_client(monkeypatch, store_decision_behavior=_behavior)

    response = client.post(
        f"/panel/corpus-curation/{_VALID_ITEM_ID}/decision",
        json={"decision": "reject"},
        headers=_auth_header(),
    )

    assert response.status_code == 422


def test_decision_concurrency_exhausted_is_502(monkeypatch):
    def _behavior(item_id, decision, note, decided_by):
        raise _CurationConcurrencyExhausted(item_id)

    client, _mod, _ = _build_client(monkeypatch, store_decision_behavior=_behavior)

    response = client.post(
        f"/panel/corpus-curation/{_VALID_ITEM_ID}/decision",
        json={"decision": "approve"},
        headers=_auth_header(),
    )

    assert response.status_code == 502


def test_decision_success_returns_contract_shape(monkeypatch):
    def _behavior(item_id, decision, note, decided_by):
        return decision, "2026-01-01T00:00:00Z", False

    client, _mod, _ = _build_client(monkeypatch, store_decision_behavior=_behavior)

    response = client.post(
        f"/panel/corpus-curation/{_VALID_ITEM_ID}/decision",
        json={"decision": "approve", "note": "looks fine"},
        headers=_auth_header(),
    )

    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"item_id", "decision", "decided_at"}
    assert body["decision"] == "approve"
    assert body["item_id"] == _VALID_ITEM_ID


def test_decision_replay_is_idempotent(monkeypatch):
    calls = []

    def _behavior(item_id, decision, note, decided_by):
        calls.append(decision)
        return decision, "2026-01-01T00:00:00Z", len(calls) > 1

    client, _mod, _ = _build_client(monkeypatch, store_decision_behavior=_behavior)

    first = client.post(
        f"/panel/corpus-curation/{_VALID_ITEM_ID}/decision",
        json={"decision": "approve"},
        headers=_auth_header(),
    )
    second = client.post(
        f"/panel/corpus-curation/{_VALID_ITEM_ID}/decision",
        json={"decision": "approve"},
        headers=_auth_header(),
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()


# ---------------------------------------------------------------------------
# No Conversations client -- structural regression guard.
# ---------------------------------------------------------------------------


def test_module_never_imports_a_conversations_client():
    source = Path(__file__).resolve().parents[1].joinpath("api", "panel_operator.py").read_text()
    store_source = (
        Path(__file__)
        .resolve()
        .parents[1]
        .joinpath("tools", "corpus_curation_store.py")
        .read_text()
    )

    forbidden = ["ConversationsClient", "foundry", "agents.conversations", "Conversation("]
    for needle in forbidden:
        assert needle not in source
        assert needle not in store_source
