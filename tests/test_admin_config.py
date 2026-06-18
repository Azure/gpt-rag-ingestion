"""Tests for the Configuration tab endpoints in ``api.admin``.

Covers:

* ``GET /api/config`` shape (sections, every allowlisted key present, type info)
* ``GET /api/config`` open to unauthenticated callers, ``canEdit`` reflects role
* ``PUT /api/config`` admin gate (403 for non-admin, accepted for admin)
* ``PUT /api/config`` validation: int range, bool, cron expression, denylist,
  unknown key — all rejected with field-level errors and no write attempted
* ``PUT /api/config`` success path: writes via AppConfig, reschedules cron jobs
* ``POST /api/config/reload`` and ``POST /api/config/apply`` admin-only smoke

Mirrors the stubbing pattern in ``tests/test_admin_run_now.py`` so this file
never imports ``main`` (which would pull in the full ingestion stack).
"""

from __future__ import annotations

import asyncio
import importlib
import sys
import types
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Stubs
# ---------------------------------------------------------------------------


def _install_stubs(
    monkeypatch,
    *,
    tenant_id: str | None,
    claims: dict | Exception | None,
    config_values: dict[str, str] | None = None,
):
    """Wire fake `dependencies` + `main` + `azure.appconfiguration` modules."""

    cfg_values = dict(config_values or {})
    refresh_calls: list[str] = []

    class _FakeConfig:
        def __init__(self, tenant: str | None, values: dict[str, str]) -> None:
            self._tenant = tenant
            self._values = values

        def get(self, key, default=None, allow_none=True):
            if key == "OAUTH_AZURE_AD_TENANT_ID":
                return self._tenant
            return self._values.get(key, default)

    def _get_config(action=None):
        if action == "refresh":
            refresh_calls.append("refresh")
        return _FakeConfig(tenant_id, cfg_values)

    async def _validate_bearer_jwt(_request):
        if isinstance(claims, Exception):
            raise claims
        if claims is None:
            raise HTTPException(status_code=401, detail="Missing token")
        return claims

    dependencies_stub = types.ModuleType("dependencies")
    dependencies_stub.get_config = _get_config
    dependencies_stub.validate_bearer_jwt = _validate_bearer_jwt
    monkeypatch.setitem(sys.modules, "dependencies", dependencies_stub)

    # tools.credentials stub
    tools_pkg = types.ModuleType("tools")
    tools_pkg.__path__ = []
    credentials_stub = types.ModuleType("tools.credentials")
    credentials_stub.get_azure_client_id = lambda cfg=None: "fake-client-id"
    monkeypatch.setitem(sys.modules, "tools", tools_pkg)
    monkeypatch.setitem(sys.modules, "tools.credentials", credentials_stub)

    # main stub — only what api.admin imports lazily
    main_stub = types.ModuleType("main")

    async def _noop_job():
        return None

    main_stub.JOB_REGISTRY = {
        "sharepoint_index": _noop_job,
        "sharepoint_purge": _noop_job,
        "multimodality_images_purge": _noop_job,
        "blob_index": _noop_job,
        "blob_purge": _noop_job,
        "nl2sql_index": _noop_job,
        "nl2sql_purge": _noop_job,
    }
    main_stub.JOB_CRON_MAP = {
        "CRON_RUN_SHAREPOINT_INDEX": "sharepoint_index",
        "CRON_RUN_SHAREPOINT_PURGE": "sharepoint_purge",
        "CRON_RUN_IMAGES_PURGE": "multimodality_images_purge",
        "CRON_RUN_BLOB_INDEX": "blob_index",
        "CRON_RUN_BLOB_PURGE": "blob_purge",
        "CRON_RUN_NL2SQL_INDEX": "nl2sql_index",
        "CRON_RUN_NL2SQL_PURGE": "nl2sql_purge",
    }
    main_stub._running_jobs = set()
    main_stub._running_jobs_lock = asyncio.Lock()

    class _FakeScheduler:
        def __init__(self) -> None:
            import datetime

            self.timezone = datetime.timezone.utc
            self.jobs: dict[str, dict] = {}
            self.rescheduled: list[tuple[str, object]] = []

        def add_job(self, func, **kwargs):
            jid = kwargs.get("id")
            self.jobs[jid] = {"func": func, **kwargs}

        def get_job(self, jid):
            return self.jobs.get(jid)

        def reschedule_job(self, jid, trigger=None):
            self.rescheduled.append((jid, trigger))

        def remove_job(self, jid):
            self.jobs.pop(jid, None)

    main_stub.scheduler = _FakeScheduler()
    monkeypatch.setitem(sys.modules, "main", main_stub)

    # azure.appconfiguration write client stub — capture set_configuration_setting
    written: list[object] = []

    class _FakeWriteClient:
        def set_configuration_setting(self, setting):
            written.append(setting)

    return {
        "main": main_stub,
        "refresh_calls": refresh_calls,
        "written": written,
        "write_client": _FakeWriteClient(),
    }


def _build_client(monkeypatch, *, tenant_id, claims, config_values=None):
    state = _install_stubs(
        monkeypatch,
        tenant_id=tenant_id,
        claims=claims,
        config_values=config_values,
    )

    # Re-import api.admin so it picks up the stubs.
    if "api.admin" in sys.modules:
        del sys.modules["api.admin"]
    if "api" not in sys.modules:
        api_pkg = types.ModuleType("api")
        api_pkg.__path__ = [str(Path(__file__).resolve().parents[1] / "api")]
        sys.modules["api"] = api_pkg
    admin_module = importlib.import_module("api.admin")

    # Patch the write-client factory to return our fake, so the test never
    # tries to instantiate a real AzureAppConfigurationClient.
    monkeypatch.setattr(
        admin_module, "_get_app_config_write_client", lambda: state["write_client"]
    )

    app = FastAPI()
    app.include_router(admin_module.router)
    return TestClient(app), state, admin_module


# ---------------------------------------------------------------------------
# GET /api/config
# ---------------------------------------------------------------------------


def test_get_config_returns_sections_shape(monkeypatch):
    client, _, admin_module = _build_client(
        monkeypatch,
        tenant_id=None,
        claims=None,
        config_values={
            "CRON_RUN_BLOB_INDEX": "0 * * * *",
            "CHUNKING_NUM_TOKENS": "512",
            "MULTIMODAL": "true",
        },
    )
    r = client.get("/api/config")
    assert r.status_code == 200, r.text
    body = r.json()

    assert body["canEdit"] is True
    section_ids = [s["id"] for s in body["sections"]]
    assert section_ids == [
        "scheduling",
        "chunking",
        "indexing",
        "throughput",
        "limits",
        "multimodal",
        "sharepoint",
    ]

    # Every allowlisted key is present exactly once across all sections.
    flat_keys = [
        setting["key"]
        for section in body["sections"]
        for setting in section["settings"]
    ]
    assert sorted(flat_keys) == sorted(admin_module.ALLOWED_KEYS)
    assert len(flat_keys) == len(set(flat_keys))

    # Values are coerced to their declared types.
    by_key = {
        s["key"]: s
        for section in body["sections"]
        for s in section["settings"]
    }
    assert by_key["CHUNKING_NUM_TOKENS"]["value"] == 512
    assert by_key["MULTIMODAL"]["value"] is True
    assert by_key["CRON_RUN_BLOB_INDEX"]["value"] == "0 * * * *"


def test_get_config_sections_have_frontend_contract_fields(monkeypatch):
    """Each section must include `title` and `keys` (the names the typed
    frontend `ConfigSection` reads). The legacy `label` and nested `settings`
    fields stay for backwards compatibility. Without `title`/`keys` the
    Configuration tab crashes with a TypeError and renders blank (issue
    https://github.com/Azure/gpt-rag-ingestion/issues/242 follow-up)."""
    client, _, _ = _build_client(monkeypatch, tenant_id=None, claims=None)
    body = client.get("/api/config").json()
    settings_by_key = {s["key"]: s for s in body["settings"]}
    for section in body["sections"]:
        assert "title" in section, f"section {section.get('id')!r} missing `title`"
        assert section["title"] == section["label"], "title must mirror label"
        assert "keys" in section, f"section {section.get('id')!r} missing `keys`"
        assert isinstance(section["keys"], list)
        # Every key must resolve to a real entry in the flat settings list.
        for key in section["keys"]:
            assert key in settings_by_key, (
                f"section {section['id']!r} references unknown key {key!r}"
            )
        # And keys must match the nested settings order, so the UI renders
        # fields in the same order under both views.
        nested_keys = [s["key"] for s in section["settings"]]
        assert section["keys"] == nested_keys


def test_get_config_open_when_auth_enabled_but_no_token(monkeypatch):
    client, _, _ = _build_client(monkeypatch, tenant_id="tenant-guid", claims=None)
    r = client.get("/api/config")
    assert r.status_code == 200
    assert r.json()["canEdit"] is False  # auth on + no admin token = read-only


def test_get_config_returns_flat_settings_and_auth_enabled(monkeypatch):
    """Issue #242: the typed `ConfigResponse` contract requires a top-level
    flat `settings` array and `authEnabled` flag in addition to `sections`.
    The frontend reads `res.settings` directly; without it the Configuration
    tab crashes with `TypeError: undefined is not iterable`.
    """
    # Auth-off case: `authEnabled` must be False, `settings` must be the
    # flattened section settings (same objects, same order).
    client, _, admin_module = _build_client(
        monkeypatch,
        tenant_id=None,
        claims=None,
        config_values={
            "CRON_RUN_BLOB_INDEX": "0 * * * *",
            "CHUNKING_NUM_TOKENS": "512",
            "MULTIMODAL": "true",
        },
    )
    r = client.get("/api/config")
    assert r.status_code == 200, r.text
    body = r.json()

    assert "settings" in body, "top-level `settings` array missing (issue #242)"
    assert "authEnabled" in body, "top-level `authEnabled` flag missing (issue #242)"
    assert body["authEnabled"] is False

    flat = body["settings"]
    assert isinstance(flat, list) and flat, "`settings` must be a non-empty list"

    # Flat list must equal the flattened section settings, preserving the
    # declared `SETTINGS` order so the two views can never disagree.
    expected = [
        setting
        for section in body["sections"]
        for setting in section["settings"]
    ]
    assert flat == expected
    assert sorted(s["key"] for s in flat) == sorted(admin_module.ALLOWED_KEYS)

    # Auth-on case: `authEnabled` must reflect `_auth_enabled()`.
    client_authed, _, _ = _build_client(
        monkeypatch, tenant_id="tenant-guid", claims=None
    )
    body_authed = client_authed.get("/api/config").json()
    assert body_authed["authEnabled"] is True
    assert "settings" in body_authed and body_authed["settings"]


# ---------------------------------------------------------------------------
# PUT /api/config — auth gate
# ---------------------------------------------------------------------------


def test_put_config_requires_admin_when_auth_enabled(monkeypatch):
    client, state, _ = _build_client(
        monkeypatch,
        tenant_id="tenant-guid",
        claims={"roles": ["Reader"]},
    )
    r = client.put(
        "/api/config",
        json={"updates": [{"key": "CHUNKING_NUM_TOKENS", "value": 256}]},
    )
    assert r.status_code == 403, r.text
    assert state["written"] == []


# ---------------------------------------------------------------------------
# PUT /api/config — validation
# ---------------------------------------------------------------------------


def test_put_config_rejects_int_out_of_range(monkeypatch):
    client, state, _ = _build_client(monkeypatch, tenant_id=None, claims=None)
    r = client.put(
        "/api/config",
        json={"updates": [{"key": "CHUNKING_NUM_TOKENS", "value": -5}]},
    )
    assert r.status_code == 422, r.text
    body = r.json()
    assert body["applied"] == []
    assert body["failed"][0]["key"] == "CHUNKING_NUM_TOKENS"
    assert ">=" in body["failed"][0]["error"]
    assert state["written"] == []


def test_put_config_rejects_invalid_cron(monkeypatch):
    client, state, _ = _build_client(monkeypatch, tenant_id=None, claims=None)
    r = client.put(
        "/api/config",
        json={"updates": [{"key": "CRON_RUN_BLOB_INDEX", "value": "not a cron"}]},
    )
    assert r.status_code == 422, r.text
    assert state["written"] == []
    body = r.json()
    assert "invalid cron" in body["failed"][0]["error"].lower()


def test_put_config_rejects_denylisted_and_unknown(monkeypatch):
    client, state, _ = _build_client(monkeypatch, tenant_id=None, claims=None)
    r = client.put(
        "/api/config",
        json={
            "updates": [
                {"key": "STORAGE_ACCOUNT_NAME", "value": "evil"},  # unknown
                {"key": "MCP_APP_APIKEY", "value": "leaked"},       # denylist
            ]
        },
    )
    assert r.status_code == 422, r.text
    body = r.json()
    by_key = {f["key"]: f["error"] for f in body["failed"]}
    assert "denylist" in by_key["MCP_APP_APIKEY"]
    assert "allowlist" in by_key["STORAGE_ACCOUNT_NAME"]
    assert state["written"] == []


# ---------------------------------------------------------------------------
# PUT /api/config — success path
# ---------------------------------------------------------------------------


def test_put_config_writes_and_reschedules_cron(monkeypatch):
    client, state, _ = _build_client(monkeypatch, tenant_id=None, claims=None)
    # Seed the scheduler with an existing job so reschedule_job is exercised.
    state["main"].scheduler.add_job(lambda: None, id="blob_index")

    r = client.put(
        "/api/config",
        json={
            "updates": [
                {"key": "CHUNKING_NUM_TOKENS", "value": 1024},
                {"key": "MULTIMODAL", "value": True},
                {"key": "CRON_RUN_BLOB_INDEX", "value": "*/15 * * * *"},
            ]
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert sorted(body["applied"]) == [
        "CHUNKING_NUM_TOKENS",
        "CRON_RUN_BLOB_INDEX",
        "MULTIMODAL",
    ]
    assert body["failed"] == []
    assert body["rescheduled"] == ["blob_index"]

    # AppConfig got the values normalized to strings.
    written_by_key = {s.key: s for s in state["written"]}
    assert written_by_key["CHUNKING_NUM_TOKENS"].value == "1024"
    assert written_by_key["MULTIMODAL"].value == "true"
    assert written_by_key["CRON_RUN_BLOB_INDEX"].label == "gpt-rag"
    # AppConfig cache was refreshed.
    assert "refresh" in state["refresh_calls"]
    # Scheduler was rescheduled rather than re-added.
    assert state["main"].scheduler.rescheduled
    assert state["main"].scheduler.rescheduled[0][0] == "blob_index"


# ---------------------------------------------------------------------------
# POST /api/config/reload + /api/config/apply
# ---------------------------------------------------------------------------


def test_reload_endpoint_requires_admin(monkeypatch):
    client, state, _ = _build_client(
        monkeypatch,
        tenant_id="tenant-guid",
        claims={"roles": ["Reader"]},
    )
    r = client.post("/api/config/reload")
    assert r.status_code == 403, r.text
    assert state["refresh_calls"] == []


def test_apply_endpoint_admin_smoke(monkeypatch):
    client, state, _ = _build_client(
        monkeypatch,
        tenant_id=None,
        claims=None,
        config_values={"CRON_RUN_BLOB_INDEX": "*/30 * * * *"},
    )
    r = client.post("/api/config/apply")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "ok"
    assert "note" in body
    # Cache refreshed at least once.
    assert "refresh" in state["refresh_calls"]
