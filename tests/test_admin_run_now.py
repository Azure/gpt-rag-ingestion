"""Tests for the admin gate on `POST /api/jobs/{job_type}/run`.

Documents the four behaviours of `api.admin.require_admin`:

* auth disabled (no ``OAUTH_AZURE_AD_TENANT_ID``) → endpoint is reachable
* auth enabled, no token                          → 401 from `validate_bearer_jwt`
* auth enabled, token without ``Admin`` role      → 403 from `require_admin`
* auth enabled, token with ``Admin`` role         → endpoint is reachable

The tests avoid importing `main` (which pulls in the full ingestion stack)
by mounting only `api.admin.router` on a throwaway FastAPI app and stubbing
out the bits of `main` that the run-now endpoint imports at call time.
"""

from __future__ import annotations

import asyncio
import importlib
import sys
import types

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient


def _install_stubs(monkeypatch, *, tenant_id: str | None, claims: dict | Exception | None):
    """Wire fake `dependencies` + `main` modules so `api.admin` can import them."""

    # --- dependencies stub --------------------------------------------------
    class _FakeConfig:
        def __init__(self, tenant: str | None) -> None:
            self._tenant = tenant

        def get(self, key, default=None, allow_none=True):
            if key == "OAUTH_AZURE_AD_TENANT_ID":
                return self._tenant
            return default

    async def _validate_bearer_jwt(_request):
        if isinstance(claims, Exception):
            raise claims
        if claims is None:
            raise HTTPException(status_code=401, detail="Missing token")
        return claims

    dependencies_stub = types.ModuleType("dependencies")
    dependencies_stub.get_config = lambda: _FakeConfig(tenant_id)
    dependencies_stub.validate_bearer_jwt = _validate_bearer_jwt
    monkeypatch.setitem(sys.modules, "dependencies", dependencies_stub)

    # --- tools.credentials stub --------------------------------------------
    tools_pkg = types.ModuleType("tools")
    tools_pkg.__path__ = []  # mark as package
    credentials_stub = types.ModuleType("tools.credentials")
    credentials_stub.get_azure_client_id = lambda: "fake-client-id"
    monkeypatch.setitem(sys.modules, "tools", tools_pkg)
    monkeypatch.setitem(sys.modules, "tools.credentials", credentials_stub)

    # --- main stub (only the symbols run_job_now imports) -------------------
    main_stub = types.ModuleType("main")

    async def _noop_job():
        return None

    main_stub.JOB_REGISTRY = {"blob_index": _noop_job}
    main_stub._running_jobs = {}
    main_stub._running_jobs_lock = asyncio.Lock()

    class _FakeScheduler:
        def __init__(self) -> None:
            self.jobs: list[dict] = []

        def add_job(self, func, **kwargs):
            self.jobs.append({"func": func, **kwargs})

    main_stub.scheduler = _FakeScheduler()
    monkeypatch.setitem(sys.modules, "main", main_stub)
    return main_stub


def _build_client(monkeypatch, *, tenant_id, claims):
    main_stub = _install_stubs(monkeypatch, tenant_id=tenant_id, claims=claims)
    # Re-import api.admin so it picks up the freshly-stubbed `dependencies`.
    if "api.admin" in sys.modules:
        del sys.modules["api.admin"]
    if "api" not in sys.modules:
        api_pkg = types.ModuleType("api")
        api_pkg.__path__ = [
            str(__import__("pathlib").Path(__file__).resolve().parents[1] / "api")
        ]
        sys.modules["api"] = api_pkg
    admin_module = importlib.import_module("api.admin")
    app = FastAPI()
    app.include_router(admin_module.router)
    return TestClient(app), main_stub


def test_run_now_auth_disabled_is_open(monkeypatch):
    client, main_stub = _build_client(monkeypatch, tenant_id=None, claims=None)
    r = client.post("/api/jobs/blob_index/run")
    assert r.status_code == 202, r.text
    assert r.json()["jobType"] == "blob_index"
    assert len(main_stub.scheduler.jobs) == 1


def test_run_now_auth_enabled_no_token_rejected(monkeypatch):
    client, main_stub = _build_client(monkeypatch, tenant_id="tenant-guid", claims=None)
    r = client.post("/api/jobs/blob_index/run")
    assert r.status_code == 401, r.text
    assert main_stub.scheduler.jobs == []


def test_run_now_auth_enabled_non_admin_rejected(monkeypatch):
    client, main_stub = _build_client(
        monkeypatch,
        tenant_id="tenant-guid",
        claims={"roles": ["Reader"]},
    )
    r = client.post("/api/jobs/blob_index/run")
    assert r.status_code == 403, r.text
    assert "Admin" in r.json()["detail"]
    assert main_stub.scheduler.jobs == []


def test_run_now_auth_enabled_admin_accepted(monkeypatch):
    client, main_stub = _build_client(
        monkeypatch,
        tenant_id="tenant-guid",
        claims={"roles": ["Admin", "Reader"]},
    )
    r = client.post("/api/jobs/blob_index/run")
    assert r.status_code == 202, r.text
    assert len(main_stub.scheduler.jobs) == 1


def test_run_now_unknown_job_type_returns_404(monkeypatch):
    client, _ = _build_client(monkeypatch, tenant_id=None, claims=None)
    r = client.post("/api/jobs/not_a_real_job/run")
    assert r.status_code == 404


def test_run_now_already_running_returns_409(monkeypatch):
    client, main_stub = _build_client(monkeypatch, tenant_id=None, claims=None)
    main_stub._running_jobs["blob_index"] = {
        "run_id": "blob_index",
        "started_at": __import__("datetime").datetime.now(
            tz=__import__("datetime").timezone.utc
        ),
    }
    r = client.post("/api/jobs/blob_index/run")
    assert r.status_code == 409
    assert main_stub.scheduler.jobs == []


def test_identity_auth_disabled(monkeypatch):
    client, _ = _build_client(monkeypatch, tenant_id=None, claims=None)
    r = client.get("/api/identity")
    assert r.status_code == 200
    assert r.json() == {"authEnabled": False, "isAdmin": True}


def test_identity_auth_enabled_admin(monkeypatch):
    client, _ = _build_client(
        monkeypatch,
        tenant_id="tenant-guid",
        claims={"roles": ["Admin"]},
    )
    r = client.get("/api/identity")
    assert r.status_code == 200
    assert r.json() == {"authEnabled": True, "isAdmin": True}


def test_identity_auth_enabled_missing_token_silent(monkeypatch):
    client, _ = _build_client(monkeypatch, tenant_id="tenant-guid", claims=None)
    r = client.get("/api/identity")
    # Endpoint never raises even when validate_bearer_jwt would 401 elsewhere.
    assert r.status_code == 200
    assert r.json() == {"authEnabled": True, "isAdmin": False}
