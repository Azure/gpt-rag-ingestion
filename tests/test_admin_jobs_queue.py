"""Tests for ``GET /api/jobs/queue`` (the Queue panel data source).

Covers the three shapes the operator dashboard relies on:

* empty queue: no in-flight runs, every cron-driven job_type has a non-null
  ``next_scheduled_at`` and its ``cron`` string is returned
* in-flight present: ``in_flight`` carries the recorded ``run_id`` and an
  ISO-8601 UTC ``started_at`` with a ``Z`` suffix
* missing / invalid ``CRON_RUN_*``: ``cron`` is ``null`` and
  ``next_scheduled_at`` is ``null`` (no fabricated schedule)

Mirrors the stubbing pattern in ``tests/test_admin_run_now.py`` /
``tests/test_admin_config.py`` so this file never imports ``main`` (which
would pull in the full ingestion stack).
"""

from __future__ import annotations

import asyncio
import datetime
import importlib
import re
import sys
import types
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Stubs (kept local to this file — the queue endpoint needs JOB_CRON_MAP,
# JOB_REGISTRY, _running_jobs and a scheduler whose ``get_job`` returns
# something with a ``next_run_time`` attribute).
# ---------------------------------------------------------------------------


_ISO_Z_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


def _install_stubs(
    monkeypatch,
    *,
    tenant_id: str | None = None,
    claims: dict | Exception | None = None,
    config_values: dict[str, str] | None = None,
    scheduled_jobs: dict[str, datetime.datetime | None] | None = None,
    running_jobs: dict[str, dict] | None = None,
):
    cfg_values = dict(config_values or {})

    class _FakeConfig:
        def __init__(self, tenant: str | None, values: dict[str, str]) -> None:
            self._tenant = tenant
            self._values = values

        def get(self, key, default=None, allow_none=True):
            if key == "OAUTH_AZURE_AD_TENANT_ID":
                return self._tenant
            return self._values.get(key, default)

    def _get_config(action=None):
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

    tools_pkg = types.ModuleType("tools")
    tools_pkg.__path__ = []
    credentials_stub = types.ModuleType("tools.credentials")
    credentials_stub.get_azure_client_id = lambda cfg=None: "fake-client-id"
    monkeypatch.setitem(sys.modules, "tools", tools_pkg)
    monkeypatch.setitem(sys.modules, "tools.credentials", credentials_stub)

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
    main_stub._running_jobs = dict(running_jobs or {})
    main_stub._running_jobs_lock = asyncio.Lock()

    class _FakeJob:
        def __init__(self, next_run_time: datetime.datetime | None) -> None:
            self.next_run_time = next_run_time

    class _FakeScheduler:
        def __init__(self, jobs_with_next: dict[str, datetime.datetime | None]) -> None:
            self.timezone = datetime.timezone.utc
            self._jobs = {
                jid: _FakeJob(when) for jid, when in jobs_with_next.items()
            }

        def get_job(self, jid):
            return self._jobs.get(jid)

        def add_job(self, *args, **kwargs):  # pragma: no cover - unused here
            pass

    main_stub.scheduler = _FakeScheduler(scheduled_jobs or {})
    monkeypatch.setitem(sys.modules, "main", main_stub)

    return {"main": main_stub}


def _build_client(monkeypatch, **kwargs):
    state = _install_stubs(monkeypatch, **kwargs)

    if "api.admin" in sys.modules:
        del sys.modules["api.admin"]
    if "api" not in sys.modules:
        api_pkg = types.ModuleType("api")
        api_pkg.__path__ = [str(Path(__file__).resolve().parents[1] / "api")]
        sys.modules["api"] = api_pkg
    admin_module = importlib.import_module("api.admin")

    app = FastAPI()
    app.include_router(admin_module.router)
    return TestClient(app), state, admin_module


# ---------------------------------------------------------------------------
# Shape
# ---------------------------------------------------------------------------


def test_queue_returns_one_row_per_job_type(monkeypatch):
    client, _, _ = _build_client(monkeypatch)
    r = client.get("/api/jobs/queue")
    assert r.status_code == 200, r.text
    body = r.json()
    job_types = [row["job_type"] for row in body["items"]]
    # Same set the Run-now row exposes (sorted alphabetically).
    assert job_types == sorted(
        [
            "blob_index",
            "blob_purge",
            "multimodality_images_purge",
            "nl2sql_index",
            "nl2sql_purge",
            "sharepoint_index",
            "sharepoint_purge",
        ]
    )
    for row in body["items"]:
        # Every row has the documented keys, no extras.
        assert set(row.keys()) == {"job_type", "in_flight", "next_scheduled_at", "cron"}


def test_queue_open_to_unauthenticated_callers(monkeypatch):
    # Same network-only posture as `GET /api/jobs` and `GET /api/config`.
    client, _, _ = _build_client(monkeypatch, tenant_id="tenant-guid", claims=None)
    r = client.get("/api/jobs/queue")
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Empty queue (no in-flight, valid cron everywhere)
# ---------------------------------------------------------------------------


def test_queue_empty_with_all_crons_registered(monkeypatch):
    in_one_hour = datetime.datetime(2026, 6, 18, 21, 0, tzinfo=datetime.timezone.utc)
    in_ten_minutes = datetime.datetime(2026, 6, 18, 20, 10, tzinfo=datetime.timezone.utc)
    client, _, _ = _build_client(
        monkeypatch,
        config_values={
            "CRON_RUN_BLOB_INDEX": "0 * * * *",
            "CRON_RUN_BLOB_PURGE": "10 * * * *",
        },
        scheduled_jobs={
            "blob_index": in_one_hour,
            "blob_purge": in_ten_minutes,
        },
    )
    body = client.get("/api/jobs/queue").json()
    rows = {row["job_type"]: row for row in body["items"]}

    assert rows["blob_index"]["in_flight"] is None
    assert rows["blob_index"]["cron"] == "0 * * * *"
    assert rows["blob_index"]["next_scheduled_at"] == "2026-06-18T21:00:00.000Z"
    assert _ISO_Z_RE.match(rows["blob_index"]["next_scheduled_at"])

    assert rows["blob_purge"]["in_flight"] is None
    assert rows["blob_purge"]["cron"] == "10 * * * *"
    assert rows["blob_purge"]["next_scheduled_at"] == "2026-06-18T20:10:00.000Z"


# ---------------------------------------------------------------------------
# In-flight present
# ---------------------------------------------------------------------------


def test_queue_reports_in_flight_run_id_and_started_at(monkeypatch):
    started = datetime.datetime(2026, 6, 18, 20, 5, 30, 123000, tzinfo=datetime.timezone.utc)
    client, _, _ = _build_client(
        monkeypatch,
        config_values={"CRON_RUN_BLOB_INDEX": "0 * * * *"},
        scheduled_jobs={"blob_index": None},  # job is running so APScheduler can return None
        running_jobs={
            "blob_index": {
                "run_id": "manual-blob_index-1735592812345",
                "started_at": started,
            }
        },
    )
    body = client.get("/api/jobs/queue").json()
    rows = {row["job_type"]: row for row in body["items"]}

    assert rows["blob_index"]["in_flight"] == {
        "run_id": "manual-blob_index-1735592812345",
        "started_at": "2026-06-18T20:05:30.123Z",
    }
    # When the job is currently running APScheduler may report no next_run_time
    # for the cron job — the endpoint must surface ``null`` rather than 500.
    assert rows["blob_index"]["next_scheduled_at"] is None
    # The cron string still comes through so the UI can display it.
    assert rows["blob_index"]["cron"] == "0 * * * *"


def test_queue_normalizes_naive_started_at_to_utc(monkeypatch):
    # The wrapper always stores tz-aware UTC, but defensively the endpoint
    # should not crash if a naive datetime ever slips in (e.g. a future
    # refactor or a test seeding the registry directly).
    naive = datetime.datetime(2026, 6, 18, 20, 0, 0)
    client, _, _ = _build_client(
        monkeypatch,
        running_jobs={"blob_index": {"run_id": "blob_index", "started_at": naive}},
    )
    body = client.get("/api/jobs/queue").json()
    rows = {row["job_type"]: row for row in body["items"]}
    assert rows["blob_index"]["in_flight"]["started_at"] == "2026-06-18T20:00:00.000Z"


# ---------------------------------------------------------------------------
# Missing / unregistered cron
# ---------------------------------------------------------------------------


def test_queue_no_cron_registered_returns_nulls(monkeypatch):
    # CRON_RUN_SHAREPOINT_INDEX is unset and the scheduler has no matching
    # job — the row must still appear, with both ``cron`` and
    # ``next_scheduled_at`` as null.
    client, _, _ = _build_client(monkeypatch)
    body = client.get("/api/jobs/queue").json()
    rows = {row["job_type"]: row for row in body["items"]}

    assert rows["sharepoint_index"]["in_flight"] is None
    assert rows["sharepoint_index"]["next_scheduled_at"] is None
    assert rows["sharepoint_index"]["cron"] is None


def test_queue_cron_set_but_scheduler_has_no_next_run(monkeypatch):
    # Operator set CRON_RUN_* but the scheduler entry has no next_run_time
    # (e.g. job is currently executing). The cron string is still returned;
    # next_scheduled_at is null.
    client, _, _ = _build_client(
        monkeypatch,
        config_values={"CRON_RUN_BLOB_PURGE": "10 * * * *"},
        scheduled_jobs={"blob_purge": None},
    )
    body = client.get("/api/jobs/queue").json()
    rows = {row["job_type"]: row for row in body["items"]}

    assert rows["blob_purge"]["cron"] == "10 * * * *"
    assert rows["blob_purge"]["next_scheduled_at"] is None
