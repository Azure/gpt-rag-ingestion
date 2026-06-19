"""Tests for ``GET /api/jobs/queue`` (the Queue panel data source).

Covers the four shapes the operator dashboard relies on:

* empty queue: no in-flight runs, every cron-driven job_type has a non-null
  ``next_scheduled_at`` and its ``cron`` string is returned (read from the
  registered APScheduler trigger, not App Configuration)
* in-flight present: ``in_flight`` carries the recorded ``run_id`` and an
  ISO-8601 UTC ``started_at`` with a ``Z`` suffix
* missing / unregistered cron: ``cron`` is ``null`` and
  ``next_scheduled_at`` is ``null`` (no fabricated schedule)
* ``last_run`` reflects the most recent run-summary blob for the job_type's
  ``indexerType``, projecting started/finished/status/indexed_count

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

from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Stubs (kept local to this file — the queue endpoint needs JOB_REGISTRY,
# _running_jobs and a scheduler whose ``get_job`` returns something with a
# ``next_run_time`` and a ``trigger`` attribute. ``JOB_CRON_MAP`` is left
# present on the stub for parity with the real module but is no longer read
# by the endpoint — cron now comes from the trigger.)
# ---------------------------------------------------------------------------


_ISO_Z_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


def _install_stubs(
    monkeypatch,
    *,
    tenant_id: str | None = None,
    claims: dict | Exception | None = None,
    config_values: dict[str, str] | None = None,
    scheduled_jobs: dict[str, dict] | None = None,
    running_jobs: dict[str, dict] | None = None,
    runs: list[dict] | None = None,
):
    """Install fake ``dependencies``, ``tools.credentials`` and ``main`` modules.

    ``scheduled_jobs`` is a mapping ``job_id -> {next_run_time, cron}`` where
    ``next_run_time`` is a tz-aware ``datetime`` (or ``None``) and ``cron``
    is the crontab expression used to build a real ``CronTrigger`` (or
    ``None`` to register the job with no trigger, simulating a non-cron
    job).
    """

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
        def __init__(self, next_run_time, trigger) -> None:
            self.next_run_time = next_run_time
            self.trigger = trigger

    class _FakeScheduler:
        def __init__(self, jobs: dict[str, dict]) -> None:
            self.timezone = datetime.timezone.utc
            self._jobs: dict[str, _FakeJob] = {}
            for jid, spec in jobs.items():
                cron_expr = spec.get("cron") if isinstance(spec, dict) else None
                trigger = (
                    CronTrigger.from_crontab(cron_expr) if cron_expr else None
                )
                self._jobs[jid] = _FakeJob(
                    spec.get("next_run_time") if isinstance(spec, dict) else None,
                    trigger,
                )

        def get_job(self, jid):
            return self._jobs.get(jid)

        def add_job(self, *args, **kwargs):  # pragma: no cover - unused here
            pass

    main_stub.scheduler = _FakeScheduler(scheduled_jobs or {})
    monkeypatch.setitem(sys.modules, "main", main_stub)

    return {"main": main_stub, "runs": list(runs or [])}


def _build_client(monkeypatch, **kwargs):
    runs = kwargs.pop("runs", None)
    state = _install_stubs(monkeypatch, runs=runs, **kwargs)

    if "api.admin" in sys.modules:
        del sys.modules["api.admin"]
    if "api" not in sys.modules:
        api_pkg = types.ModuleType("api")
        api_pkg.__path__ = [str(Path(__file__).resolve().parents[1] / "api")]
        sys.modules["api"] = api_pkg
    admin_module = importlib.import_module("api.admin")

    # Stub out the blob-store-backed runs loader so the endpoint reads from
    # the test fixture rather than hitting Azure. Bypassing the 60s cache
    # keeps every test independent.
    runs_fixture = list(state["runs"])

    async def _fake_cached_load(key, _loader):
        if key == "runs":
            indexer_types = sorted({r.get("indexerType", "") for r in runs_fixture})
            return runs_fixture, indexer_types
        return [], []

    monkeypatch.setattr(admin_module, "_cached_load", _fake_cached_load)

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
        assert set(row.keys()) == {
            "job_type",
            "in_flight",
            "next_scheduled_at",
            "cron",
            "last_run",
        }


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
        scheduled_jobs={
            "blob_index": {"next_run_time": in_one_hour, "cron": "0 * * * *"},
            "blob_purge": {"next_run_time": in_ten_minutes, "cron": "10 * * * *"},
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


def test_queue_cron_is_read_from_trigger_not_app_config(monkeypatch):
    """Regression: v2.4.10 returned ``cron: null`` for every job because the
    endpoint read from a CRON_RUN_* App Config key that did not match.
    v2.4.11 reads cron directly from the registered trigger — the single
    source of truth for what is actually firing — so even if App Config is
    out of sync, the operator still sees the live schedule.
    """
    # config_values is intentionally empty; the trigger alone should drive
    # the returned cron string.
    client, _, _ = _build_client(
        monkeypatch,
        config_values={},
        scheduled_jobs={
            "blob_index": {"next_run_time": None, "cron": "*/5 * * * *"},
            "blob_purge": {"next_run_time": None, "cron": "0 0 * * *"},
        },
    )
    body = client.get("/api/jobs/queue").json()
    rows = {row["job_type"]: row for row in body["items"]}
    assert rows["blob_index"]["cron"] == "*/5 * * * *"
    assert rows["blob_purge"]["cron"] == "0 0 * * *"


# ---------------------------------------------------------------------------
# In-flight present
# ---------------------------------------------------------------------------


def test_queue_reports_in_flight_run_id_and_started_at(monkeypatch):
    started = datetime.datetime(2026, 6, 18, 20, 5, 30, 123000, tzinfo=datetime.timezone.utc)
    client, _, _ = _build_client(
        monkeypatch,
        scheduled_jobs={"blob_index": {"next_run_time": None, "cron": "0 * * * *"}},
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
    # The cron string still comes through (from the trigger) so the UI can
    # display it.
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
    # sharepoint_index has no scheduler entry — the row must still appear,
    # with both ``cron`` and ``next_scheduled_at`` as null.
    client, _, _ = _build_client(monkeypatch)
    body = client.get("/api/jobs/queue").json()
    rows = {row["job_type"]: row for row in body["items"]}

    assert rows["sharepoint_index"]["in_flight"] is None
    assert rows["sharepoint_index"]["next_scheduled_at"] is None
    assert rows["sharepoint_index"]["cron"] is None
    assert rows["sharepoint_index"]["last_run"] is None


def test_queue_cron_set_but_scheduler_has_no_next_run(monkeypatch):
    # Operator set a cron but the scheduler entry has no next_run_time
    # (e.g. job is currently executing). The cron string is still returned;
    # next_scheduled_at is null.
    client, _, _ = _build_client(
        monkeypatch,
        scheduled_jobs={"blob_purge": {"next_run_time": None, "cron": "10 * * * *"}},
    )
    body = client.get("/api/jobs/queue").json()
    rows = {row["job_type"]: row for row in body["items"]}

    assert rows["blob_purge"]["cron"] == "10 * * * *"
    assert rows["blob_purge"]["next_scheduled_at"] is None


# ---------------------------------------------------------------------------
# last_run column (v2.4.11 — sourced from the run-summary blob store)
# ---------------------------------------------------------------------------


def test_queue_last_run_populated_from_runs_store(monkeypatch):
    runs_fixture = [
        # blob_index → indexerType "blob-storage-indexer". Most recent first
        # in the fixture, but the endpoint must pick by runFinishedAt.
        {
            "indexerType": "blob-storage-indexer",
            "runId": "blob-index-older",
            "runStartedAt": "2026-06-18T18:00:00.000Z",
            "runFinishedAt": "2026-06-18T18:01:00.000Z",
            "status": "finished",
            "indexedItems": 99,
        },
        {
            "indexerType": "blob-storage-indexer",
            "runId": "blob-index-latest",
            "runStartedAt": "2026-06-18T20:00:00.000Z",
            "runFinishedAt": "2026-06-18T20:00:03.000Z",
            "status": "finished",
            "indexedItems": 0,
        },
        # blob_purge → indexerType "blob-storage-purger" uses indexParentsPurged.
        {
            "indexerType": "blob-storage-purger",
            "runId": "blob-purge-1",
            "runStartedAt": "2026-06-18T19:30:00.000Z",
            "runFinishedAt": "2026-06-18T19:30:05.000Z",
            "status": "finished",
            "indexParentsPurged": 7,
        },
    ]
    client, _, _ = _build_client(monkeypatch, runs=runs_fixture)
    body = client.get("/api/jobs/queue").json()
    rows = {row["job_type"]: row for row in body["items"]}

    assert rows["blob_index"]["last_run"] == {
        "started_at": "2026-06-18T20:00:00.000Z",
        "finished_at": "2026-06-18T20:00:03.000Z",
        "status": "finished",
        "indexed_count": 0,
    }
    assert rows["blob_purge"]["last_run"] == {
        "started_at": "2026-06-18T19:30:00.000Z",
        "finished_at": "2026-06-18T19:30:05.000Z",
        "status": "finished",
        "indexed_count": 7,
    }
    # nl2sql_index has no runs in the fixture.
    assert rows["nl2sql_index"]["last_run"] is None


def test_queue_last_run_handles_failed_run_without_indexed_count(monkeypatch):
    runs_fixture = [
        {
            "indexerType": "blob-storage-indexer",
            "runId": "blob-index-failed",
            "runStartedAt": "2026-06-18T20:00:00.000Z",
            "runFinishedAt": "2026-06-18T20:00:01.000Z",
            "status": "failed",
        },
    ]
    client, _, _ = _build_client(monkeypatch, runs=runs_fixture)
    body = client.get("/api/jobs/queue").json()
    rows = {row["job_type"]: row for row in body["items"]}
    last = rows["blob_index"]["last_run"]
    assert last is not None
    assert last["status"] == "failed"
    assert last["indexed_count"] is None

