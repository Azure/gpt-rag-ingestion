"""Top-level scheduled workers must expose primary-operation failures."""

import importlib
import asyncio
import logging
import sys
import types

import pytest

import main


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("function", "module_name", "class_name"),
    [
        ("run_blob_index", "jobs.blob_storage_indexer", "BlobStorageDocumentIndexer"),
        ("run_blob_purge", "jobs.blob_storage_indexer", "BlobStorageDeletedItemsCleaner"),
        ("run_sharepoint_index", "jobs.sharepoint_indexer", "SharePointIndexer"),
        ("run_sharepoint_purge", "jobs.sharepoint_purger", "SharePointPurger"),
        ("run_nl2sql_index", "jobs.nl2sql_indexer", "NL2SQLIndexer"),
        ("run_nl2sql_purge", "jobs.nl2sql_purger", "NL2SQLPurger"),
        ("run_images_purge", "jobs.multimodal_images_purger", "ImagesDeletedFilesPurger"),
    ],
)
async def test_worker_failure_reaches_scheduler_and_clears_running_slot(
    monkeypatch, caplog, function, module_name, class_name
):
    class FailingWorker:
        async def run(self):
            raise RuntimeError("worker failed")

    module = types.ModuleType(module_name)
    setattr(module, class_name, FailingWorker)
    monkeypatch.setitem(sys.modules, module_name, module)
    monkeypatch.setattr(main, "app_config_client", types.SimpleNamespace(get=lambda *args: "true"))

    caplog.set_level(logging.INFO, logger="gptrag.audit")
    monkeypatch.setattr(main, "audit", importlib.import_module("telemetry.audit"))
    runtime = importlib.import_module("jobs.runtime")
    wrapped = runtime.track_running(function, getattr(main, function))
    with pytest.raises(RuntimeError, match="worker failed"):
        await wrapped()
    assert [record.event_type for record in caplog.records if record.name == "gptrag.audit"] == [
        "ingestion.run.started", "ingestion.run.failed",
    ]
    assert function not in runtime.running_jobs


@pytest.mark.asyncio
async def test_startup_continues_independent_jobs_after_one_worker_fails(monkeypatch):
    """Cron/manual errors propagate without changing startup's per-job isolation."""
    class Config:
        def get(self, key, default=None, **kwargs):
            return {"RUN_JOBS_ON_STARTUP": "true"}.get(key, default)

    class Scheduler:
        def start(self):
            pass

        def add_job(self, *args, **kwargs):
            pass

        def shutdown(self, **kwargs):
            pass

    monkeypatch.setattr(main, "get_config", lambda: Config())
    monkeypatch.setattr(main, "is_azure_environment", lambda: True)
    monkeypatch.setattr(main, "_mount_admin_and_panel_surface", lambda mode: None)
    monkeypatch.setattr(main.Telemetry, "configure_monitoring", lambda *args: None)
    monkeypatch.setattr(main.audit, "configure", lambda config: None)
    runtime = importlib.import_module("jobs.runtime")
    scheduler = Scheduler()
    registry = {}
    monkeypatch.setattr(main, "scheduler", scheduler)
    monkeypatch.setattr(runtime, "_scheduler", scheduler)
    monkeypatch.setattr(main, "JOB_REGISTRY", registry)
    monkeypatch.setattr(runtime, "JOB_REGISTRY", registry)
    # Lifespan writes these globals, so make monkeypatch restore their prior values.
    monkeypatch.setattr(main, "app_config_client", None)
    monkeypatch.setattr(main, "DEPLOYMENT_MODE", None)

    calls = []
    completed = asyncio.Event()

    async def cleanup():
        calls.append("cleanup")

    async def fail():
        calls.append("index")
        raise RuntimeError("worker failed")

    async def succeed():
        calls.append("purge")
        completed.set()

    admin = types.ModuleType("api.admin")
    admin._cleanup_old_runs = cleanup
    monkeypatch.setitem(sys.modules, "api.admin", admin)
    monkeypatch.setattr(main, "run_blob_index", fail)
    monkeypatch.setattr(main, "run_blob_purge", succeed)

    async with main.lifespan(main.app):
        await asyncio.wait_for(completed.wait(), timeout=1)
    assert calls == ["cleanup", "index", "purge"]
