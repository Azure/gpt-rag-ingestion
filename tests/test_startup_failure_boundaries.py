"""Startup configuration must fail before scheduling, without changing job isolation."""

import asyncio
import importlib
from types import ModuleType, SimpleNamespace
import sys
from unittest.mock import Mock

import pytest

import main


@pytest.fixture
def startup(monkeypatch):
    state = {"values": {"RUN_JOBS_ON_STARTUP": "false"}, "calls": []}

    class Config:
        def get(self, key, default=None, **kwargs):
            value = state["values"].get(key, default)
            if isinstance(value, Exception):
                raise value
            return value

    scheduler = SimpleNamespace(start=Mock(), add_job=Mock(), shutdown=Mock())
    monkeypatch.setattr(main, "get_config", lambda: Config())
    monkeypatch.setattr(main, "is_azure_environment", lambda: True)
    monkeypatch.setattr(main, "_mount_admin_and_panel_surface", lambda mode: None)
    monkeypatch.setattr(main.Telemetry, "configure_monitoring", lambda *args: None)
    monkeypatch.setattr(main.audit, "configure", lambda config: None)
    monkeypatch.setattr(main, "app_config_client", None)
    monkeypatch.setattr(main, "DEPLOYMENT_MODE", None)
    monkeypatch.setattr(main, "scheduler", scheduler)
    runtime = importlib.import_module("jobs.runtime")
    registry = {}
    monkeypatch.setattr(main, "JOB_REGISTRY", registry)
    monkeypatch.setattr(runtime, "JOB_REGISTRY", registry)
    monkeypatch.setattr(runtime, "_scheduler", scheduler)
    monkeypatch.setattr(main, "_track_running", lambda key, function: function)
    for name in ("run_blob_index", "run_blob_purge"):
        async def worker():
            state["calls"].append("worker")
        monkeypatch.setattr(main, name, worker)

    admin = ModuleType("api.admin")

    async def cleanup():
        state["calls"].append("cleanup")
        if state.get("cleanup_error"):
            raise state["cleanup_error"]

    admin._cleanup_old_runs = cleanup
    monkeypatch.setitem(sys.modules, "api.admin", admin)
    state["scheduler"] = scheduler
    return state


@pytest.mark.asyncio
async def test_startup_provider_error_cannot_disable_jobs_silently(startup):
    failure = RuntimeError("private-provider-canary")
    startup["values"]["RUN_JOBS_ON_STARTUP"] = failure
    with pytest.raises(RuntimeError) as raised:
        async with main.lifespan(main.app):
            pass
    assert raised.value is failure
    startup["scheduler"].start.assert_not_called()
    startup["scheduler"].add_job.assert_not_called()


@pytest.mark.asyncio
async def test_logging_configuration_defect_is_not_ignored(startup, monkeypatch):
    def fail(level):
        raise RuntimeError("logging configuration failed")

    monkeypatch.setattr(main.logging.getLogger("azure"), "setLevel", fail)
    with pytest.raises(RuntimeError, match="logging configuration failed"):
        async with main.lifespan(main.app):
            pass
    startup["scheduler"].start.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("run_jobs", ["true", "false"])
async def test_cleanup_failure_cannot_replace_startup_or_leak_payload(startup, caplog, run_jobs):
    startup["values"]["RUN_JOBS_ON_STARTUP"] = run_jobs
    startup["cleanup_error"] = RuntimeError("private-cleanup-canary")
    async with main.lifespan(main.app):
        await asyncio.sleep(0)
    assert startup["calls"] == ["cleanup"] + (["worker", "worker"] if run_jobs == "true" else [])
    startup["scheduler"].shutdown.assert_called_once_with(wait=False)
    assert "log-cleanup" in caplog.text
    assert "private-cleanup-canary" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [FileNotFoundError("private-cli"), RuntimeError("cli defect")])
async def test_cli_probe_recovers_only_process_start_errors(startup, monkeypatch, caplog, failure):
    monkeypatch.setattr(main, "is_azure_environment", lambda: False)
    monkeypatch.setenv("REQUIRE_AUTH_ON_STARTUP", "false")
    for name in ("IDENTITY_ENDPOINT", "MSI_ENDPOINT", "MSI_SECRET", "AZURE_CLIENT_SECRET"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(main.subprocess, "run", Mock(side_effect=failure))
    if isinstance(failure, RuntimeError):
        with pytest.raises(RuntimeError, match="cli defect"):
            async with main.lifespan(main.app):
                pass
        startup["scheduler"].start.assert_not_called()
    else:
        async with main.lifespan(main.app):
            await asyncio.sleep(0)
        assert "Azure CLI" in caplog.text
        assert "private-cli" not in caplog.text
