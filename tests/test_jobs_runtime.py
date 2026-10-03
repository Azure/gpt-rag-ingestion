"""Scheduler ownership and completion/cancellation behavior."""

import asyncio
import importlib
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.asyncio
async def test_tracking_preserves_manual_id_and_cleans_up_after_failure():
    runtime = importlib.import_module("jobs.runtime")
    runtime.running_jobs.clear()
    runtime.running_jobs["blob_index"] = {"run_id": "manual-blob_index-1", "started_at": None}

    async def fail():
        assert runtime.running_jobs["blob_index"]["run_id"] == "manual-blob_index-1"
        raise RuntimeError("dependency unavailable")

    with pytest.raises(RuntimeError, match="dependency unavailable"):
        await runtime.track_running("blob_index", fail)()
    assert runtime.running_jobs == {}


@pytest.mark.asyncio
async def test_cron_tracking_cleans_up_cancellation():
    runtime = importlib.import_module("jobs.runtime")
    runtime.running_jobs.clear()
    started = asyncio.Event()

    async def wait():
        started.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(runtime.track_running("blob_index", wait)())
    await started.wait()
    assert runtime.running_jobs["blob_index"]["run_id"] == "blob_index"
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert runtime.running_jobs == {}


def test_importing_runtime_does_not_initialize_workers_or_configuration():
    result = subprocess.run(
        [sys.executable, "-c", "import sys; import jobs.runtime; "
         "assert 'dependencies' not in sys.modules; "
         "assert 'jobs.blob_storage_indexer' not in sys.modules; "
         "assert 'jobs.sharepoint_indexer' not in sys.modules"],
        cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
