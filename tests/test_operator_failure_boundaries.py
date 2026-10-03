"""Exercise operator diagnostics without hiding adapter or scheduler defects."""

import asyncio
import builtins
from datetime import datetime, timezone
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from azure.core.exceptions import AzureError, ResourceNotFoundError
import pytest

from tests.test_admin_jobs_queue import _build_client


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["client", "download", "parse"])
async def test_log_download_does_not_hide_programming_errors(monkeypatch, stage):
    _, _, admin = _build_client(monkeypatch)
    failure = RuntimeError("private-log-canary")
    downloader = SimpleNamespace(readall=AsyncMock(return_value=b"{}"))
    blob = SimpleNamespace(download_blob=AsyncMock(return_value=downloader))
    container = SimpleNamespace(get_blob_client=lambda name: blob)
    if stage == "client":
        def fail(name):
            raise failure
        container.get_blob_client = fail
    elif stage == "download":
        blob.download_blob.side_effect = failure
    else:
        monkeypatch.setattr(admin.json, "loads", lambda value: (_ for _ in ()).throw(failure))
    with pytest.raises(RuntimeError) as raised:
        await admin._download_blob(container, "job/runs/log.json", asyncio.Semaphore(1))
    assert raised.value is failure


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [b"not-json", b"\xff"])
async def test_unreadable_optional_log_is_safe_and_explicit(monkeypatch, caplog, raw):
    _, _, admin = _build_client(monkeypatch)
    blob = SimpleNamespace(download_blob=AsyncMock(return_value=SimpleNamespace(
        readall=AsyncMock(return_value=raw),
    )))
    container = SimpleNamespace(get_blob_client=lambda name: blob)
    assert await admin._download_blob(container, "job/runs/log.json", asyncio.Semaphore(1)) is None
    assert "Failed to read" in caplog.text
    assert raw.decode(errors="replace") not in caplog.text


@pytest.mark.asyncio
async def test_optional_log_sdk_failure_does_not_leak_payload(monkeypatch, caplog):
    _, _, admin = _build_client(monkeypatch)
    blob = SimpleNamespace(download_blob=AsyncMock(side_effect=AzureError("private-log-canary")))
    container = SimpleNamespace(get_blob_client=lambda name: blob)
    assert await admin._download_blob(container, "job/runs/log.json", asyncio.Semaphore(1)) is None
    assert "AzureError" in caplog.text
    assert "private-log-canary" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [AzureError("private-delete-canary"), RuntimeError("defect")])
async def test_log_cleanup_counts_only_confirmed_deletes(monkeypatch, caplog, failure):
    _, _, admin = _build_client(monkeypatch)

    async def blobs(**kwargs):
        for day in (1, 2, 3):
            yield SimpleNamespace(name=f"job/runs/{day}.json",
                                  last_modified=datetime(2026, 1, day, tzinfo=timezone.utc))

    async def delete(name):
        if name.endswith("1.json"):
            raise failure

    container = SimpleNamespace(list_blobs=blobs, delete_blob=AsyncMock(side_effect=delete))
    monkeypatch.setattr(admin, "get_config", lambda: SimpleNamespace(
        get=lambda key, default=None, **kwargs: 1 if key == "MAX_LOG_RUN_FILES" else default,
    ))
    monkeypatch.setattr(admin, "_get_blob_service", AsyncMock(return_value=SimpleNamespace(
        get_container_client=lambda name: container,
    )))
    with caplog.at_level(logging.INFO):
        if isinstance(failure, RuntimeError):
            with pytest.raises(RuntimeError, match="defect"):
                await admin._cleanup_old_runs()
            assert "Log cleanup:" not in caplog.text
        else:
            await admin._cleanup_old_runs()
            assert "deleted 1" in caplog.text
            assert "failed 1" in caplog.text
            assert "private-delete-canary" not in caplog.text
    assert container.delete_blob.await_count == 2


@pytest.mark.parametrize("failure", [AzureError("private-history"), RuntimeError("defect")])
def test_queue_history_outage_is_not_a_programming_error(monkeypatch, caplog, failure):
    client, _, admin = _build_client(monkeypatch)
    monkeypatch.setattr(admin, "_cached_load", AsyncMock(side_effect=failure))
    if isinstance(failure, RuntimeError):
        with pytest.raises(RuntimeError, match="defect"):
            client.get("/api/jobs/queue")
    else:
        response = client.get("/api/jobs/queue")
        assert response.status_code == 200
        assert all(row["last_run"] is None for row in response.json()["items"])
        assert "history" in caplog.text
        assert "private-history" not in caplog.text


def test_queue_does_not_fabricate_missing_schedule_on_scheduler_defect(monkeypatch):
    client, state, _ = _build_client(monkeypatch)

    def fail(job_id):
        raise RuntimeError("scheduler defect")

    monkeypatch.setattr(state["main"].scheduler, "get_job", fail)
    with pytest.raises(RuntimeError, match="scheduler defect"):
        client.get("/api/jobs/queue")


def test_cron_import_defect_is_not_reported_as_no_trigger(monkeypatch):
    _, _, admin = _build_client(monkeypatch)
    original = builtins.__import__

    def fail(name, *args, **kwargs):
        if name == "apscheduler.triggers.cron":
            raise RuntimeError("import defect")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fail)
    with pytest.raises(RuntimeError, match="import defect"):
        admin._cron_trigger_to_string(object())


def test_datetime_adapter_defect_is_not_reported_as_missing_time(monkeypatch):
    _, _, admin = _build_client(monkeypatch)

    class BrokenDateTime(datetime):
        def astimezone(self, tz=None):
            raise RuntimeError("timezone defect")

    with pytest.raises(RuntimeError, match="timezone defect"):
        admin._iso_utc(BrokenDateTime(2026, 1, 1, tzinfo=timezone.utc))


@pytest.mark.parametrize("value", [None, "invalid", 42])
def test_missing_or_invalid_timestamp_keeps_null_shape(monkeypatch, value):
    _, _, admin = _build_client(monkeypatch)
    assert admin._iso_utc(value) is None


@pytest.mark.parametrize("endpoint", ["/api/identity", "/api/config"])
@pytest.mark.parametrize("failure", [RuntimeError("private-auth-canary"), TypeError("bad claims")])
def test_read_only_auth_probe_stays_200_but_never_grants_admin(monkeypatch, caplog, endpoint, failure):
    client, _, _ = _build_client(monkeypatch, tenant_id="tenant", claims=failure)
    response = client.get(endpoint)
    assert response.status_code == 200
    assert response.json()["authEnabled"] is True
    assert response.json()["isAdmin" if endpoint.endswith("identity") else "canEdit"] is False
    assert "private-auth-canary" not in response.text + caplog.text


def test_put_schedule_failure_preserves_write_and_reports_partial_failure(monkeypatch, caplog):
    from tests.test_admin_config import _build_client as config_client

    client, state, admin = config_client(monkeypatch, tenant_id=None, claims=None)

    def fail(*args):
        raise RuntimeError("private-scheduler-canary")

    monkeypatch.setattr(admin, "_reschedule_cron_job", fail)
    response = client.put("/api/config", json={"updates": [
        {"key": "CRON_RUN_BLOB_INDEX", "value": "0 * * * *"},
    ]})
    assert response.status_code == 207
    assert response.json() == {
        "applied": ["CRON_RUN_BLOB_INDEX"],
        "failed": [{"key": "CRON_RUN_BLOB_INDEX", "error": "reschedule failed"}],
        "rescheduled": [],
    }
    assert len(state["written"]) == 1
    assert "private-scheduler-canary" not in response.text + caplog.text


@pytest.mark.parametrize("failure", [AzureError("private-history"), RuntimeError("defect")])
def test_optional_retry_enrichment_only_recovers_sdk_outages(monkeypatch, caplog, failure):
    client, _, admin = _build_client(monkeypatch)

    async def load(key, loader):
        if key == "files":
            raise failure
        return [], []

    monkeypatch.setattr(admin, "_cached_load", load)
    if isinstance(failure, RuntimeError):
        with pytest.raises(RuntimeError, match="defect"):
            client.get("/api/jobs")
    else:
        response = client.get("/api/jobs")
        assert response.status_code == 200
        assert response.json()["items"] == []
        assert "Retry history unavailable" in caplog.text
        assert "private-history" not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["download", "read"])
@pytest.mark.parametrize("failure,status", [
    (ResourceNotFoundError("private-read"), 404),
    (AzureError("private-read"), 500),
    (RuntimeError("private-read BlobNotFound"), 500),
])
async def test_unblock_read_failure_without_writing(monkeypatch, caplog, stage, failure, status):
    client, _, admin = _build_client(monkeypatch)
    reader = SimpleNamespace(readall=AsyncMock(return_value=b"{}"))
    blob = SimpleNamespace(download_blob=AsyncMock(return_value=reader), upload_blob=AsyncMock())
    (blob.download_blob if stage == "download" else reader.readall).side_effect = failure
    container = SimpleNamespace(get_blob_client=lambda name: blob)
    monkeypatch.setattr(admin, "_get_blob_service", AsyncMock(return_value=SimpleNamespace(
        get_container_client=lambda name: container,
    )))
    invalidate = Mock()
    monkeypatch.setattr(admin, "_invalidate_cache", invalidate)
    response = client.post("/api/files/unblock", params={"blobName": "job/files/document.json"})
    assert response.status_code == status
    assert response.json()["detail"] == ("File log not found" if status == 404 else "File log could not be read")
    blob.upload_blob.assert_not_awaited()
    invalidate.assert_not_called()
    assert "private-read" not in caplog.text
    assert "private-read" not in response.text


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", [b"private-corrupt", b"\xff", b"[]", b"null", b'"private-corrupt"', b"{}"])
async def test_unblock_requires_valid_log_before_writing(monkeypatch, caplog, raw):
    client, _, admin = _build_client(monkeypatch)
    blob = SimpleNamespace(
        download_blob=AsyncMock(return_value=SimpleNamespace(readall=AsyncMock(return_value=raw))),
        upload_blob=AsyncMock(),
    )
    monkeypatch.setattr(admin, "_get_blob_service", AsyncMock(return_value=SimpleNamespace(
        get_container_client=lambda name: SimpleNamespace(get_blob_client=lambda name: blob),
    )))
    invalidate = Mock()
    monkeypatch.setattr(admin, "_invalidate_cache", invalidate)
    response = client.post("/api/files/unblock", params={"blobName": "job/files/document.json"})
    if raw == b"{}":
        assert response.status_code == 200
        blob.upload_blob.assert_awaited_once()
        invalidate.assert_called_once_with("files")
    else:
        assert response.status_code == 500
        assert response.json()["detail"] == "File log is invalid"
        blob.upload_blob.assert_not_awaited()
        invalidate.assert_not_called()
    assert "private-corrupt" not in response.text + caplog.text
