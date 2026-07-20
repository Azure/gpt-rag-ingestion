"""Tests for telemetry.audit: run lifecycle, document events, and safety bounds.

Covers the disabled-mode regression, strict/lenient governance propagation,
confirmed-delete semantics, cancellation preservation, and a broken exporter
never turning a successful ingestion outcome into a failure.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from pathlib import Path

import jsonschema
import pytest
from azure.monitor.opentelemetry.exporter.export.logs._exporter import (
    _convert_log_to_envelope,
    _log_data_is_event,
)
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import (
    InMemoryLogExporter,
    SimpleLogRecordProcessor,
)

from telemetry import audit
from telemetry.audit_contract import (
    AUDIT_EVENT_PREFIX,
    AUDIT_LOG_BODY,
    ROOT_PARENT_EVENT_ID,
    AuditStatus,
    EventType,
    ReasonCode,
)


AUDIT_LOGGER = "gptrag.audit"


class Config:
    def __init__(self, values=None):
        self.values = values or {}

    def get(self, key, default=None, allow_none=False, **_kwargs):
        return self.values.get(key, default)


@pytest.fixture(autouse=True)
def _reset_audit_settings():
    """Every test starts from the fully-disabled default settings."""
    audit._settings = None
    audit._environment = "unknown"
    yield
    audit._settings = None
    audit._environment = "unknown"


def _events(caplog):
    return [
        record
        for record in caplog.records
        if record.name == AUDIT_LOGGER
    ]


class _FakeIndexingResult:
    def __init__(self, key, succeeded, error_message=None):
        self.key = key
        self.succeeded = succeeded
        self.error_message = error_message


# ---------------------------------------------------------------------------
# Run lifecycle
# ---------------------------------------------------------------------------


def test_successful_run_emits_exactly_one_started_and_one_completed_event(caplog):
    caplog.set_level(logging.INFO, logger=AUDIT_LOGGER)

    async def _scenario():
        async with audit.audit_run("blob_index"):
            pass

    asyncio.run(_scenario())

    events = _events(caplog)
    event_types = [record.event_type for record in events]
    assert event_types == ["ingestion.run.started", "ingestion.run.completed"]
    assert events[0].correlation_id == events[1].correlation_id
    assert events[0].parent_event_id == ROOT_PARENT_EVENT_ID
    assert events[1].parent_event_id == events[0].event_id
    assert (
        getattr(events[0], "microsoft.custom_event.name")
        == "gptrag.audit.ingestion.run.started"
    )
    assert (
        getattr(events[1], "microsoft.custom_event.name")
        == "gptrag.audit.ingestion.run.completed"
    )


@pytest.mark.parametrize(
    ("event_type", "status", "reason_code", "operation"),
    [
        (
            EventType.RUN_STARTED,
            AuditStatus.STARTED,
            ReasonCode.REQUEST_RECEIVED,
            "ingestion.run",
        ),
        (
            EventType.RUN_COMPLETED,
            AuditStatus.COMPLETED,
            ReasonCode.REQUEST_COMPLETED,
            "ingestion.run",
        ),
        (
            EventType.RUN_FAILED,
            AuditStatus.FAILED,
            ReasonCode.REQUEST_FAILED,
            "ingestion.run",
        ),
        (
            EventType.RUN_CANCELLED,
            AuditStatus.CANCELLED,
            ReasonCode.REQUEST_CANCELLED,
            "ingestion.run",
        ),
        (
            EventType.DOCUMENT_INDEXED,
            AuditStatus.COMPLETED,
            ReasonCode.OUTCOME_PRODUCED,
            "ingestion.document",
        ),
        (
            EventType.DOCUMENT_REJECTED,
            AuditStatus.REJECTED,
            ReasonCode.VALIDATION_FAILED,
            "ingestion.document",
        ),
        (
            EventType.DOCUMENT_DELETED,
            AuditStatus.COMPLETED,
            ReasonCode.OUTCOME_PRODUCED,
            "ingestion.document",
        ),
    ],
)
def test_all_event_types_export_with_valid_application_insights_wire_shape(
    event_type,
    status,
    reason_code,
    operation,
):
    log_provider = LoggerProvider()
    log_exporter = InMemoryLogExporter()
    log_provider.add_log_record_processor(SimpleLogRecordProcessor(log_exporter))
    handler = LoggingHandler(logger_provider=log_provider)
    previous_level = audit._logger.level
    audit._logger.setLevel(logging.INFO)
    audit._logger.addHandler(handler)

    try:
        event = audit._base_event(
            event_type,
            status=status,
            reason_code=reason_code,
            operation=operation,
            correlation_id="req_" + ("1" * 32),
            parent_event_id=None,
        )
        audit._emit(event)
    finally:
        audit._logger.removeHandler(handler)
        audit._logger.setLevel(previous_level)

    readable = log_exporter.get_finished_logs()[0]
    assert readable.log_record.body == AUDIT_LOG_BODY
    assert _log_data_is_event(readable) is True

    envelope = _convert_log_to_envelope(readable)
    assert envelope.data.base_data.name == f"{AUDIT_EVENT_PREFIX}{event_type.value}"
    assert (
        envelope.data.base_data.properties["parent_event_id"]
        == ROOT_PARENT_EVENT_ID
    )

    wire_schema = json.loads(
        (
            Path(__file__).resolve().parents[1]
            / "contracts"
            / "audit-event-v1.application-insights.schema.json"
        ).read_bytes()
    )
    jsonschema.Draft202012Validator(wire_schema).validate(
        {
            "name": envelope.data.base_data.name,
            "properties": dict(envelope.data.base_data.properties),
        }
    )


def test_marked_failed_run_emits_failed_terminal_event_without_raising(caplog):
    caplog.set_level(logging.INFO, logger=AUDIT_LOGGER)

    async def _scenario():
        async with audit.audit_run("blob_index") as run:
            try:
                raise RuntimeError("boom")
            except RuntimeError:
                run.mark_failed()

    asyncio.run(_scenario())

    events = _events(caplog)
    assert [record.event_type for record in events] == [
        "ingestion.run.started",
        "ingestion.run.failed",
    ]


def test_unhandled_exception_emits_failed_and_reraises(caplog):
    caplog.set_level(logging.INFO, logger=AUDIT_LOGGER)

    async def _scenario():
        async with audit.audit_run("blob_index"):
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        asyncio.run(_scenario())

    events = _events(caplog)
    assert [record.event_type for record in events] == [
        "ingestion.run.started",
        "ingestion.run.failed",
    ]


def test_cancelled_error_is_preserved_and_emits_cancelled_event(caplog):
    caplog.set_level(logging.INFO, logger=AUDIT_LOGGER)

    async def _scenario():
        async with audit.audit_run("blob_index"):
            raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(_scenario())

    events = _events(caplog)
    assert [record.event_type for record in events] == [
        "ingestion.run.started",
        "ingestion.run.cancelled",
    ]


def test_broken_exporter_does_not_raise_and_does_not_fail_the_run(monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger=AUDIT_LOGGER)

    def _boom(*_args, **_kwargs):
        raise OSError("exporter unavailable")

    monkeypatch.setattr(audit._logger, "info", _boom)

    async def _scenario():
        # Must not raise even though every _emit call fails.
        async with audit.audit_run("blob_index"):
            pass

    asyncio.run(_scenario())


# ---------------------------------------------------------------------------
# Document events
# ---------------------------------------------------------------------------


def test_disabled_mode_regression_emits_no_provenance_fields(caplog):
    caplog.set_level(logging.INFO, logger=AUDIT_LOGGER)
    audit.configure(Config())  # fully disabled defaults

    documents = [{"id": "doc-1", "content": "hello world"}]
    result = [_FakeIndexingResult("doc-1", succeeded=True)]

    audit.record_search_batch_result(
        operation="upload_documents",
        documents=documents,
        result=result,
        source_type="blob_storage",
    )

    events = _events(caplog)
    assert len(events) == 1
    assert events[0].event_type == "ingestion.document.indexed"
    for field in (
        "provenance_id",
        "source_uri_id",
        "content_checksum_sha256",
        "data_classification",
        "right_to_use",
    ):
        assert not hasattr(events[0], field)


def test_provenance_enabled_attaches_opaque_fields_and_never_the_raw_path(caplog):
    caplog.set_level(logging.INFO, logger=AUDIT_LOGGER)
    audit.configure(Config({"INGESTION_PROVENANCE_ENABLED": "true"}))

    raw_path = "documents/very/secret/path/contract.pdf"
    documents = [
        {
            "id": "doc-1",
            "parent_id": raw_path,
            "metadata_storage_last_modified": "2026-01-01T00:00:00Z",
            "content": "confidential content",
        }
    ]
    result = [_FakeIndexingResult("doc-1", succeeded=True)]

    audit.record_search_batch_result(
        operation="upload_documents",
        documents=documents,
        result=result,
        source_type="blob_storage",
    )

    event = _events(caplog)[0]
    assert event.source_uri_id.startswith("sha256:")
    assert raw_path not in event.source_uri_id
    assert event.content_checksum_sha256 == hashlib.sha256(
        b"confidential content"
    ).hexdigest()
    assert event.data_classification == "unclassified"
    assert event.right_to_use == "not_asserted"


def test_strict_governance_omits_fields_for_documents_without_explicit_metadata(caplog):
    caplog.set_level(logging.INFO, logger=AUDIT_LOGGER)
    audit.configure(
        Config(
            {
                "INGESTION_PROVENANCE_ENABLED": "true",
                "INGESTION_REQUIRE_GOVERNANCE_METADATA": "true",
            }
        )
    )

    documents = [{"id": "doc-1"}]
    result = [_FakeIndexingResult("doc-1", succeeded=True)]

    audit.record_search_batch_result(
        operation="upload_documents",
        documents=documents,
        result=result,
        source_type="blob_storage",
    )

    event = _events(caplog)[0]
    assert not hasattr(event, "data_classification")
    assert not hasattr(event, "right_to_use")
    assert hasattr(event, "provenance_id")  # provenance itself still attached


def test_indexed_and_rejected_are_derived_from_the_confirmed_batch_result(caplog):
    caplog.set_level(logging.INFO, logger=AUDIT_LOGGER)

    documents = [{"id": "doc-ok"}, {"id": "doc-bad"}]
    result = [
        _FakeIndexingResult("doc-ok", succeeded=True),
        _FakeIndexingResult("doc-bad", succeeded=False, error_message="quota exceeded"),
    ]

    audit.record_search_batch_result(
        operation="upload_documents",
        documents=documents,
        result=result,
        source_type="blob_storage",
    )

    by_type = sorted(record.event_type for record in _events(caplog))
    assert by_type == ["ingestion.document.indexed", "ingestion.document.rejected"]
    rejected = next(r for r in _events(caplog) if r.event_type == "ingestion.document.rejected")
    assert rejected.failure_type == "quota exceeded"


def test_deletion_only_emits_for_confirmed_successes(caplog):
    caplog.set_level(logging.INFO, logger=AUDIT_LOGGER)

    documents = [{"id": "doc-ok"}, {"id": "doc-bad"}]
    result = [
        _FakeIndexingResult("doc-ok", succeeded=True),
        _FakeIndexingResult("doc-bad", succeeded=False),
    ]

    audit.record_search_batch_result(
        operation="delete_documents",
        documents=documents,
        result=result,
        source_type="blob_storage",
    )

    events = _events(caplog)
    assert len(events) == 1
    assert events[0].event_type == "ingestion.document.deleted"


def test_document_events_correlate_to_the_active_run(caplog):
    caplog.set_level(logging.INFO, logger=AUDIT_LOGGER)

    async def _scenario():
        async with audit.audit_run("blob_index") as run:
            audit.record_search_batch_result(
                operation="upload_documents",
                documents=[{"id": "doc-1"}],
                result=[_FakeIndexingResult("doc-1", succeeded=True)],
                source_type="blob_storage",
            )
            return run

    run = asyncio.run(_scenario())

    events = _events(caplog)
    started = next(r for r in events if r.event_type == "ingestion.run.started")
    indexed = next(r for r in events if r.event_type == "ingestion.document.indexed")
    assert indexed.correlation_id == started.correlation_id == run.correlation_id
    assert indexed.parent_event_id == started.event_id
    assert indexed.parent_event_id != ROOT_PARENT_EVENT_ID


def test_document_audit_never_raises_even_with_a_malformed_result(caplog):
    caplog.set_level(logging.INFO, logger=AUDIT_LOGGER)

    # `result` is not iterable in the expected shape; this must be swallowed.
    audit.record_search_batch_result(
        operation="upload_documents",
        documents=[{"id": "doc-1"}],
        result=object(),
        source_type="blob_storage",
    )
    # No exception means the ingestion result was never put at risk.


def test_canary_large_batch_stays_within_bounds(caplog):
    caplog.set_level(logging.INFO, logger=AUDIT_LOGGER)

    documents = [{"id": f"doc-{i}"} for i in range(500)]
    result = [_FakeIndexingResult(f"doc-{i}", succeeded=True) for i in range(500)]

    audit.record_search_batch_result(
        operation="upload_documents",
        documents=documents,
        result=result,
        source_type="blob_storage",
    )

    events = _events(caplog)
    assert len(events) == 500
    assert all(len(record.getMessage()) < 2048 for record in events)
