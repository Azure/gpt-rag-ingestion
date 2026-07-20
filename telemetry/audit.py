"""Bounded, best-effort emission of GPT-RAG ingestion audit events over OpenTelemetry.

Reuses the existing OpenTelemetry logging pipeline configured in
``telemetry.telemetry.Telemetry.configure_monitoring`` (Azure Monitor /
Application Insights) — no separate audit backend or durable queue is
introduced. Every public function here is best-effort: a failure while
building, sanitizing, or exporting an audit event is logged as a warning and
swallowed. It must never turn a successful ingestion run, document index, or
document deletion into a failure.
"""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Iterable

from opentelemetry import trace

from .audit_contract import (
    AUDIT_LOG_BODY,
    AUDIT_LOGGER_NAME,
    AUDIT_WARNING_LOGGER_NAME,
    MAX_ENVIRONMENT_LENGTH,
    SCHEMA_VERSION,
    SERVICE_NAME,
    AuditStatus,
    CaptureMode,
    EventType,
    GovernanceSettings,
    ReasonCode,
    format_utc,
    new_correlation_id,
    new_event_id,
    utc_now,
)
from .audit_sanitizer import AuditSanitizationError, sanitize_event


_logger = logging.getLogger(AUDIT_LOGGER_NAME)
_warning_logger = logging.getLogger(AUDIT_WARNING_LOGGER_NAME)

# The shared contract's ``operation`` field is the event category (matching
# the orchestrator's convention and the ingestion golden fixtures), not the
# specific job/source name — that identity is carried in the optional
# ``source_type`` field instead.
_RUN_OPERATION = "ingestion.run"
_DOCUMENT_OPERATION = "ingestion.document"

_settings: GovernanceSettings | None = None
_environment = "unknown"
_service_version: str | None = None

_VERSION_FILE = Path(__file__).resolve().parent.parent / "VERSION"


def _read_service_version() -> str:
    global _service_version
    if _service_version is not None:
        return _service_version
    try:
        _service_version = _VERSION_FILE.read_text(encoding="utf-8").strip() or "0.0.0"
    except Exception:
        _service_version = "0.0.0"
    return _service_version


def configure(config: Any) -> GovernanceSettings:
    """Resolve governance settings from App Configuration at startup.

    Raises ``AuditConfigurationError`` (a ``ValueError``) for a contradictory
    configuration (``INGESTION_REQUIRE_GOVERNANCE_METADATA=true`` while
    ``INGESTION_PROVENANCE_ENABLED=false``) so misconfiguration fails fast at
    boot with an actionable message instead of silently ingesting.
    """
    global _settings, _environment
    _settings = GovernanceSettings.from_config(config)
    try:
        env_value = config.get(
            "ENVIRONMENT_NAME",
            default=config.get("AZURE_ENV_NAME", default="unknown", allow_none=True),
            allow_none=True,
        )
    except Exception:
        env_value = "unknown"
    _environment = str(env_value or "unknown")[:MAX_ENVIRONMENT_LENGTH]
    _logger.setLevel(logging.INFO)
    return _settings


def settings() -> GovernanceSettings:
    """Return the resolved governance settings, defaulting to fully disabled."""
    global _settings
    if _settings is None:
        _settings = GovernanceSettings(
            provenance_enabled=False,
            require_governance_metadata=False,
            default_classification="unclassified",
            default_right_to_use="not_asserted",
        )
    return _settings


@dataclass(slots=True)
class RunContext:
    """Tracks the single active ingestion run for contextvar propagation."""

    source_type: str
    correlation_id: str
    started_event_id: str
    started_at_utc: str
    _start_monotonic: float = field(default_factory=time.monotonic)
    _failed: bool = False

    def mark_failed(self) -> None:
        """Record that the wrapped run body caught and logged an error.

        Callers that swallow exceptions internally (matching this service's
        existing "log and continue" scheduler behavior) call this so the
        terminal audit event reports ``failed`` instead of ``completed``.
        """
        self._failed = True

    @property
    def has_failed(self) -> bool:
        return self._failed

    @property
    def duration_ms(self) -> float:
        return max((time.monotonic() - self._start_monotonic) * 1000.0, 0.0)


_current_run: contextvars.ContextVar[RunContext | None] = contextvars.ContextVar(
    "gptrag_ingestion_audit_run", default=None
)


def _trace_fields() -> tuple[str, str]:
    span_context = trace.get_current_span().get_span_context()
    if not span_context.is_valid:
        return "0" * 32, "0" * 16
    return f"{span_context.trace_id:032x}", f"{span_context.span_id:016x}"


def _base_event(
    event_type: EventType,
    *,
    status: AuditStatus,
    reason_code: ReasonCode,
    operation: str,
    correlation_id: str,
    parent_event_id: str | None,
) -> dict[str, Any]:
    trace_id, span_id = _trace_fields()
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": new_event_id(),
        "event_type": event_type.value,
        "event_time_utc": format_utc(utc_now()),
        "correlation_id": correlation_id,
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_event_id": parent_event_id,
        "service_name": SERVICE_NAME,
        "service_version": _read_service_version(),
        "environment": _environment,
        "operation": operation,
        "status": status.value,
        "reason_code": reason_code.value,
        "capture_mode": CaptureMode.METADATA_ONLY.value,
        "redaction_applied": False,
        "omitted_fields": [],
        "truncated_fields": [],
    }


def _emit(event: dict[str, Any]) -> None:
    """Sanitize and log an audit event. Never raises."""
    try:
        result = sanitize_event(event, additional_redacted_keys=frozenset())
    except AuditSanitizationError:
        _warning_logger.warning(
            "Audit event failed sanitization and was dropped (event_type=%s).",
            event.get("event_type"),
            exc_info=True,
        )
        return
    except Exception:
        _warning_logger.warning(
            "Audit event sanitization raised unexpectedly; event dropped (event_type=%s).",
            event.get("event_type"),
            exc_info=True,
        )
        return
    try:
        _logger.info(AUDIT_LOG_BODY, extra=result.attributes)
    except Exception:
        _warning_logger.warning(
            "Audit event export failed and was dropped (event_type=%s).",
            event.get("event_type"),
            exc_info=True,
        )


@asynccontextmanager
async def audit_run(operation: str) -> AsyncIterator[RunContext]:
    """Bracket one ingestion run with exactly one started and one terminal event.

    Usage::

        async with audit.audit_run("blob_index") as run:
            try:
                await BlobStorageDocumentIndexer().run()
            except Exception:
                logging.exception(...)
                run.mark_failed()

    ``asyncio.CancelledError`` is always re-raised after emitting
    ``ingestion.run.cancelled`` best-effort, preserving task cancellation
    semantics. Any other exception that escapes the ``async with`` body is
    reported as ``ingestion.run.failed`` and re-raised unchanged.
    """
    correlation_id = new_correlation_id()
    started_event_id = new_event_id()
    started_at_utc = format_utc(utc_now())
    run = RunContext(
        source_type=operation,
        correlation_id=correlation_id,
        started_event_id=started_event_id,
        started_at_utc=started_at_utc,
    )

    started = _base_event(
        EventType.RUN_STARTED,
        status=AuditStatus.STARTED,
        reason_code=ReasonCode.REQUEST_RECEIVED,
        operation=_RUN_OPERATION,
        correlation_id=correlation_id,
        parent_event_id=None,
    )
    started["event_id"] = started_event_id
    started["source_type"] = operation[:512]
    _emit(started)

    token = _current_run.set(run)
    try:
        yield run
    except asyncio.CancelledError:
        _emit_terminal(run, EventType.RUN_CANCELLED, AuditStatus.CANCELLED, ReasonCode.REQUEST_CANCELLED)
        raise
    except Exception:
        _emit_terminal(run, EventType.RUN_FAILED, AuditStatus.FAILED, ReasonCode.REQUEST_FAILED)
        raise
    else:
        if run.has_failed:
            _emit_terminal(run, EventType.RUN_FAILED, AuditStatus.FAILED, ReasonCode.REQUEST_FAILED)
        else:
            _emit_terminal(run, EventType.RUN_COMPLETED, AuditStatus.COMPLETED, ReasonCode.REQUEST_COMPLETED)
    finally:
        _current_run.reset(token)


def _emit_terminal(
    run: RunContext, event_type: EventType, status: AuditStatus, reason_code: ReasonCode
) -> None:
    event = _base_event(
        event_type,
        status=status,
        reason_code=reason_code,
        operation=_RUN_OPERATION,
        correlation_id=run.correlation_id,
        parent_event_id=run.started_event_id,
    )
    event["source_type"] = run.source_type[:512]
    event["started_at_utc"] = run.started_at_utc
    event["duration_ms"] = round(run.duration_ms, 3)
    _emit(event)


# ---------------------------------------------------------------------------
# Document-level events (indexed / rejected / deleted)
# ---------------------------------------------------------------------------


def _opaque_id(*parts: str) -> str:
    """A stable, non-reversible, unkeyed reference — never the raw value.

    Deliberately plain SHA-256 (not HMAC): this service has no existing
    keyed-pseudonymization convention and the goal here is only to keep raw
    URIs/paths/filenames out of telemetry, not to defend against offline
    dictionary attacks on a known, low-cardinality identifier space. See the
    README governance section for this caveat.
    """
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def _result_key(item: Any) -> str | None:
    if isinstance(item, dict):
        return item.get("key")
    return getattr(item, "key", None)


def _result_succeeded(item: Any) -> bool:
    if item is None:
        return True
    if isinstance(item, dict):
        return bool(item.get("succeeded", True))
    return bool(getattr(item, "succeeded", True))


def _result_error(item: Any) -> str | None:
    if item is None:
        return None
    if isinstance(item, dict):
        return item.get("error_message") or item.get("errorMessage")
    return getattr(item, "error_message", None)


def _provenance_fields(document: dict[str, Any], *, ingest_run_id: str) -> dict[str, Any]:
    doc_id = str(document.get("id") or "")
    parent_id = str(document.get("parent_id") or document.get("metadata_storage_path") or "")
    source_uri_id = _opaque_id("source_uri", parent_id or doc_id)
    version_hint = str(document.get("metadata_storage_last_modified") or "")
    source_version_id = _opaque_id("source_version", doc_id, version_hint) if version_hint else None
    provenance_id = _opaque_id("provenance", source_uri_id, ingest_run_id)

    fields: dict[str, Any] = {
        "provenance_id": provenance_id,
        "source_uri_id": source_uri_id,
        "ingested_at": format_utc(utc_now()),
        "ingest_run_id": ingest_run_id,
    }
    if source_version_id:
        fields["source_version_id"] = source_version_id

    content = document.get("content")
    if isinstance(content, str) and content:
        fields["content_checksum_sha256"] = hashlib.sha256(
            content.encode("utf-8")
        ).hexdigest()

    classification, right_to_use, _explicit = settings().resolve_governance(document)
    if classification:
        fields["data_classification"] = classification
    if right_to_use:
        fields["right_to_use"] = right_to_use

    # Pass-through only: these carry no INGESTION_DEFAULT_* flag and are never
    # fabricated. `delete_after` is policy intent only — it never triggers an
    # automatic purge; only a confirmed deletion emits ingestion.document.deleted.
    retention_class = document.get("retention_class")
    if retention_class:
        fields["retention_class"] = str(retention_class)
    delete_after = document.get("delete_after")
    if delete_after:
        fields["delete_after"] = str(delete_after)

    return fields


def record_search_batch_result(
    *,
    operation: str,
    documents: Iterable[dict[str, Any]] | None,
    result: Any,
    source_type: str,
    key_field: str = "id",
) -> None:
    """Best-effort ``ingestion.document.*`` emission for a confirmed Azure AI Search batch result.

    ``operation`` is ``"upload_documents"`` or ``"delete_documents"`` (the
    Azure SDK method name — callers pass ``func.__name__`` directly).
    ``documents`` is the request payload in call order; ``result`` is the
    list the SDK returned (each item exposes ``key``/``succeeded``, as either
    an ``IndexingResult`` object or a dict, matched back to ``documents`` by
    key so ordering differences cannot mis-attribute an outcome). ``key_field``
    identifies the document key property when it is not ``"id"``. This never
    raises: a bug here must never affect an already-confirmed index or
    delete result.
    """
    try:
        _record_search_batch_result(
            operation=operation,
            documents=documents,
            result=result,
            source_type=source_type,
            key_field=key_field,
        )
    except Exception:
        _warning_logger.warning(
            "Document audit emission failed; the indexing/deletion result is unaffected.",
            exc_info=True,
        )


def _record_search_batch_result(
    *,
    operation: str,
    documents: Iterable[dict[str, Any]] | None,
    result: Any,
    source_type: str,
    key_field: str = "id",
) -> None:
    if operation not in ("upload_documents", "delete_documents"):
        return
    docs = [
        doc for doc in (documents or []) if isinstance(doc, dict) and doc.get(key_field)
    ]
    if not docs or result is None:
        return

    by_key: dict[str, Any] = {}
    for item in result:
        key = _result_key(item)
        if key is not None:
            by_key[key] = item

    run = _current_run.get()
    correlation_id = run.correlation_id if run else new_correlation_id()
    parent_event_id = run.started_event_id if run else None
    ingest_run_id = run.correlation_id if run else correlation_id

    provenance_enabled = settings().provenance_enabled

    for document in docs:
        doc_id = str(document[key_field])
        item = by_key.get(doc_id)
        succeeded = _result_succeeded(item)

        if operation == "upload_documents":
            event_type = (
                EventType.DOCUMENT_INDEXED if succeeded else EventType.DOCUMENT_REJECTED
            )
            status = AuditStatus.COMPLETED if succeeded else AuditStatus.REJECTED
            reason_code = (
                ReasonCode.OUTCOME_PRODUCED if succeeded else ReasonCode.VALIDATION_FAILED
            )
        else:  # delete_documents
            if not succeeded:
                # No "deletion failed" event exists in the canonical taxonomy;
                # a confirmed deletion is the only positive signal we report.
                continue
            event_type = EventType.DOCUMENT_DELETED
            status = AuditStatus.COMPLETED
            reason_code = ReasonCode.OUTCOME_PRODUCED

        event = _base_event(
            event_type,
            status=status,
            reason_code=reason_code,
            operation=_DOCUMENT_OPERATION,
            correlation_id=correlation_id,
            parent_event_id=parent_event_id,
        )
        event["source_type"] = source_type[:512]
        event["output_count"] = 1
        if not succeeded:
            error = _result_error(item)
            if error:
                event["failure_type"] = str(error)[:512]

        if provenance_enabled and event_type in (
            EventType.DOCUMENT_INDEXED,
            EventType.DOCUMENT_DELETED,
        ):
            event.update(_provenance_fields(document, ingest_run_id=ingest_run_id))

        _emit(event)
