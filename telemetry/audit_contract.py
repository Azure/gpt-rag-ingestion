"""Versioned contract and governance configuration for ingestion audit events.

This module mirrors the shared, versioned audit event contract owned by
``Azure/GPT-RAG`` and pinned in ``contracts/audit-event-v1.schema.json`` /
``contracts/audit-event-v1.application-insights.schema.json``. The
orchestrator (``Azure/gpt-rag-orchestrator``) emits ``request.*``,
``route.*``, ``tool.*`` and ``outcome.*`` event types; this service emits
only the seven ``ingestion.*`` event types the shared schema reserves for it.
The two schema files and their SHA-256 pins in ``contracts/`` must stay
byte-identical to the orchestrator's copies — see
``tests/test_audit_contract.py``.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any


SCHEMA_VERSION = 1
SERVICE_NAME = "gpt-rag-ingestion"
AUDIT_LOGGER_NAME = "gptrag.audit"
AUDIT_WARNING_LOGGER_NAME = "gptrag.audit_warning"
AUDIT_EVENT_PREFIX = "gptrag.audit."
AUDIT_LOG_BODY = "GPT-RAG audit event"
ROOT_PARENT_EVENT_ID = f"evt_{'0' * 32}"

# Bounds mirrored from the shared contract implementation so a single
# ingestion run (or one document batch) can never grow an unbounded audit
# payload. These are enforced by ``audit_sanitizer.sanitize_event``.
MAX_EVENT_BYTES = 16 * 1024
MAX_ATTRIBUTES = 60
MAX_METADATA_STRING = 512
MAX_SENSITIVE_STRING = 2048
MAX_DEPTH = 6
MAX_COLLECTION_ITEMS = 64
MAX_EMITTED_ARRAY_ITEMS = 32
MAX_SANITIZER_NODES = 256
MAX_AUDIT_DURATION_MS = 86_400_000.0
MAX_ENVIRONMENT_LENGTH = 64


class AuditConfigurationError(ValueError):
    """Raised when enabled governance configuration is unsafe or incomplete."""


class EventType(StrEnum):
    RUN_STARTED = "ingestion.run.started"
    RUN_COMPLETED = "ingestion.run.completed"
    RUN_FAILED = "ingestion.run.failed"
    RUN_CANCELLED = "ingestion.run.cancelled"
    DOCUMENT_INDEXED = "ingestion.document.indexed"
    DOCUMENT_REJECTED = "ingestion.document.rejected"
    DOCUMENT_DELETED = "ingestion.document.deleted"


# The shared JSON Schema also reserves the orchestrator's request/route/tool/
# outcome event names. This service deliberately never emits them.
RUN_TERMINAL_EVENT_TYPES = frozenset(
    {EventType.RUN_COMPLETED, EventType.RUN_FAILED, EventType.RUN_CANCELLED}
)


class AuditStatus(StrEnum):
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class ReasonCode(StrEnum):
    NONE = "none"
    REQUEST_RECEIVED = "request_received"
    REQUEST_COMPLETED = "request_completed"
    REQUEST_FAILED = "request_failed"
    REQUEST_CANCELLED = "request_cancelled"
    OUTCOME_PRODUCED = "outcome_produced"
    OUTCOME_REJECTED = "outcome_rejected"
    VALIDATION_FAILED = "validation_failed"
    SERIALIZATION_FAILURE = "serialization_failure"
    EXPORT_FAILURE = "export_failure"
    UNKNOWN = "unknown"


class CaptureMode(StrEnum):
    # Ingestion audit events never capture document content, so the shared
    # contract's sensitive-content capture mode does not apply here.
    METADATA_ONLY = "metadata_only"


# Ingestion never emits prompt/response/tool-argument style sensitive
# content, so no field is exempted from the metadata-string bound.
SENSITIVE_FIELDS: frozenset[str] = frozenset()

# The ten optional provenance fields this issue introduces. They are additive
# to the shared contract (which allows unknown optional properties) and are
# only attached when INGESTION_PROVENANCE_ENABLED=true.
PROVENANCE_FIELDS = frozenset(
    {
        "provenance_id",
        "source_uri_id",
        "source_version_id",
        "content_checksum_sha256",
        "ingested_at",
        "ingest_run_id",
        "data_classification",
        "right_to_use",
        "retention_class",
        "delete_after",
    }
)

REQUIRED_FIELDS = frozenset(
    {
        "schema_version",
        "event_id",
        "event_type",
        "event_time_utc",
        "correlation_id",
        "trace_id",
        "span_id",
        "parent_event_id",
        "service_name",
        "service_version",
        "environment",
        "operation",
        "status",
        "reason_code",
        "capture_mode",
        "redaction_applied",
        "omitted_fields",
        "truncated_fields",
    }
)

OPTIONAL_FIELDS = frozenset(
    {
        "started_at_utc",
        "duration_ms",
        "input_count",
        "output_count",
        "source_id",
        "source_type",
        "failure_type",
        "outcome_type",
        *PROVENANCE_FIELDS,
    }
)


def new_event_id() -> str:
    return f"evt_{uuid.uuid4().hex}"


def new_correlation_id() -> str:
    return f"req_{uuid.uuid4().hex}"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def logical_parent_to_wire(parent_event_id: str | None) -> str:
    """Encode a logical root parent for Azure Monitor string properties."""
    return ROOT_PARENT_EVENT_ID if parent_event_id is None else parent_event_id


def wire_parent_to_logical(parent_event_id: str) -> str | None:
    """Decode the Azure Monitor root sentinel to the logical null parent."""
    return None if parent_event_id == ROOT_PARENT_EVENT_ID else parent_event_id


def _config_value(config: Any, key: str, default: Any) -> Any:
    try:
        return config.get(key, default=default, allow_none=True)
    except TypeError:
        return config.get(key, default)
    except Exception:
        return default


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class GovernanceSettings:
    """Resolved ``INGESTION_*`` governance flags.

    ``provenance_enabled`` controls whether the ten optional provenance
    fields are attached to ``ingestion.document.*`` events at all. Audit
    event emission itself (run lifecycle and document lifecycle) is always
    best-effort and is never gated by these flags — disabling provenance
    only omits the provenance fields, it does not disable the audit trail.
    """

    provenance_enabled: bool
    require_governance_metadata: bool
    default_classification: str
    default_right_to_use: str

    @classmethod
    def from_config(cls, config: Any) -> "GovernanceSettings":
        provenance_enabled = _as_bool(
            _config_value(config, "INGESTION_PROVENANCE_ENABLED", "false")
        )
        require_governance_metadata = _as_bool(
            _config_value(config, "INGESTION_REQUIRE_GOVERNANCE_METADATA", "false")
        )
        if require_governance_metadata and not provenance_enabled:
            raise AuditConfigurationError(
                "INGESTION_REQUIRE_GOVERNANCE_METADATA=true requires "
                "INGESTION_PROVENANCE_ENABLED=true. Strict governance metadata "
                "cannot be enforced while provenance capture itself is disabled; "
                "set INGESTION_PROVENANCE_ENABLED=true or disable "
                "INGESTION_REQUIRE_GOVERNANCE_METADATA."
            )

        default_classification = (
            str(
                _config_value(
                    config, "INGESTION_DEFAULT_CLASSIFICATION", "unclassified"
                )
                or "unclassified"
            ).strip()
            or "unclassified"
        )
        default_right_to_use = (
            str(
                _config_value(
                    config, "INGESTION_DEFAULT_RIGHT_TO_USE", "not_asserted"
                )
                or "not_asserted"
            ).strip()
            or "not_asserted"
        )

        return cls(
            provenance_enabled=provenance_enabled,
            require_governance_metadata=require_governance_metadata,
            default_classification=default_classification,
            default_right_to_use=default_right_to_use,
        )

    def resolve_governance(
        self, document: dict[str, Any] | None
    ) -> tuple[str | None, str | None, bool]:
        """Resolve ``(data_classification, right_to_use, was_explicit)`` for a document.

        In strict mode (``require_governance_metadata=True``) the configured
        defaults are never substituted for a document that did not supply its
        own classification and right-to-use — a default must not falsely
        satisfy strict governance. The document is still indexed normally;
        only the audit event omits the fields it cannot truthfully claim.
        """
        doc = document or {}
        explicit_classification = doc.get("data_classification")
        explicit_right_to_use = doc.get("right_to_use")
        was_explicit = bool(explicit_classification) and bool(explicit_right_to_use)

        if self.require_governance_metadata:
            if not was_explicit:
                return None, None, False
            return str(explicit_classification), str(explicit_right_to_use), True

        classification = (
            str(explicit_classification)
            if explicit_classification
            else self.default_classification
        )
        right_to_use = (
            str(explicit_right_to_use)
            if explicit_right_to_use
            else self.default_right_to_use
        )
        return classification, right_to_use, was_explicit
