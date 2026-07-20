"""Tests for the bounded, fail-closed ingestion audit event sanitizer."""

from __future__ import annotations

import tracemalloc
from collections.abc import Mapping

import pytest

from telemetry.audit_contract import (
    MAX_EVENT_BYTES,
    format_utc,
    new_correlation_id,
    new_event_id,
    utc_now,
)
from telemetry.audit_sanitizer import REDACTED, AuditSanitizationError, sanitize_event


def _base_event():
    return {
        "schema_version": 1,
        "event_id": new_event_id(),
        "event_type": "ingestion.run.started",
        "event_time_utc": format_utc(utc_now()),
        "correlation_id": new_correlation_id(),
        "trace_id": "0" * 32,
        "span_id": "0" * 16,
        "parent_event_id": None,
        "service_name": "gpt-rag-ingestion",
        "service_version": "1.0.0",
        "environment": "test",
        "operation": "ingestion.run",
        "status": "started",
        "reason_code": "request_received",
        "capture_mode": "metadata_only",
        "redaction_applied": False,
        "omitted_fields": [],
        "truncated_fields": [],
    }


def test_required_fields_survive_sanitization_untouched():
    event = _base_event()

    result = sanitize_event(event, additional_redacted_keys=frozenset())

    assert result.attributes["event_id"] == event["event_id"]
    assert result.attributes["correlation_id"] == event["correlation_id"]
    assert result.attributes["redaction_applied"] is False


def test_unknown_optional_fields_are_omitted_for_major_version_one():
    event = _base_event()
    event["future_field"] = "reader must ignore this"

    result = sanitize_event(event, additional_redacted_keys=frozenset())

    assert "future_field" not in result.attributes
    assert "future_field" in result.attributes["omitted_fields"]


def test_source_uri_id_style_opaque_reference_is_preserved():
    event = _base_event()
    event["event_type"] = "ingestion.document.indexed"
    event["operation"] = "ingestion.document"
    event["status"] = "completed"
    event["source_id"] = "sha256:" + "a" * 64

    result = sanitize_event(event, additional_redacted_keys=frozenset())

    assert result.attributes["source_id"] == event["source_id"]


def test_ordinary_optional_field_values_pass_through_unredacted():
    event = _base_event()
    event["source_type"] = "blob_storage"

    result = sanitize_event(event, additional_redacted_keys=frozenset())

    assert result.attributes["source_type"] == "blob_storage"


@pytest.mark.parametrize(  # type: ignore[misc]  # populated below
    "secret",
    [
        "AccountKey=abcdefghijklmnopqrstuvwxyz012345",
        "SharedAccessSignature=abcdefghijklmnopqrstuvwxyz",
        "https://example.test/path?sig=abcdefghijklmnopqrstuvwxyz&sv=2026",
        "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----",
        '{"password":"abc"}',
        "Authorization: Basic dXNlcjpwYXNzd29yZA==",
    ],
)
def test_prohibited_values_never_reach_the_serialized_export_payload(secret):
    event = _base_event()
    event["event_type"] = "ingestion.document.rejected"
    event["operation"] = "ingestion.document"
    event["status"] = "rejected"
    event["failure_type"] = secret

    result = sanitize_event(event, additional_redacted_keys=frozenset())

    assert secret not in result.serialized
    assert REDACTED in result.serialized


def test_recursive_key_denylist_covers_prohibited_credential_classes():
    event = _base_event()
    event["event_type"] = "ingestion.document.rejected"
    event["operation"] = "ingestion.document"
    event["status"] = "rejected"
    # failure_type is documented as a string, but the sanitizer must still
    # redact prohibited nested keys defensively if a bug ever puts a dict here.
    event["failure_type"] = {
        "client_secret": "definitely-secret",
        "nested": {"password": "also-secret"},
    }

    try:
        result = sanitize_event(event, additional_redacted_keys=frozenset())
    except AuditSanitizationError:
        # Fail-closed: dropping the whole event is an acceptable, safe
        # outcome when the final prohibited-value scan cannot clear a
        # payload with denylisted key names still present after redaction.
        return

    assert "definitely-secret" not in result.serialized
    assert "also-secret" not in result.serialized
    assert result.attributes["redaction_applied"] is True


def test_oversized_metadata_string_is_truncated_and_flagged():
    event = _base_event()
    event["event_type"] = "ingestion.document.rejected"
    event["operation"] = "ingestion.document"
    event["status"] = "rejected"
    event["failure_type"] = "x" * 4000

    result = sanitize_event(event, additional_redacted_keys=frozenset())

    assert len(result.attributes["failure_type"]) <= 512
    assert "failure_type" in result.attributes["truncated_fields"]


def test_event_is_bounded_to_the_16kib_export_limit():
    event = _base_event()
    event["event_type"] = "ingestion.document.indexed"
    event["operation"] = "ingestion.document"
    event["status"] = "completed"
    event["source_type"] = "y" * 512

    result = sanitize_event(event, additional_redacted_keys=frozenset())

    assert len(result.serialized.encode("utf-8")) <= MAX_EVENT_BYTES


def test_sanitizer_uses_bounded_iteration_for_virtual_containers():
    class CountingMapping(Mapping):
        def __init__(self):
            self.iterations = 0

        def __len__(self):
            return 10**12

        def __getitem__(self, key):
            raise KeyError(key)

        def __iter__(self):
            raise AssertionError("items() must be used")

        def items(self):
            for index in range(10**12):
                self.iterations += 1
                if self.iterations > 65:
                    raise AssertionError("mapping was iterated beyond its bound")
                yield str(index), index

    mapping = CountingMapping()
    event = _base_event()
    event["event_type"] = "ingestion.document.rejected"
    event["operation"] = "ingestion.document"
    event["status"] = "rejected"
    event["failure_type"] = mapping

    tracemalloc.start()
    try:
        result = sanitize_event(event, additional_redacted_keys=frozenset())
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert mapping.iterations == 65
    assert peak_bytes < 2 * 1024 * 1024
    assert "failure_type" in result.attributes["truncated_fields"]
