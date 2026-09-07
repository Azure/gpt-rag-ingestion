"""Preserve bounded hostile-protocol omission and optional audit context recovery."""

from collections.abc import Mapping, Sequence
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from telemetry import audit
from telemetry.audit_sanitizer import sanitize_event
from tests.test_audit_sanitizer import _base_event


class HostileMapping(Mapping):
    def __init__(self, stage):
        self.stage = stage

    def __len__(self):
        return 1

    def __iter__(self):
        yield "key"

    def __getitem__(self, key):
        raise RuntimeError("private-mapping")

    def items(self):
        if self.stage == "initial":
            raise RuntimeError("private-mapping")
        return super().items()


class HostileSequence(Sequence):
    def __len__(self):
        return 1

    def __getitem__(self, key):
        raise RuntimeError("private-sequence")

    def __iter__(self):
        raise RuntimeError("private-sequence")


@pytest.mark.parametrize("stage", ["mapping-initial", "mapping-late", "sequence-initial"])
def test_hostile_nested_protocol_is_omitted_without_changing_required_event(stage):
    event = _base_event()
    event["failure_type"] = HostileSequence() if stage == "sequence-initial" else HostileMapping(
        "initial" if stage == "mapping-initial" else "late",
    )
    result = sanitize_event(event, additional_redacted_keys=frozenset())
    assert result.attributes["event_id"] == event["event_id"]
    assert "failure_type" in result.attributes["omitted_fields"]
    assert "failure_type" not in result.attributes
    assert "private-" not in result.serialized


@pytest.mark.parametrize("field", ["omitted_fields", "truncated_fields"])
def test_hostile_diagnostic_path_sequence_is_recorded_as_omitted(field):
    event = _base_event()
    event[field] = HostileSequence()
    result = sanitize_event(event, additional_redacted_keys=frozenset())
    assert field in result.attributes["omitted_fields"]
    assert result.attributes["event_id"] == event["event_id"]
    assert "private-" not in result.serialized


@pytest.mark.parametrize("failure", [
    FileNotFoundError("private-version"), UnicodeError("private-version"), RuntimeError("defect"),
])
def test_version_lookup_recovers_only_file_or_encoding_failures(monkeypatch, caplog, failure):
    monkeypatch.setattr(audit, "_service_version", None)
    monkeypatch.setattr(audit, "_VERSION_FILE", SimpleNamespace(read_text=Mock(side_effect=failure)))
    if isinstance(failure, RuntimeError):
        with pytest.raises(RuntimeError) as raised:
            audit._read_service_version()
        assert raised.value is failure
    else:
        assert audit._read_service_version() == "0.0.0"
        assert "version" in caplog.text.lower()
    assert "private-version" not in caplog.text


def test_optional_environment_context_failure_does_not_change_governance(monkeypatch, caplog):
    monkeypatch.setattr(audit, "_settings", None)
    monkeypatch.setattr(audit, "_environment", "original")

    def get(key, default=None, **kwargs):
        if key in {"ENVIRONMENT_NAME", "AZURE_ENV_NAME"}:
            raise RuntimeError("private-environment")
        return {"INGESTION_PROVENANCE_ENABLED": True}.get(key, default)

    settings = audit.configure(SimpleNamespace(get=get))
    assert settings.provenance_enabled is True
    assert audit._environment == "unknown"
    assert "environment" in caplog.text.lower()
    assert "private-environment" not in caplog.text
