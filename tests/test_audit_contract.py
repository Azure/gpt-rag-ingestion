"""Tests for the pinned audit-event-v1 contract and INGESTION_* governance settings.

Mirrors the validation style used by ``Azure/gpt-rag-orchestrator`` for the
same shared contract: hash-pinned artifacts, an exact ingestion event
taxonomy, and settings that fail closed on contradictory configuration.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import jsonschema
import pytest

from telemetry.audit_contract import (
    EventType,
    GovernanceSettings,
    AuditConfigurationError,
    format_utc,
    new_correlation_id,
    new_event_id,
    utc_now,
)


ROOT = Path(__file__).resolve().parents[1]

EXPECTED_INGESTION_EVENT_TYPES = {
    "ingestion.run.started",
    "ingestion.run.completed",
    "ingestion.run.failed",
    "ingestion.run.cancelled",
    "ingestion.document.indexed",
    "ingestion.document.rejected",
    "ingestion.document.deleted",
}


class Config:
    """Minimal stand-in for ``tools.appconfig.AppConfigClient``."""

    def __init__(self, values=None):
        self.values = values or {}

    def get(self, key, default=None, allow_none=False, **_kwargs):
        return self.values.get(key, default)


def test_published_contract_hashes_match_artifacts():
    """The vendored schema copies must stay byte-identical to the pinned SHA-256s.

    Reads raw bytes with no line-ending normalization so the pin is stable
    whether the check runs on Windows or inside the Linux container.
    """
    expected = {}
    for line in (ROOT / "contracts" / "audit-event-v1.sha256").read_bytes().decode(
        "utf-8"
    ).splitlines():
        digest, name = line.split(maxsplit=1)
        expected[name] = digest

    assert expected == {
        "audit-event-v1.schema.json": "825db8ef40a81e2c19e5d80d37c565b6b47fc9a6540e9881d35cc12b8fde5aab",
        "audit-event-v1.application-insights.schema.json": "066c8f5408610ab839d5121d06ca5bc59e8797e551d5c47c875c5ba52f7e0588",
    }

    for name, digest in expected.items():
        content = (ROOT / "contracts" / name).read_bytes()
        assert hashlib.sha256(content).hexdigest() == digest


def test_ingestion_taxonomy_is_exact_and_matches_both_schemas():
    logical_schema = json.loads(
        (ROOT / "contracts" / "audit-event-v1.schema.json").read_bytes()
    )
    wire_schema = json.loads(
        (
            ROOT / "contracts" / "audit-event-v1.application-insights.schema.json"
        ).read_bytes()
    )

    python_event_types = {event_type.value for event_type in EventType}
    assert python_event_types == EXPECTED_INGESTION_EVENT_TYPES

    assert EXPECTED_INGESTION_EVENT_TYPES <= set(
        logical_schema["properties"]["event_type"]["enum"]
    )
    assert EXPECTED_INGESTION_EVENT_TYPES <= set(
        wire_schema["properties"]["properties"]["properties"]["event_type"]["enum"]
    )
    assert EXPECTED_INGESTION_EVENT_TYPES <= {
        name.removeprefix("gptrag.audit.")
        for name in wire_schema["properties"]["name"]["enum"]
    }


def test_legacy_ingestion_aliases_are_rejected_by_the_schema():
    """No document-level alias (``ingestion.document.selected``, etc.) is canonical."""
    logical_schema = json.loads(
        (ROOT / "contracts" / "audit-event-v1.schema.json").read_bytes()
    )
    legacy_aliases = {
        "ingestion.request.started",
        "ingestion.request.completed",
        "ingestion.document.selected",
        "ingestion.outcome.produced",
    }
    schema_event_types = set(logical_schema["properties"]["event_type"]["enum"])
    assert not (legacy_aliases & schema_event_types)


@pytest.mark.parametrize(
    "fixture_name",
    ["audit_event_v1_ingestion_run.json", "audit_event_v1_ingestion_document.json"],
)
def test_ingestion_goldens_validate_against_the_logical_schema(fixture_name):
    schema = json.loads((ROOT / "contracts" / "audit-event-v1.schema.json").read_bytes())
    golden = json.loads((ROOT / "tests" / "golden" / fixture_name).read_bytes())
    jsonschema.Draft202012Validator(schema).validate(golden)


def test_ids_and_timestamp_have_canonical_shapes():
    import re

    assert re.fullmatch(r"evt_[0-9a-f]{32}", new_event_id())
    assert re.fullmatch(r"req_[0-9a-f]{32}", new_correlation_id())
    assert re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z", format_utc(utc_now())
    )


# ---------------------------------------------------------------------------
# GovernanceSettings
# ---------------------------------------------------------------------------


def test_defaults_are_fully_disabled():
    settings = GovernanceSettings.from_config(Config())

    assert settings.provenance_enabled is False
    assert settings.require_governance_metadata is False
    assert settings.default_classification == "unclassified"
    assert settings.default_right_to_use == "not_asserted"


def test_provenance_off_and_governance_on_is_an_invalid_configuration():
    with pytest.raises(AuditConfigurationError):
        GovernanceSettings.from_config(
            Config(
                {
                    "INGESTION_PROVENANCE_ENABLED": "false",
                    "INGESTION_REQUIRE_GOVERNANCE_METADATA": "true",
                }
            )
        )


def test_provenance_and_governance_both_enabled_is_valid():
    settings = GovernanceSettings.from_config(
        Config(
            {
                "INGESTION_PROVENANCE_ENABLED": "true",
                "INGESTION_REQUIRE_GOVERNANCE_METADATA": "true",
            }
        )
    )
    assert settings.provenance_enabled is True
    assert settings.require_governance_metadata is True


def test_non_strict_mode_falls_back_to_configured_defaults():
    settings = GovernanceSettings.from_config(
        Config(
            {
                "INGESTION_PROVENANCE_ENABLED": "true",
                "INGESTION_DEFAULT_CLASSIFICATION": "internal",
                "INGESTION_DEFAULT_RIGHT_TO_USE": "licensed",
            }
        )
    )

    classification, right_to_use, was_explicit = settings.resolve_governance({})

    assert classification == "internal"
    assert right_to_use == "licensed"
    assert was_explicit is False


def test_strict_mode_never_fabricates_governance_metadata_from_defaults():
    settings = GovernanceSettings.from_config(
        Config(
            {
                "INGESTION_PROVENANCE_ENABLED": "true",
                "INGESTION_REQUIRE_GOVERNANCE_METADATA": "true",
            }
        )
    )

    classification, right_to_use, was_explicit = settings.resolve_governance({})

    assert classification is None
    assert right_to_use is None
    assert was_explicit is False


def test_strict_mode_accepts_explicitly_supplied_document_metadata():
    settings = GovernanceSettings.from_config(
        Config(
            {
                "INGESTION_PROVENANCE_ENABLED": "true",
                "INGESTION_REQUIRE_GOVERNANCE_METADATA": "true",
            }
        )
    )

    classification, right_to_use, was_explicit = settings.resolve_governance(
        {"data_classification": "confidential", "right_to_use": "licensed"}
    )

    assert classification == "confidential"
    assert right_to_use == "licensed"
    assert was_explicit is True


def test_strict_mode_requires_both_classification_and_right_to_use():
    settings = GovernanceSettings.from_config(
        Config(
            {
                "INGESTION_PROVENANCE_ENABLED": "true",
                "INGESTION_REQUIRE_GOVERNANCE_METADATA": "true",
            }
        )
    )

    classification, right_to_use, was_explicit = settings.resolve_governance(
        {"data_classification": "confidential"}
    )

    assert classification is None
    assert right_to_use is None
    assert was_explicit is False
