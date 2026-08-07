"""Conformance tests for the ingestion-side shapes of
``contracts/conversations-panel-v1.schema.json`` (issue #611, ADR-0004).

Mirrors ``tests/test_audit_contract.py``'s hash-pinning style for the shared
audit contract: the vendored schema copy must stay byte-identical to the
published Azure/GPT-RAG platform contract (PR #637), and the panel operator
Pydantic models must produce/accept payloads that validate against the exact
``$defs`` fragments this service consumes
(``OperatorOverviewMetricsResponse``, ``CorpusCurationQueueResponse``,
``CorpusCurationItem``, ``CorpusCurationDecisionRequest``,
``CorpusCurationDecisionResponse``, ``ErrorResponse``).
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import jsonschema
import pytest

from api.panel_operator import (
    CorpusCurationDecisionRequest,
    CorpusCurationDecisionResponse,
    CorpusCurationItem,
    CorpusCurationQueueResponse,
    OperatorOverviewMetricsResponse,
    OverviewCounts,
)

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "contracts" / "conversations-panel-v1.schema.json"
SHA256_PATH = ROOT / "contracts" / "conversations-panel-v1.sha256"


def test_vendored_schema_matches_pinned_hash():
    expected = {}
    for line in SHA256_PATH.read_bytes().decode("utf-8").splitlines():
        if not line.strip():
            continue
        digest, name = line.split(maxsplit=1)
        expected[name] = digest

    assert expected == {
        "conversations-panel-v1.schema.json": (
            "af432a8c17d217af539a92088dc4372b9270eef7f5331b0195eddbd01da00dc4"
        ),
    }

    content = SCHEMA_PATH.read_bytes()
    assert hashlib.sha256(content).hexdigest() == expected["conversations-panel-v1.schema.json"]


def _validator_for(fragment: str) -> jsonschema.Draft202012Validator:
    schema = json.loads(SCHEMA_PATH.read_bytes())
    sub_schema = {"$ref": f"#/$defs/{fragment}"}
    registry_schema = dict(schema)
    resolver = jsonschema.validators.RefResolver.from_schema(schema)
    return jsonschema.Draft202012Validator(sub_schema, resolver=resolver)


def test_overview_metrics_response_validates_against_schema():
    validator = _validator_for("OperatorOverviewMetricsResponse")
    body = OperatorOverviewMetricsResponse(
        schema_version=1,
        generated_at="2026-01-01T00:00:00.000000Z",
        correlation_id="req_" + "0" * 32,
        counts=OverviewCounts(
            conversation_count=10,
            feedback_count=None,
            corpus_pending_count=7,
            corpus_decided_count=None,
        ),
    ).model_dump(mode="json")

    validator.validate(body)


def test_overview_metrics_response_strictness_rejects_extra_field():
    validator = _validator_for("OperatorOverviewMetricsResponse")
    body = OperatorOverviewMetricsResponse(
        schema_version=1,
        generated_at="2026-01-01T00:00:00.000000Z",
        correlation_id="req_" + "0" * 32,
        counts=OverviewCounts(),
    ).model_dump(mode="json")
    body["unexpected"] = "value"

    with pytest.raises(jsonschema.ValidationError):
        validator.validate(body)


def test_corpus_curation_queue_response_validates_against_schema():
    validator = _validator_for("CorpusCurationQueueResponse")
    body = CorpusCurationQueueResponse(
        items=[
            CorpusCurationItem(
                item_id="cur_" + "a" * 32,
                document_id="blob-container/doc.pdf",
                title="doc.pdf",
                reason_code="processing_blocked",
                submitted_at="2026-01-01T00:00:00Z",
            )
        ],
        next_cursor=None,
    ).model_dump(mode="json")

    validator.validate(body)


def test_corpus_curation_item_never_carries_content_fields():
    schema = json.loads(SCHEMA_PATH.read_bytes())
    item_schema = schema["$defs"]["CorpusCurationItem"]

    assert set(item_schema["properties"]) == {
        "item_id",
        "document_id",
        "title",
        "reason_code",
        "submitted_at",
    }
    assert item_schema["additionalProperties"] is False


def test_corpus_curation_decision_request_validates_against_schema():
    validator = _validator_for("CorpusCurationDecisionRequest")
    body = CorpusCurationDecisionRequest(decision="approve", note="looks fine").model_dump(
        mode="json"
    )

    validator.validate(body)


def test_corpus_curation_decision_response_validates_against_schema():
    validator = _validator_for("CorpusCurationDecisionResponse")
    body = CorpusCurationDecisionResponse(
        item_id="cur_" + "a" * 32,
        decision="approve",
        decided_at="2026-01-01T00:00:00Z",
    ).model_dump(mode="json")

    validator.validate(body)


def test_correlation_id_pattern_matches_audit_event_v1():
    schema = json.loads(SCHEMA_PATH.read_bytes())
    assert schema["$defs"]["CorrelationId"]["pattern"] == r"^req_[0-9a-f]{32}$"
