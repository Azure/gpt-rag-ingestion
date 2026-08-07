"""Operator-facing hosted administrative panel surfaces (issue #611, ADR-0004).

Exposes:

- ``GET /panel/overview/metrics`` -- privacy-safe aggregate counts.
- ``GET /panel/corpus-curation/queue`` -- document/corpus curation queue.
- ``POST /panel/corpus-curation/{item_id}/decision`` -- record a curation
  decision.

These are the exact shapes the platform contract publishes under
``contracts/conversations-panel-v1.schema.json``
(``OperatorOverviewMetricsResponse``, ``CorpusCurationQueueResponse``,
``CorpusCurationItem``, ``CorpusCurationDecisionRequest``,
``CorpusCurationDecisionResponse``) -- every model here is strict
(``extra="forbid"``) and bounded to match.

This module never reads or exposes Foundry managed Conversation message
bodies and holds no Conversations data-plane access at all:

- The overview metrics endpoint only ever crosses the Cosmos Data Reader
  boundary (owner-index / feedback containers, issue #611 platform
  contract) with an aggregate ``COUNT(1)`` query -- never a per-item read
  -- and suppresses any bucket below ``PANEL_OVERVIEW_MIN_CARDINALITY``.
- The corpus curation queue and decision endpoints operate entirely over
  ``tools.corpus_curation_store``, the existing blocked-file-log control
  store this service identity already owns (see that module's docstring for
  why Cosmos is never used for curation writes).

Fails closed (503) unless ``DEPLOY_ADMINISTRATIVE_PANEL=true``,
``PANEL_OPERATOR_SURFACES_ENABLED=true``, and an explicit operator app role
or group is configured (``PANEL_OPERATOR_APP_ROLE`` /
``PANEL_OPERATOR_GROUP_ID``). Every endpoint requires a validated delegated
(per-user) bearer token carrying that role/group; app-only tokens are always
rejected. Only correlation ids and coarse audit metadata are logged --
never tokens, queries, or document content.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, List, Literal, Optional

from fastapi import APIRouter, HTTPException, Query, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field

from dependencies import get_config, validate_delegated_operator_bearer
from dependencies import operator_role_or_group_configured
from telemetry.audit_contract import new_correlation_id
from tools.corpus_curation_store import (
    CurationConcurrencyExhausted,
    CurationDecisionConflict,
    CurationItemNotFound,
)

router = APIRouter()
_bearer_scheme = HTTPBearer(
    auto_error=False,
    description="Delegated operator bearer token (app-only tokens are rejected).",
)

_QUEUE_PAGE_SIZE = 25
_ITEM_ID_RE = re.compile(r"^cur_[0-9a-f]{32}$")
_CURSOR_VERSION = 1


def _flag(config: Any, key: str) -> bool:
    value = config.get(key, default="false", allow_none=True)
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _int_setting(config: Any, key: str, default: int) -> int:
    try:
        return int(config.get(key, default=default, allow_none=True) or default)
    except (TypeError, ValueError):
        return default


def _cursor_secret(config: Any) -> str:
    return str(config.get("DATA_INGEST_APP_APIKEY", default="", allow_none=True) or "")


def _require_gate_enabled(config: Any) -> None:
    """Fail closed (503) unless every panel operator surface gate is met."""
    if not (
        _flag(config, "DEPLOY_ADMINISTRATIVE_PANEL")
        and _flag(config, "PANEL_OPERATOR_SURFACES_ENABLED")
        and operator_role_or_group_configured()
        and _cursor_secret(config)
    ):
        raise HTTPException(
            status_code=503,
            detail=(
                "Panel operator surfaces are disabled until "
                "DEPLOY_ADMINISTRATIVE_PANEL=true, "
                "PANEL_OPERATOR_SURFACES_ENABLED=true, an explicit operator "
                "app role or group is configured, and the cursor signing "
                "secret is available."
            ),
        )


def _redact_oid(oid: Optional[str]) -> str:
    if not oid:
        return "<none>"
    return hashlib.sha256(oid.encode("utf-8")).hexdigest()[:12]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    padded = data + "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def _encode_cursor(offset: int, *, oid: str, config: Any) -> str:
    ttl = _int_setting(config, "PANEL_CURSOR_TTL_SECONDS", 600)
    payload = {
        "v": _CURSOR_VERSION,
        "oid": oid,
        "offset": offset,
        "exp": int(time.time()) + ttl,
    }
    payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    secret = _cursor_secret(config).encode("utf-8")
    signature = hmac.new(secret, payload_bytes, hashlib.sha256).digest()
    return f"{_b64url_encode(payload_bytes)}.{_b64url_encode(signature)}"


def _decode_cursor(cursor: str, *, expected_oid: str, config: Any) -> int:
    """Decode and verify an opaque cursor. Any tamper/expiry/oid mismatch is 422."""
    try:
        payload_part, sig_part = cursor.split(".", 1)
        payload_bytes = _b64url_decode(payload_part)
        signature = _b64url_decode(sig_part)
    except Exception:
        raise HTTPException(status_code=422, detail="Malformed pagination cursor.")

    secret = _cursor_secret(config).encode("utf-8")
    expected_signature = hmac.new(secret, payload_bytes, hashlib.sha256).digest()
    if not hmac.compare_digest(signature, expected_signature):
        raise HTTPException(status_code=422, detail="Invalid pagination cursor.")

    try:
        payload = json.loads(payload_bytes)
    except Exception:
        raise HTTPException(status_code=422, detail="Malformed pagination cursor.")

    if not isinstance(payload, dict) or payload.get("v") != _CURSOR_VERSION:
        raise HTTPException(status_code=422, detail="Unsupported pagination cursor version.")
    if payload.get("oid") != expected_oid:
        raise HTTPException(status_code=422, detail="Pagination cursor does not match caller.")
    exp = payload.get("exp")
    if not isinstance(exp, int) or exp < int(time.time()):
        raise HTTPException(status_code=422, detail="Pagination cursor expired.")
    offset = payload.get("offset")
    if not isinstance(offset, int) or offset < 0:
        raise HTTPException(status_code=422, detail="Malformed pagination cursor.")
    return offset


# ---------------------------------------------------------------------------
# Wire models -- mirror contracts/conversations-panel-v1.schema.json exactly.
# ---------------------------------------------------------------------------


class OverviewCounts(BaseModel):
    model_config = ConfigDict(extra="forbid")

    conversation_count: Optional[int] = Field(default=None, ge=0)
    feedback_count: Optional[int] = Field(default=None, ge=0)
    corpus_pending_count: Optional[int] = Field(default=None, ge=0)
    corpus_decided_count: Optional[int] = Field(default=None, ge=0)


class OperatorOverviewMetricsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    generated_at: str
    correlation_id: str = Field(pattern=r"^req_[0-9a-f]{32}$")
    counts: OverviewCounts


class CorpusCurationItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    item_id: str = Field(min_length=1, max_length=128)
    document_id: str = Field(min_length=1, max_length=512)
    title: str = Field(max_length=512)
    reason_code: str = Field(max_length=64)
    submitted_at: str


class CorpusCurationQueueResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: List[CorpusCurationItem] = Field(default_factory=list, max_length=100)
    next_cursor: Optional[str] = Field(default=None, min_length=1, max_length=2048)


class CorpusCurationDecisionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision: Literal["approve", "reject", "defer"]
    note: Optional[str] = Field(default=None, max_length=2000)


class CorpusCurationDecisionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    item_id: str = Field(min_length=1, max_length=128)
    decision: Literal["approve", "reject", "defer"]
    decided_at: str


# ---------------------------------------------------------------------------
# GET /panel/overview/metrics
# ---------------------------------------------------------------------------


@router.get(
    "/panel/overview/metrics",
    response_model=OperatorOverviewMetricsResponse,
    responses={
        401: {"description": "Missing or invalid bearer token."},
        403: {"description": "App-only token, or missing operator role/group."},
        502: {"description": "Panel metadata or corpus control store query failed."},
        503: {"description": "Panel operator surfaces are disabled."},
    },
)
async def overview_metrics(
    request: Request,
    _authorization: Optional[HTTPAuthorizationCredentials] = Security(_bearer_scheme),
) -> OperatorOverviewMetricsResponse:
    config = get_config()
    _require_gate_enabled(config)
    bearer = await validate_delegated_operator_bearer(request)
    oid = bearer.claims.get("oid")

    correlation_id = new_correlation_id()
    min_cardinality = _int_setting(config, "PANEL_OVERVIEW_MIN_CARDINALITY", 5)

    owner_index_container = config.get(
        "PANEL_OWNER_INDEX_DATABASE_CONTAINER",
        default="panel-conversation-owner-index",
        allow_none=True,
    )
    feedback_container = config.get(
        "PANEL_FEEDBACK_DATABASE_CONTAINER", default="panel-feedback", allow_none=True
    )

    try:
        from tools.cosmosdb import CosmosDBClient

        cosmos = CosmosDBClient()
        conversation_count = await cosmos.count_documents(owner_index_container)
        feedback_count = await cosmos.count_documents(feedback_container)
    except Exception:
        logging.error(
            "[panel-operator] overview_metrics correlation_id=%s: Cosmos query failed.",
            correlation_id,
        )
        raise HTTPException(status_code=502, detail="Panel metadata store query failed.")

    try:
        from tools.corpus_curation_store import CorpusCurationStore

        store = CorpusCurationStore()
        pending_count, decided_count = await store.count_pending_and_decided()
    except Exception:
        logging.error(
            "[panel-operator] overview_metrics correlation_id=%s: corpus control store query failed.",
            correlation_id,
        )
        raise HTTPException(status_code=502, detail="Corpus control store query failed.")

    def _suppress(count: int) -> Optional[int]:
        return count if count >= min_cardinality else None

    logging.info(
        "[panel-operator] overview_metrics correlation_id=%s operator=%s",
        correlation_id,
        _redact_oid(oid),
    )

    return OperatorOverviewMetricsResponse(
        schema_version=1,
        generated_at=_utc_now_iso(),
        correlation_id=correlation_id,
        counts=OverviewCounts(
            conversation_count=_suppress(conversation_count),
            feedback_count=_suppress(feedback_count),
            corpus_pending_count=_suppress(pending_count),
            corpus_decided_count=_suppress(decided_count),
        ),
    )


# ---------------------------------------------------------------------------
# GET /panel/corpus-curation/queue
# ---------------------------------------------------------------------------


@router.get(
    "/panel/corpus-curation/queue",
    response_model=CorpusCurationQueueResponse,
    responses={
        401: {"description": "Missing or invalid bearer token."},
        403: {"description": "App-only token, or missing operator role/group."},
        422: {"description": "Tampered, expired, or cross-principal cursor."},
        502: {"description": "Corpus control store query failed."},
        503: {"description": "Panel operator surfaces are disabled."},
    },
)
async def corpus_curation_queue(
    request: Request,
    cursor: Optional[str] = Query(default=None, min_length=1, max_length=2048),
    _authorization: Optional[HTTPAuthorizationCredentials] = Security(_bearer_scheme),
) -> CorpusCurationQueueResponse:
    config = get_config()
    _require_gate_enabled(config)
    bearer = await validate_delegated_operator_bearer(request)
    oid = str(bearer.claims.get("oid") or "")

    offset = 0
    if cursor:
        offset = _decode_cursor(cursor, expected_oid=oid, config=config)

    correlation_id = new_correlation_id()
    try:
        from tools.corpus_curation_store import CorpusCurationStore

        store = CorpusCurationStore()
        page, total = await store.list_pending_items(limit=_QUEUE_PAGE_SIZE, offset=offset)
    except Exception:
        logging.error(
            "[panel-operator] curation_queue correlation_id=%s: corpus control store query failed.",
            correlation_id,
        )
        raise HTTPException(status_code=502, detail="Corpus control store query failed.")

    next_offset = offset + len(page)
    next_cursor = _encode_cursor(next_offset, oid=oid, config=config) if next_offset < total else None

    logging.info(
        "[panel-operator] curation_queue correlation_id=%s operator=%s count=%d",
        correlation_id,
        _redact_oid(oid),
        len(page),
    )

    return CorpusCurationQueueResponse(
        items=[
            CorpusCurationItem(
                item_id=i.item_id,
                document_id=i.document_id,
                title=i.title,
                reason_code=i.reason_code,
                submitted_at=i.submitted_at,
            )
            for i in page
        ],
        next_cursor=next_cursor,
    )


# ---------------------------------------------------------------------------
# POST /panel/corpus-curation/{item_id}/decision
# ---------------------------------------------------------------------------


@router.post(
    "/panel/corpus-curation/{item_id}/decision",
    response_model=CorpusCurationDecisionResponse,
    responses={
        401: {"description": "Missing or invalid bearer token."},
        403: {"description": "App-only token, or missing operator role/group."},
        404: {"description": "Curation item not found or not visible."},
        422: {
            "description": (
                "Malformed item_id, or the item already carries a "
                "conflicting recorded decision."
            )
        },
        502: {"description": "Corpus control store write failed."},
        503: {"description": "Panel operator surfaces are disabled."},
    },
)
async def corpus_curation_decision(
    item_id: str,
    body: CorpusCurationDecisionRequest,
    request: Request,
    _authorization: Optional[HTTPAuthorizationCredentials] = Security(_bearer_scheme),
) -> CorpusCurationDecisionResponse:
    config = get_config()
    _require_gate_enabled(config)
    bearer = await validate_delegated_operator_bearer(request)
    oid = str(bearer.claims.get("oid") or "")

    if not _ITEM_ID_RE.match(item_id):
        raise HTTPException(status_code=422, detail="Malformed item_id.")

    correlation_id = new_correlation_id()

    from tools.corpus_curation_store import CorpusCurationStore

    store = CorpusCurationStore()
    try:
        decision, decided_at, _was_replay = await store.record_decision(
            item_id,
            decision=body.decision,
            note=body.note,
            decided_by=oid,
        )
    except CurationItemNotFound:
        raise HTTPException(status_code=404, detail="Curation item not found.")
    except CurationDecisionConflict:
        raise HTTPException(
            status_code=422,
            detail="Item already has a different recorded decision.",
        )
    except CurationConcurrencyExhausted:
        logging.error(
            "[panel-operator] decision correlation_id=%s: concurrency exhausted for item.",
            correlation_id,
        )
        raise HTTPException(
            status_code=502,
            detail="Could not record decision due to concurrent updates; retry.",
        )
    except Exception:
        logging.error(
            "[panel-operator] decision correlation_id=%s: corpus control store write failed.",
            correlation_id,
        )
        raise HTTPException(status_code=502, detail="Corpus control store write failed.")

    logging.info(
        "[panel-operator] decision correlation_id=%s operator=%s decision=%s",
        correlation_id,
        _redact_oid(oid),
        decision,
    )

    return CorpusCurationDecisionResponse(
        item_id=item_id, decision=decision, decided_at=decided_at
    )
