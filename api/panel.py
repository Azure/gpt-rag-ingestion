"""ADR-0001 hosted/panel administrative API (Azure/GPT-RAG#592).

Mounted **only** when the resolved deployment mode is
``DeploymentMode.HOSTED_PANEL`` (see `utils/deployment_mode.py` and the
mounting logic in `main.py`). Never mounted for classic or hosted/no-panel.

Scope, per ADR-0001's decision matrix ("Administrative panel boundary" /
"History and feedback ownership"):

* Feedback and administrative curation metadata are Cosmos-backed and owned
  by this service (reusing `tools/cosmosdb.py`'s generic client) — this is
  genuinely local to gpt-rag-ingestion, which already owns Cosmos tooling
  and the admin dashboard surface.
* A dashboard overview aggregates existing ingestion admin data
  (jobs/files, via `api.admin`) plus feedback counts.
* Full Foundry-managed **Conversation history** retrieval is explicitly
  **not implemented here** — ADR-0001's decision matrix names
  ``gpt-rag-orchestrator`` / ``gpt-rag-ui`` / ``Azure/GPT-RAG`` (not
  ``gpt-rag-ingestion``) as owners of that contract, and today conversation
  history persistence is still coupled to the orchestrator's chat flow.
  Retrieving it here would require a cross-repo API decision (which SDK/REST
  call surfaces Foundry Conversation messages, the conversation-id mapping,
  and the auth model) that has not been made. The history endpoint below
  returns an explicit 501 with that context rather than a fabricated
  integration.

This module never routes chat through the Container App and never logs
tokens or conversation/feedback content (only bounded counts/ids).
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator

from dependencies import get_config, validate_bearer_jwt
from utils.deployment_mode import panel_surface_enabled, resolve_deployment_mode

router = APIRouter(prefix="/api/panel")

_FEEDBACK_CONTAINER_SETTING = "PANEL_FEEDBACK_CONTAINER"
_DEFAULT_FEEDBACK_CONTAINER = "panel-feedback"

_MAX_CONVERSATION_ID_CHARS = 256
_MAX_MESSAGE_ID_CHARS = 256
_MAX_COMMENT_CHARS = 4_000
_MAX_TAG_CHARS = 64
_MAX_TAGS = 16

_HISTORY_BLOCKED_DETAIL = (
    "Foundry-managed Conversation history retrieval is not implemented in "
    "gpt-rag-ingestion. Per ADR-0001's decision matrix, the hosted/panel "
    "conversation-history contract (the SDK/REST call to fetch Foundry "
    "Conversation messages, the conversation-id mapping, and the auth model "
    "to use) is owned by gpt-rag-orchestrator / gpt-rag-ui / Azure/GPT-RAG, "
    "not gpt-rag-ingestion, and requires a cross-repo API decision that has "
    "not yet been made. Tracking: Azure/GPT-RAG#592."
)


# ---------------------------------------------------------------------------
# Auth — strict, fail-closed. Unlike `api.admin.require_admin` (which is a
# silent no-op without an Entra tenant, for local-dev convenience on the
# jobs/schedules/files/config surface), panel endpoints must NEVER bypass
# auth just because the panel is enabled without Entra configured. Every
# panel route (reads and writes) depends on this.
# ---------------------------------------------------------------------------


async def require_panel_admin(request: Request) -> Dict[str, Any]:
    """Require a valid bearer token carrying the ``Admin`` Entra app role.

    Raises 500 (not a silent bypass) when Entra is not configured — hosted
    panel deployments must always have Entra wired up; there is no
    development-mode auth bypass when the panel is enabled.
    """
    cfg = get_config()
    tenant_id = cfg.get("OAUTH_AZURE_AD_TENANT_ID", default=None, allow_none=True)
    if not tenant_id:
        raise HTTPException(
            status_code=500,
            detail=(
                "Administrative panel authorization is not configured "
                "(OAUTH_AZURE_AD_TENANT_ID missing). DEPLOY_ADMINISTRATIVE_"
                "PANEL=true requires Entra ID authorization; there is no "
                "development-mode bypass for panel endpoints."
            ),
        )
    claims = await validate_bearer_jwt(request)
    roles = claims.get("roles") or []
    if "Admin" not in roles:
        raise HTTPException(status_code=403, detail="Admin role required")
    return claims


# ---------------------------------------------------------------------------
# GET /api/panel/status — open, non-sensitive mode/readiness metadata.
# Analogous to `GET /api/version` / `GET /api/identity`: safe to call
# without auth so the SPA can decide what to render.
# ---------------------------------------------------------------------------


class PanelStatus(BaseModel):
    mode: str
    panelEnabled: bool
    authEnabled: bool


@router.get("/status", response_model=PanelStatus)
async def get_panel_status() -> PanelStatus:
    cfg = get_config()
    mode = resolve_deployment_mode(cfg)
    if not panel_surface_enabled(mode):
        raise HTTPException(status_code=404, detail="Not found")
    tenant_id = cfg.get("OAUTH_AZURE_AD_TENANT_ID", default=None, allow_none=True)
    return PanelStatus(
        mode=mode.value,
        panelEnabled=True,
        authEnabled=bool(tenant_id),
    )


# ---------------------------------------------------------------------------
# Feedback / curation metadata — Cosmos-backed.
# ---------------------------------------------------------------------------


class FeedbackCreateRequest(BaseModel):
    """Strict, bounded input. No free-form structural fields."""

    model_config = ConfigDict(extra="forbid")

    conversationId: str = Field(..., min_length=1, max_length=_MAX_CONVERSATION_ID_CHARS)
    messageId: Optional[str] = Field(default=None, max_length=_MAX_MESSAGE_ID_CHARS)
    rating: Literal["up", "down"]
    comment: Optional[str] = Field(default=None, max_length=_MAX_COMMENT_CHARS)
    tags: List[str] = Field(default_factory=list, max_length=_MAX_TAGS)

    @field_validator("tags")
    @classmethod
    def _bound_tags(cls, tags: List[str]) -> List[str]:
        for tag in tags:
            if len(tag) > _MAX_TAG_CHARS:
                raise ValueError(f"tag exceeds max length of {_MAX_TAG_CHARS} characters")
        return tags


class FeedbackRecord(BaseModel):
    id: str
    conversationId: str
    messageId: Optional[str] = None
    rating: Literal["up", "down"]
    comment: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    createdBy: Optional[str] = None
    createdAt: str


def _feedback_container_name(cfg: Any) -> str:
    return str(
        cfg.get(
            _FEEDBACK_CONTAINER_SETTING,
            default=_DEFAULT_FEEDBACK_CONTAINER,
            allow_none=True,
        )
        or _DEFAULT_FEEDBACK_CONTAINER
    )


@router.get(
    "/feedback",
    response_model=List[FeedbackRecord],
    dependencies=[Depends(require_panel_admin)],
    responses={502: {"description": "Cosmos DB read failed."}},
)
async def list_feedback(
    conversationId: Optional[str] = Query(default=None, max_length=_MAX_CONVERSATION_ID_CHARS),
) -> List[FeedbackRecord]:
    from tools.cosmosdb import CosmosDBClient

    cfg = get_config()
    client = CosmosDBClient()
    try:
        documents = await client.list_documents(_feedback_container_name(cfg))
    except Exception:
        logging.error("[panel] Failed to list feedback documents from Cosmos.")
        raise HTTPException(status_code=502, detail="Failed to read feedback from Cosmos DB.")

    records = [FeedbackRecord(**doc) for doc in documents if isinstance(doc, dict) and "rating" in doc]
    if conversationId:
        records = [r for r in records if r.conversationId == conversationId]
    logging.info("[panel] feedback list returned count=%d", len(records))
    return records


@router.post(
    "/feedback",
    response_model=FeedbackRecord,
    status_code=201,
    responses={502: {"description": "Cosmos DB write failed."}},
)
async def create_feedback(
    body: FeedbackCreateRequest,
    claims: Dict[str, Any] = Depends(require_panel_admin),
) -> FeedbackRecord:
    from tools.cosmosdb import CosmosDBClient

    created_by = claims.get("oid") or claims.get("sub") if isinstance(claims, dict) else None

    record = FeedbackRecord(
        id=str(uuid.uuid4()),
        conversationId=body.conversationId,
        messageId=body.messageId,
        rating=body.rating,
        comment=body.comment,
        tags=body.tags,
        createdBy=created_by,
        createdAt=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    )

    cfg = get_config()
    client = CosmosDBClient()
    created = await client.create_document(
        _feedback_container_name(cfg), record.id, record.model_dump()
    )
    if created is None:
        logging.error("[panel] Failed to create feedback document in Cosmos.")
        raise HTTPException(status_code=502, detail="Failed to write feedback to Cosmos DB.")

    # Never log comment/tag content — only ids and rating.
    logging.info(
        "[panel] feedback created id=%s conversationId=%s rating=%s",
        record.id,
        record.conversationId,
        record.rating,
    )
    return record


# ---------------------------------------------------------------------------
# GET /api/panel/overview — dashboard overview combining existing ingestion
# admin data (jobs/files) with feedback counts. Reuses `api.admin`'s cached
# loaders rather than re-implementing blob scanning.
# ---------------------------------------------------------------------------


class JobsOverview(BaseModel):
    availableJobTypes: List[str]
    runningJobTypes: List[str]
    totalRuns: int


class FilesOverview(BaseModel):
    totalFiles: int


class FeedbackOverview(BaseModel):
    totalRecords: int
    upCount: int
    downCount: int


class PanelOverview(BaseModel):
    mode: str
    jobs: JobsOverview
    files: FilesOverview
    feedback: FeedbackOverview
    historyAvailable: bool = Field(
        default=False,
        description="Always false until the cross-repo Foundry Conversation "
        "history contract lands (Azure/GPT-RAG#592).",
    )


@router.get(
    "/overview",
    response_model=PanelOverview,
    dependencies=[Depends(require_panel_admin)],
)
async def get_panel_overview() -> PanelOverview:
    # Late imports: avoid pulling the full ingestion stack (and any
    # circular import through `main`) at module load, matching the
    # existing convention in `api/admin.py`.
    from api.admin import _available_job_types, _cached_load, _load_all_files, _load_all_runs, _running_job_types
    from tools.cosmosdb import CosmosDBClient

    cfg = get_config()
    mode = resolve_deployment_mode(cfg)

    all_runs, _ = await _cached_load("runs", _load_all_runs)
    all_files, _ = await _cached_load("files", _load_all_files)

    up_count = down_count = 0
    try:
        client = CosmosDBClient()
        feedback_docs = await client.list_documents(_feedback_container_name(cfg))
        for doc in feedback_docs:
            if not isinstance(doc, dict):
                continue
            if doc.get("rating") == "up":
                up_count += 1
            elif doc.get("rating") == "down":
                down_count += 1
    except Exception:
        # Non-critical enrichment: an overview page should still render the
        # jobs/files summary even if Cosmos is briefly unavailable.
        logging.warning("[panel] Failed to load feedback summary for overview.")

    return PanelOverview(
        mode=mode.value,
        jobs=JobsOverview(
            availableJobTypes=_available_job_types(),
            runningJobTypes=_running_job_types(),
            totalRuns=len(all_runs),
        ),
        files=FilesOverview(totalFiles=len(all_files)),
        feedback=FeedbackOverview(
            totalRecords=up_count + down_count,
            upCount=up_count,
            downCount=down_count,
        ),
        historyAvailable=False,
    )


# ---------------------------------------------------------------------------
# GET /api/panel/conversations/{conversation_id}/history — explicit blocker.
# ---------------------------------------------------------------------------


@router.get(
    "/conversations/{conversation_id}/history",
    dependencies=[Depends(require_panel_admin)],
    responses={
        501: {"description": "Not implemented pending cross-repo contract (Azure/GPT-RAG#592)."}
    },
)
async def get_conversation_history(conversation_id: str) -> None:
    raise HTTPException(status_code=501, detail=_HISTORY_BLOCKED_DETAIL)
