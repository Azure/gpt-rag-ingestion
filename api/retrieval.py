"""Fail-closed retrieval API for the Foundry Toolbox hosted-agent path."""

from __future__ import annotations

import logging
from typing import Any, List, Optional

from fastapi import APIRouter, HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field, field_validator

from dependencies import get_config, validate_delegated_user_bearer

router = APIRouter()
_bearer_scheme = HTTPBearer(
    auto_error=False,
    description="Delegated UserEntraToken validated and forwarded to Azure AI Search.",
)

_MAX_TOP = 10
_DEFAULT_TOP = 5
_MAX_CONTENT_CHARS = 8_000
_MAX_TITLE_CHARS = 512
_MAX_URL_CHARS = 2_048
_MAX_METADATA_CHARS = 256
_SELECT_FIELDS = ["id", "content", "title", "url", "category", "source"]

_ENABLED_SETTING = "HOSTED_RETRIEVAL_ENABLED"
_INV_002_SETTING = "HOSTED_RETRIEVAL_INV_002_VALIDATED"


class RetrieveRequest(BaseModel):
    """Strict, bounded input. Identity and index selection are server-owned."""

    model_config = ConfigDict(extra="forbid")

    query: str = Field(
        ...,
        min_length=1,
        max_length=1000,
        description="Natural-language or keyword search query.",
    )
    top: int = Field(
        default=_DEFAULT_TOP,
        ge=1,
        le=_MAX_TOP,
        description=f"Maximum number of chunks to return (1-{_MAX_TOP}).",
    )

    @field_validator("query")
    @classmethod
    def query_not_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("query must not be blank")
        return value


class ChunkResult(BaseModel):
    """A bounded result without vectors or authorization metadata."""

    id: str = Field(max_length=_MAX_METADATA_CHARS)
    content: str = Field(max_length=_MAX_CONTENT_CHARS)
    title: Optional[str] = Field(default=None, max_length=_MAX_TITLE_CHARS)
    url: Optional[str] = Field(default=None, max_length=_MAX_URL_CHARS)
    category: Optional[str] = Field(default=None, max_length=_MAX_METADATA_CHARS)
    source: Optional[str] = Field(default=None, max_length=_MAX_METADATA_CHARS)
    score: Optional[float] = None


class RetrieveResponse(BaseModel):
    results: List[ChunkResult]
    count: int = Field(ge=0, le=_MAX_TOP)


def _setting_enabled(config: Any, key: str) -> bool:
    value = config.get(key, default="false", allow_none=True)
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _bounded_optional(value: Any, limit: int) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    return text[:limit] if text else None


def _bounded_required(value: Any, limit: int) -> str:
    return str(value or "")[:limit]


def _native_retrieval_is_available(config: Any) -> bool:
    return _setting_enabled(config, _ENABLED_SETTING) and _setting_enabled(
        config, _INV_002_SETTING
    )


@router.post(
    "/retrieve",
    response_model=RetrieveResponse,
    status_code=200,
    responses={
        401: {"description": "Missing or invalid per-user bearer token."},
        403: {"description": "The bearer does not represent a delegated user."},
        422: {"description": "Request schema or bounds violation."},
        500: {"description": "Required server configuration is missing."},
        502: {"description": "Azure AI Search rejected or failed the query."},
        503: {"description": "Native hosted retrieval is disabled or unvalidated."},
    },
)
async def retrieve(
    body: RetrieveRequest,
    request: Request,
    _authorization: Optional[HTTPAuthorizationCredentials] = Security(
        _bearer_scheme
    ),
) -> RetrieveResponse:
    """Retrieve chunks using Azure AI Search native token-based authorization."""

    config = get_config()
    if not _native_retrieval_is_available(config):
        raise HTTPException(
            status_code=503,
            detail=(
                "Hosted retrieval is unavailable until native identity "
                "passthrough satisfies the INV-002 validation gate."
            ),
        )

    user_bearer = await validate_delegated_user_bearer(request)

    index_name = config.get(
        "SEARCH_RAG_INDEX_NAME", default=None, allow_none=True
    )
    if not isinstance(index_name, str) or not index_name.strip():
        raise HTTPException(
            status_code=500,
            detail="Search index not configured (SEARCH_RAG_INDEX_NAME).",
        )
    index_name = index_name.strip()

    logging.info(
        "[retrieve] native_auth=true query_len=%d top=%d index=%s",
        len(body.query),
        body.top,
        index_name,
    )

    from tools.aisearch import AISearchClient

    client = AISearchClient()
    try:
        try:
            search_result = await client.search_documents(
                index_name=index_name,
                search_text=body.query,
                filter_str=None,
                select_fields=_SELECT_FIELDS,
                top=body.top,
                use_elevated_read=False,
                query_source_authorization=user_bearer.access_token,
            )
        except Exception as exc:
            logging.error(
                "[retrieve] Azure AI Search query raised %s.",
                type(exc).__name__,
            )
            raise HTTPException(
                status_code=502,
                detail="Azure AI Search query failed.",
            ) from None
    finally:
        await client.close()

    if search_result.get("error"):
        logging.error("[retrieve] Azure AI Search query failed.")
        raise HTTPException(
            status_code=502,
            detail="Azure AI Search query failed.",
        )

    results = [
        ChunkResult(
            id=_bounded_required(doc.get("id"), _MAX_METADATA_CHARS),
            content=_bounded_required(doc.get("content"), _MAX_CONTENT_CHARS),
            title=_bounded_optional(doc.get("title"), _MAX_TITLE_CHARS),
            url=_bounded_optional(doc.get("url"), _MAX_URL_CHARS),
            category=_bounded_optional(
                doc.get("category"), _MAX_METADATA_CHARS
            ),
            source=_bounded_optional(doc.get("source"), _MAX_METADATA_CHARS),
            score=doc.get("@search.score"),
        )
        for doc in search_result.get("documents", [])[: body.top]
    ]

    logging.info("[retrieve] returned count=%d index=%s", len(results), index_name)
    return RetrieveResponse(results=results, count=len(results))
