"""Retrieval API for Foundry Toolbox / hosted-agent path.

Exposes ``POST /retrieve`` as an MCP-compatible retrieval source so the
hosted orchestrator can retrieve documents without the Authorization header
that the Foundry gateway strips.

Security contract
-----------------
* Service authentication: ``X-API-KEY`` header (same key as all other
  ingestion API calls).  The calling service (hosted orchestrator) is
  responsible for injecting the key.
* User-context: ``userContext.oid`` **must** be present and non-empty.
  The service fails closed — missing or blank OID is rejected by Pydantic
  schema validation (HTTP 422) before the search is attempted, so there
  is no success-shaped fallback to an elevated or unfiltered query.
* Document-level filtering: an OData filter scopes results to documents
  that carry the caller's OID in ``metadata_security_user_ids``, plus
  documents that are truly unrestricted (BOTH ``metadata_security_user_ids``
  AND ``metadata_security_group_ids`` are empty).  A document with a
  non-empty group ACL but empty user ACL is **not** classified as public.
  The elevated-read bypass header is intentionally omitted so AI Search
  applies its own permission semantics on top of the OData filter.
* Caller-supplied index selection is not supported; the index is always
  taken from ``SEARCH_RAG_INDEX_NAME`` configuration so the service
  identity cannot be directed at arbitrary indexes.
* Search failures are surfaced as HTTP 503 rather than silently returning
  an empty result set — the caller must not mistake a backend error for a
  legitimate "no documents found" response.
* No bearer tokens, raw OIDs, or authorization payloads appear in logs.
  Correlation metadata (query length, index name, result count) is emitted
  at INFO level; the OID is never written to a log record.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from dependencies import get_config, validate_api_key_header

router = APIRouter()

# ---------------------------------------------------------------------------
# Hard limits — keep output bounded so the caller cannot extract large blobs.
# ---------------------------------------------------------------------------
_MAX_TOP = 10
_DEFAULT_TOP = 5

# Output fields — never include embedding vectors, security IDs, or raw
# authorization metadata in the response payload.
_SELECT_FIELDS = ["id", "content", "title", "url", "category", "source"]


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class UserContext(BaseModel):
    """Explicit user identity forwarded by the trusted orchestrator.

    The ``oid`` claim (Entra Object ID) is used exclusively for OData
    filtering inside this service; it is never written to any log record.
    """

    oid: str = Field(
        ...,
        min_length=1,
        max_length=256,
        description="Entra Object ID of the end user for document-level filtering.",
    )

    @field_validator("oid")
    @classmethod
    def oid_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("oid must not be blank")
        return v.strip()


class RetrieveRequest(BaseModel):
    """Bounded retrieval request.

    All fields are strictly validated so the endpoint never accepts
    untyped or over-sized payloads.
    """

    query: str = Field(
        ...,
        min_length=1,
        max_length=1000,
        description="Natural language or keyword search query.",
    )
    userContext: UserContext = Field(
        ...,
        description="User identity forwarded by the trusted orchestrator.",
    )
    top: int = Field(
        default=_DEFAULT_TOP,
        ge=1,
        le=_MAX_TOP,
        description=f"Maximum number of results to return (1–{_MAX_TOP}).",
    )

    @field_validator("query")
    @classmethod
    def query_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("query must not be blank")
        return v


class ChunkResult(BaseModel):
    """A single retrieved chunk — vectors and security fields are excluded."""

    id: str
    content: str
    title: Optional[str] = None
    url: Optional[str] = None
    category: Optional[str] = None
    source: Optional[str] = None
    score: Optional[float] = None


class RetrieveResponse(BaseModel):
    results: List[ChunkResult]
    count: int


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.post("/retrieve", response_model=RetrieveResponse, status_code=200)
async def retrieve(
    body: RetrieveRequest,
    _: Any = Depends(validate_api_key_header),
) -> JSONResponse:
    """Retrieve document chunks filtered by user identity.

    Requires a valid ``X-API-KEY`` header (service-to-service auth) and
    a non-empty ``userContext.oid`` in the request body.  Fails closed:
    missing or blank OID is rejected by Pydantic validation (HTTP 422)
    before any search is attempted, so there is no success-shaped fallback
    to an unfiltered or elevated result set.
    """
    cfg = get_config()

    index_name: Optional[str] = cfg.get(
        "SEARCH_RAG_INDEX_NAME", default=None, allow_none=True
    )
    if not index_name:
        raise HTTPException(
            status_code=500,
            detail="Search index not configured (SEARCH_RAG_INDEX_NAME).",
        )

    user_oid = body.userContext.oid  # already stripped and validated by Pydantic

    # Correlation log — no OID, no query content.
    logging.info(
        "[retrieve] query_len=%d top=%d index=%s",
        len(body.query),
        body.top,
        index_name,
    )

    # OData permission filter:
    #   Return documents where:
    #     (a) the user's OID appears in the user security list, OR
    #     (b) the document is truly unrestricted — BOTH the user and group
    #         security lists are empty.
    #
    # A document with an empty user ACL but a non-empty group ACL is
    # group-restricted and must NOT be classified as public.
    #
    # Escape single quotes to prevent OData injection.
    escaped_oid = user_oid.replace("'", "''")
    filter_str = (
        f"metadata_security_user_ids/any(uid: uid eq '{escaped_oid}')"
        f" or (not metadata_security_user_ids/any()"
        f" and not metadata_security_group_ids/any())"
    )

    from tools.aisearch import AISearchClient

    client = AISearchClient()
    search_result = await client.search_documents(
        index_name=index_name,
        search_text=body.query,
        filter_str=filter_str,
        select_fields=_SELECT_FIELDS,
        top=body.top,
        use_elevated_read=False,
    )

    if search_result.get("error"):
        logging.error("[retrieve] search error index=%s", index_name)
        raise HTTPException(
            status_code=503,
            detail="Search service unavailable or index error.",
        )

    results: List[Dict[str, Any]] = []
    for doc in search_result.get("documents", []):
        results.append(
            {
                "id": str(doc.get("id") or ""),
                "content": str(doc.get("content") or ""),
                "title": doc.get("title") or None,
                "url": doc.get("url") or None,
                "category": doc.get("category") or None,
                "source": doc.get("source") or None,
                "score": doc.get("@search.score") or None,
            }
        )

    logging.info("[retrieve] returned count=%d index=%s", len(results), index_name)

    return JSONResponse(
        content={
            "results": results,
            "count": len(results),
        }
    )
