"""Corpus curation control store for the operator panel (issue #611, ADR-0004).

The optional hosted administrative panel's ingestion-side operator surfaces
must never read or expose Foundry managed Conversation content, and the
platform contract (Azure/GPT-RAG PR #637) grants this service only
container-scoped Cosmos **Data Reader** on the panel's owner-index/feedback
metadata containers -- never a write, and never a corpus-curation Cosmos
container. Curation decisions are therefore never written to Cosmos.

Instead, this module reuses the *existing* corpus control store this service
identity already owns and legitimately writes to for every ingested
document: the per-file log blobs under the ``jobs`` blob container's
``*/files/*.json`` prefix (the same store that already powers the classic
dashboard's Files tab and the `blocked` / `unblock` flow -- see
``api/admin.py``). A curation item is a *blocked* per-file log: a document
that failed automatic processing and needs an explicit operator decision.
This is "documents ingestion already legitimately indexes/accesses", never a
Foundry managed Conversation and never conversation content.

Decisions are persisted with Azure Blob Storage's native ETag optimistic
concurrency (conditional ``upload_blob`` with ``MatchConditions.IfNotModified``
and bounded retry-on-conflict), not a bespoke Cosmos write path this
identity cannot use. Because the pinned ``CorpusCurationDecisionRequest``
wire shape (``contracts/conversations-panel-v1.schema.json``) is strict
(``additionalProperties: false`` with only ``decision``/``note``) and does
not carry a client idempotency key or a version/etag field, idempotency is
achieved structurally instead: re-posting the identical decision for an
already-decided item is a no-op that returns the existing recorded outcome,
and a *conflicting* second decision on an already-decided item is rejected
(422) rather than silently overwritten. This is a deliberate, narrow
interpretation documented here rather than inventing new request fields
that would break the strict schema; if a future revision needs an explicit
client-supplied idempotency key, that is an umbrella schema change, not a
unilateral addition here.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from azure.core import MatchConditions
from azure.core.exceptions import ResourceModifiedError, ResourceNotFoundError
from azure.identity.aio import (
    AzureCliCredential,
    ChainedTokenCredential,
    ManagedIdentityCredential,
)
from azure.storage.blob import ContentSettings
from azure.storage.blob.aio import BlobServiceClient

from dependencies import get_config
from tools.credentials import get_azure_client_id

_ITEM_ID_PREFIX = "cur_"
_MAX_DECISION_RETRIES = 3
_DL_CONCURRENCY = 50

VALID_DECISIONS = ("approve", "reject", "defer")


class CurationItemNotFound(Exception):
    """Raised when *item_id* does not resolve to a visible curation item."""

    def __init__(self, item_id: str):
        super().__init__(f"Curation item not found: {item_id}")
        self.item_id = item_id


class CurationDecisionConflict(Exception):
    """Raised when an item already carries a *different* recorded decision."""

    def __init__(self, item_id: str, existing_decision: str, requested_decision: str):
        super().__init__(
            f"Curation item {item_id} already decided as {existing_decision!r}; "
            f"cannot record conflicting decision {requested_decision!r}."
        )
        self.item_id = item_id
        self.existing_decision = existing_decision
        self.requested_decision = requested_decision


class CurationConcurrencyExhausted(Exception):
    """Raised when repeated concurrent writers prevent recording a decision."""

    def __init__(self, item_id: str):
        super().__init__(f"Could not record a decision for {item_id}: too many concurrent writers.")
        self.item_id = item_id


@dataclass(frozen=True)
class CurationItem:
    item_id: str
    document_id: str
    title: str
    reason_code: str
    submitted_at: str
    blob_name: str


def item_id_for_blob(blob_name: str) -> str:
    """Deterministic, bounded-length, opaque item id for a file-log blob path.

    Never the raw blob path itself (blob paths can exceed the contract's
    128-char ``item_id`` bound and can reveal internal storage layout).
    """
    digest = hashlib.sha256(blob_name.encode("utf-8")).hexdigest()[:32]
    return f"{_ITEM_ID_PREFIX}{digest}"


class CorpusCurationStore:
    """Reads/writes curation state in the existing `jobs` blob container."""

    def __init__(self):
        self._cfg = get_config()
        self._blob_service: Optional[BlobServiceClient] = None

    def _container_name(self) -> str:
        return self._cfg.get("JOBS_LOG_CONTAINER", "jobs")

    async def _get_blob_service(self) -> BlobServiceClient:
        if self._blob_service is None:
            account = self._cfg.get("STORAGE_ACCOUNT_NAME")
            client_id = get_azure_client_id(self._cfg)
            credential = ChainedTokenCredential(
                ManagedIdentityCredential(client_id=client_id),
                AzureCliCredential(),
            )
            self._blob_service = BlobServiceClient(
                account_url=f"https://{account}.blob.core.windows.net",
                credential=credential,
            )
        return self._blob_service

    async def _iter_file_logs(self):
        """Yield ``(blob_name, data)`` for every blocked per-file log blob."""
        svc = await self._get_blob_service()
        container = svc.get_container_client(self._container_name())

        blob_names: List[str] = []
        async for blob in container.list_blobs(name_starts_with=""):
            if "/files/" in blob.name and blob.name.endswith(".json"):
                blob_names.append(blob.name)

        sem = asyncio.Semaphore(_DL_CONCURRENCY)

        async def _download(name: str):
            async with sem:
                try:
                    bc = container.get_blob_client(name)
                    dl = await bc.download_blob()
                    raw = await dl.readall()
                    return name, json.loads(raw)
                except Exception as exc:  # pragma: no cover - defensive
                    logging.warning(f"[corpus-curation] could not read {name}: {exc}")
                    return None

        for result in await asyncio.gather(*[_download(n) for n in blob_names]):
            if result is not None:
                yield result

    async def _blocked_items(self) -> List[Tuple[CurationItem, bool]]:
        """Return every blocked file log as ``(item, has_decision)``."""
        items: List[Tuple[CurationItem, bool]] = []
        async for blob_name, data in self._iter_file_logs():
            if not data.get("blocked"):
                continue
            document_id = (
                data.get("blob")
                or data.get("fileName")
                or blob_name.split("/files/", 1)[-1].replace(".json", "")
            )
            item = CurationItem(
                item_id=item_id_for_blob(blob_name),
                document_id=str(document_id)[:512],
                title=str(document_id)[:512],
                reason_code=str(data.get("blockedReason") or "processing_blocked")[:64],
                submitted_at=str(data.get("blockedAt") or data.get("startedAt") or ""),
                blob_name=blob_name,
            )
            items.append((item, bool(data.get("curationDecision"))))
        return items

    async def list_pending_items(
        self, *, limit: int, offset: int
    ) -> Tuple[List[CurationItem], int]:
        """Return a stable-ordered page of undecided curation items.

        ``offset``/``limit`` are internal pagination primitives -- callers
        must never return them to a client except wrapped inside the
        opaque, signed, expiring, operator-bound cursor.
        """
        pending = [item for item, decided in await self._blocked_items() if not decided]
        pending.sort(key=lambda i: (i.submitted_at, i.blob_name))
        total = len(pending)
        return pending[offset : offset + limit], total

    async def count_pending_and_decided(self) -> Tuple[int, int]:
        """Return ``(pending_count, decided_count)`` over blocked file logs."""
        pending = 0
        decided = 0
        for _item, has_decision in await self._blocked_items():
            if has_decision:
                decided += 1
            else:
                pending += 1
        return pending, decided

    async def _resolve_blob_name(self, item_id: str) -> Optional[str]:
        svc = await self._get_blob_service()
        container = svc.get_container_client(self._container_name())
        async for blob in container.list_blobs(name_starts_with=""):
            if "/files/" not in blob.name or not blob.name.endswith(".json"):
                continue
            if item_id_for_blob(blob.name) == item_id:
                return blob.name
        return None

    async def record_decision(
        self,
        item_id: str,
        *,
        decision: str,
        note: Optional[str],
        decided_by: str,
    ) -> Tuple[str, str, bool]:
        """Record *decision* for *item_id*.

        Returns ``(decision, decided_at, was_replay)``. Uses the blob's
        native ETag for optimistic concurrency: a genuinely racing
        concurrent decision on the same item is retried against the
        freshest state; an item already decided *differently* raises
        ``CurationDecisionConflict`` rather than being silently overwritten.
        """
        if decision not in VALID_DECISIONS:
            raise ValueError(f"Unsupported decision: {decision!r}")

        blob_name = await self._resolve_blob_name(item_id)
        if blob_name is None:
            raise CurationItemNotFound(item_id)

        svc = await self._get_blob_service()
        container = svc.get_container_client(self._container_name())
        bc = container.get_blob_client(blob_name)

        for _attempt in range(_MAX_DECISION_RETRIES):
            try:
                dl = await bc.download_blob()
                raw = await dl.readall()
                etag = dl.properties.etag
            except ResourceNotFoundError:
                raise CurationItemNotFound(item_id)

            data = json.loads(raw)
            if not data.get("blocked"):
                raise CurationItemNotFound(item_id)

            existing_decision = data.get("curationDecision")
            if existing_decision:
                if existing_decision == decision:
                    return existing_decision, str(data.get("curationDecidedAt") or ""), True
                raise CurationDecisionConflict(item_id, existing_decision, decision)

            decided_at = datetime.now(timezone.utc).isoformat()
            data["curationDecision"] = decision
            data["curationDecisionNote"] = note
            data["curationDecidedBy"] = decided_by
            data["curationDecidedAt"] = decided_at

            try:
                await bc.upload_blob(
                    json.dumps(data, default=str, indent=2),
                    overwrite=True,
                    etag=etag,
                    match_condition=MatchConditions.IfNotModified,
                    content_settings=ContentSettings(content_type="application/json"),
                )
                return decision, decided_at, False
            except ResourceModifiedError:
                continue  # concurrent writer changed the blob -- retry with fresh state

        raise CurationConcurrencyExhausted(item_id)
