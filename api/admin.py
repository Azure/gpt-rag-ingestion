"""Admin API endpoints for the ingestion dashboard.

All routes are prefixed with ``/api`` and expose read-only access to the
**jobs** blob container (run summaries & per-file logs) plus a small
write endpoint to unblock a file.
"""

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple

from azure.identity.aio import (
    AzureCliCredential,
    ChainedTokenCredential,
    ManagedIdentityCredential,
)
from azure.storage.blob.aio import BlobServiceClient
from fastapi import APIRouter, HTTPException, Query
from pathlib import Path

from dependencies import get_config

router = APIRouter(prefix="/api")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_blob_service: Optional[BlobServiceClient] = None


async def _get_blob_service() -> BlobServiceClient:
    """Lazy-init an async BlobServiceClient (singleton)."""
    global _blob_service
    if _blob_service is None:
        cfg = get_config()
        account = cfg.get("STORAGE_ACCOUNT_NAME")
        client_id = cfg.get("AZURE_CLIENT_ID", None, allow_none=True) or None
        credential = ChainedTokenCredential(
            ManagedIdentityCredential(client_id=client_id),
            AzureCliCredential(),
        )
        _blob_service = BlobServiceClient(
            account_url=f"https://{account}.blob.core.windows.net",
            credential=credential,
        )
    return _blob_service


def _jobs_container() -> str:
    return get_config().get("JOBS_LOG_CONTAINER", "jobs")


# ---------------------------------------------------------------------------
# In-memory cache (avoids re-reading every blob on each page load)
# ---------------------------------------------------------------------------
_CACHE_TTL = 60  # seconds
_cache: Dict[str, Tuple[float, Any]] = {}
_cache_lock = asyncio.Lock()


async def _cached_load(key: str, loader: Callable[[], Coroutine[Any, Any, Any]]) -> Any:
    """Return cached data if fresh, otherwise call *loader* and cache the result."""
    now = time.monotonic()
    entry = _cache.get(key)
    if entry and (now - entry[0]) < _CACHE_TTL:
        return entry[1]
    async with _cache_lock:
        # Double-check after acquiring lock
        entry = _cache.get(key)
        if entry and (now - entry[0]) < _CACHE_TTL:
            return entry[1]
        data = await loader()
        _cache[key] = (time.monotonic(), data)
        return data


def _invalidate_cache(*keys: str) -> None:
    for k in keys:
        _cache.pop(k, None)


# ---------------------------------------------------------------------------
# Blob loaders (called by cache on miss)
# ---------------------------------------------------------------------------
_DL_CONCURRENCY = 50  # max parallel blob downloads


async def _download_blob(container, blob_name: str, sem: asyncio.Semaphore) -> Optional[Tuple[str, dict]]:
    """Download and parse a single blob JSON. Returns (blob_name, data) or None on error."""
    async with sem:
        try:
            bc = container.get_blob_client(blob_name)
            dl = await bc.download_blob()
            raw = await dl.readall()
            return blob_name, json.loads(raw)
        except Exception as exc:
            logging.warning(f"[admin-api] Failed to read {blob_name}: {exc}")
            return None


async def _load_all_runs() -> Tuple[List[dict], List[str]]:
    """Read every run-summary blob. Returns (runs_list, indexer_types)."""
    svc = await _get_blob_service()
    container = svc.get_container_client(_jobs_container())

    # 1) Collect matching blob names
    blob_names: List[str] = []
    async for blob in container.list_blobs(name_starts_with=""):
        if "/runs/" in blob.name and blob.name.endswith(".json"):
            blob_names.append(blob.name)

    # 2) Download in parallel
    sem = asyncio.Semaphore(_DL_CONCURRENCY)
    results = await asyncio.gather(
        *[_download_blob(container, n, sem) for n in blob_names]
    )

    # 3) Process results
    runs: List[dict] = []
    indexer_types: set[str] = set()
    for res in results:
        if res is None:
            continue
        blob_name, data = res
        data["_blobName"] = blob_name
        if "indexerType" not in data:
            data["indexerType"] = blob_name.split("/")[0]
        if not data.get("runId"):
            try:
                data["runId"] = blob_name.split("/runs/")[1].replace(".json", "")
            except (IndexError, AttributeError):
                pass
        if not data.get("status"):
            data["status"] = "finished" if data.get("runFinishedAt") else "running"
        indexer_types.add(data.get("indexerType", ""))
        runs.append(data)
    return runs, sorted(indexer_types)


async def _load_all_files() -> Tuple[List[dict], List[str]]:
    """Read every per-file log blob. Returns (files_list, indexer_types)."""
    svc = await _get_blob_service()
    container = svc.get_container_client(_jobs_container())

    # 1) Collect matching blob names
    blob_names: List[str] = []
    async for blob in container.list_blobs(name_starts_with=""):
        if "/files/" in blob.name and blob.name.endswith(".json"):
            blob_names.append(blob.name)

    # 2) Download in parallel
    sem = asyncio.Semaphore(_DL_CONCURRENCY)
    results = await asyncio.gather(
        *[_download_blob(container, n, sem) for n in blob_names]
    )

    # 3) Process results
    files: List[dict] = []
    indexer_types: set[str] = set()
    for res in results:
        if res is None:
            continue
        blob_name, data = res
        data["_blobName"] = blob_name
        if "indexerType" not in data:
            data["indexerType"] = blob_name.split("/")[0]
        if not data.get("fileName"):
            data["fileName"] = (
                data.get("blob")
                or data.get("parent_id", "").rsplit("/", 1)[-1]
                or blob_name.split("/files/", 1)[-1].replace(".json", "")
            )
        indexer_types.add(data.get("indexerType", ""))
        files.append(data)
    return files, sorted(indexer_types)


# ---------------------------------------------------------------------------
# GET /api/version
# ---------------------------------------------------------------------------
VERSION_FILE = Path(__file__).resolve().parent.parent / "VERSION"
try:
    _app_version = VERSION_FILE.read_text().strip()
except FileNotFoundError:
    _app_version = "0.0.0"


@router.get("/version")
async def get_version():
    return {"version": _app_version}


# ---------------------------------------------------------------------------
# Log cleanup – keep at most MAX_LOG_RUN_FILES run-summary blobs
# ---------------------------------------------------------------------------

async def _cleanup_old_runs() -> None:
    """Delete oldest run-summary blobs when count exceeds the configured max."""
    try:
        max_run_files = int(get_config().get(
            "MAX_LOG_RUN_FILES", 500, allow_none=True
        ) or 500)
    except (ValueError, TypeError):
        max_run_files = 500

    svc = await _get_blob_service()
    container = svc.get_container_client(_jobs_container())

    # Collect all run blobs with last_modified
    run_blobs: List[Tuple[str, datetime]] = []
    async for blob in container.list_blobs(name_starts_with=""):
        if "/runs/" in blob.name and blob.name.endswith(".json"):
            run_blobs.append((blob.name, blob.last_modified))

    if len(run_blobs) <= max_run_files:
        return

    # Sort oldest first, delete excess
    run_blobs.sort(key=lambda x: x[1])
    to_delete = run_blobs[: len(run_blobs) - max_run_files]
    sem = asyncio.Semaphore(_DL_CONCURRENCY)

    async def _del(name: str) -> None:
        async with sem:
            try:
                await container.delete_blob(name)
            except Exception as exc:
                logging.warning(f"[admin-api] Failed to delete {name}: {exc}")

    await asyncio.gather(*[_del(name) for name, _ in to_delete])
    logging.info(f"[admin-api] Log cleanup: deleted {len(to_delete)} old run blobs (max={max_run_files})")
    _invalidate_cache("runs")


# ---------------------------------------------------------------------------
# GET /api/jobs  – paginated list of run summaries
# ---------------------------------------------------------------------------
@router.get("/jobs")
async def list_jobs(
    page: int = Query(1, ge=1),
    pageSize: int = Query(20, ge=1, le=100),
    search: str = Query("", max_length=200),
    sortField: str = Query("runStartedAt", max_length=50),
    sortOrder: str = Query("desc", regex="^(asc|desc)$"),
    indexerType: str = Query("", max_length=100),
):
    all_runs, all_types = await _cached_load("runs", _load_all_runs)

    runs = list(all_runs)  # shallow copy for filtering

    # Enrich runs with retriedFiles count from file logs
    # A file is a "retry" in a run if it appears in runHistory at position > 0
    try:
        all_files, _ = await _cached_load("files", _load_all_files)
        retries_by_run: Dict[str, int] = {}
        for f in all_files:
            rh = f.get("runHistory")
            if isinstance(rh, list) and len(rh) > 1:
                for entry in rh[1:]:
                    rid = entry.get("runId", "")
                    if rid:
                        retries_by_run[rid] = retries_by_run.get(rid, 0) + 1
        if retries_by_run:
            # Avoid mutating cached dicts — only copy those that need enrichment
            enriched: List[dict] = []
            for r in runs:
                rid = r.get("runId", "")
                if rid and rid in retries_by_run:
                    r = {**r, "retriedFiles": retries_by_run[rid]}
                enriched.append(r)
            runs = enriched
    except Exception:
        pass  # Non-critical enrichment

    if indexerType:
        runs = [r for r in runs if r.get("indexerType") == indexerType]

    if search:
        q = search.lower()
        runs = [r for r in runs if q in json.dumps(r, default=str).lower()]

    def _sort_key(item):
        val = item.get(sortField, "")
        if val is None:
            val = ""
        # Numeric fields: sort by number so "9" < "10"
        if isinstance(val, (int, float)):
            return (0, val, "")
        return (1, 0, str(val))

    runs.sort(key=_sort_key, reverse=(sortOrder == "desc"))

    total = len(runs)
    start = (page - 1) * pageSize
    end = start + pageSize

    return {
        "items": runs[start:end],
        "total": total,
        "page": page,
        "pageSize": pageSize,
        "indexerTypes": all_types,
    }


# ---------------------------------------------------------------------------
# GET /api/files  – paginated list of per-file logs
# ---------------------------------------------------------------------------
@router.get("/files")
async def list_files(
    page: int = Query(1, ge=1),
    pageSize: int = Query(20, ge=1, le=100),
    search: str = Query("", max_length=200),
    sortField: str = Query("startedAt", max_length=50),
    sortOrder: str = Query("desc", regex="^(asc|desc)$"),
    blocked: Optional[bool] = Query(None),
    indexerType: str = Query("", max_length=100),
):
    all_files, all_types = await _cached_load("files", _load_all_files)
    files = list(all_files)  # shallow copy for filtering

    if indexerType:
        files = [f for f in files if f.get("indexerType") == indexerType]

    if blocked is not None:
        files = [f for f in files if f.get("blocked", False) is blocked]

    if search:
        q = search.lower()
        files = [f for f in files if q in json.dumps(f, default=str).lower()]

    def _sort_key(item):
        val = item.get(sortField, "")
        if val is None:
            val = ""
        if isinstance(val, (int, float)):
            return (0, val, "")
        return (1, 0, str(val))

    files.sort(key=_sort_key, reverse=(sortOrder == "desc"))

    total = len(files)
    start = (page - 1) * pageSize
    end = start + pageSize

    return {
        "items": files[start:end],
        "total": total,
        "page": page,
        "pageSize": pageSize,
        "indexerTypes": all_types,
    }


# ---------------------------------------------------------------------------
# POST /api/files/{blobPath}/unblock  – clear blocked flag on a file log
# ---------------------------------------------------------------------------
@router.post("/files/unblock")
async def unblock_file(blobName: str = Query(..., min_length=1)):
    """Reset the blocked flag for a file log blob.

    ``blobName`` is the full blob path inside the jobs container, e.g.
    ``blob-indexer/files/some-doc.pdf.json``.
    """
    # Basic validation – must be under */files/*.json
    if "/files/" not in blobName or not blobName.endswith(".json"):
        raise HTTPException(400, "Invalid blob path")

    svc = await _get_blob_service()
    container = svc.get_container_client(_jobs_container())
    bc = container.get_blob_client(blobName)

    try:
        dl = await bc.download_blob()
        raw = await dl.readall()
        data = json.loads(raw)
    except Exception:
        raise HTTPException(404, "File log not found")

    data["blocked"] = False
    data["blockedAt"] = None
    data["blockedReason"] = None
    data["processingAttempts"] = 0
    data["unblockedAt"] = datetime.now(timezone.utc).isoformat()

    await bc.upload_blob(
        json.dumps(data, default=str, indent=2),
        overwrite=True,
        content_settings=__import__(
            "azure.storage.blob", fromlist=["ContentSettings"]
        ).ContentSettings(content_type="application/json"),
    )

    _invalidate_cache("files")
    return {"status": "ok", "blobName": blobName}
