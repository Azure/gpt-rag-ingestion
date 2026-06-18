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
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pathlib import Path

from dependencies import get_config, validate_bearer_jwt
from tools.credentials import get_azure_client_id

router = APIRouter(prefix="/api")


# ---------------------------------------------------------------------------
# Admin gate – mirrors the orchestrator dashboard pattern.
#
# Read endpoints stay open (network-only auth, same as today). Only mutating
# routes — currently `POST /api/jobs/{job_type}/run` — get the gate. When
# `OAUTH_AZURE_AD_TENANT_ID` is not configured the gate is a no-op so local
# dev keeps working without an Entra app registration.
# ---------------------------------------------------------------------------


def _auth_enabled() -> bool:
    cfg = get_config()
    tenant_id = cfg.get("OAUTH_AZURE_AD_TENANT_ID", default=None, allow_none=True)
    return bool(tenant_id)


async def require_admin(request: Request) -> None:
    """Raise 403 unless the caller has the ``Admin`` Entra app role.

    No-op when auth is not configured. The token must be issued for this API's
    scope (``api://<client_id>/...``) so the ``roles`` claim is present.
    """
    if not _auth_enabled():
        return
    claims = await validate_bearer_jwt(request)
    roles = claims.get("roles") or []
    if "Admin" not in roles:
        raise HTTPException(status_code=403, detail="Admin role required")

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
        client_id = get_azure_client_id(cfg)
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
        # Detect stale runs: if status is non-terminal and started > 2 h ago, mark interrupted
        if data.get("status") in ("running", "started", "finishing"):
            _started_raw = data.get("runStartedAt") or ""
            try:
                _st = datetime.fromisoformat(_started_raw.replace("Z", "+00:00"))
                if (datetime.now(timezone.utc) - _st).total_seconds() > 7200:
                    data["status"] = "interrupted"
                    if not data.get("runFinishedAt"):
                        data["runFinishedAt"] = _started_raw  # best-effort placeholder
            except (ValueError, TypeError):
                pass
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
        "availableJobTypes": _available_job_types(),
        "runningJobTypes": _running_job_types(),
    }


def _available_job_types() -> List[str]:
    """Return the canonical job_type identifiers accepted by ``/api/jobs/{job_type}/run``."""
    from main import JOB_REGISTRY  # local import to avoid circular import at module load

    return sorted(JOB_REGISTRY.keys())


def _running_job_types() -> List[str]:
    from main import _running_jobs

    return sorted(_running_jobs)


# ---------------------------------------------------------------------------
# GET /api/identity – tells the frontend whether auth is on and if the caller
# has the Admin role. Always returns 200; never logs or raises on invalid
# tokens — it's a state-probe endpoint, not an access check.
# ---------------------------------------------------------------------------
@router.get("/identity")
async def get_identity(request: Request) -> Dict[str, Any]:
    auth_enabled = _auth_enabled()
    if not auth_enabled:
        return {"authEnabled": False, "isAdmin": True}

    is_admin = False
    try:
        claims = await validate_bearer_jwt(request)
        is_admin = "Admin" in (claims.get("roles") or [])
    except Exception:
        # Silent by design: a missing or invalid token here just means the
        # caller isn't admin. Failing loudly would spam logs on every page load.
        is_admin = False

    return {"authEnabled": True, "isAdmin": is_admin}


# ---------------------------------------------------------------------------
# POST /api/jobs/{job_type}/run – manually trigger a scheduled ingestion job.
# Requires Admin when auth is enabled; idempotent guard returns 409 if a run
# of the same job_type is already in flight (manual or cron-triggered).
# ---------------------------------------------------------------------------
_JOB_TYPE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


@router.post("/jobs/{job_type}/run", status_code=202, dependencies=[Depends(require_admin)])
async def run_job_now(job_type: str) -> Dict[str, Any]:
    if not _JOB_TYPE_RE.match(job_type):
        raise HTTPException(status_code=400, detail="Invalid job_type")

    # Late import keeps api/admin import-time light and avoids circular import.
    from main import JOB_REGISTRY, _running_jobs, _running_jobs_lock, scheduler

    if job_type not in JOB_REGISTRY:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown job_type '{job_type}'. Known: {sorted(JOB_REGISTRY.keys())}",
        )

    async with _running_jobs_lock:
        if job_type in _running_jobs:
            raise HTTPException(
                status_code=409,
                detail=f"Job '{job_type}' is already running",
            )

    func = JOB_REGISTRY[job_type]
    trigger_id = f"manual-{job_type}-{int(time.time() * 1000)}"
    try:
        # `trigger='date'` with no run_date defaults to "now" — APScheduler
        # picks it up on the next event loop tick.
        scheduler.add_job(
            func,
            trigger="date",
            id=trigger_id,
            name=f"manual:{job_type}",
            replace_existing=False,
            misfire_grace_time=None,
        )
    except Exception as exc:  # pragma: no cover - depends on APScheduler state
        logging.exception("Failed to enqueue manual run for %s", job_type)
        raise HTTPException(status_code=500, detail=f"Failed to schedule job: {exc}") from exc

    logging.info("[admin] Manual run requested for job_type=%s trigger_id=%s", job_type, trigger_id)
    return {"jobType": job_type, "triggerId": trigger_id, "status": "queued"}


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


# ---------------------------------------------------------------------------
# Configuration tab — read-only listing + admin-gated mutation.
#
# Exposes a curated allowlist of App Configuration keys that operators can
# tweak from the dashboard (see GPT-RAG#512). Nothing outside `SETTINGS` is
# ever returned or written, and a defense-in-depth denylist regex rejects any
# key that looks like a secret/endpoint even if it sneaks into the allowlist.
# ---------------------------------------------------------------------------

from dataclasses import dataclass, field as _dc_field
from typing import Literal


SettingType = Literal["int", "bool", "cron"]


@dataclass(frozen=True)
class SettingSpec:
    key: str
    type: SettingType
    section: str
    default: Optional[str] = None
    min: Optional[int] = None
    max: Optional[int] = None


# Sections render in this order in the UI. Labels and descriptions stay in
# the frontend `settingMetadata.ts` (single source of truth for copy); the
# backend only ships the structural `id` so the UI can group reliably.
CONFIG_SECTIONS: List[Dict[str, str]] = [
    {"id": "scheduling", "label": "Scheduling"},
    {"id": "chunking", "label": "Chunking"},
    {"id": "indexing", "label": "Indexing"},
    {"id": "throughput", "label": "Throughput and concurrency"},
    {"id": "limits", "label": "Processing limits"},
    {"id": "multimodal", "label": "Multimodal"},
    {"id": "sharepoint", "label": "SharePoint"},
]


# Allowlist of the only keys the Configuration tab is allowed to read/write.
# Order within a section is preserved for rendering.
SETTINGS: List[SettingSpec] = [
    # Scheduling (7 cron expressions)
    SettingSpec("CRON_RUN_SHAREPOINT_INDEX", "cron", "scheduling"),
    SettingSpec("CRON_RUN_SHAREPOINT_PURGE", "cron", "scheduling"),
    SettingSpec("CRON_RUN_IMAGES_PURGE", "cron", "scheduling"),
    SettingSpec("CRON_RUN_BLOB_INDEX", "cron", "scheduling", default="0 * * * *"),
    SettingSpec("CRON_RUN_BLOB_PURGE", "cron", "scheduling", default="10 * * * *"),
    SettingSpec("CRON_RUN_NL2SQL_INDEX", "cron", "scheduling"),
    SettingSpec("CRON_RUN_NL2SQL_PURGE", "cron", "scheduling"),
    # Chunking
    SettingSpec("CHUNKING_NUM_TOKENS", "int", "chunking", min=64, max=8192),
    SettingSpec("CHUNKING_MIN_CHUNK_SIZE", "int", "chunking", min=0, max=4096),
    SettingSpec("SPREADSHEET_CHUNKING_BY_ROW", "bool", "chunking"),
    # Indexing
    SettingSpec("INDEXER_BATCH_SIZE", "int", "indexing", min=1, max=1000),
    # Throughput and concurrency
    SettingSpec("INDEXER_MAX_CONCURRENCY", "int", "throughput", min=1, max=64),
    SettingSpec("AOAI_MAX_CONCURRENCY", "int", "throughput", min=1, max=64),
    # Processing limits
    SettingSpec("MAX_FILE_PROCESSING_ATTEMPTS", "int", "limits", min=1, max=10),
    SettingSpec("MAX_PAGES_PER_ANALYSIS", "int", "limits", min=1, max=10000),
    # Multimodal
    SettingSpec("MULTIMODAL", "bool", "multimodal"),
    # SharePoint
    SettingSpec("SHAREPOINT_MAX_FILE_COUNT", "int", "sharepoint", min=1, max=100000),
]

ALLOWED_KEYS: set[str] = {s.key for s in SETTINGS}
SETTINGS_BY_KEY: Dict[str, SettingSpec] = {s.key: s for s in SETTINGS}

# Defense-in-depth: even if a future edit accidentally drops a sensitive key
# into SETTINGS, the write handler refuses to touch anything whose underscore-
# delimited segments overlap with these terms. Token-level matching avoids
# false positives like ``NUM_TOKENS`` (which is not a token/secret).
_DENY_TOKENS: frozenset[str] = frozenset(
    {
        "SECRET",
        "PASSWORD",
        "CONNECTION",
        "ENDPOINT",
        "APIKEY",
        "CLIENTID",
        "TENANTID",
        "URI",
        "URL",
        "CONNSTR",
        "CONNSTRING",
    }
)

# App Configuration label all writes go to. Matches the `gpt-rag` selector
# in `tools/appconfig.py` so updates surface to every service in the deploy.
_APP_CONFIG_LABEL = "gpt-rag"


def _is_denied(key: str) -> bool:
    # Compare underscore-delimited segments (case-insensitive) against the
    # denylist. Sensitive keys virtually always carry a dedicated token
    # (e.g. ``MCP_APP_APIKEY``, ``STORAGE_CONNECTION_STRING``), and this
    # avoids the substring trap where ``TOKEN`` would block ``NUM_TOKENS``.
    segments = {seg.upper() for seg in key.split("_") if seg}
    if segments & _DENY_TOKENS:
        return True
    # Also reject the merged ``APIKEY`` spelling without underscore split,
    # since some legacy keys use ``APIKEY`` directly without a separator.
    upper = key.upper()
    if "APIKEY" in upper or "APIKEYNAME" in upper:
        return True
    return False


def _read_setting(cfg, spec: SettingSpec) -> Dict[str, Any]:
    """Return the current value for a setting, coerced to its declared type."""
    raw = cfg.get(spec.key, default=None, allow_none=True)
    if raw is None and spec.default is not None:
        raw = spec.default

    value: Any
    if raw is None:
        value = None
    elif spec.type == "int":
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = None
    elif spec.type == "bool":
        if isinstance(raw, bool):
            value = raw
        else:
            value = str(raw).strip().lower() in ("true", "1", "yes")
    else:  # cron / string
        value = str(raw)

    return {
        "key": spec.key,
        "type": spec.type,
        "section": spec.section,
        "value": value,
        "default": spec.default,
        "min": spec.min,
        "max": spec.max,
    }


def _coerce_and_validate(
    spec: SettingSpec, raw_value: Any
) -> Tuple[Optional[str], Optional[str]]:
    """Validate a candidate value for a setting.

    Returns ``(normalized_string, None)`` on success or ``(None, error)`` on
    failure. App Configuration only stores strings, so booleans/ints are
    serialized to a canonical form.
    """
    if raw_value is None or (isinstance(raw_value, str) and raw_value.strip() == ""):
        # Empty value means "unset" — only allowed for cron (disables the job).
        if spec.type == "cron":
            return "", None
        return None, f"{spec.key}: value is required"

    if spec.type == "int":
        try:
            n = int(raw_value)
        except (TypeError, ValueError):
            return None, f"{spec.key}: must be an integer"
        if spec.min is not None and n < spec.min:
            return None, f"{spec.key}: must be >= {spec.min}"
        if spec.max is not None and n > spec.max:
            return None, f"{spec.key}: must be <= {spec.max}"
        return str(n), None

    if spec.type == "bool":
        if isinstance(raw_value, bool):
            return "true" if raw_value else "false", None
        s = str(raw_value).strip().lower()
        if s in ("true", "1", "yes"):
            return "true", None
        if s in ("false", "0", "no"):
            return "false", None
        return None, f"{spec.key}: must be a boolean"

    if spec.type == "cron":
        # Reuse APScheduler's parser so what we validate is exactly what
        # `_schedule()` would accept at startup — avoids drift with croniter.
        from apscheduler.triggers.cron import CronTrigger

        try:
            CronTrigger.from_crontab(str(raw_value).strip())
        except (ValueError, Exception) as exc:  # pragma: no cover - exotic input
            return None, f"{spec.key}: invalid cron expression ({exc})"
        return str(raw_value).strip(), None

    return None, f"{spec.key}: unsupported setting type"


# Lazy singleton for the **write** client. The provider in tools/appconfig.py
# is read-only (it's a SettingSelector load), so for PUTs we need the raw SDK.
_app_config_write_client = None


def _get_app_config_write_client():
    global _app_config_write_client
    if _app_config_write_client is not None:
        return _app_config_write_client

    import os

    from azure.appconfiguration import AzureAppConfigurationClient
    from azure.identity import (
        AzureCliCredential as SyncAzureCliCredential,
        ChainedTokenCredential as SyncChainedTokenCredential,
        ManagedIdentityCredential as SyncManagedIdentityCredential,
    )

    endpoint = os.environ.get("APP_CONFIG_ENDPOINT")
    if not endpoint:
        raise HTTPException(
            status_code=500,
            detail="APP_CONFIG_ENDPOINT is not configured on this service",
        )

    client_id = os.environ.get("AZURE_CLIENT_ID", "") or None
    credential = SyncChainedTokenCredential(
        SyncManagedIdentityCredential(client_id=client_id),
        SyncAzureCliCredential(),
    )
    _app_config_write_client = AzureAppConfigurationClient(
        base_url=endpoint, credential=credential
    )
    return _app_config_write_client


def _reschedule_cron_job(env_key: str, cron_expr: str) -> Optional[str]:
    """Reschedule (or add) the APScheduler job tied to *env_key*.

    Returns the job_id when something happened, or ``None`` if the key isn't
    a cron-driven job or the scheduler has no matching registry entry yet.
    """
    from main import JOB_CRON_MAP, JOB_REGISTRY, scheduler

    job_id = JOB_CRON_MAP.get(env_key)
    if not job_id:
        return None

    from apscheduler.triggers.cron import CronTrigger

    # Empty cron means "disable the job" — same semantics as the bootstrap.
    if not cron_expr:
        try:
            scheduler.remove_job(job_id)
            logging.info("[admin] Removed cron job %s (empty cron)", job_id)
            return job_id
        except Exception:
            return None

    trigger = CronTrigger.from_crontab(cron_expr, timezone=scheduler.timezone)
    existing = None
    try:
        existing = scheduler.get_job(job_id)
    except Exception:
        existing = None

    if existing is not None:
        scheduler.reschedule_job(job_id, trigger=trigger)
    else:
        func = JOB_REGISTRY.get(job_id)
        if func is None:
            return None
        scheduler.add_job(func, trigger=trigger, id=job_id, replace_existing=True)
    logging.info("[admin] Rescheduled cron job %s @ %s", job_id, cron_expr)
    return job_id


@router.get("/config")
async def get_config_settings(request: Request) -> Dict[str, Any]:
    """Return the curated Configuration-tab payload.

    Open to any caller (including unauthenticated) so the UI can render the
    tab in read-only mode without an extra auth round-trip — writes are still
    guarded by `require_admin`. ``canEdit`` is `true` when auth is off, or
    when the caller presents a valid bearer token carrying the ``Admin`` role;
    the token check is silent (same pattern as `/api/identity`) so a missing
    or invalid token simply yields `canEdit: false` without raising.
    """
    cfg = get_config()
    # Build each setting exactly once, then surface it under both the grouped
    # `sections` view (used by the section-aware UI) and a flat `settings`
    # list (the contract the typed frontend `ConfigResponse` reads). This
    # keeps the two views in lock-step and matches the documented API shape.
    by_section: Dict[str, List[Dict[str, Any]]] = {s["id"]: [] for s in CONFIG_SECTIONS}
    flat_settings: List[Dict[str, Any]] = []
    for spec in SETTINGS:
        setting = _read_setting(cfg, spec)
        by_section.setdefault(spec.section, []).append(setting)
        flat_settings.append(setting)

    auth_enabled = _auth_enabled()
    can_edit = True
    if auth_enabled:
        can_edit = False
        try:
            claims = await validate_bearer_jwt(request)
            can_edit = "Admin" in (claims.get("roles") or [])
        except Exception:
            can_edit = False

    return {
        "sections": [
            {"id": s["id"], "label": s["label"], "settings": by_section.get(s["id"], [])}
            for s in CONFIG_SECTIONS
        ],
        "settings": flat_settings,
        "canEdit": can_edit,
        "authEnabled": auth_enabled,
    }


@router.put("/config", dependencies=[Depends(require_admin)])
async def update_config_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and write a batch of setting updates.

    Body shape: ``{"updates": [{"key": "...", "value": ...}, ...]}``.
    Returns ``{applied: [...], failed: [{key, error}], rescheduled: [...]}``.
    Status is 200 when every update succeeded, 207 when partially applied,
    and 422 when nothing could be written (all entries invalid or denied).
    """
    updates = payload.get("updates")
    if not isinstance(updates, list) or not updates:
        raise HTTPException(status_code=422, detail="`updates` must be a non-empty list")

    validated: List[Tuple[SettingSpec, str]] = []
    failed: List[Dict[str, str]] = []

    for entry in updates:
        if not isinstance(entry, dict):
            failed.append({"key": "?", "error": "entry must be an object"})
            continue
        key = entry.get("key")
        if not isinstance(key, str) or not key:
            failed.append({"key": str(key), "error": "missing key"})
            continue
        if _is_denied(key):
            failed.append({"key": key, "error": "key is in the secret denylist"})
            continue
        spec = SETTINGS_BY_KEY.get(key)
        if spec is None:
            failed.append({"key": key, "error": "key is not in the allowlist"})
            continue
        normalized, err = _coerce_and_validate(spec, entry.get("value"))
        if err is not None:
            failed.append({"key": key, "error": err})
            continue
        validated.append((spec, normalized))

    if not validated:
        # Nothing to do — surface the validation errors so the UI can mark fields.
        from fastapi.responses import JSONResponse

        return JSONResponse(
            status_code=422,
            content={"applied": [], "failed": failed, "rescheduled": []},
        )

    # Perform the writes. Each setting is independent: a failure on one does
    # not roll back the others (App Configuration has no transactional batch).
    from azure.appconfiguration import ConfigurationSetting
    from azure.core.exceptions import AzureError

    write_client = _get_app_config_write_client()

    applied: List[str] = []
    rescheduled: List[str] = []
    for spec, value in validated:
        try:
            write_client.set_configuration_setting(
                ConfigurationSetting(key=spec.key, value=value, label=_APP_CONFIG_LABEL)
            )
            applied.append(spec.key)
        except AzureError as exc:
            logging.exception("Failed to write %s to App Configuration", spec.key)
            failed.append({"key": spec.key, "error": f"write failed: {exc}"})

    if not applied:
        # Every write failed — surface as a hard error.
        raise HTTPException(
            status_code=500,
            detail={"applied": applied, "failed": failed, "rescheduled": rescheduled},
        )

    # Refresh the in-process cache so subsequent reads inside this container
    # see the new values immediately. Other replicas pick them up at their
    # own refresh cadence; the operator can force this via /api/config/reload.
    try:
        get_config("refresh")
    except Exception:  # pragma: no cover - cache refresh is best-effort
        logging.exception("Failed to refresh AppConfig cache after PUT /api/config")

    # If any cron expression was applied, reschedule the matching job so the
    # change takes effect without a container restart.
    for spec, value in validated:
        if spec.key not in applied:
            continue
        if spec.type != "cron":
            continue
        try:
            jid = _reschedule_cron_job(spec.key, value)
            if jid:
                rescheduled.append(jid)
        except Exception as exc:
            logging.exception("Failed to reschedule cron for %s", spec.key)
            failed.append({"key": spec.key, "error": f"reschedule failed: {exc}"})

    status_code = 200 if not failed else 207
    if status_code == 207:
        from fastapi.responses import JSONResponse

        return JSONResponse(
            status_code=207,
            content={"applied": applied, "failed": failed, "rescheduled": rescheduled},
        )
    return {"applied": applied, "failed": failed, "rescheduled": rescheduled}


@router.post("/config/reload", dependencies=[Depends(require_admin)])
async def reload_config_cache() -> Dict[str, Any]:
    """Force a refresh of the in-process App Configuration cache."""
    get_config("refresh")
    _invalidate_cache("runs", "files")
    return {"status": "ok"}


@router.post("/config/apply", dependencies=[Depends(require_admin)])
async def apply_config_changes() -> Dict[str, Any]:
    """Soft restart: refresh AppConfig + reschedule every known cron job.

    A real Container App revision restart would need `azure-mgmt-appcontainers`
    and the resource group/app name plumbed in. Until that ships, this endpoint
    performs an in-process refresh so configuration changes take effect without
    a container reboot, and is named `/config/apply` (not `/restart`) so the
    response honestly reflects what happens.
    """
    get_config("refresh")
    _invalidate_cache("runs", "files")

    rescheduled: List[str] = []
    from main import JOB_CRON_MAP

    cfg = get_config()
    for env_key in JOB_CRON_MAP:
        cron_expr = cfg.get(env_key, default=None, allow_none=True)
        if cron_expr is None and env_key in {"CRON_RUN_BLOB_INDEX", "CRON_RUN_BLOB_PURGE"}:
            # Preserve the defaults applied at startup.
            cron_expr = SETTINGS_BY_KEY[env_key].default
        try:
            jid = _reschedule_cron_job(env_key, (cron_expr or "").strip())
            if jid:
                rescheduled.append(jid)
        except Exception:
            logging.exception("Failed to reschedule %s during /config/apply", env_key)
    return {
        "status": "ok",
        "rescheduled": rescheduled,
        "note": "In-process refresh. Hard container restart is not supported by this endpoint.",
    }
