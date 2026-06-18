import datetime
import asyncio
import json
import logging
import os
import time
import subprocess
import jsonschema
import uvicorn
from tzlocal import get_localzone
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from pathlib import Path

from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

import base64

from utils.file_utils import get_filename
from dependencies import get_config, validate_api_key_header
from telemetry import Telemetry
from constants import APPLICATION_INSIGHTS_CONNECTION_STRING, APP_NAME
from utils.tools import is_azure_environment

# -------------------------------
# App Configuration (initialized at runtime)
# -------------------------------
app_config_client = None  # set inside lifespan after auth checks

# FastAPI app + Scheduler
# -------------------------------
def _resolve_timezone():
    tz_name = os.getenv("SCHEDULER_TIMEZONE")
    if tz_name:
        try:
            return ZoneInfo(tz_name)
        except Exception:
            logging.warning(f"Invalid SCHEDULER_TIMEZONE '{tz_name}', defaulting to machine timezone")
    return get_localzone()

local_tz = _resolve_timezone()
scheduler = AsyncIOScheduler(timezone=local_tz)

# -------------------------------
# Manual run-now coordination
# -------------------------------
# `_running_jobs` is the source of truth for "is this job_type executing right now?".
# It is consulted both by the manual `POST /api/jobs/{job_type}/run` endpoint to
# return 409 on concurrent triggers, and by the scheduler wrapper so cron-triggered
# runs participate in the same mutual exclusion as manual runs.
_running_jobs: set[str] = set()
_running_jobs_lock = asyncio.Lock()


def _track_running(job_id: str, func):
    """Wrap an async job function so its execution is reflected in `_running_jobs`."""

    async def _wrapped():
        async with _running_jobs_lock:
            _running_jobs.add(job_id)
        try:
            return await func()
        finally:
            async with _running_jobs_lock:
                _running_jobs.discard(job_id)

    _wrapped.__name__ = getattr(func, "__name__", job_id)
    return _wrapped


# Populated inside lifespan once the job functions are defined.
JOB_REGISTRY: dict[str, "object"] = {}

# Maps CRON_RUN_* App Configuration keys to the APScheduler `job_id` they drive.
# Exposed at module scope so `api.admin` (Configuration tab) can reschedule the
# right job after a PUT /api/config that updates a cron expression — without
# having to mirror the mapping inside `lifespan` and risk drift.
JOB_CRON_MAP: dict[str, str] = {
    "CRON_RUN_SHAREPOINT_INDEX": "sharepoint_index",
    "CRON_RUN_SHAREPOINT_PURGE": "sharepoint_purge",
    "CRON_RUN_IMAGES_PURGE": "multimodality_images_purge",
    "CRON_RUN_BLOB_INDEX": "blob_index",
    "CRON_RUN_BLOB_PURGE": "blob_purge",
    "CRON_RUN_NL2SQL_INDEX": "nl2sql_index",
    "CRON_RUN_NL2SQL_PURGE": "nl2sql_purge",
}

@asynccontextmanager
async def lifespan(app: FastAPI):

    # scheduler helper
    def _schedule(env_key: str, func, job_id: str, human_name: str, default_cron: str | None = None) -> bool:
        """Schedule a cron job from App Configuration.

        Returns True when a cron expression is available and the job was added.
        """
        cron_expr = app_config_client.get(env_key, default=None, allow_none=True)
        if not cron_expr and default_cron:
            cron_expr = default_cron
            logging.info(f"[{human_name}] {env_key} not set - using default cron: {default_cron}")

        if cron_expr:
            try:
                trigger = CronTrigger.from_crontab(cron_expr, timezone=local_tz)
                # Do not request an immediate run via next_run_time; we will
                # optionally run scheduled jobs explicitly and sequentially below.
                scheduler.add_job(
                    func,
                    trigger=trigger,
                    id=job_id,
                    replace_existing=True,
                )
                logging.info(f"[{human_name}] Scheduled @ {cron_expr}")
                return True
            except ValueError:
                raise RuntimeError(f"Invalid {env_key}: {cron_expr!r}")
        else:
            logging.warning(f"[{human_name}] {env_key} not set — skipping job")
            return False

    # Compact authentication check.
    # NOTE: In Azure Container Apps, Managed Identity works via IMDS and does not necessarily
    # surface as IDENTITY_ENDPOINT/MSI_* environment variables. Failing fast here can cause
    # restart loops (and looks like a port/probe issue). Default to non-fatal in Azure.
    def _ensure_auth_or_exit() -> None:
        env = os.environ
        has_mi = any(env.get(k) for k in ("IDENTITY_ENDPOINT", "MSI_ENDPOINT", "MSI_SECRET"))
        has_sp = all(env.get(k) for k in ("AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"))
        has_cli = False
        if not is_azure_environment():
            try:
                has_cli = subprocess.run(["az", "account", "show", "-o", "none"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
            except Exception:
                has_cli = False

        default_require_auth = "false" if is_azure_environment() else "true"
        require_auth = (env.get("REQUIRE_AUTH_ON_STARTUP") or default_require_auth).lower() in ("true", "1", "yes")
        if not (has_sp or has_mi or has_cli):
            msg = "The service did not detect authentication env vars (configure Managed Identity / Service Principal in Azure, or run 'az login' locally)."
            if require_auth:
                logging.warning(msg + " Exiting...")
                logging.shutdown()
                os._exit(1)
            logging.warning(msg + " Continuing startup (REQUIRE_AUTH_ON_STARTUP!=true).")

    _ensure_auth_or_exit()

    # Reduce Azure SDK noise in local/dev logs
    def _quiet_azure_sdks():
        try:
            # Reduce noisy Azure SDK and HTTP logging. For the http_logging_policy
            # (which prints request headers/body) set CRITICAL so info/debug are
            # suppressed. Also disable propagation and attach a NullHandler to
            # prevent the messages from reaching the root logger.
            noisy = [
                "azure.core.pipeline.policies.http_logging_policy",
                "azure.core.pipeline.policies",
                "azure.identity",
                "azure",
                "urllib3",
            ]
            for name in noisy:
                lg = logging.getLogger(name)
                # hide info/debug logs from these loggers
                lg.setLevel(logging.CRITICAL if name.endswith("http_logging_policy") else logging.WARNING)
                lg.propagate = False
                lg.addHandler(logging.NullHandler())
        except Exception:
            pass

    _quiet_azure_sdks()

    # Initialize App Configuration only after passing auth checks
    global app_config_client
    app_config_client = get_config()

    Telemetry.configure_monitoring(app_config_client, APPLICATION_INSIGHTS_CONNECTION_STRING, APP_NAME)

    # Start the scheduler before scheduling any jobs
    scheduler.start()
    logging.info(f"Scheduler timezone: {local_tz}")

    now = datetime.datetime.now(tz=local_tz)

    # Wrap every run_* function so manual and cron-triggered executions share
    # the same _running_jobs set. The wrapper is the value stored in JOB_REGISTRY
    # and the callable passed to APScheduler, so cron runs also block manual
    # 409 collisions and vice-versa.
    _wrapped_jobs: dict[str, object] = {
        "sharepoint_index": _track_running("sharepoint_index", run_sharepoint_index),
        "sharepoint_purge": _track_running("sharepoint_purge", run_sharepoint_purge),
        "multimodality_images_purge": _track_running("multimodality_images_purge", run_images_purge),
        "blob_index": _track_running("blob_index", run_blob_index),
        "blob_purge": _track_running("blob_purge", run_blob_purge),
        "nl2sql_index": _track_running("nl2sql_index", run_nl2sql_index),
        "nl2sql_purge": _track_running("nl2sql_purge", run_nl2sql_purge),
    }
    JOB_REGISTRY.clear()
    JOB_REGISTRY.update(_wrapped_jobs)

    s_sharepoint_index = _schedule("CRON_RUN_SHAREPOINT_INDEX", _wrapped_jobs["sharepoint_index"], "sharepoint_index", "sharepoint-indexer")
    s_sharepoint_purge = _schedule("CRON_RUN_SHAREPOINT_PURGE", _wrapped_jobs["sharepoint_purge"], "sharepoint_purge", "sharepoint-purger")
    s_images_purge = _schedule("CRON_RUN_IMAGES_PURGE", _wrapped_jobs["multimodality_images_purge"], "multimodality_images_purge", "multimodality-images-purger")
    s_blob_index = _schedule("CRON_RUN_BLOB_INDEX", _wrapped_jobs["blob_index"], "blob_index", "blob-storage-indexer", default_cron="0 * * * *")
    s_blob_purge = _schedule("CRON_RUN_BLOB_PURGE", _wrapped_jobs["blob_purge"], "blob_purge", "blob-storage-indexer-purger", default_cron="10 * * * *")
    s_nl2sql_index = _schedule("CRON_RUN_NL2SQL_INDEX", _wrapped_jobs["nl2sql_index"], "nl2sql_index", "nl2sql-indexer")
    s_nl2sql_purge = _schedule("CRON_RUN_NL2SQL_PURGE", _wrapped_jobs["nl2sql_purge"], "nl2sql_purge", "nl2sql-indexer-purger")

    # Log cleanup: runs immediately then every hour (not shown in dashboard)
    from api.admin import _cleanup_old_runs
    _schedule("CRON_RUN_LOG_CLEANUP", _cleanup_old_runs, "log_cleanup", "log-cleanup", default_cron="0 * * * *")

    # Optional: run scheduled jobs once at startup.
    # WARNING: In Azure, long-running jobs can block app startup/health probes and
    # cause the container to restart in a loop. Default to OFF in Azure.
    try:
        # default_startup_run = "false" if is_azure_environment() else "true"
        default_startup_run = "true"
        startup_run = (app_config_client.get("RUN_JOBS_ON_STARTUP", default_startup_run) or "").lower() in (
            "true",
            "1",
            "yes",
        )
    except Exception:
        startup_run = False

    startup_task: asyncio.Task | None = None

    async def _run_startup_jobs() -> None:
        # Always run log cleanup first to reduce blob count
        try:
            logging.info("[startup] Running log-cleanup immediately")
            await _cleanup_old_runs()
        except Exception:
            logging.exception("[startup] Error running log-cleanup")

        # If a job was scheduled (env var provided or default cron fallback), run it once sequentially.
        # Only run jobs whose `_schedule` helper returned True.
        try:
            if s_blob_index:
                logging.info("[startup] Running blob-storage-indexer immediately")
                await _wrapped_jobs["blob_index"]()
            if s_blob_purge:
                logging.info("[startup] Running blob-purge immediately")
                await _wrapped_jobs["blob_purge"]()
            if s_nl2sql_index:
                logging.info("[startup] Running nl2sql-indexer immediately")
                await _wrapped_jobs["nl2sql_index"]()
            if s_nl2sql_purge:
                logging.info("[startup] Running nl2sql-purge immediately")
                await _wrapped_jobs["nl2sql_purge"]()
            if s_sharepoint_index:
                logging.info("[startup] Running sharepoint-indexer immediately")
                await _wrapped_jobs["sharepoint_index"]()
            if s_sharepoint_purge:
                logging.info("[startup] Running sharepoint-purger immediately")
                await _wrapped_jobs["sharepoint_purge"]()
            if s_images_purge:
                logging.info("[startup] Running multimodality-images-purger immediately")
                await _wrapped_jobs["multimodality_images_purge"]()
        except asyncio.CancelledError:
            logging.info("[startup] Startup jobs cancelled")
            raise
        except Exception:
            logging.exception("[startup] Error while running immediate scheduled jobs")

    if startup_run:
        # Critical: do NOT block lifespan startup. Uvicorn binds its listen socket
        # only after lifespan startup completes; blocking here causes Container Apps
        # startup/readiness probes to fail and the app to restart in a loop.
        logging.info("[startup] Scheduling immediate job runs in background")
        startup_task = asyncio.create_task(_run_startup_jobs())
    else:
        # Even without startup jobs, always run log cleanup
        logging.info("[startup] Skipping immediate job runs (RUN_JOBS_ON_STARTUP!=true)")
        async def _run_cleanup_only() -> None:
            try:
                logging.info("[startup] Running log-cleanup immediately")
                await _cleanup_old_runs()
            except Exception:
                logging.exception("[startup] Error running log-cleanup")
        startup_task = asyncio.create_task(_run_cleanup_only())

    yield

    if startup_task:
        startup_task.cancel()
        try:
            await startup_task
        except asyncio.CancelledError:
            pass

    scheduler.shutdown(wait=False)

# Load version from VERSION file 
VERSION_FILE = Path(__file__).resolve().parent / "VERSION"
try:
    APP_VERSION = VERSION_FILE.read_text().strip()
except FileNotFoundError:
    APP_VERSION = "0.0.0"

app = FastAPI(
    title="GPT-RAG Ingestion",
    description="GPT-RAG Data Ingestion FastAPI",
    version=APP_VERSION,
    lifespan=lifespan
)


@app.get("/", include_in_schema=False)
async def root():
    return {"status": "ok", "name": APP_NAME, "version": APP_VERSION}


@app.get("/healthz", include_in_schema=False)
async def healthz():
    # Liveness: keep it fast and dependency-free.
    return {"status": "ok"}


@app.get("/readyz", include_in_schema=False)
async def readyz():
    # Readiness: report whether config has been initialized.
    if app_config_client is None:
        return Response("not ready", status_code=503)
    return {"status": "ready"}

# -------------------------------
# Timer job wrappers
# -------------------------------
async def run_sharepoint_index():
    logging.debug("[sharepoint-indexer] Starting")
    try:
        from jobs.sharepoint_indexer import SharePointIndexer
        await SharePointIndexer().run()
    except Exception:
        logging.exception("[sharepoint-indexer] Unexpected error")

async def run_sharepoint_purge():
    logging.debug("[sharepoint-purger] Starting")
    try:
        from jobs.sharepoint_purger import SharepointPurger
        await SharepointPurger().run()
    except Exception:
        logging.exception("[sharepoint-purger] Unexpected error")

async def run_images_purge():
    logging.info("[multimodality_images_purger] Starting")
    multi_var = (app_config_client.get("MULTIMODAL") or "").lower()
    if multi_var not in ("true", "1", "yes"):
        logging.info("[multimodality_images_purger] Skipped (MULTIMODAL!=true)")
        return
    try:
        from jobs.multimodal_images_purger import ImagesDeletedFilesPurger
        await ImagesDeletedFilesPurger().run()
    except Exception:
        logging.exception("[multimodality_images_purger] Error")

async def run_blob_index():
    logging.debug("[blob-storage-indexer] Starting")
    try:
        from jobs.blob_storage_indexer import BlobStorageDocumentIndexer
        await BlobStorageDocumentIndexer().run()
    except Exception:
        logging.exception("[blob-storage-indexer] Unexpected error")

async def run_blob_purge():
    logging.debug("[blob-storage-indexer-purger] Starting")
    try:
        from jobs.blob_storage_indexer import BlobStorageDeletedItemsCleaner
        await BlobStorageDeletedItemsCleaner().run()
    except Exception:
        logging.exception("[blob-storage-indexer-purger] Unexpected error")

async def run_sharepoint_index():
    logging.debug("[sharepoint-indexer] Starting")
    try:
        from jobs.sharepoint_indexer import SharePointIndexer
        await SharePointIndexer().run()
    except Exception:
        logging.exception("[sharepoint-indexer] Unexpected error")

async def run_sharepoint_purge():
    logging.debug("[sharepoint-purger] Starting")
    try:
        from jobs.sharepoint_purger import SharePointPurger
        await SharePointPurger().run()
    except Exception:
        logging.exception("[sharepoint-purger] Unexpected error")        

async def run_nl2sql_index():
    logging.debug("[nl2sql-indexer] Starting")
    try:
        from jobs.nl2sql_indexer import NL2SQLIndexer
        await NL2SQLIndexer().run()
    except Exception:
        logging.exception("[nl2sql-indexer] Unexpected error")

async def run_nl2sql_purge():
    logging.debug("[nl2sql-indexer-purger] Starting")
    try:
        from jobs.nl2sql_purger import NL2SQLPurger
        await NL2SQLPurger().run()
    except Exception:
        logging.exception("[nl2sql-indexer-purger] Unexpected error")

# -------------------------------
# HTTP-triggered document-chunking
# -------------------------------
@app.post("/document-chunking", dependencies=[Depends(validate_api_key_header)])
async def document_chunking(request: Request):
    start_time = time.time()
    # --- parse JSON ---
    try:
        body = await request.json()
    except json.JSONDecodeError as e:
        logging.error(f"[document_chunking] Invalid JSON: {e}")
        return Response(f"Invalid JSON: {e}", status_code=400)

    # --- validate schema ---
    try:
        jsonschema.validate(body, schema=get_document_chunking_request_schema())
    except jsonschema.ValidationError as e:
        logging.error(f"[document_chunking] Validation error: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid request: {e}")

    values_list = body.get("values")
    if not values_list:
        logging.error("[document_chunking] Invalid body: missing values")
        return Response("Invalid body: missing values", status_code=400)

    logging.info(f'[document_chunking] Invoked document_chunking skill. Number of items: {len(values_list)}.')

    # Only process the last item if >1
    if len(values_list) > 1:
        logging.warning('BatchSize should be set to 1; processing only the last item.')
    item = values_list[-1]
    input_data = item["data"]
    filename = get_filename(input_data["documentUrl"])
    logging.info(f'[document_chunking] Chunking document: File {filename}, Content Type {input_data["documentContentType"]}.')

    # download and enrich
    from tools import BlobClient
    blob_client = BlobClient(input_data["documentUrl"])
    document_bytes = blob_client.download_blob()
    input_data['documentBytes'] = document_bytes
    input_data['fileName'] = filename

    # chunk
    from chunking import DocumentChunker
    chunks, errors, warnings = DocumentChunker().chunk_documents(input_data)
    for c in chunks:
        c["source"] = "blob"

    # debug log first 100 chars of each
    for idx, chunk in enumerate(chunks):
        preview = chunk.get("content", "")[:100]
        logging.debug(f"[document_chunking][{filename}] Chunk {idx+1}: {preview!r}")

    # build result
    record_id = item.get("recordId")
    result_payload = {
        "values": [
            {
                "recordId": record_id,
                "data": {"chunks": chunks},
                "errors": errors,
                "warnings": warnings
            }
        ]
    }

    elapsed = time.time() - start_time
    logging.info(f'[document_chunking] Finished in {elapsed:.2f} seconds.')

    return JSONResponse(content=result_payload)

def get_document_chunking_request_schema():
    return {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "type": "object",
        "properties": {
            "values": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "properties": {
                        "recordId": {"type": "string"},
                        "data": {
                            "type": "object",
                            "properties": {
                                "documentUrl": {"type": "string", "minLength": 1},
                              
                                "documentSasToken": {"type": "string", "minLength": 0},

                                "documentContentType": {"type": "string", "minLength": 1}
                            },
                            "required": ["documentUrl", "documentContentType"],
                        },
                    },
                    "required": ["recordId", "data"],
                },
            }
        },
        "required": ["values"],
    }

# -------------------------------
# HTTP-triggered text-embedding
# -------------------------------
@app.post("/text-embedding", dependencies=[Depends(validate_api_key_header)])
async def text_embedding(request: Request):
    start_time = time.time()
    try:
        body = await request.json()
    except json.JSONDecodeError as e:
        logging.error(f"[text_embedding] Invalid JSON: {e}")
        return Response(f"Invalid JSON: {e}", status_code=400)

    if not body or "values" not in body:
        logging.error("[text_embedding] Invalid body.")
        return Response("Invalid body.", status_code=400)

    logging.info(f'[text_embedding] Invoked text_embedding skill. Number of items: {len(body["values"])}.')

    from tools import AzureOpenAIClient
    aoai_client = AzureOpenAIClient()
    values = []

    for item in body["values"]:
        record_id = item.get("recordId")
        input_data = item.get("data", {}).get("text", "")
        logging.info(f'[text_embedding] Generating embeddings for: {input_data[:10]}…')

        errors = []
        warnings = []
        data_payload = {}

        try:
            contentVector = aoai_client.get_embeddings(input_data)
            data_payload = {"embedding": contentVector}
        except Exception as e:
            error_message = f"Error generating embeddings: {e}"
            logging.error(f'[text_embedding] {error_message}', exc_info=True)
            errors.append({"message": error_message})

        values.append({
            "recordId": record_id,
            "data": data_payload,
            "errors": errors,
            "warnings": warnings
        })

    results = {"values": values}

    elapsed = time.time() - start_time
    logging.info(f'[text_embedding] Finished in {elapsed:.2f} seconds.')

    return JSONResponse(content=results)
    
# -------------------------------
# HTTP-triggered ingest-documents (base64 upload, chunk, embed, index)
# -------------------------------
@app.post("/ingest-documents", dependencies=[Depends(validate_api_key_header)])
async def ingest_documents(request: Request):
    start_time = time.time()

    try:
        body = await request.json()
    except json.JSONDecodeError as e:
        logging.error(f"[ingest_documents] Invalid JSON: {e}")
        return Response(f"Invalid JSON: {e}", status_code=400)

    # --- validate schema ---
    try:
        jsonschema.validate(body, schema=get_ingest_documents_request_schema())
    except jsonschema.ValidationError as e:
        logging.error(f"[ingest_documents] Validation error: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid request: {e}") from e

    values_list = body.get("values")
    conversation_id = body.get("conversationId")

    # Optional ACL: object ids of the user uploading the documents. When the
    # search index has permissionFilterOption enabled, chunks with empty
    # security ids are trimmed out by AI Search and the uploader can never
    # retrieve them (issue #478). Sanitize out anonymous/placeholder values so
    # we never stamp a meaningless acl.
    _ACL_PLACEHOLDERS = {"", "no-auth", "anonymous", "00000000-0000-0000-0000-000000000000"}
    raw_security_user_ids = body.get("securityUserIds") or []
    security_user_ids = [
        str(uid).strip()
        for uid in raw_security_user_ids
        if isinstance(uid, str) and str(uid).strip().lower() not in _ACL_PLACEHOLDERS
    ]

    if not values_list or not isinstance(values_list, list):
        return Response("Invalid body: missing or invalid values array", status_code=400)

    if len(values_list) > 5:
        return Response("Too many files (max 5)", status_code=400)

    from chunking import DocumentChunker
    from tools import AzureOpenAIClient
    from tools import AISearchClient
    from tools.blob import upload_bytes_to_container
    from jobs.sharepoint_ingestion_config import _make_chunk_key

    aoai_client = AzureOpenAIClient()
    search_client = AISearchClient()

    index_name = app_config_client.get("SEARCH_RAG_INDEX_NAME")

    results = []

    for item in values_list:
        record_id = item.get("recordId")
        data = item.get("data", {})
        file_name = data.get("fileName")
        content_type = data.get("contentType")
        file_b64 = data.get("fileBase64")

        errors = []
        warnings = []

        if not (file_name and content_type and file_b64):
            errors.append({"message": "Missing fileName, contentType or fileBase64 in data"})
            results.append({"recordId": record_id, "errors": errors, "warnings": warnings})
            continue

        # --- Decode base64 ---
        try:
            file_bytes = base64.b64decode(file_b64)
        except Exception as e:
            errors.append({"message": f"Error decoding base64: {e}"})
            results.append({"recordId": record_id, "errors": errors, "warnings": warnings})
            continue

        # --- Chunk document ---
        norm_file_name = get_filename(file_name)

        # --- Persist original bytes to per-conversation blob container (best-effort) ---
        conv_docs_container = app_config_client.get(
            "CONVERSATION_DOCUMENTS_STORAGE_CONTAINER", None, allow_none=True
        )
        safe_record_id = (record_id or "").strip() or "record"
        blob_path = f"conversations/{conversation_id}/{safe_record_id}/{norm_file_name}"
        blob_url = ""
        if conv_docs_container:
            try:
                blob_url = upload_bytes_to_container(
                    container_name=conv_docs_container,
                    blob_name=blob_path,
                    data=file_bytes,
                    content_type=content_type or "application/octet-stream",
                    metadata={
                        "conversationId": conversation_id,
                        "recordId": safe_record_id,
                    },
                )
            except Exception as e:
                warnings.append({"message": f"Blob persistence failed: {e}"})
        else:
            warnings.append(
                {"message": "CONVERSATION_DOCUMENTS_STORAGE_CONTAINER not configured; original file not persisted"}
            )

        input_data = {
            "documentUrl": blob_url or blob_path,
            "documentBytes": file_bytes,
            "fileName": norm_file_name,
            "documentContentType": content_type
        }

        try:
            chunks, chunk_errors, chunk_warnings = DocumentChunker().chunk_documents(input_data)
            errors.extend(chunk_errors)
            warnings.extend(chunk_warnings)
        except Exception as e:
            errors.append({"message": f"Chunking error: {e}"})
            results.append({"recordId": record_id, "errors": errors, "warnings": warnings})
            continue

        # --- Prepare documents batch (fields must match AI Search index / blob indexer schema) ---
        documents_to_upload = []
        parent_id = f"/ingest/{conversation_id}/{record_id}/{norm_file_name}"
        last_modified = datetime.datetime.now(datetime.timezone.utc)
        emb_dims = int(app_config_client.get("EMBEDDINGS_VECTOR_DIMENSIONS", "3072"))

        for chunk in chunks:
            try:
                content = chunk.get("content", "")
                if not content.strip():
                    continue

                embedding = aoai_client.get_embeddings(content)
                chunk_id = int(chunk.get("chunk_id", 0))
                caption_vec = chunk.get("captionVector")
                if not caption_vec:
                    caption_vec = [0.0] * emb_dims

                document = {
                    "id": _make_chunk_key(parent_id, chunk_id),
                    "parent_id": parent_id,
                    "conversationId": conversation_id,
                    "metadata_storage_path": parent_id,
                    "metadata_storage_name": norm_file_name,
                    "metadata_storage_last_modified": last_modified,
                    "metadata_security_user_ids": list(security_user_ids),
                    "metadata_security_group_ids": [],
                    "metadata_security_rbac_scope": "",
                    "chunk_id": chunk_id,
                    "content": content,
                    "imageCaptions": chunk.get("imageCaptions", ""),
                    "page": int(chunk.get("page", 0)),
                    "offset": int(chunk.get("offset", 0)),
                    "length": int(chunk.get("length", len(content))),
                    "title": chunk.get("title", ""),
                    "category": chunk.get("category", ""),
                    "filepath": blob_path if blob_url else chunk.get("filepath", ""),
                    "url": blob_url or chunk.get("url", ""),
                    "summary": chunk.get("summary", ""),
                    "relatedImages": chunk.get("relatedImages", []),
                    "relatedFiles": chunk.get("relatedFiles", []),
                    "source": "ingest-documents",
                    "contentVector": embedding,
                    "captionVector": caption_vec,
                }

                documents_to_upload.append(document)

            except Exception as e:
                errors.append({"message": f"Embedding error: {e}"})

        # --- Batch upload ---
        indexed_count = 0

        if documents_to_upload:
            try:
                logging.info("About to load")
                client = await search_client.get_search_client(index_name)
                result = await client.upload_documents(documents=documents_to_upload)
                logging.info(result)

                indexed_count = sum(1 for r in result if r.succeeded)

                failed = [r for r in result if not r.succeeded]
                for f in failed:
                    errors.append({"message": f"Indexing failed for document id {f.key}"})

            except Exception as e:
                errors.append({"message": f"Batch indexing error: {e}"})

        logging.info(
            f"[ingest_documents] File {norm_file_name}: "
            f"{indexed_count}/{len(documents_to_upload)} chunks indexed."
        )

        results.append({
            "recordId": record_id,
            "indexedChunks": indexed_count,
            "errors": errors,
            "warnings": warnings
        })

    elapsed = time.time() - start_time
    logging.info(f"[ingest_documents] Finished in {elapsed:.2f} seconds.")

    return JSONResponse(content={"values": results})

def get_ingest_documents_request_schema():
    return {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "type": "object",
        "properties": {
            "conversationId": {
                "type": "string",
                "minLength": 1
            },
            "securityUserIds": {
                "type": "array",
                "items": {
                    "type": "string"
                }
            },
            "values": {
                "type": "array",
                "minItems": 1,
                "maxItems": 5,
                "items": {
                    "type": "object",
                    "properties": {
                        "recordId": {
                            "type": "string",
                            "minLength": 1
                        },
                        "data": {
                            "type": "object",
                            "properties": {
                                "fileName": {
                                    "type": "string",
                                    "minLength": 1
                                },
                                "contentType": {
                                    "type": "string",
                                    "minLength": 1
                                },
                                "fileBase64": {
                                    "type": "string",
                                    "minLength": 1
                                }
                            },
                            "required": [
                                "fileName",
                                "contentType",
                                "fileBase64"
                            ],
                            "additionalProperties": False
                        }
                    },
                    "required": ["recordId", "data"],
                    "additionalProperties": False
                }
            }
        },
        "required": ["conversationId", "values"],
        "additionalProperties": False
    }

HTTPXClientInstrumentor().instrument()
FastAPIInstrumentor.instrument_app(app)

# Admin API router
from api.admin import router as admin_router
app.include_router(admin_router)

# Serve frontend static files (built by Vite into ./static)
_static_dir = Path(__file__).resolve().parent / "static"
if _static_dir.is_dir():
    from fastapi.responses import FileResponse

    app.mount("/assets", StaticFiles(directory=str(_static_dir / "assets")), name="static-assets")

    @app.get("/logo.png", include_in_schema=False)
    async def logo():
        return FileResponse(str(_static_dir / "logo.png"))

    @app.get("/dashboard", include_in_schema=False)
    async def dashboard():
        return FileResponse(str(_static_dir / "index.html"))


# Only run Uvicorn directly when executing this file as a script.
# When launched via `uvicorn main:app ...`, this block will not run.
if __name__ == "__main__":
    if not is_azure_environment():
        uvicorn.run("main:app", host="0.0.0.0", port=80, log_level="debug", timeout_keep_alive=60, reload=False)
