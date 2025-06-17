import datetime
import json
import logging
import os
import time
import jsonschema

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from contextlib import asynccontextmanager

from chunking import DocumentChunker
from connectors import (
    ImagesDeletedFilesPurger,
    SharepointDeletedFilesPurger,
    SharepointFilesIndexer,
)
from tools import (
    AppConfigClient,
    AzureOpenAIClient,
    BlobClient
)
from utils.file_utils import get_filename

# -------------------------------
# Logging configuration
# -------------------------------
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
log_level = getattr(logging, log_level, logging.INFO)
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# -------------------------------
# Load App Configuration into ENV
# -------------------------------
app_config_client = AppConfigClient()
app_config_client.apply_environment_settings()

# -------------------------------
# FastAPI app + Scheduler
# -------------------------------
scheduler = AsyncIOScheduler(timezone="UTC")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicia o scheduler antes de agendar qualquer tarefa
    scheduler.start()

    # 1) SharePoint index job
    cron_expr = os.getenv("CRON_RUN_SHAREPOINT_INDEX")
    if cron_expr:
        try:
            trigger = CronTrigger.from_crontab(cron_expr)
            scheduler.add_job(
                run_sharepoint_index,
                trigger=trigger,
                id="sharepoint_index_files",
                replace_existing=True,
            )
            logging.info(f"Scheduled sharepoint_index_files @ {cron_expr}")
        except ValueError:
            raise RuntimeError(f"Invalid CRON_RUN_SHAREPOINT_INDEX: {cron_expr!r}")
    else:
        logging.warning("CRON_RUN_SHAREPOINT_INDEX not set — skipping sharepoint_index_files")

    # 2) SharePoint purge job
    cron_expr = os.getenv("CRON_RUN_SHAREPOINT_PURGE")
    if cron_expr:
        try:
            trigger = CronTrigger.from_crontab(cron_expr)
            scheduler.add_job(
                run_sharepoint_purge,
                trigger=trigger,
                id="sharepoint_purge_deleted_files",
                replace_existing=True,
            )
            logging.info(f"Scheduled sharepoint_purge_deleted_files @ {cron_expr}")
        except ValueError:
            raise RuntimeError(f"Invalid CRON_RUN_SHAREPOINT_PURGE: {cron_expr!r}")
    else:
        logging.warning("CRON_RUN_SHAREPOINT_PURGE not set — skipping sharepoint_purge_deleted_files")

    # 3) Images purge job
    cron_expr = os.getenv("CRON_RUN_IMAGES_PURGE")
    if cron_expr:
        try:
            trigger = CronTrigger.from_crontab(cron_expr)
            scheduler.add_job(
                run_images_purge,
                trigger=trigger,
                id="multimodality_images_purger",
                replace_existing=True,
            )
            logging.info(f"Scheduled multimodality_images_purger @ {cron_expr}")
        except ValueError:
            raise RuntimeError(f"Invalid CRON_RUN_IMAGES_PURGE: {cron_expr!r}")
    else:
        logging.warning("CRON_RUN_IMAGES_PURGE not set — skipping multimodality_images_purger")

    yield

    scheduler.shutdown(wait=False)

app = FastAPI(lifespan=lifespan)


# -------------------------------
# Timer job wrappers
# -------------------------------
async def run_sharepoint_index():
    logging.debug("[sharepoint_index_files] Starting")
    try:
        await SharepointFilesIndexer().run()
    except Exception:
        logging.exception("[sharepoint_index_files] Unexpected error")


async def run_sharepoint_purge():
    logging.debug("[sharepoint_purge_deleted_files] Starting")
    try:
        await SharepointDeletedFilesPurger().run()
    except Exception:
        logging.exception("[sharepoint_purge_deleted_files] Unexpected error")


async def run_images_purge():
    logging.info("[multimodality_images_purger] Starting")
    multi_var = (os.getenv("MULTIMODAL") or "").lower()
    if multi_var not in ("true", "1", "yes"):
        logging.info("[multimodality_images_purger] Skipped (MULTIMODAL!=true)")
        return
    try:
        await ImagesDeletedFilesPurger().run()
    except Exception:
        logging.exception("[multimodality_images_purger] Error")


# -------------------------------
# HTTP-triggered document-chunking
# -------------------------------
@app.post("/document-chunking")
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
    blob_client = BlobClient(input_data["documentUrl"])
    document_bytes = blob_client.download_blob()
    input_data['documentBytes'] = document_bytes
    input_data['fileName'] = filename

    # chunk
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
@app.post("/text-embedding")
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