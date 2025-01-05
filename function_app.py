import logging
import json
# import asyncio
import os
import time
import datetime
from json import JSONEncoder

import jsonschema
import azure.functions as func

from chunking import DocumentChunker
from connectors import SharepointFilesIndexer, SharepointDeletedFilesPurger
from connectors import ImagesDeletedFilesPurger
from tools import BlobStorageClient
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
suppress_loggers = [
    'azure',
    'azure.core',
    'azure.core.pipeline',
    'azure.core.pipeline.policies.http_logging_policy',
    'azsdk-python-search-documents',
    'azsdk-python-identity',
    'azure.ai.openai',  # Assuming 'aoai' refers to Azure OpenAI
    'azure.identity',
    'azure.storage',
    'azure.ai.*',  # Wildcard-like suppression for any azure.ai sub-loggers
    # Add any other specific loggers if necessary
]
for logger_name in suppress_loggers:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.WARNING)
    logger.propagate = False  

# -------------------------------
# Azure Functions
# -------------------------------

app = func.FunctionApp()

# ---------------------------------------------
# SharePoint Connector Functions (Timer Triggered)
# ---------------------------------------------

@app.function_name(name="sharepoint_index_files")
@app.schedule(
    schedule="0 */10 * * * *", 
    arg_name="timer", 
    run_on_startup=True
)
async def sharepoint_index_files(timer: func.TimerRequest) -> None:
    logging.debug("[sharepoint_index_files] Started sharepoint files indexing function.")
    try:
        indexer = SharepointFilesIndexer()
        await indexer.run() 
    except Exception as e:
        logging.error(f"[sharepoint_index_files] An unexpected error occurred: {e}", exc_info=True)

@app.function_name(name="sharepoint_purge_deleted_files")
@app.schedule(
    schedule="0 */10 * * * *", 
    arg_name="timer", 
    run_on_startup=False
)
async def sharepoint_purge_deleted_files(timer: func.TimerRequest) -> None:
    logging.debug("[sharepoint_purge_deleted_files] Started sharepoint purge deleted files function.")
    try:
        purger = SharepointDeletedFilesPurger()
        await purger.run() 
    except Exception as e:
        logging.error(f"[sharepoint_purge_deleted_files] An unexpected error occurred: {e}", exc_info=True)

# ---------------------------------------------
# Deleted Files Image Purger (Timer Triggered)
# ---------------------------------------------

@app.function_name(name="multimodality_images_purger")
@app.schedule(schedule="0 0 0 * * *",   # runs at 00:00 UTC daily
             arg_name="timer",
             run_on_startup=True,
             use_monitor=True)
async def images_purge_timer(timer: func.TimerRequest):
    if timer.past_due:
        logging.info("[multimodality_images_purger] Timer is past due.")
    
    logging.info("[multimodality_images_purger] Timer trigger started.")

    # Purge only runs when MULTIMODALITY == 'true'
    multi_var = (os.getenv("MULTIMODALITY") or "").lower()
    should_run_multimodality = multi_var in ["true", "1", "yes"]

    # Only run if MULTIMODALITY == true
    if not should_run_multimodality:
        logging.info("[multimodality_images_purger] MULTIMODALITY != true. Skipping purge.")
        return

    try:
        purger = ImagesDeletedFilesPurger()
        await purger.run()
    except Exception as e:
        logging.error(f"[multimodality_images_purger] Error running images purge: {e}")

# -------------------------------
# Document Chunking Function (HTTP Triggered by AI Search)
# -------------------------------

# Document Chunking Function (HTTP Triggered by AI Search)
@app.route(route="document-chunking", auth_level=func.AuthLevel.FUNCTION)
def document_chunking(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        jsonschema.validate(body, schema=get_request_schema())

        if body:
            # Log the incoming request
            logging.info(f'[document_chunking_function] Invoked document_chunking skill. Number of items: {len(body["values"])}.')

            input_data = {}

            # Processing one item at a time to avoid exceeding the AI Search custom skill timeout (230 seconds)
            # BatchSize should be set to 1 in the Skillset definition, if it is not set, will process just the last item
            count_items = len(body["values"])
            filename = ""
            if count_items > 1:
                logging.warning('BatchSize should be set to 1 in the Skillset definition. Processing only the last item.')
            for i, item in enumerate(body["values"]):
                input_data = item["data"]
                filename = get_filename(input_data["documentUrl"])
                logging.info(f'[document_chunking_function] Chunking document: File {filename}, Content Type {input_data["documentContentType"]}.')
            
            start_time = time.time()

            # Enrich the input data with the document bytes and file name
            blob_client = BlobStorageClient(input_data["documentUrl"])
            document_bytes = blob_client.download_blob()
            input_data['documentBytes'] = document_bytes          
            input_data['fileName'] = filename

            # Chunk the document
            chunks, errors, warnings = DocumentChunker().chunk_documents(input_data)

            # Enrich chunks with metadata to be indexed
            for chunk in chunks: chunk["source"] = "blob"
         
            # Debug logging
            for idx, chunk in enumerate(chunks):
                processed_chunk = chunk.copy()
                processed_chunk.pop('contentVector', None)
                if 'content' in processed_chunk and isinstance(processed_chunk['content'], str):
                    processed_chunk['content'] = processed_chunk['content'][:100]
                logging.debug(f"[document_chunking][{filename}] Chunk {idx + 1}: {json.dumps(processed_chunk, indent=4)}")


            # Format results
            values = {
                "recordId": item['recordId'],
                "data": {"chunks": chunks},
                "errors": errors,
                "warnings": warnings
            }
            
            results = {"values": [values]}
            result = json.dumps(results, ensure_ascii=False, cls=DateTimeEncoder)

            end_time = time.time()
            elapsed_time = end_time - start_time
            
            logging.info(f'[document_chunking_function] Finished document_chunking skill in {elapsed_time:.2f} seconds.')
            return func.HttpResponse(result, mimetype="application/json")
        else:
            error_message = "Invalid body."
            logging.error(f"[document_chunking_function] {error_message}", exc_info=True)
            return func.HttpResponse(error_message, status_code=400)
    except ValueError as e:
        error_message = f"Invalid body: {e}"
        logging.error(f"[document_chunking_function] {error_message}", exc_info=True)
        return func.HttpResponse(error_message, status_code=400)
    except jsonschema.exceptions.ValidationError as e:
        error_message = f"Invalid request: {e}"
        logging.error(f"[document_chunking_function] {error_message}", exc_info=True)
        return func.HttpResponse(error_message, status_code=400)
    
class DateTimeEncoder(JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return super().default(obj)    
    
def get_request_schema():
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