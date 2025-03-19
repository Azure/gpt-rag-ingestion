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


# -------------------------------
# Document Chunking Function (HTTP Triggered by AI Search)
# -------------------------------

# Document Chunking Function (HTTP Triggered by AI Search)
@app.route(route="document-chunking", auth_level=func.AuthLevel.FUNCTION)
async def document_chunking(req: func.HttpRequest) -> func.HttpResponse:
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
            chunks, errors, warnings = await DocumentChunker().chunk_documents(input_data)
         
            # Debug logging
            for idx, chunk in enumerate(chunks):
                processed_chunk = chunk.copy()
                processed_chunk.pop('vector', None)
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
    except Exception as e: 
        error_message = f"An unexpected error occured: {str(e)}"
        logging.error(f"[document_chunking_function] {error_message}", exc_info=True)
        return func.HttpResponse(error_message, status_code=500)
    
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