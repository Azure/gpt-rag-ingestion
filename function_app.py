import datetime
import time
import json
import logging
import os
from json import JSONEncoder

import azure.functions as func
import azurefunctions.extensions.bindings.blob as blob
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import yaml

from actions.extractor import Extractor
from actions.transformer import Transformer
from chunker import chunk_documents_docint, chunk_documents_raw

###############################################################################
# Read Pipeline Configurations
###############################################################################

def read_config(config_type):
    config_file_map = {
        'extract': 'pipeline_config/extract_config.yaml',
        'transform': 'pipeline_config/transform_config.yaml'
    }
    
    try:
        config_file = config_file_map.get(config_type)
        if not config_file:
            raise ValueError(f"Invalid config type: {config_type}")
        
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
        
        logging.info(f"[pipeline_initialization] {config_type.capitalize()} configuration loaded.")
        return config
    except Exception as e:
        logging.error(f"[pipeline_initialization] An unexpected error occurred: {e}")
        return None


###############################################################################
# Pipeline Functions
###############################################################################

app = func.FunctionApp()

# # Extract Function (Timer Triggered)
# @app.function_name(name="extract_function")
# @app.timer_trigger(schedule="0 */5 * * * *", arg_name="mytimer", run_on_startup=True) 
# def extract_function(mytimer: func.TimerRequest) -> None: 
#     utc_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
#     if mytimer.past_due:
#         logging.info('[extract_function] The timer is past due!')
#     extractor = Extractor(read_config('extract'))
#     extractor.extract()    
#     logging.info('[extract_function] Python timer trigger function ran at %s', utc_timestamp)

# def check_blob_exists(container_name, blob_name):
#     blob_service_client = BlobServiceClient.from_connection_string("your_connection_string")
#     blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
#     return blob_client.exists()

# # Transform Function (Blob Triggered)
# @app.blob_trigger(arg_name="client", path="rawdata/{name}", connection="RagStorageConnection")
# def blob_trigger_func(client: blob.BlobClient):
#     logging.info(f"[transform_function] New blob arrived: {client.get_blob_properties().name}")
    
#     connection_string = os.getenv("AzureWebJobsRagStorageConnection")
#     if not connection_string:
#         logging.error("[transform_function] Connection string is either blank or malformed.")
#         return
    
#     try:
#         if not check_blob_exists("rawdata", client.get_blob_properties().name):
#             raise FileNotFoundError(f"Blob {client.get_blob_properties().name} does not exist.")
        
#         transformer = Transformer(read_config('transform'))
#         transformer.transform(client)
#         logging.info(f"[transform_function] Blob trigger function processed blob: {client.get_blob_properties().name}")
#     except Exception as e:
#         logging.error(f"[transform_function] Error processing blob: {client.get_blob_properties().name}. Exception: {e}")

# Document Chunking Function (HTTP Triggered by AI Search)
@app.route(route="document-chunking", auth_level=func.AuthLevel.FUNCTION)
def document_chunking(req: func.HttpRequest) -> func.HttpResponse:
    import jsonschema
    
    logging.info('[document_chunking] Invoked document_chunking function.')
    try:
        body = req.get_json()
        logging.debug(f'[document_chunking] REQUEST BODY: {body}')
        jsonschema.validate(body, schema=get_request_schema())

        if body:
            start_time = time.time()
            result = process_documents(body)
            end_time = time.time()
            elapsed_time = end_time - start_time
            logging.info(f'[document_chunking] Finished document_chunking skill in {elapsed_time:.2f} seconds.')
            return func.HttpResponse(result, mimetype="application/json")
        else:
            error_message = "Invalid body."
            logging.error(f"[document_chunking] {error_message}")
            return func.HttpResponse(error_message, status_code=400)
    except ValueError as e:
        error_message = "Invalid body: {0}".format(e)
        logging.error(f"[document_chunking] {error_message}")
        return func.HttpResponse(error_message, status_code=400)
    except jsonschema.exceptions.ValidationError as e:
        error_message = "Invalid request: {0}".format(e)
        logging.error(f"[document_chunking] {error_message}")
        return func.HttpResponse(error_message, status_code=400)

def format_messages(messages):
    formatted = [{"message": msg} for msg in messages]
    return formatted

def process_documents(body):
    values = body['values']
    results = {}
    results["values"] = []
    for value in values:
        # perform operation on each record (document)
        data = value['data']
        
        chunks = []
        errors = []
        warnings = []
        
        output_record = {
            "recordId": value['recordId'],
            "data": None,
            "errors": None,
            "warnings": None
        }

        if chunk_documents_docint.has_supported_file_extension(data['documentUrl']):
            logging.info(f"[document_chunking] Chunking (doc intelligence) {data['documentUrl'].split('/')[-1]}.")
            chunks, errors, warnings = chunk_documents_docint.chunk_document(data)

        elif chunk_documents_raw.has_supported_file_extension(data['documentUrl']):
            logging.info(f"[ddocument_chunking] Chunking (raw) {data['documentUrl'].split('/')[-1]}.")
            chunks, errors, warnings = chunk_documents_raw.chunk_document(data)
        
        # errors = []
        # warnings = []
        # chunks = [{
        #             "filepath": '123',
        #             "chunk_id": 0,
        #             "offset": 0,
        #             "length": 0,
        #             "page": 1,                    
        #             "title": "default",
        #             "category": "default",
        #             "url": '123',
        #             "content": data['documentUrl'],
        #             "contentVector": [0.1] * 1536,                    
        #             },
        #             {
        #                 "filepath": '123',
        #                 "chunk_id": 2,
        #                 "offset": 0,
        #                 "length": 0,
        #                 "page": 1,                           
        #                 "title": "default",
        #                 "category": "default",
        #                 "url": '123',
        #                 "content": data['documentUrl'],
        #                 "contentVector": [0.1] * 1536,
        #             }]

        if len(warnings) > 0:
            output_record["warnings"] = format_messages(warnings)

        if len(errors) > 0:
            output_record["errors"] = format_messages(errors)
        
        if len(chunks) > 0:
            output_record["data"] = {
                "chunks": chunks
            }

        if output_record != None:
            results["values"].append(output_record)
            
        return json.dumps(results, ensure_ascii=False, cls=DateTimeEncoder)

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
                                "documentContent": {"type": "string"},                                                                
                                "documentSasToken": {"type": "string", "minLength": 1},
                                "documentContentType": {"type": "string", "minLength": 1}
                            },
                            "required": ["documentContent", "documentUrl", "documentSasToken", "documentContentType"],
                        },
                    },
                    "required": ["recordId", "data"],
                },
            }
        },
        "required": ["values"],
    }

class DateTimeEncoder(JSONEncoder):
    #Override the default method
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()