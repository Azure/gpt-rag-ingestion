import azure.functions as func
import json
import jsonschema
import logging
import datetime
from json import JSONEncoder
from chunker.chunk_documents import chunk_document
from setup.search_setup import execute_setup

app = func.FunctionApp()

class DateTimeEncoder(JSONEncoder):
        #Override the default method
        def default(self, obj):
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()

@app.route(route="document-chunking", auth_level=func.AuthLevel.FUNCTION)
def document_chunking(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Invoked document_chunking skill.')
    try:
        body = req.get_json()
        jsonschema.validate(body, schema=get_request_schema())
        if body:
            result = process_documents(body)
            return func.HttpResponse(result, mimetype="application/json")
        else:
            return func.HttpResponse(
                "Invalid body",
                status_code=400
            )
    except ValueError:
        return func.HttpResponse(
             "Invalid body",
             status_code=400
        )
    except jsonschema.exceptions.ValidationError as e:
        return func.HttpResponse(
            "Invalid request: {0}".format(e), 
            status_code=400
        )

@app.route(route="search-setup", auth_level=func.AuthLevel.FUNCTION)
def setup(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Invoking setup operation.')
    try:
        execute_setup()
        return func.HttpResponse(
             f"Finished setup operation.",
             status_code=200
        )
    except Exception as e:
        return func.HttpResponse(
             f"Error invoking setup operation. Error: {e}",
             status_code=400
        )

def process_documents(body):
    values = body['values']
    results = {}
    results["values"] = []
    for value in values:
        # perform operation on each record (document)
        data = value['data']
        logging.info(f"Chunking {data['documentUrl'].split('/')[-1]}.")
        chunks, errors, warnings = chunk_document(data)
        output_record = {
            "recordId": value['recordId'],
            "data": {
                "chunks": chunks
            },
            "errors": errors,  
            "warnings": warnings
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
                                "documentContent": {"type": "string", "minLength": 1},
                                "documentUrl": {"type": "string", "minLength": 1},
                                "documentUrlencoded": {"type": "string", "minLength": 1},                                
                                "documentSasToken": {"type": "string", "minLength": 1},
                                "documentContentType": {"type": "string", "minLength": 1}
                            },
                            "required": ["documentContent", "documentUrl", "documentUrlencoded", "documentSasToken", "documentContentType"],
                        },
                    },
                    "required": ["recordId", "data"],
                },
            }
        },
        "required": ["values"],
    }