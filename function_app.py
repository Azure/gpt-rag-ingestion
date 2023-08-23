import azure.functions as func
import json
import jsonschema
import logging
import datetime
from json import JSONEncoder
from chunker.chunk_documents_formrec import chunk_document
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
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
        logging.debug(f'REQUEST BODY: {body}')
        jsonschema.validate(body, schema=get_request_schema())

        if body:
            result = process_documents(body)
            return func.HttpResponse(result, mimetype="application/json")
        else:
            error_message = "Invalid body."
            logging.error(error_message)
            return func.HttpResponse(error_message, status_code=400)
    except ValueError as e:
        error_message = "Invalid body: {0}".format(e)
        logging.error(error_message)
        return func.HttpResponse(error_message, status_code=400)
    except jsonschema.exceptions.ValidationError as e:
        error_message = "Invalid request: {0}".format(e)
        logging.error(error_message)
        return func.HttpResponse(error_message, status_code=400)

def process_documents(body):
    values = body['values']
    results = {}
    results["values"] = []
    for value in values:
        # perform operation on each record (document)
        data = value['data']
        logging.info(f"Chunking {data['documentUrl'].split('/')[-1]}.")
        
        chunks, errors, warnings = chunk_document(data)

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