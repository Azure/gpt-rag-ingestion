import logging
import jsonschema
import json
import time
import azure.functions as func
import datetime
from chunking import DocumentChunker
from json import JSONEncoder

###############################################################################
# Pipeline Functions
###############################################################################

app = func.FunctionApp()

###################################################################################
# Extract Function (Timer Triggered)
###################################################################################

# Extract Function implementation
        
###################################################################################
# Document Chunking Function (HTTP Triggered by AI Search)
###################################################################################

class DateTimeEncoder(JSONEncoder):
    #Override the default method
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        
# Document Chunking Function (HTTP Triggered by AI Search)
@app.route(route="document-chunking", auth_level=func.AuthLevel.FUNCTION)
def document_chunking(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        jsonschema.validate(body, schema=_get_request_schema())

        if body:
            # Log the incoming request
            logging.info(f'[document_chunking] Invoked document_chunking skill. Number of items: {len(body["values"])}.')
            for i, item in enumerate(body["values"]):
                data = item["data"]
                logging.info(f'[document_chunking] Item {i + 1}: File {data["documentUrl"].split("/")[-1]}, Content Type {data["documentContentType"]}.')
            start_time = time.time()
            # Chunk the documents
            result = _chunk_documents(body)
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

def _format_messages(messages):
    formatted = [{"message": msg} for msg in messages]
    return formatted

def _chunk_documents(body):
    """
    Processes and chunks the documents provided in the input body, creating a structured response
    that includes the chunked content along with any errors or warnings encountered during the process.

    Args:
    -----
    body (dict): 
        A dictionary containing a list of document records under the key 'values'. 
        Each record should have a 'data' field that includes the document's metadata and content.

    Returns:
    --------
    str: 
        A JSON-encoded string representing the results of the chunking process. The structure of the
        returned JSON is as follows:

        - "values" (list[dict]): 
            A list of dictionaries, each corresponding to a processed document record. 
            Each dictionary contains:
            
            - "recordId" (str): 
                The identifier of the document record.
            
            - "data" (dict or None): 
                A dictionary containing the following key:
                
                - "chunks" (list[dict]): 
                    An array of chunk dictionaries as described in the Chunk Dictionary Structure. 
                    This field is present if chunking was successful.
            
            - "errors" (list[str] or None): 
                A list of error messages encountered during processing, if any. 
                This field is present only if errors were encountered.
            
            - "warnings" (list[str] or None): 
                A list of warning messages encountered during processing, if any. 
                This field is present only if warnings were encountered.
    
    The JSON is encoded using `json.dumps`, ensuring it is safely serialized for transmission or storage.
    The `DateTimeEncoder` is used to handle any date-time objects within the data.

    Chunk Dictionary Structure:
    ===========================

    The chunk dictionary represents a segment of a document, including its content and associated metadata. 
    Each key in the dictionary serves a specific purpose, aiding in the management and utilization of the chunked content.

    Fields:
    -------

    - chunk_id (str): 
        A unique identifier for the chunk. This ID allows for easy reference and retrieval of specific chunks within the document.

    - url (str): 
        The original URL of the document from which this chunk was created. Provides a direct link to the source document, 
        ensuring traceability and context preservation.

    - filepath (str): 
        The name of the file from which the chunk was derived, extracted from the document's URL. Useful for identifying the document, 
        especially when processing multiple documents.

    - content (str): 
        The actual content of the chunk. This field contains the segment of the document that has been processed and split 
        according to the chunking logic.

    - contentVector (list[float]): 
        A vector representation of the chunk's content, generated using embeddings from Azure OpenAI. This is essential for operations 
        like similarity searches and clustering.

    - summary (str, optional): 
        A brief summary of the content. Defaults to an empty string.

    - title (str): 
        The title of the chunk, typically extracted and formatted from the document's filename. If no specific title is provided, 
        this field ensures the chunk has a meaningful, human-readable identifier.

    - page (int): 
        The page number from which the chunk was extracted. Particularly useful for paginated documents, enabling users 
        to locate the content within the original document.

    - offset (int): 
        The position within the page or document where the chunk's content begins. This allows for precise tracking of content 
        within the document.

    - relatedImages (list[str]): 
        A list of URLs or file paths to images related to the chunk's content. These images may be embedded within the document 
        or contextually associated with the chunk's content.

    - relatedFiles (list[str]): 
        A list of URLs or file paths to files related to the chunk's content. These files may provide additional context 
        or supplementary information relevant to the chunk.

    """
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

        logging.info(f"[document_chunking][{data['documentUrl'].split('/')[-1]}] chunking document.")
        chunks, errors, warnings = DocumentChunker().chunk_document(data)

        if len(warnings) > 0:
            output_record["warnings"] = _format_messages(warnings)

        if len(errors) > 0:
            output_record["errors"] = _format_messages(errors)
        
        if len(chunks) > 0:
            output_record["data"] = {
                "chunks": chunks
            }

        if output_record != None:
            results["values"].append(output_record)
            
        return json.dumps(results, ensure_ascii=False, cls=DateTimeEncoder)

def _get_request_schema():
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
                                "documentSasToken": {"type": "string", "minLength": 1},
                                "documentContentType": {"type": "string", "minLength": 1}
                            },
                            "required": ["documentUrl", "documentSasToken", "documentContentType"],
                        },
                    },
                    "required": ["recordId", "data"],
                },
            }
        },
        "required": ["values"],
    }