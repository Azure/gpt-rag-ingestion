import base64
import json
import logging
import openai
import os
import requests
import time
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from chunker import table_utils as tb
from embedder.text_embedder import TextEmbedder
from .token_estimator import TokenEstimator
from urllib.parse import urlparse
from utils.file_utils import get_file_extension

# Chunker parameters
NUM_TOKENS = int(os.environ["NUM_TOKENS"]) # max chunk size in tokens
MIN_CHUNK_SIZE = int(os.environ["MIN_CHUNK_SIZE"]) # min chunk size in tokens
TOKEN_OVERLAP = int(os.environ["TOKEN_OVERLAP"])

NETWORK_ISOLATION = os.environ["NETWORK_ISOLATION"]
network_isolation = True if NETWORK_ISOLATION.lower() == 'true' else False

FORM_REC_API_VERSION = os.getenv('FORM_REC_API_VERSION', '2023-07-31')
if FORM_REC_API_VERSION == '2023-10-31-preview':
    formrec_or_docint = "documentintelligence"
else:
    formrec_or_docint = "formrecognizer"
        
TOKEN_ESTIMATOR = TokenEstimator()

FILE_EXTENSION_DICT = [
    "pdf",
    "bmp",
    "jpeg",
    "png",
    "tiff",
]

##########################################################################################
# UTILITY FUNCTIONS
##########################################################################################

def check_timeout(start_time):
    max_time = 230 # webapp timeout is 230 seconds
    elapsed_time = time.time() - start_time
    if elapsed_time > max_time:
        return True
    else:
        return False    

def indexer_error_message(error_type, exception=None):
    error_message = "no error message"
    if error_type == 'timeout':
        error_message =  "Terminating the function so it doesn't run indefinitely. The AI Search indexer's timout is 3m50s. If the document is large (more than 100 pages), try dividing it into smaller files. If you are encountering many 429 errors in the function log, try increasing the embedding model's quota as the retrial logic delays processing."
    elif error_type == 'embedding':
        error_message = "Error when embedding the chunk, if it is a 429 error code please consider increasing your embeddings model quota: " + str(exception)
    logging.info(f"Error: {error_message}")
    return {"message": error_message}

def has_supported_file_extension(file_path: str) -> bool:
    """Checks if the given file format is supported based on its file extension.
    Args:
        file_path (str): The file path of the file whose format needs to be checked.
    Returns:
        bool: True if the format is supported, False otherwise.
    """
    file_extension = get_file_extension(file_path)
    return file_extension in FILE_EXTENSION_DICT

def get_secret(secretName):
    keyVaultName = os.environ["AZURE_KEY_VAULT_NAME"]
    KVUri = f"https://{keyVaultName}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    logging.info(f"Retrieving {secretName} secret from {keyVaultName}.")   
    retrieved_secret = client.get_secret(secretName)
    return retrieved_secret.value

##########################################################################################
# DOCUMENT INTELLIGENCE FUNCTIONS
##########################################################################################

def analyze_document_rest(filepath, model):

    result = {}

    request_endpoint = f"https://{os.environ['AZURE_FORMREC_SERVICE']}.cognitiveservices.azure.com/{formrec_or_docint}/documentModels/{model}:analyze?api-version={FORM_REC_API_VERSION}&features=ocr.highResolution"

    if not network_isolation:
        headers = {
            "Content-Type": "application/json",
            "Ocp-Apim-Subscription-Key": get_secret('formRecKey'),
            "x-ms-useragent": "gpt-rag/1.0.0"
        }
        body = {
            "urlSource": filepath
        }
        try:
            # Send request
            response = requests.post(request_endpoint, headers=headers, json=body)
        except requests.exceptions.ConnectionError as e:
            logging.info("Connection error, retrying in 10seconds...")
            time.sleep(10)
            response = requests.post(request_endpoint, headers=headers, json=body)
            
    else:

        parsed_url = urlparse(filepath)
        account_url = parsed_url.scheme + "://" + parsed_url.netloc
        container_name = parsed_url.path.split("/")[1]
        blob_name = parsed_url.path.split("/")[2]

        logging.info(f"Conecting to blob to get {blob_name}.")

        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        headers = {
            "Content-Type": "application/pdf",
            "Ocp-Apim-Subscription-Key": get_secret('formRecKey'),
            "x-ms-useragent": "gpt-rag/1.0.0"
        }

        try:
            data = blob_client.download_blob().readall()
            response = requests.post(request_endpoint, headers=headers, data=data)
        except requests.exceptions.ConnectionError as e:
            logging.info("Connection error, retrying in 10seconds...")
            time.sleep(10)
            data = blob_client.download_blob().readall()            
            response = requests.post(request_endpoint, headers=headers, data=data)

        
        logging.info(f"Removed file: {blob_name}.")

    if response.status_code != 202:
        # Request failed
        logging.info(f"Doc Intelligence API error: {response.text}")
        logging.info(f"urlSource: {filepath}")
        return(result)

    # Poll for result
    get_url = response.headers["Operation-Location"]
    result_headers = headers.copy()
    result_headers["Content-Type"] = "application/json-patch+json"

    while True:
        result_response = requests.get(get_url, headers=result_headers)
        result_json = json.loads(result_response.text)

        if result_response.status_code != 200 or result_json["status"] == "failed":
            # Request failed
            print("Error result: ", result_response.text)
            break

        if result_json["status"] == "succeeded":
            result = result_json['analyzeResult']
            break

        # Request still processing, wait and try again
        time.sleep(2)

    return result 

##########################################################################################
# CHUNKING FUNCTIONS
########################################################################################## 

def get_chunk(content, url, page, chunk_id, text_embedder):
    filepath = url.split('/')[-1]
    chunk =  {
            "chunk_id": chunk_id,
            "offset": 0,
            "length": 0,
            "page": page,                    
            "title": "default",
            "category": "default",
            "url": url,
            "filepath": filepath,            
            "content": content,
            "contentVector": text_embedder.embed_content(content)
    }
    logging.info(f"Chunk: {chunk}.")
    return chunk

def chunk_document(data):
    chunks = []
    errors = []
    warnings = []
    chunk_id = 0
    error_occurred = False
    start_time = time.time()

    text_embedder = TextEmbedder()
    filepath = f"{data['documentUrl']}{data['documentSasToken']}"
    doc_name = filepath.split('/')[-1].split('?')[0]

    # 1) Analyze document with layout model
    logging.info(f"Analyzing {doc_name}.") 
    document = analyze_document_rest(filepath, 'prebuilt-layout')
    n_pages = len(document['pages'])
    logging.info(f"Analyzed {doc_name} ({n_pages} pages). Content: {document['content'][:200]}.") 
    if n_pages > 100:
        logging.warn(f"DOCUMENT {doc_name} HAS MANY ({n_pages}) PAGES. Please consider splitting it into smaller documents of 100 pages.")

    # 2) Chunk tables
    if 'tables' in document:

        # 2.1) merge consecutive tables if they have the same structure 
        
        document["tables"] = tb.merge_tables_if_same_structure(document["tables"], document["pages"])

        # 2.2) create chunks for each table
        
        processed_tables = []
        for idx, table in enumerate(document["tables"]):
            if idx not in processed_tables:
                processed_tables.append(idx)
                # TODO: check if table is too big for one chunck and split it to avoid truncation
                table_content = tb.table_to_html(table) 
                chunk_id += 1
                page = table['cells'][0]['boundingRegions'][0]['pageNumber']

                # if there is text before the table add it to the beggining of the chunk to improve context.
                text = tb.text_before_table(document, table, document["tables"])
                table_content = text + table_content

                # if there is text after the table add it to the end of the chunk to improve context.
                text = tb.text_after_table(document, table, document["tables"])
                table_content = table_content + text
                try:
                    chunk = get_chunk(table_content, data['documentUrl'], page, chunk_id, text_embedder)
                    chunks.append(chunk)
                except Exception as e:
                    errors.append(indexer_error_message('embedding', e))
                    error_occurred = True
                    break
                if check_timeout(start_time):
                    errors.append(indexer_error_message('timeout'))
                    error_occurred = True
                    break

    # 3) Chunk paragraphs
    if 'paragraphs' in document and not error_occurred:    
        paragraph_content = ""
        for paragraph in document['paragraphs']:
            page = paragraph['boundingRegions'][0]['pageNumber']
            if not tb.paragraph_in_a_table(paragraph, document['tables']):
                chunk_size = TOKEN_ESTIMATOR.estimate_tokens(paragraph_content + paragraph['content'])
                if chunk_size < NUM_TOKENS:
                    paragraph_content = paragraph_content + "\n" + paragraph['content']
                else:
                    chunk_id += 1
                    try:
                        chunk = get_chunk(paragraph_content, data['documentUrl'], page, chunk_id, text_embedder)
                        chunks.append(chunk)
                    except Exception as e:
                        errors.append(indexer_error_message('embedding', e))
                        error_occurred = True
                        break
                    # overlap logic
                    overlapped_text = paragraph_content
                    overlapped_text = overlapped_text.split()
                    overlapped_text = overlapped_text[-round(TOKEN_OVERLAP/0.75):] 
                    overlapped_text = " ".join(overlapped_text)
                    paragraph_content = overlapped_text
                    
                    if check_timeout(start_time):
                        errors.append(indexer_error_message('timeout'))
                        error_occurred = True
                        break

        if not error_occurred:
            chunk_id += 1
            # last section
            chunk_size = TOKEN_ESTIMATOR.estimate_tokens(paragraph_content)
            try:
                if chunk_size > MIN_CHUNK_SIZE:
                    chunk = get_chunk(paragraph_content, data['documentUrl'], page, chunk_id, text_embedder)
                    chunks.append(chunk)
            except Exception as e:
                errors.append(indexer_error_message('embedding', e))
    
    logging.info(f"Finished chunking {doc_name}. {len(chunks)} chunks. {len(errors)} errors. {len(warnings)} warnings.")

    return chunks, errors, warnings
