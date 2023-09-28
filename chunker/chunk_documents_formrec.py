import os
import logging
import requests
import time
import json
import html
import base64
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from embedder.text_embedder import TextEmbedder
from .token_estimator import TokenEstimator
from urllib.parse import urlparse
from utils.file_utils import get_file_extension

MIN_CHUNK_SIZE = int(os.environ["MIN_CHUNK_SIZE"])
TOKEN_OVERLAP = int(os.environ["TOKEN_OVERLAP"])
NUM_TOKENS = int(os.environ["NUM_TOKENS"])
NETWORK_ISOLATION = os.environ["NETWORK_ISOLATION"]
network_isolation = True if NETWORK_ISOLATION.lower() == 'true' else False

FORM_REC_API_VERSION = "2023-07-31"

TOKEN_ESTIMATOR = TokenEstimator()

FILE_EXTENSION_DICT = [
    "pdf",
    "bmp",
    "jpeg",
    "png",
    "tiff",
]

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
    
def analyze_document_rest(filepath, model):
    logging.info(f"Analyzing document {filepath}.")
    
    result = {}
    
    request_endpoint = f"https://{os.environ['AZURE_FORMREC_SERVICE']}.cognitiveservices.azure.com/formrecognizer/documentModels/{model}:analyze?api-version={FORM_REC_API_VERSION}&features=ocr.highResolution"

    if not network_isolation:

        headers = {
            "Content-Type": "application/json",
            "Ocp-Apim-Subscription-Key": get_secret('formRecKey')
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
            "Ocp-Apim-Subscription-Key": get_secret('formRecKey')
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

    # Parse response
    if response.status_code == 202:
        operation_id = response.headers["Operation-Location"].split("/")[-1]
        # print("Operation ID:", operation_id)
    else:
        # Request failed
        logging.info(f"Doc Intelligence API error: {response.text}")
        logging.info(f"urlSource: {filepath}")
        return(result)

    # Poll for result
    result_endpoint = f"https://{os.environ['AZURE_FORMREC_SERVICE']}.cognitiveservices.azure.com/formrecognizer/documentModels/prebuilt-layout/analyzeResults/{operation_id}"
    result_headers = headers.copy()
    result_headers["Content-Type"] = "application/json-patch+json"

    while True:
        result_response = requests.get(result_endpoint, headers=result_headers)
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

def table_to_html(table):
    table_html = "<table>"
    rows = [sorted([cell for cell in table['cells'] if cell['rowIndex'] == i], key=lambda cell: cell['columnIndex']) for i in range(table['rowCount'])]
    for row_cells in rows:
        table_html += "<tr>"
        for cell in row_cells:
            tag =  "td"
            if 'kind' in cell:
                if (cell['kind'] == "columnHeader" or cell['kind'] == "rowHeader"): tag = "th"
            
            cell_spans = ""
            
            if 'columnSpan' in cell:
                if cell['columnSpan'] > 1: cell_spans += f" colSpan={cell['columnSpan']}"
            
            if 'rowSpan' in cell:
                if cell['rowSpan'] > 1: cell_spans += f" rowSpan={cell['rowSpan']}"

            table_html += f"<{tag}{cell_spans}>{html.escape(cell['content'])}</{tag}>"
        table_html +="</tr>"
    table_html += "</table>"
    return table_html

def in_a_table(paragraph, tables):
    for table in tables:
        for cell in table['cells']:
            if len(cell['spans']) > 0 and paragraph['spans'][0]['offset'] == cell['spans'][0]['offset']:
                return True
    return False

def get_chunk(content, url, page, chunk_id, text_embedder):
    filepath = url.split('/')[-1]
    chunk =  {
            "chunk_id": chunk_id,
            "unique_id": base64.b64encode(f"{filepath}_{chunk_id}".encode("utf-8")).decode("utf-8"),
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

    text_embedder = TextEmbedder()
    filepath = f"{data['documentUrl']}{data['documentSasToken']}"
    # filepath = f"{data['documentUrl']}"

    # analyze document
    document = analyze_document_rest(filepath, 'prebuilt-layout')
    logging.info(f"Analyzed document: {document}.") 

    if 'tables' in document:
        # split into chunks
        # tables
        for table in document["tables"]:
            table_content = table_to_html(table)
            chunk_id += 1
            page = table['cells'][0]['boundingRegions'][0]['pageNumber']
            chunk = get_chunk(table_content, data['documentUrl'], page, chunk_id, text_embedder)
            chunks.append(chunk)

    # paragraphs
    if 'paragraphs' in document:    
        paragraph_content = ""
        for paragraph in document['paragraphs']:
            page = paragraph['boundingRegions'][0]['pageNumber']
            if not in_a_table(paragraph, document['tables']):
                chunk_size = TOKEN_ESTIMATOR.estimate_tokens(paragraph_content + paragraph['content'])
                if chunk_size < NUM_TOKENS:
                    paragraph_content = paragraph_content + "\n" + paragraph['content']
                else:
                    chunk_id += 1
                    chunk = get_chunk(paragraph_content, data['documentUrl'], page, chunk_id, text_embedder)
                    chunks.append(chunk)
                    # overlap logic
                    overlapped_text = paragraph_content
                    overlapped_text = overlapped_text.split()
                    overlapped_text = overlapped_text[-round(TOKEN_OVERLAP/0.75):] 
                    overlapped_text = " ".join(overlapped_text)
                    paragraph_content = overlapped_text

        chunk_id += 1
        # last section
        chunk_size = TOKEN_ESTIMATOR.estimate_tokens(paragraph_content)
        if chunk_size > MIN_CHUNK_SIZE: 
            chunk = get_chunk(paragraph_content, data['documentUrl'], page, chunk_id, text_embedder)
            chunks.append(chunk)

    return chunks, errors, warnings