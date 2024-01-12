import base64
import html
import json
import logging
import os
import requests
import sys
import time
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from embedder.text_embedder import TextEmbedder
from .token_estimator import TokenEstimator
from urllib.parse import urlparse
from utils.file_utils import get_file_extension

# Chunker parameters
NUM_TOKENS = int(os.environ["NUM_TOKENS"]) # max chunk size in tokens
MIN_CHUNK_SIZE = int(os.environ["MIN_CHUNK_SIZE"]) # min chunk size in tokens
TOKEN_OVERLAP = int(os.environ["TOKEN_OVERLAP"])
TABLE_DISTANCE_THRESHOLD = 3 # inches

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

    request_endpoint = f"https://{os.environ['AZURE_FORMREC_SERVICE']}.cognitiveservices.azure.com/{formrec_or_docint}/documentModels/{model}:analyze?api-version={FORM_REC_API_VERSION}&features=ocr.highResolution"

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

def char_in_a_table(offset, tables):
    for table in tables:
        for cell in table['cells']:
            if len(cell['spans']) > 0 and cell['spans'][0]['offset'] <= offset < (cell['spans'][0]['offset'] + cell['spans'][0]['length']):
                return True
    return False

def paragraph_in_a_table(paragraph, tables):
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

    # 1) Analyze document with layout model
    document = analyze_document_rest(filepath, 'prebuilt-layout')
    logging.info(f"Analyzed document: {document}.") 

    # 2) Chunk tables
    if 'tables' in document:

        # 2.1) merge consecutive tables if they have the same structure 
        #        
        # The definition of two tables with the same structure is given two tables, table1 and table2, they have the same structure if:
        #   - They have same columnCount.
        #   - table2 first boundingRegion pageNumber difference to table1 last boundingRegion pageNumber is less then 2.
        #   - The absolute difference between table1 first boundingRegion last y coordinate and table2 first boundingRegion first y coordinate is less than TABLE_DISTANCE_THRESHOLD 

        def merge_tables_if_same_structure(tables, pages):
            merged_tables = []
            for table in tables:
                if not merged_tables or not same_structure(merged_tables[-1], table, pages):
                    merged_tables.append(table)
                else:
                    merged_tables[-1] = merge_tables(merged_tables[-1], table)
            return merged_tables
        
        def same_structure(table1, table2, pages):
            if table1['columnCount'] != table2['columnCount']:
                return False

            bounding_region1 = table1['boundingRegions'][-1]
            bounding_region2 = table2['boundingRegions'][0]

            page_difference = bounding_region2['pageNumber'] - bounding_region1['pageNumber']

            if page_difference >= 2:
                return False
            
            if page_difference == 1:
               pageIdx = bounding_region1['pageNumber'] - 1
               tables_distance = bounding_region2['polygon'][0] + (pages[pageIdx]['height'] - bounding_region1['polygon'][-1])
            else: 
                tables_distance = bounding_region2['polygon'][0] - bounding_region1['polygon'][-1]

            # tables distance in inches
            if tables_distance >= TABLE_DISTANCE_THRESHOLD:
                return False

            return True

        def merge_tables(table1, table2):
            merged_table = table1.copy()
            merged_table['rowCount'] += table2['rowCount']
            table1_row_count = table1['rowCount']
            
            for cell in table2['cells']:
                cell['rowIndex'] += table1_row_count
                merged_table['cells'].append(cell)
            
            merged_table['boundingRegions'].extend(table2['boundingRegions'])
            
            return merged_table

        def text_before_table(document, table, tables):
            first_cell_offset = sys.maxsize
            # get first cell
            for cell in table['cells']:
                # Check if the cell has content
                if cell.get('content'):
                    # Get the offset of the cell content
                    first_cell_offset = cell['spans'][0]['offset'] 
                    break
            text_before_offset = max(0,first_cell_offset - TOKEN_OVERLAP)
            text_before_str = ''
            # we don't want to add text before the table if it is contained in a table
            for idx, c in enumerate(document['content'][text_before_offset:first_cell_offset]):
                if not char_in_a_table(text_before_offset+idx, tables):
                    text_before_str += c
            return text_before_str
        
        def text_after_table(document, table, tables):
            last_cell_offset = 0
            # get last cell
            for cell in table['cells']:
                # Check if the cell has content
                if cell.get('content'):
                    # Get the offset of the cell content
                    last_cell_offset = cell['spans'][0]['offset']
            text_after_offset = last_cell_offset + len(cell['content'])
            text_after_str = ''
            # we don't want to add text after the table if it is contained in a table
            for idx, c in enumerate(document['content'][text_after_offset:text_after_offset+TOKEN_OVERLAP]):
                if not char_in_a_table(text_after_offset+idx, tables):
                    text_after_str += c
            return text_after_str
        document["tables"] = merge_tables_if_same_structure(document["tables"], document["pages"])

        # 2.4) create chunks for each table
        
        content = document["content"]
        processed_tables = []
        for idx, table in enumerate(document["tables"]):
            if idx not in processed_tables:
                processed_tables.append(idx)
                # TODO: check if table is too big for one chunck and split it to avoid truncation
                table_content = table_to_html(table)
                chunk_id += 1
                page = table['cells'][0]['boundingRegions'][0]['pageNumber']

                # if there is text before the table add it to the beggining of the chunk to improve context.
                text = text_before_table(document, table, document["tables"])
                table_content = text + table_content

                # if there is text after the table add it to the end of the chunk to improve context.
                text = text_after_table(document, table, document["tables"])
                table_content = table_content + text

                chunk = get_chunk(table_content, data['documentUrl'], page, chunk_id, text_embedder)
                chunks.append(chunk)

    # 3) Chunk paragaphs
    if 'paragraphs' in document:    
        paragraph_content = ""
        for paragraph in document['paragraphs']:
            page = paragraph['boundingRegions'][0]['pageNumber']
            if not paragraph_in_a_table(paragraph, document['tables']):
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