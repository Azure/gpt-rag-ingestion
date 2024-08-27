import json
import logging
import os
import re
import requests
import time
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob.aio import BlobServiceClient
from azure.keyvault.secrets.aio import SecretClient
from chunker import table_utils as tb
from embedder.text_embedder import TextEmbedder
from .token_estimator import TokenEstimator
from urllib.parse import urlparse
from utils.file_utils import get_file_extension
from utils.file_utils import get_filename
import aiohttp
import asyncio

##########################################################################################
# CONFIGURATION
##########################################################################################

# Chunker parameters
NUM_TOKENS = int(os.environ["NUM_TOKENS"]) # max chunk size in tokens
MIN_CHUNK_SIZE = int(os.environ["MIN_CHUNK_SIZE"]) # min chunk size in tokens
TOKEN_OVERLAP = int(os.environ["TOKEN_OVERLAP"])

# Doc int version
DOCINT_40_API = '2023-10-31-preview'
default_api_version = '2023-07-31'
DOCINT_API_VERSION = os.getenv('FORM_REC_API_VERSION', os.getenv('DOCINT_API_VERSION', default_api_version))

# Network isolation active?
NETWORK_ISOLATION = os.environ["NETWORK_ISOLATION"]
network_isolation = True if NETWORK_ISOLATION.lower() == 'true' else False

# Supported file extensions
FILE_EXTENSION_DICT = [
    "pdf",
    "bmp",
    "jpeg",
    "png",
    "tiff"
]
if DOCINT_API_VERSION >= DOCINT_40_API:
    formrec_or_docint = "documentintelligence"
    FILE_EXTENSION_DICT.extend(["docx", "pptx", "xlsx", "html"])
else:
    formrec_or_docint = "formrecognizer"

TOKEN_ESTIMATOR = TokenEstimator()

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

def get_content_type(file_ext):
    extensions = {
        "pdf": "application/pdf", 
        "bmp": "image/bmp",
        "jpeg": "image/jpeg",
        "png": "image/png",
        "tiff": "image/tiff",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "html": "text/html" 
    }
    if file_ext in extensions:
        return extensions[file_ext]
    else:
        return "application/octet-stream"

async def get_secret(secretName):
    keyVaultName = os.environ["AZURE_KEY_VAULT_NAME"]
    KVUri = f"https://{keyVaultName}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    logging.info(f"Retrieving {secretName} secret from {keyVaultName}.")   
    retrieved_secret = await client.get_secret(secretName)
    await client.close()
    await credential.close()
    return retrieved_secret.value

##########################################################################################
# DOCUMENT INTELLIGENCE ANALYSIS
##########################################################################################

async def analyze_document_rest(filepath, model):

    result = {}
    errors = []

    if get_file_extension(filepath) in ["pdf"]:
        docint_features = "ocr.highResolution"
    else:
        docint_features = ""

    request_endpoint = f"https://{os.environ['AZURE_FORMREC_SERVICE']}.cognitiveservices.azure.com/{formrec_or_docint}/documentModels/{model}:analyze?api-version={DOCINT_API_VERSION}&features={docint_features}&includeKeys=true"
    session=aiohttp.ClientSession()
    try:
        if not network_isolation:

            headers = {
                "Content-Type": "application/json",
                "Ocp-Apim-Subscription-Key": await get_secret('formRecKey'),
                "x-ms-useragent": "gpt-rag/1.0.0"
            }
            body = {
                "urlSource": filepath
            }
            try:
                # Send request
                response = await session.post(request_endpoint, headers=headers, json=body)
            except aiohttp.ClientError as conn_error:
                logging.info("Connection error, retrying in 10seconds...")
                await asyncio.sleep(10)
                response = await session.post(request_endpoint, headers=headers, json=body)
                    
        else:
            # With network isolation doc int can't access container with no public access, so we download it and send its content as a stream.

            parsed_url = urlparse(filepath)
            account_url = parsed_url.scheme + "://" + parsed_url.netloc
            container_name = parsed_url.path.split("/")[1]
            blob_name = parsed_url.path.split("/")[2]
            file_ext = blob_name.split(".")[-1]

            logging.info(f"Conecting to blob to get {blob_name}.")

            credential = DefaultAzureCredential()
            blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

            headers = {
                "Content-Type": get_content_type(file_ext),
                "Ocp-Apim-Subscription-Key": await get_secret('formRecKey'),
                "x-ms-useragent": "gpt-rag/1.0.0"
            }

            blob_error = None

            try:
                blob = await blob_client.download_blob()
                data= await blob.readall()
                response = await session.post(request_endpoint, headers=headers, data=data)
            except aiohttp.ClientError as conn_error:
                logging.info("Connection error, retrying in 10seconds...")
                await asyncio.sleep(10)
                try:
                    blob =await blob_client.download_blob()
                    data= await blob.readall()
                    response = await session.post(request_endpoint, headers=headers, data=data)
                except Exception as e:
                    blob_error = e
            except Exception as e:
                blob_error = e
            finally:
                if response is not None:
                    response.close()
            await blob_client.close()
            await blob_service_client.close()
            await credential.close()
            if blob_error:
                error_message = f"Blob client error when reading from blob storage. {blob_error}"
                logging.info(error_message)
                errors.append(error_message)
                await session.close()
                logging.info("Closing session.")
                return result, errors

        if response.status != 202:
            # Request failed
            error_message = f"Doc Intelligence request error, code {response.status}: {response.text}"
            logging.info(error_message)
            logging.info(f"filepath: {filepath}")
            errors.append(error_message)
            await session.close()
            logging.info("Closing session.")

            return result, errors

        # Poll for result
        get_url = response.headers["Operation-Location"]
        result_headers = headers.copy()
        result_headers["Content-Type"] = "application/json-patch+json"

        while True:
            result_response = await session.get(get_url, headers=result_headers)
            result_json = await result_response.json()
            if result_response is not None:
                result_response.close()
            if result_response.status != 200 or result_json["status"] == "failed":
                # Request failed
                error_message = f"Doc Intelligence polling error, code {result_response.status}: {response.text}"
                print(error_message)
                errors.append(error_message)
                break

            if result_json["status"] == "succeeded":
                result = result_json['analyzeResult']
                break

            # Request still processing, wait and try again
            await asyncio.sleep(2)
    except Exception as e:
        error_message = f"Error when analyzing document with Doc Intelligence: {e}"
        logging.info(error_message)
        raise e
    finally:
        logging.info("Closing session.")
        await session.close()
        
    

    return result, errors

##########################################################################################
# CHUNKING FUNCTIONS
########################################################################################## 

async def get_chunk(content, url, page, chunk_id, text_embedder):

    chunk =  {
            "chunk_id": chunk_id,
            "offset": 0,
            "length": 0,
            "page": page,                    
            "title": "default",
            "category": "default",
            "url": url,
            "filepath": get_filename(url),            
            "content": content,
            "contentVector": await text_embedder.embed_content(content)
    }
    logging.info(f"Chunk: {chunk}.")
    return chunk

async def chunk_document(data):
    try:
        chunks = []
        errors = []
        warnings = []
        chunk_id = 0
        error_occurred = False
        start_time = time.time()

        text_embedder = await TextEmbedder.create()
        filepath = f"{data['documentUrl']}{data['documentSasToken']}"
        doc_name = filepath.split('/')[-1].split('?')[0]

        # 1) Analyze document with layout model
        logging.info(f"Analyzing {doc_name}.") 
        document, analysis_errors = await analyze_document_rest(filepath, 'prebuilt-layout')
        if len(analysis_errors) > 0:
            errors = errors + analysis_errors
            error_occurred = True

        # 2) Check number of pages
        if 'pages' in document and not error_occurred:
            n_pages = len(document['pages'])
            logging.info(f"Analyzed {doc_name} ({n_pages} pages). Content: {document['content'][:200]}.") 
            if n_pages > 100:
                logging.warn(f"DOCUMENT {doc_name} HAS MANY ({n_pages}) PAGES. Please consider splitting it into smaller documents of 100 pages.")      

        # 3) Chunk tables
        if 'tables' in document and not error_occurred:

            # 3.1) merge consecutive tables if they have the same structure 
            
            document["tables"] = tb.merge_tables_if_same_structure(document["tables"], document["pages"])

            # 3.2) create chunks for each table
            
            processed_tables = []
            for idx, table in enumerate(document["tables"]):
                if idx not in processed_tables:
                    processed_tables.append(idx)
                    # TODO: check if table is too big for one chunck and split it to avoid truncation
                    table_content = tb.table_to_html(table) 
                    chunk_id += 1

                    # page number logic
                    page = 1
                    bounding_regions = table['cells'][0].get('boundingRegions')
                    if bounding_regions is not None:
                        page = bounding_regions[0].get('pageNumber', 1)

                    # if there is text before the table add it to the beggining of the chunk to improve context.
                    text = tb.text_before_table(document, table, document["tables"])
                    table_content = text + table_content

                    # if there is text after the table add it to the end of the chunk to improve context.
                    text = tb.text_after_table(document, table, document["tables"])
                    table_content = table_content + text
                    try:
                        chunk = await get_chunk(table_content, data['documentUrl'], page, chunk_id, text_embedder)
                        chunks.append(chunk)
                    except Exception as e:
                        errors.append(indexer_error_message('embedding', e))
                        error_occurred = True
                        break
                    if check_timeout(start_time):
                        errors.append(indexer_error_message('timeout'))
                        error_occurred = True
                        break

        # 4) Chunk paragraphs
        if 'paragraphs' in document and not error_occurred:    
            paragraph_content = ""
            for paragraph in document['paragraphs']:

                # page number logic
                page = 1
                bounding_regions = paragraph.get('boundingRegions')
                if bounding_regions is not None:
                    page = bounding_regions[0].get('pageNumber', 1)

                if not tb.paragraph_in_a_table(paragraph, document['tables']):
                    chunk_size = TOKEN_ESTIMATOR.estimate_tokens(paragraph_content + paragraph['content'])
                    if chunk_size < NUM_TOKENS:
                        paragraph_content = paragraph_content + "\n" + paragraph['content']
                    else:
                        chunk_id += 1
                        try:
                            chunk = await get_chunk(paragraph_content, data['documentUrl'], page, chunk_id, text_embedder)
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
                        chunk = await get_chunk(paragraph_content, data['documentUrl'], page, chunk_id, text_embedder)
                        chunks.append(chunk)
                except Exception as e:
                    errors.append(indexer_error_message('embedding', e))
    except Exception as e:
        logging.info(f"Error when chunking {doc_name}: {e}")
        errors.append(indexer_error_message('embedding', e))
    logging.info(f"Finished chunking {doc_name}. {len(chunks)} chunks. {len(errors)} errors. {len(warnings)} warnings.")

    return chunks, errors, warnings
