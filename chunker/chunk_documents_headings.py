import json
import logging
import os
import re
import requests
import time
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from langchain.text_splitter import MarkdownHeaderTextSplitter
from chunker import table_utils as tb
from embedder.text_embedder import TextEmbedder
from .token_estimator import TokenEstimator
from urllib.parse import urlparse
from utils.file_utils import get_file_extension
from utils.file_utils import get_filename

##########################################################################################
# CONFIGURATION
##########################################################################################

# Chunker parameters
NUM_TOKENS = int(os.environ["NUM_TOKENS"])  # max chunk size in tokens
MIN_CHUNK_SIZE = int(os.environ["MIN_CHUNK_SIZE"])  # min chunk size in tokens
TOKEN_OVERLAP = int(os.environ["TOKEN_OVERLAP"])

# Doc int version
DOCINT_40_API = "2023-10-31-preview"
default_api_version = "2023-07-31"
DOCINT_API_VERSION = os.getenv(
    "FORM_REC_API_VERSION", os.getenv("DOCINT_API_VERSION", default_api_version)
)

# Network isolation active?
NETWORK_ISOLATION = os.environ["NETWORK_ISOLATION"]
network_isolation = True if NETWORK_ISOLATION.lower() == "true" else False

# Supported file extensions
FILE_EXTENSION_DICT = ["pdf", "bmp", "jpeg", "png", "tiff"]
if DOCINT_API_VERSION >= DOCINT_40_API:
    formrec_or_docint = "documentintelligence"
    FILE_EXTENSION_DICT.extend(["docx", "pptx", "xlsx", "html"])
else:
    formrec_or_docint = "formrecognizer"

TOKEN_ESTIMATOR = TokenEstimator()

DOC_INTELLIGENCE_ENDPOINT = os.getenv("DOC_INTELLIGENCE_ENDPOINT")
DOC_INTELLIGENCE_KEY = os.getenv("DOC_INTELLIGENCE_KEY")

##########################################################################################
# UTILITY FUNCTIONS
##########################################################################################


def check_timeout(start_time):
    max_time = 230  # webapp timeout is 230 seconds
    elapsed_time = time.time() - start_time
    if elapsed_time > max_time:
        return True
    else:
        return False


def indexer_error_message(error_type, exception=None):
    error_message = "no error message"
    if error_type == "timeout":
        error_message = "Terminating the function so it doesn't run indefinitely. The AI Search indexer's timout is 3m50s. If the document is large (more than 100 pages), try dividing it into smaller files. If you are encountering many 429 errors in the function log, try increasing the embedding model's quota as the retrial logic delays processing."
    elif error_type == "embedding":
        error_message = (
            "Error when embedding the chunk, if it is a 429 error code please consider increasing your embeddings model quota: "
            + str(exception)
        )
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
        "html": "text/html",
    }
    if file_ext in extensions:
        return extensions[file_ext]
    else:
        return "application/octet-stream"


def get_secret(secretName):
    keyVaultName = os.environ["AZURE_KEY_VAULT_NAME"]
    KVUri = f"https://{keyVaultName}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    logging.info(f"Retrieving {secretName} secret from {keyVaultName}.")
    retrieved_secret = client.get_secret(secretName)
    return retrieved_secret.value


##########################################################################################
# DOCUMENT INTELLIGENCE ANALYSIS
##########################################################################################


def analyze_document_rest(filepath, model):

    result = {}
    errors = []
    
    logging.info(f"Analyzing {filepath} with model {model}.")

    if get_file_extension(filepath) in ["pdf"]:
        docint_features = "ocr.highResolution"
    else:
        docint_features = ""

    request_endpoint = f"https://{os.environ['AZURE_FORMREC_SERVICE']}.cognitiveservices.azure.com/{formrec_or_docint}/documentModels/{model}:analyze?api-version={DOCINT_API_VERSION}&features={docint_features}&outputContentFormat=markdown"

    if not network_isolation:
        logging.info(f"Conecting to doc int to analyze {filepath[:100]}.")

        headers = {
            "Content-Type": "application/json",
            "Ocp-Apim-Subscription-Key": get_secret("formRecKey"),
            "x-ms-useragent": "gpt-rag/1.0.0",
        }
        body = {"urlSource": filepath}
        try:
            # Send request
            response = requests.post(request_endpoint, headers=headers, json=body)
        except requests.exceptions.ConnectionError as conn_error:
            logging.info("Connection error, retrying in 10seconds...")
            time.sleep(10)
            response = requests.post(request_endpoint, headers=headers, json=body)

    else:
        # With network isolation doc int can't access container with no public access, so we download it and send its content as a stream.
        logging.info(f"Network isolation active, downloading {filepath[:100]}.")

        parsed_url = urlparse(filepath)
        account_url = parsed_url.scheme + "://" + parsed_url.netloc
        container_name = parsed_url.path.split("/")[1]
        blob_name = parsed_url.path.split("/")[2]
        file_ext = blob_name.split(".")[-1]

        logging.info(f"Conecting to blob to get {blob_name}.")

        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(
            account_url=account_url, credential=credential
        )
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )

        headers = {
            "Content-Type": get_content_type(file_ext),
            "Ocp-Apim-Subscription-Key": get_secret("formRecKey"),
            "x-ms-useragent": "gpt-rag/1.0.0",
        }

        blob_error = None

        try:
            data = blob_client.download_blob().readall()
            response = requests.post(request_endpoint, headers=headers, data=data)
        except requests.exceptions.ConnectionError as conn_error:
            logging.info("Connection error, retrying in 10seconds...")
            time.sleep(10)
            try:
                data = blob_client.download_blob().readall()
                response = requests.post(request_endpoint, headers=headers, data=data)
            except Exception as e:
                logging.error(f"Exception on blob downloading process after retry. {e}")
                blob_error = e
        except Exception as e:
            logging.error(f"Exception on blob downloading process. {e}")
            blob_error = e

        if blob_error:
            error_message = (
                f"Blob client error when reading from blob storage. {blob_error}"
            )
            logging.info(error_message)
            errors.append(error_message)
            return result, errors

    if response.status_code != 202:
        # Request failed
        error_message = f"Doc Intelligence request error, code {response.status_code}: {response.text}"
        logging.info(error_message)
        logging.info(f"filepath: {filepath}")
        errors.append(error_message)
        return result, errors

    # Poll for result
    get_url = response.headers["Operation-Location"]
    result_headers = headers.copy()
    result_headers["Content-Type"] = "application/json-patch+json"

    while True:
        result_response = requests.get(get_url, headers=result_headers)
        result_json = json.loads(result_response.text)

        if result_response.status_code != 200 or result_json["status"] == "failed":
            # Request failed
            error_message = f"Doc Intelligence polling error, code {result_response.status_code}: {response.text}"
            print(error_message)
            errors.append(error_message)
            break

        if result_json["status"] == "succeeded":
            result = result_json["analyzeResult"]
            break

        # Request still processing, wait and try again
        time.sleep(2)

    return result, errors


# Function to process a single blob using its SAS URL
def process_blob_with_sas_url(blob_url):
    from langchain_community.document_loaders import AzureAIDocumentIntelligenceLoader
    from langchain.text_splitter import MarkdownHeaderTextSplitter

    try:
        # Process the blob with Azure DI using the SAS URL
        loader = AzureAIDocumentIntelligenceLoader(
            url_path=blob_url,
            api_key=DOC_INTELLIGENCE_KEY,
            api_endpoint=DOC_INTELLIGENCE_ENDPOINT,
            api_model="prebuilt-layout",
            mode="markdown",
        )
        documents = loader.load()

        # Define headers to split on
        headers_to_split_on = [
            ("#", "Header 1"),
            ("##", "Header 2"),
            ("###", "Header 3"),
            ("####", "Header 4"),
            ("#####", "Header 5"),
            ("######", "Header 6"),
            ("===", "Alternative Header 1"),
            ("---", "Alternative Header 2"),
        ]

        # Initialize the header splitter with the given chunk size, overlap, and headers
        header_splitter = MarkdownHeaderTextSplitter(
            headers_to_split_on=headers_to_split_on
        )
        # Process each document and split the text
        processed_split = []
        for document in documents:
            docs_string = document.page_content
            splits = header_splitter.split_text(docs_string)
            print(f"Number of splits for {blob_url}: {len(splits)}")
            processed_split.append(splits)
        # Return the processed splits
        return processed_split
    except Exception as e:
        print(f"Error processing {blob_url}: {e}")
        return []


##########################################################################################
# CHUNKING FUNCTIONS
##########################################################################################

def get_chunk(content, url, page, chunk_id, text_embedder: TextEmbedder):

    chunk = {
        "chunk_id": chunk_id,
        "offset": 0,
        "length": 0,
        "page": page,
        "title": "default",
        "category": "default",
        "url": url,
        "filepath": get_filename(url),
        "content": content,
        "contentVector": text_embedder.embed_content(content.page_content),
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
    doc_name = filepath.split("/")[-1].split("?")[0]

    # 1) Analyze document with layout model
    logging.info(f"Analyzing {doc_name}.")
    document, analysis_errors = analyze_document_rest(filepath, "prebuilt-layout")
    if len(analysis_errors) > 0:
        errors = errors + analysis_errors
        error_occurred = True

    # 2) Check number of pages
    if "pages" in document and not error_occurred:
        n_pages = len(document["pages"])
        logging.info(
            f"Analyzed {doc_name} ({n_pages} pages). Content: {document['content'][:200]}."
        )
        if n_pages > 100:
            logging.warn(
                f"DOCUMENT {doc_name} HAS MANY ({n_pages}) PAGES. Please consider splitting it into smaller documents of 100 pages."
            )

        # Define headers to split on
    headers_to_split_on = [
        ("#", "Header 1"),
        ("##", "Header 2"),
        ("###", "Header 3"),
        ("####", "Header 4"),
        ("#####", "Header 5"),
        ("######", "Header 6"),
        ("===", "Alternative Header 1"),
        ("---", "Alternative Header 2"),
    ]

    # Initialize the header splitter with the given chunk size, overlap, and headers
    header_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=headers_to_split_on
    )

    docs_string = document["content"]
    splits = header_splitter.split_text(docs_string)

    for index, split in enumerate(splits, start=1):
        page = index
        chunk = get_chunk(split, data["documentUrl"], page, chunk_id, text_embedder)
        if chunk:
            chunks.append(chunk)
    logging.info(
        f"Finished chunking {doc_name}. {len(chunks)} chunks. {len(errors)} errors. {len(warnings)} warnings."
    )

    return chunks, errors, warnings
