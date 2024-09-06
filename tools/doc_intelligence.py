import os
import time
import json
import logging
import requests
from urllib.parse import urlparse, unquote
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

class DocumentIntelligenceClient:
    """
    A client for interacting with Azure's Document Intelligence service.

    Attributes:
        service_name (str): The name of the Azure Document Intelligence service.
        api_version (str): The API version to use for the service.
        network_isolation (bool): Flag to indicate if network isolation is enabled.

    Methods:
        analyze_document(filepath, model):
            Analyzes a document using the specified model.
    """
    def __init__(self, document_filename=""):
        """
        Initializes the DocumentIntelligence client.

        Parameters:
        document_filename (str, optional): Additional attribute for improved log traceability.
        """
        self.document_filename = f"[{document_filename}]" if document_filename else ""
        
        # ai service resource name
        self.service_name = os.environ['AZURE_FORMREC_SERVICE']
        
        # API configuration
        self.DOCINT_40_API = '2023-10-31-preview'
        self.DEFAULT_API_VERSION = '2023-07-31'
        self.api_version = os.getenv('FORM_REC_API_VERSION', os.getenv('DOCINT_API_VERSION', self.DEFAULT_API_VERSION))
        self.docint_40_api = self.api_version >= self.DOCINT_40_API
                
        # Network isolation
        network_isolation = os.getenv('NETWORK_ISOLATION', self.DEFAULT_API_VERSION)
        self.network_isolation = True if network_isolation.lower() == 'true' else False

        # Supported extensions
        self.file_extensions = [
            "pdf",
            "bmp",
            "jpeg",
            "png",
            "tiff"
        ]
        self.ai_service_type = "formrecognizer"
        self.output_content_format = ""
        self.docint_features = "" 
        self.analyse_output_options = ""
        
        if self.docint_40_api:
            self.ai_service_type = "documentintelligence"
            self.file_extensions.extend(["docx", "pptx", "xlsx", "html"])
            self.output_content_format = "markdown"            
            self.analyse_output_options = "figures"

    def _get_file_extension(self, filepath):
        # Split the filepath at '?' and take the first part
        clean_filepath = filepath.split('?')[0]
        # Split the clean filepath at '.' and take the last part
        return clean_filepath.split('.')[-1]

    def _get_content_type(self, file_ext):
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

    def analyze_document(self, file_url, model='prebuilt-layout'):
        """
        Analyzes a document using the specified model.

        Args:
            file_url (str): The url to the document to be analyzed.
            model (str): The model to use for document analysis.

        Returns:
            tuple: A tuple containing the analysis result and any errors encountered.
        """
        result = {}
        errors = []

        file_ext = self._get_file_extension(file_url)

        if file_ext in ["pdf"]:
            self.docint_features = "ocr.highResolution"

        # Set request endpoint
        request_endpoint = f"https://{self.service_name}.cognitiveservices.azure.com/{self.ai_service_type}/documentModels/{model}:analyze?api-version={self.api_version}"
        if self.docint_features:
            request_endpoint += f"&features={self.docint_features}" 
        if self.output_content_format:
            request_endpoint += f"&outputContentFormat={self.output_content_format}"
        if self.analyse_output_options:
            request_endpoint += f"&output={self.analyse_output_options}"

        # Set request headers
        token = DefaultAzureCredential().get_token("https://cognitiveservices.azure.com/.default")

        headers = {
                    "Content-Type": self._get_content_type(file_ext),
                    "Authorization": f"Bearer {token.token}",
                    "x-ms-useragent": "gpt-rag/1.0.0"
                }            
        parsed_url = urlparse(file_url)
        account_url = parsed_url.scheme + "://" + parsed_url.netloc
        container_name = parsed_url.path.split("/")[1]
        url_decoded = unquote(parsed_url.path)
        blob_name = url_decoded[len(container_name) + 2:]
        file_ext = blob_name.split(".")[-1]

        logging.info(f"[docintelligence]{self.document_filename} Connecting to blob.")

        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        blob_error = None

        try:
            data = blob_client.download_blob().readall()
            response = requests.post(request_endpoint, headers=headers, data=data)
        except requests.exceptions.ConnectionError:
            logging.info(f"[docintelligence]{self.document_filename} Connection error, retrying in 10 seconds...")
            time.sleep(10)
            try:
                data = blob_client.download_blob().readall()
                response = requests.post(request_endpoint, headers=headers, data=data)
            except Exception as e:
                blob_error = e
        except Exception as e:
            blob_error = e

        if blob_error:
            error_message = f"Blob client error when reading from blob storage. {blob_error}"
            logging.info(f"[docintelligence]{self.document_filename} {error_message}")
            errors.append(error_message)
            return result, errors

        error_messages = {
            404: "Resource not found, please verify your request url. The Doc Intelligence API version you are using may not be supported in your region.",
        }
        
        if response.status_code in error_messages or response.status_code != 202:
            error_message = error_messages.get(response.status_code, f"Doc Intelligence request error, code {response.status_code}: {response.text}")
            logging.info(f"[docintelligence]{self.document_filename} {error_message}")
            logging.info(f"[docintelligence]{self.document_filename} filepath: {file_url}")
            errors.append(error_message)
            return result, errors

        get_url = response.headers["Operation-Location"]
        result_headers = headers.copy()
        result_headers["Content-Type"] = "application/json-patch+json"

        while True:
            result_response = requests.get(get_url, headers=result_headers)
            result_json = json.loads(result_response.text)

            if result_response.status_code != 200 or result_json["status"] == "failed":
                error_message = f"Doc Intelligence polling error, code {result_response.status_code}: {response.text}"
                logging.info(f"[docintelligence]{self.document_filename} {error_message}")
                errors.append(error_message)
                break

            if result_json["status"] == "succeeded":
                result = result_json['analyzeResult']
                break

            time.sleep(2)

        return result, errors