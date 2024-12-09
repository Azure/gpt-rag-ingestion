# DocumentIntelligenceClient.py

import os
import time
import json
import logging
import requests
from urllib.parse import urlparse, unquote
from azure.identity import ManagedIdentityCredential, AzureCliCredential, ChainedTokenCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ClientAuthenticationError, ResourceNotFoundError

class DocumentIntelligenceClient:
    """
    A client for interacting with Azure's Document Intelligence service.

    Attributes:
        service_name (str): The name of the Azure Document Intelligence service.
        api_version (str): The API version to use for the service.
        network_isolation (bool): Flag to indicate if network isolation is enabled.

    Methods:
        analyze_document(file_url, model):
            Analyzes a document using the specified model.
    """

    def __init__(self):
        """
        Initializes the DocumentIntelligence client.
        """
        # ai service resource name
        self.service_name = os.getenv('AZURE_FORMREC_SERVICE', None)
        if self.service_name is None:
            logging.error("[docintelligence] The environment variable 'AZURE_FORMREC_SERVICE' is not set.")
            raise EnvironmentError("The environment variable 'AZURE_FORMREC_SERVICE' is not set.")

        # API configuration
        self.DOCINT_40_API = '2023-10-31-preview'
        self.DEFAULT_API_VERSION = '2023-07-31'
        self.api_version = os.getenv('FORM_REC_API_VERSION', os.getenv('DOCINT_API_VERSION', self.DEFAULT_API_VERSION))
        self.docint_40_api = self.api_version >= self.DOCINT_40_API

        # Network isolation
        network_isolation = os.getenv('NETWORK_ISOLATION', 'false')
        self.network_isolation = network_isolation.lower() == 'true'

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
        self.analyze_output_options = ""

        if self.docint_40_api:
            self.ai_service_type = "documentintelligence"
            self.file_extensions.extend(["docx", "pptx", "xlsx", "html"])
            self.output_content_format = "markdown"            
            self.analyze_output_options = "figures"

        # Initialize the ChainedTokenCredential with ManagedIdentityCredential and AzureCliCredential
        try:
            self.credential = ChainedTokenCredential(
                ManagedIdentityCredential(),
                AzureCliCredential()
            )
            logging.debug("[docintelligence] Initialized ChainedTokenCredential with ManagedIdentityCredential and AzureCliCredential.")
        except Exception as e:
            logging.error(f"[docintelligence] Failed to initialize ChainedTokenCredential: {e}")
            raise

    def _get_file_extension(self, filepath):
        """
        Extracts the file extension from a given filepath.

        Args:
            filepath (str): The path or URL of the file.

        Returns:
            str: The file extension.
        """
        clean_filepath = filepath.split('?')[0]
        return clean_filepath.split('.')[-1].lower()

    def _get_content_type(self, file_ext):
        """
        Maps file extensions to their corresponding MIME types.

        Args:
            file_ext (str): The file extension.

        Returns:
            str: The MIME type.
        """
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
        return extensions.get(file_ext, "application/octet-stream")

    def analyze_document_from_bytes(self, file_bytes: bytes, filename: str, model='prebuilt-layout'):
        """
        Analyzes a document using the specified model, with input as bytes.

        Args:
            file_bytes (bytes): The bytes of the document to be analyzed.
            filename (str): The name of the document file.
            model (str): The model to use for document analysis.

        Returns:
            tuple: A tuple containing the analysis result and any errors encountered.
        """
        result = {}
        errors = []

        # Get the file extension from the filename
        file_ext = self._get_file_extension(filename)

        if file_ext not in self.file_extensions:
            error_message = f"File extension '{file_ext}' is not supported."
            logging.error(f"[docintelligence][{filename}] {error_message}")
            errors.append(error_message)
            return result, errors

        content_type = self._get_content_type(file_ext)

        if file_ext == "pdf":
            self.docint_features = "ocr.highResolution"

        # Set request endpoint
        request_endpoint = f"https://{self.service_name}.cognitiveservices.azure.com/{self.ai_service_type}/documentModels/{model}:analyze?api-version={self.api_version}"
        if self.docint_features:
            request_endpoint += f"&features={self.docint_features}" 
        if self.output_content_format:
            request_endpoint += f"&outputContentFormat={self.output_content_format}"
        if self.analyze_output_options:
            request_endpoint += f"&output={self.analyze_output_options}"

        # Set request headers
        try:
            token = self.credential.get_token("https://cognitiveservices.azure.com/.default")
            headers = {
                "Content-Type": content_type,
                "Authorization": f"Bearer {token.token}",
                "x-ms-useragent": "gpt-rag/1.0.0"
            }
            logging.debug(f"[docintelligence][{filename}] Retrieved authentication token.")
        except ClientAuthenticationError as e:
            error_message = f"Authentication failed: {e}"
            logging.error(f"[docintelligence][{filename}] {error_message}")
            errors.append(error_message)
            return result, errors
        except Exception as e:
            error_message = f"Unexpected error during authentication: {e}"
            logging.error(f"[docintelligence][{filename}] {error_message}")
            errors.append(error_message)
            return result, errors

        try:
            response = requests.post(request_endpoint, headers=headers, data=file_bytes)
            logging.info(f"[docintelligence][{filename}] Sent analysis request.")
        except Exception as e:
            error_message = f"Error when sending request to Document Intelligence API: {e}"
            logging.error(f"[docintelligence][{filename}] {error_message}")
            errors.append(error_message)
            return result, errors

        if response.status_code != 202:
            error_messages = {
                404: "Resource not found. Please verify your request URL. The Document Intelligence API version you are using may not be supported in your region.",
            }    
            error_message = error_messages.get(
                response.status_code, 
                f"Document Intelligence request error, code {response.status_code}: {response.text}"
            )
            logging.error(f"[docintelligence][{filename}] {error_message}")
            errors.append(error_message)
            return result, errors

        get_url = response.headers.get("Operation-Location")
        if not get_url:
            error_message = "Operation-Location header not found in the response."
            logging.error(f"[docintelligence][{filename}] {error_message}")
            errors.append(error_message)
            return result, errors

        result_headers = headers.copy()
        result_headers["Content-Type"] = "application/json-patch+json"

        while True:
            try:
                result_response = requests.get(get_url, headers=result_headers)
                result_json = result_response.json()

                if result_response.status_code != 200 or result_json.get("status") == "failed":
                    error_message = f"Document Intelligence polling error, code {result_response.status_code}: {result_response.text}"
                    logging.error(f"[docintelligence][{filename}] {error_message}")
                    errors.append(error_message)
                    break

                if result_json.get("status") == "succeeded":
                    result = result_json.get('analyzeResult', {})
                    logging.info(f"[docintelligence][{filename}] Analysis succeeded.")
                    break

                logging.debug(f"[docintelligence][{filename}] Analysis in progress. Waiting for 2 seconds before retrying.")
                time.sleep(2)
            except Exception as e:
                error_message = f"Error during polling for analysis result: {e}"
                logging.error(f"[docintelligence][{filename}] {error_message}")
                errors.append(error_message)
                break

        return result, errors


    def analyze_document_from_blob_url(self, file_url, model='prebuilt-layout'):
        """
        Analyzes a document in a blob container using the specified model.

        Args:
            file_url (str): The URL of the blob containing the document.
            model (str): The model to use for document analysis.

        Returns:
            tuple: A tuple containing the analysis result and any errors encountered.
        """
        result = {}
        errors = []

        filename = os.path.basename(urlparse(file_url).path)
        file_ext = self._get_file_extension(file_url)

        if file_ext == "pdf":
            self.docint_features = "ocr.highResolution"

        # Set request endpoint
        request_endpoint = f"https://{self.service_name}.cognitiveservices.azure.com/{self.ai_service_type}/documentModels/{model}:analyze?api-version={self.api_version}"
        if self.docint_features:
            request_endpoint += f"&features={self.docint_features}" 
        if self.output_content_format:
            request_endpoint += f"&outputContentFormat={self.output_content_format}"
        if self.analyze_output_options:
            request_endpoint += f"&output={self.analyze_output_options}"

        # Set request headers
        try:
            token = self.credential.get_token("https://cognitiveservices.azure.com/.default")
            headers = {
                "Content-Type": self._get_content_type(file_ext),
                "Authorization": f"Bearer {token.token}",
                "x-ms-useragent": "gpt-rag/1.0.0"
            }
            logging.debug(f"[docintelligence][{filename}] Retrieved authentication token.")
        except ClientAuthenticationError as e:
            error_message = f"Authentication failed: {e}"
            logging.error(f"[docintelligence][{filename}] {error_message}")
            errors.append(error_message)
            return result, errors
        except Exception as e:
            error_message = f"Unexpected error during authentication: {e}"
            logging.error(f"[docintelligence][{filename}] {error_message}")
            errors.append(error_message)
            return result, errors

        parsed_url = urlparse(file_url)
        account_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        container_name = parsed_url.path.split("/")[1]
        blob_name = unquote(parsed_url.path[len(f"/{container_name}/"):])

        logging.info(f"[docintelligence][{filename}] Connecting to blob storage.")

        try:
            blob_service_client = BlobServiceClient(account_url=account_url, credential=self.credential)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            data = blob_client.download_blob().readall()
            logging.info(f"[docintelligence][{filename}] Downloaded blob data.")
        except ResourceNotFoundError:
            error_message = f"Blob '{blob_name}' not found in container '{container_name}'."
            logging.error(f"[docintelligence][{filename}] {error_message}")
            errors.append(error_message)
            return result, errors
        except ClientAuthenticationError as e:
            error_message = f"Authentication failed when accessing blob storage: {e}"
            logging.error(f"[docintelligence][{filename}] {error_message}")
            errors.append(error_message)
            return result, errors
        except Exception as e:
            error_message = f"Error accessing blob storage: {e}"
            logging.error(f"[docintelligence][{filename}] {error_message}")
            errors.append(error_message)
            return result, errors

        try:
            response = requests.post(request_endpoint, headers=headers, data=data)
            logging.info(f"[docintelligence][{filename}] Sent analysis request.")
        except Exception as e:
            error_message = f"Error when sending request to Document Intelligence API: {e}"
            logging.error(f"[docintelligence][{filename}] {error_message}")
            errors.append(error_message)
            return result, errors

        if response.status_code != 202:
            error_messages = {
                404: "Resource not found. Please verify your request URL. The Document Intelligence API version you are using may not be supported in your region.",
            }
            error_message = error_messages.get(
                response.status_code, 
                f"Document Intelligence request error, code {response.status_code}: {response.text}"
            )
            logging.error(f"[docintelligence][{filename}] {error_message}")
            errors.append(error_message)
            return result, errors

        get_url = response.headers.get("Operation-Location")
        if not get_url:
            error_message = "Operation-Location header not found in the response."
            logging.error(f"[docintelligence][{filename}] {error_message}")
            errors.append(error_message)
            return result, errors

        result_headers = headers.copy()
        result_headers["Content-Type"] = "application/json-patch+json"

        while True:
            try:
                result_response = requests.get(get_url, headers=result_headers)
                result_json = result_response.json()

                if result_response.status_code != 200 or result_json.get("status") == "failed":
                    error_message = f"Document Intelligence polling error, code {result_response.status_code}: {result_response.text}"
                    logging.error(f"[docintelligence][{filename}] {error_message}")
                    errors.append(error_message)
                    break

                if result_json.get("status") == "succeeded":
                    result = result_json.get('analyzeResult', {})
                    logging.info(f"[docintelligence][{filename}] Analysis succeeded.")
                    break

                logging.debug(f"[docintelligence][{filename}] Analysis in progress. Waiting for 2 seconds before retrying.")
                time.sleep(2)
            except Exception as e:
                error_message = f"Error during polling for analysis result: {e}"
                logging.error(f"[docintelligence][{filename}] {error_message}")
                errors.append(error_message)
                break

        return result, errors
