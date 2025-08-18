import os
import time
import json
import base64
import logging
import requests
from urllib.parse import urlparse, unquote
from azure.identity import ManagedIdentityCredential, AzureCliCredential, ChainedTokenCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ClientAuthenticationError, ResourceNotFoundError

from dependencies import get_config

app_config_client = get_config()

class DocumentIntelligenceClient:
    """
    A client for interacting with Azure's Document Intelligence service.
    """

    def __init__(self):
        # AI service resource endpoint
        self.service_endpoint = app_config_client.get('AI_FOUNDRY_ACCOUNT_ENDPOINT')
        logging.debug(f"[docintelligence] AI_FOUNDRY_ACCOUNT_ENDPOINT = {self.service_endpoint!r}")
        if not self.service_endpoint:
            logging.error("[docintelligence] 'AI_FOUNDRY_ACCOUNT_ENDPOINT' not set.")
            raise EnvironmentError("The environment variable 'AI_FOUNDRY_ACCOUNT_ENDPOINT' is not set.")
        self.service_endpoint = self.service_endpoint.rstrip('/')

        # API configuration
        self.DOCINT_40_API = '2023-10-31-preview'
        self.DEFAULT_API_VERSION = '2024-11-30'
        self.api_version = app_config_client.get('DOC_INTELLIGENCE_API_VERSION', self.DEFAULT_API_VERSION)
        logging.debug(f"[docintelligence] DOC_INTELLIGENCE_API_VERSION = {self.api_version!r}")
        self.docint_40_api = self.api_version >= self.DOCINT_40_API

        # Network isolation
        self.network_isolation = app_config_client.get('NETWORK_ISOLATION', 'false').lower() == 'true'
        logging.debug(f"[docintelligence] NETWORK_ISOLATION = {self.network_isolation}")

        # File types and service type
        self.file_extensions = ["pdf", "bmp", "jpg", "jpeg", "png", "tiff"]
        self.ai_service_type = "formrecognizer"
        self.output_content_format = ""
        self.analyze_output_options = ""
        if self.docint_40_api:
            self.ai_service_type = "documentintelligence"
            self.file_extensions.extend(["docx", "pptx", "xlsx", "html"])
            self.output_content_format = "markdown"
            self.analyze_output_options = "figures"

        logging.info(f"[docintelligence] Initialized with endpoint={self.service_endpoint!r}, "
                     f"api_version={self.api_version!r}, network_isolation={self.network_isolation}")

        # Credential
        try:
            client_id = os.environ.get('AZURE_CLIENT_ID', None)

            # Prefer Azure CLI locally to avoid IMDS probes; fall back to MI when available
            self.credential = ChainedTokenCredential(
                AzureCliCredential(),
                ManagedIdentityCredential(client_id=client_id)
            )
            logging.debug("[docintelligence] ChainedTokenCredential initialized (CLI first, then MI).")
        except Exception as e:
            logging.error(f"[docintelligence] Credential init failed: {e}")
            raise

    def _get_file_extension(self, filepath):
        clean = filepath.split('?')[0]
        return clean.split('.')[-1].lower()

    def _get_content_type(self, ext):
        types = {
            "pdf": "application/pdf",
            "bmp": "image/bmp",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "png": "image/png",
            "tiff": "image/tiff",
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "html": "text/html"
        }
        return types.get(ext, "application/octet-stream")

    def analyze_document_from_bytes(self, file_bytes: bytes, filename: str, model='prebuilt-layout'):
        result, errors = {}, []

        ext = self._get_file_extension(filename)
        logging.debug(f"[docintelligence][{filename}] File extension = {ext!r}")
        if ext not in self.file_extensions:
            msg = f"Unsupported extension '{ext}'."
            logging.error(f"[docintelligence][{filename}] {msg}")
            return result, [msg]

        self.docint_features = "ocrHighResolution" if ext == "pdf" else ""
        logging.debug(f"[docintelligence][{filename}] docint_features = {self.docint_features!r}")

        # Build endpoint
        base = (self.service_endpoint if self.service_endpoint.startswith("http")
                else f"https://{self.service_endpoint}")
        endpoint = (
            f"{base}/{self.ai_service_type}/documentModels/{model}:analyze"
            f"?api-version={self.api_version}"
        )
        if self.docint_features:
            endpoint += f"&features={self.docint_features}"
        if self.output_content_format:
            endpoint += f"&outputContentFormat={self.output_content_format}"
        if self.analyze_output_options:
            endpoint += f"&output={self.analyze_output_options}"

        logging.debug(f"[docintelligence][{filename}] Analyze endpoint: {endpoint!r}")
        logging.debug(f"[docintelligence][{filename}] Output format={self.output_content_format!r}, "
                      f"Options={self.analyze_output_options!r}")

        # Get token
        try:
            token = self.credential.get_token(
                "https://cognitiveservices.azure.com/.default"
            ).token
            logging.debug(f"[docintelligence][{filename}] Acquired token length={len(token)}")
            headers = {
                "Authorization": f"Bearer {token}",
                "x-ms-useragent": "gpt-rag/1.0.0",
                "Content-Type": "application/json"
            }
            logging.debug(f"[docintelligence][{filename}] Request headers: {headers}")
        except Exception as e:
            msg = f"Auth failed: {e}"
            logging.error(f"[docintelligence][{filename}] {msg}")
            return result, [msg]

        # Prepare payload
        b64 = base64.b64encode(file_bytes).decode("utf-8")
        logging.debug(f"[docintelligence][{filename}] Payload size (base64 chars) = {len(b64)}")
        payload = {"base64Source": b64}

        # Send request
        try:
            resp = requests.post(endpoint, headers=headers, json=payload)
            logging.info(f"[docintelligence][{filename}] POST -> {resp.status_code}")
            logging.debug(f"[docintelligence][{filename}] Response headers: {resp.headers}")
            logging.debug(f"[docintelligence][{filename}] Response body (first 500 chars): {resp.text[:500]!r}…")
        except Exception as e:
            msg = f"Request error: {e}"
            logging.error(f"[docintelligence][{filename}] {msg}")
            return result, [msg]

        if resp.status_code != 202:
            msg = f"Bad response {resp.status_code}: {resp.text}"
            logging.error(f"[docintelligence][{filename}] {msg}")
            return result, [msg]

        op_loc = resp.headers.get("Operation-Location")
        if not op_loc:
            msg = "Missing Operation-Location header."
            logging.error(f"[docintelligence][{filename}] {msg}")
            return result, [msg]

        result_id = op_loc.split("/")[-1].split("?")[0]
        logging.debug(f"[docintelligence][{filename}] Operation-Location: {op_loc}")
        logging.debug(f"[docintelligence][{filename}] Result ID: {result_id}")

        # Polling loop
        poll_headers = {
            "Authorization": f"Bearer {token}",
            "x-ms-useragent": "gpt-rag/1.0.0",
            "Content-Type": "application/json-patch+json"
        }
        while True:
            logging.debug(f"[docintelligence][{filename}] Polling {op_loc}")
            time.sleep(2)
            try:
                r = requests.get(op_loc, headers=poll_headers)
                logging.debug(f"[docintelligence][{filename}] Poll status={r.status_code}, "
                              f"body (first 200 chars)={r.text[:200]!r}")
                data = r.json()
            except Exception as e:
                msg = f"Polling error: {e}"
                logging.error(f"[docintelligence][{filename}] {msg}")
                errors.append(msg)
                break

            if r.status_code != 200 or data.get("status") == "failed":
                msg = f"Polling failed {r.status_code}: {r.text}"
                logging.error(f"[docintelligence][{filename}] {msg}")
                errors.append(msg)
                break
            if data.get("status") == "succeeded":
                result = data.get("analyzeResult", {})
                logging.info(f"[docintelligence][{filename}] Analysis succeeded.")
                break

        result.update(result_id=result_id, model_id=model)
        return result, errors

    def analyze_document_from_blob_url(self, file_url, model='prebuilt-layout'):
        result, errors = {}, []

        filename = os.path.basename(urlparse(file_url).path)
        logging.debug(f"[docintelligence][{filename}] Blob URL = {file_url!r}")

        # Download blob
        parsed = urlparse(file_url)
        account_url = f"{parsed.scheme}://{parsed.netloc}"
        container = parsed.path.split("/")[1]
        blob_name = unquote(parsed.path[len(f"/{container}/"):])
        logging.debug(f"[docintelligence][{filename}] account_url={account_url}, "
                      f"container={container}, blob_name={blob_name}")

        # Get token
        try:
            token = self.credential.get_token(
                "https://cognitiveservices.azure.com/.default"
            ).token
            logging.debug(f"[docintelligence][{filename}] Acquired token length={len(token)}")
            headers = {
                "Authorization": f"Bearer {token}",
                "x-ms-useragent": "gpt-rag/1.0.0",
                "Content-Type": "application/json"
            }
            logging.debug(f"[docintelligence][{filename}] Request headers: {headers}")
        except Exception as e:
            msg = f"Auth failed: {e}"
            logging.error(f"[docintelligence][{filename}] {msg}")
            return result, [msg]

        # Download blob bytes
        try:
            client = BlobServiceClient(account_url=account_url, credential=self.credential)
            blob = client.get_blob_client(container=container, blob=blob_name)
            data = blob.download_blob().readall()
            logging.debug(f"[docintelligence][{filename}] Blob downloaded, size={len(data)} bytes")
        except Exception as e:
            msg = f"Blob error: {e}"
            logging.error(f"[docintelligence][{filename}] {msg}")
            return result, [msg]

        # Prepare payload
        b64 = base64.b64encode(data).decode("utf-8")
        logging.debug(f"[docintelligence][{filename}] Payload size (base64 chars) = {len(b64)}")
        payload = {"base64Source": b64}

        # Build endpoint
        base = (self.service_endpoint if self.service_endpoint.startswith("http")
                else f"https://{self.service_endpoint}")
        endpoint = (
            f"{base}/{self.ai_service_type}/documentModels/{model}:analyze"
            f"?api-version={self.api_version}"
        )
        if self.docint_features:
            endpoint += f"&features={self.docint_features}"
        if self.output_content_format:
            endpoint += f"&outputContentFormat={self.output_content_format}"
        if self.analyze_output_options:
            endpoint += f"&output={self.analyze_output_options}"

        logging.debug(f"[docintelligence][{filename}] Analyze endpoint: {endpoint!r}")

        # Send request
        try:
            resp = requests.post(endpoint, headers=headers, json=payload)
            logging.info(f"[docintelligence][{filename}] POST -> {resp.status_code}")
            logging.debug(f"[docintelligence][{filename}] Response headers: {resp.headers}")
            logging.debug(f"[docintelligence][{filename}] Response body (first 500 chars): {resp.text[:500]!r}…")
        except Exception as e:
            msg = f"Request error: {e}"
            logging.error(f"[docintelligence][{filename}] {msg}")
            return result, [msg]

        if resp.status_code != 202:
            msg = f"Bad response {resp.status_code}: {resp.text}"
            logging.error(f"[docintelligence][{filename}] {msg}")
            return result, [msg]

        op_loc = resp.headers.get("Operation-Location")
        if not op_loc:
            msg = "Missing Operation-Location header."
            logging.error(f"[docintelligence][{filename}] {msg}")
            return result, [msg]

        result_id = op_loc.split("/")[-1].split("?")[0]
        logging.debug(f"[docintelligence][{filename}] Operation-Location: {op_loc}")
        logging.debug(f"[docintelligence][{filename}] Result ID: {result_id}")

        # Polling loop
        poll_headers = {
            "Authorization": f"Bearer {token}",
            "x-ms-useragent": "gpt-rag/1.0.0",
            "Content-Type": "application/json-patch+json"
        }
        while True:
            logging.debug(f"[docintelligence][{filename}] Polling {op_loc}")
            time.sleep(2)
            try:
                r = requests.get(op_loc, headers=poll_headers)
                logging.debug(f"[docintelligence][{filename}] Poll status={r.status_code}, "
                              f"body (first 200 chars)={r.text[:200]!r}")
                data = r.json()
            except Exception as e:
                msg = f"Polling error: {e}"
                logging.error(f"[docintelligence][{filename}] {msg}")
                errors.append(msg)
                break

            if r.status_code != 200 or data.get("status") == "failed":
                msg = f"Polling failed {r.status_code}: {r.text}"
                logging.error(f"[docintelligence][{filename}] {msg}")
                errors.append(msg)
                break
            if data.get("status") == "succeeded":
                result = data.get("analyzeResult", {})
                logging.info(f"[docintelligence][{filename}] Analysis succeeded.")
                break

        result.update(result_id=result_id, model_id=model)
        return result, errors

    def get_figure(self, model_id: str, result_id: str, figure_id: str) -> bytes:
        base = (self.service_endpoint if self.service_endpoint.startswith("http")
                else f"https://{self.service_endpoint}")
        url = (
            f"{base}/documentintelligence/documentModels/"
            f"{model_id}/analyzeResults/{result_id}/figures/{figure_id}"
            f"?api-version={self.api_version}"
        )
        logging.debug(f"[docintelligence] Fetching figure URL: {url}")

        try:
            token = self.credential.get_token(
                "https://cognitiveservices.azure.com/.default"
            ).token
            headers = {
                "Authorization": f"Bearer {token}",
                "x-ms-useragent": "gpt-rag/1.0.0"
            }
            logging.debug(f"[docintelligence] Figure request headers: {headers}")
            resp = requests.get(url, headers=headers)
            logging.info(f"[docintelligence] Figure GET -> {resp.status_code}")
            if resp.status_code == 200:
                return resp.content
            raise Exception(f"Status {resp.status_code}: {resp.text}")
        except Exception as e:
            logging.error(f"[docintelligence] Figure fetch error: {e}")
            raise
