import base64
import json
import logging
import time

import requests
from azure.identity import (
    AzureCliCredential,
    ChainedTokenCredential,
    ManagedIdentityCredential,
)

from dependencies import get_config

app_config_client = get_config()


class ContentUnderstandingClient:
    """
    Client for Azure Content Understanding prebuilt-layout analyzer.

    Uses the Content Understanding REST API (GA 2025-11-01) to extract
    structured Markdown from documents, replacing the more expensive
    Document Intelligence Layout + High Resolution path.
    """

    DEFAULT_API_VERSION = "2025-11-01"
    COGNITIVE_SCOPE = "https://cognitiveservices.azure.com/.default"

    # Formats supported by prebuilt-layout
    SUPPORTED_EXTENSIONS = [
        "pdf", "bmp", "jpg", "jpeg", "png", "tiff",
        "docx", "pptx", "xlsx", "html",
    ]

    def __init__(self):
        self.service_endpoint = app_config_client.get("AI_FOUNDRY_ACCOUNT_ENDPOINT")
        if not self.service_endpoint:
            raise EnvironmentError(
                "The configuration 'AI_FOUNDRY_ACCOUNT_ENDPOINT' is not set."
            )
        self.service_endpoint = self.service_endpoint.rstrip("/")

        self.api_version = app_config_client.get(
            "CONTENT_UNDERSTANDING_API_VERSION", self.DEFAULT_API_VERSION
        )
        self.analyzer = app_config_client.get(
            "CONTENT_UNDERSTANDING_ANALYZER", "prebuilt-layout"
        )

        # Content Understanding always outputs markdown
        self.output_content_format = "markdown"
        self.file_extensions = list(self.SUPPORTED_EXTENSIONS)

        client_id = (
            app_config_client.get("AZURE_CLIENT_ID", None, allow_none=True) or None
        )
        self.credential = ChainedTokenCredential(
            AzureCliCredential(),
            ManagedIdentityCredential(client_id=client_id),
        )

        logging.info(
            f"[content_understanding] Initialized: endpoint={self.service_endpoint!r}, "
            f"api_version={self.api_version!r}, analyzer={self.analyzer!r}"
        )

    def _get_file_extension(self, filepath: str) -> str:
        clean = filepath.split("?")[0]
        return clean.rsplit(".", 1)[-1].lower() if "." in clean else ""

    def analyze_document_from_bytes(
        self,
        file_bytes: bytes,
        filename: str,
    ):
        """
        Analyze a document using Content Understanding prebuilt-layout.

        Returns:
            tuple: (result_dict, errors_list)
                result_dict has at least {"content": "<markdown>"} on success.
                errors_list is empty on success.
        """
        result, errors = {}, []

        ext = self._get_file_extension(filename)
        if ext not in self.file_extensions:
            msg = f"Unsupported extension '{ext}' for Content Understanding."
            logging.error(f"[content_understanding][{filename}] {msg}")
            return result, [msg]

        # Build endpoint
        base = (
            self.service_endpoint
            if self.service_endpoint.startswith("http")
            else f"https://{self.service_endpoint}"
        )
        endpoint = (
            f"{base}/contentunderstanding/analyzers/{self.analyzer}:analyze"
            f"?api-version={self.api_version}"
        )

        # Acquire token
        try:
            token = self.credential.get_token(self.COGNITIVE_SCOPE).token
        except Exception as e:
            msg = f"Auth failed: {e}"
            logging.error(f"[content_understanding][{filename}] {msg}")
            return result, [msg]

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "x-ms-useragent": "gpt-rag/1.0.0",
        }

        # Build payload with base64-encoded data
        b64 = base64.b64encode(file_bytes).decode("utf-8")
        payload = {"inputs": [{"data": b64}]}

        # Submit analysis
        try:
            resp = requests.post(endpoint, headers=headers, json=payload, timeout=120)
            logging.info(
                f"[content_understanding][{filename}] POST -> {resp.status_code}"
            )
        except Exception as e:
            msg = f"Request error: {e}"
            logging.error(f"[content_understanding][{filename}] {msg}")
            return result, [msg]

        if resp.status_code != 202:
            msg = f"Bad response {resp.status_code}: {resp.text}"
            logging.error(f"[content_understanding][{filename}] {msg}")
            return result, [msg]

        op_loc = resp.headers.get("Operation-Location")
        if not op_loc:
            msg = "Missing Operation-Location header."
            logging.error(f"[content_understanding][{filename}] {msg}")
            return result, [msg]

        logging.debug(
            f"[content_understanding][{filename}] Operation-Location: {op_loc}"
        )

        # Poll for result
        poll_headers = {
            "Authorization": f"Bearer {token}",
            "x-ms-useragent": "gpt-rag/1.0.0",
        }
        max_poll_seconds = 600
        start = time.monotonic()

        while True:
            if time.monotonic() - start > max_poll_seconds:
                msg = f"Polling timed out after {max_poll_seconds}s"
                logging.error(f"[content_understanding][{filename}] {msg}")
                errors.append(msg)
                break

            time.sleep(2)
            try:
                r = requests.get(op_loc, headers=poll_headers, timeout=60)
                data = r.json()
            except Exception as e:
                msg = f"Polling error: {e}"
                logging.error(f"[content_understanding][{filename}] {msg}")
                errors.append(msg)
                break

            status = data.get("status", "").lower()
            if status in ("failed", "canceled"):
                msg = f"Analysis {status}: {r.text}"
                logging.error(f"[content_understanding][{filename}] {msg}")
                errors.append(msg)
                break

            if status == "succeeded":
                # Extract markdown, figures, and pages from response
                contents = (
                    data.get("result", {}).get("contents", [])
                )
                if contents:
                    content_data = contents[0]
                    markdown = content_data.get("markdown", "")
                    figures = content_data.get("figures", [])
                    pages = content_data.get("pages", [])
                else:
                    markdown = ""
                    figures = []
                    pages = []

                result = {"content": markdown}
                if figures:
                    result["figures"] = figures
                if pages:
                    result["pages"] = pages

                # Set result_id and model_id for downstream compatibility
                result["result_id"] = data.get("id", "")
                result["model_id"] = self.analyzer

                logging.info(
                    f"[content_understanding][{filename}] Analysis succeeded, "
                    f"content length={len(markdown)}, "
                    f"figures={len(figures)}, pages={len(pages)}"
                )
                break

        return result, errors
