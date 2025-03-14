# BlobStorageClient.py

from azure.identity import ManagedIdentityCredential, AzureCliCredential, ChainedTokenCredential
from azure.storage.blob import BlobServiceClient
from urllib.parse import urlparse, unquote
import logging
import time

class BlobStorageClient:
    """
    BlobStorageClient provides methods to interact with Azure Blob Storage.

    Attributes:
        file_url (str): The URL of the blob to interact with.
        credential (ChainedTokenCredential): The credential used for authentication.
        blob_service_client (BlobServiceClient): The BlobServiceClient instance.
    """

    def __init__(self, file_url):
        """
        Initializes the BlobStorageClient with a specified blob URL.

        Args:
            file_url (str): The URL of the blob to interact with.

        Raises:
            EnvironmentError: If the blob URL is not properly formatted.
            Exception: If credential initialization fails.
        """
        self.file_url = file_url
        self.credential = None
        self.blob_service_client = None

        # Initialize the ChainedTokenCredential with ManagedIdentityCredential and AzureCliCredential
        try:
            self.credential = ChainedTokenCredential(
                ManagedIdentityCredential(),
                AzureCliCredential()
            )
            logging.debug("[blob] Initialized ChainedTokenCredential with ManagedIdentityCredential and AzureCliCredential.")
        except Exception as e:
            logging.error(f"[blob] Failed to initialize ChainedTokenCredential: {e}")
            raise

        # Parse the blob URL and initialize BlobServiceClient
        try:
            parsed_url = urlparse(self.file_url)
            self.account_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
            self.container_name = parsed_url.path.split("/")[1]
            self.blob_name = unquote(parsed_url.path[len(f"/{self.container_name}/"):])
            logging.debug(f"[blob][{self.blob_name}] Parsed blob URL successfully.")
        except Exception as e:
            logging.error(f"[blob] Invalid blob URL '{self.file_url}': {e}")
            raise EnvironmentError(f"Invalid blob URL '{self.file_url}': {e}")

        # Initialize BlobServiceClient
        try:
            self.blob_service_client = BlobServiceClient(account_url=self.account_url, credential=self.credential)
            logging.debug(f"[blob][{self.blob_name}] Initialized BlobServiceClient.")
        except Exception as e:
            logging.error(f"[blob][{self.blob_name}] Failed to initialize BlobServiceClient: {e}")
            raise

    def download_blob(self):
        """
        Downloads the blob data from Azure Blob Storage.

        Returns:
            bytes: The content of the blob.

        Raises:
            Exception: If downloading the blob fails after retries.
        """
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=self.blob_name)
        blob_error = None
        data = b""

        try:
            logging.debug(f"[blob][{self.blob_name}] Attempting to download blob.")
            data = blob_client.download_blob().readall()
            logging.info(f"[blob][{self.blob_name}] Blob downloaded successfully.")
        except Exception as e:
            logging.warning(f"[blob][{self.blob_name}] Connection error, retrying in 10 seconds... Error: {e}")
            time.sleep(10)
            try:
                data = blob_client.download_blob().readall()
                logging.info(f"[blob][{self.blob_name}] Blob downloaded successfully on retry.")
            except Exception as e_retry:
                blob_error = e_retry
                logging.error(f"[blob][{self.blob_name}] Failed to download blob after retry: {blob_error}")

        if blob_error:
            error_message = f"Blob client error when reading from blob storage: {blob_error}"
            logging.error(f"[blob][{self.blob_name}] {error_message}")
            # Check for specific error codes
            if "AuthorizationPermissionMismatch" in str(blob_error):
                logging.error("[blob] Authorization error: Please check your permissions.")
            raise Exception(error_message)

        return data
    
    def get_metadata(self):
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=self.blob_name)

        # Retrieve existing metadata, if desired
        blob_metadata = blob_client.get_blob_properties().metadata
        return blob_metadata