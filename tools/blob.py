import logging
import time
from urllib.parse import urlparse, unquote

from azure.identity import ManagedIdentityCredential, AzureCliCredential, ChainedTokenCredential
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient


class BlobStorageClient:
    """
    BlobStorageClient provides methods to interact with Azure Blob Storage using 
    Managed Identity or Azure CLI (chained credentials). By default, it is initialized 
    with a blob URL, from which it derives the storage account URL, container name, and blob name.
    You can also call list/delete/upload methods for different blobs in the same container if needed.
    """

    def __init__(self, file_url: str):
        """
        Initializes the BlobStorageClient with a specified blob URL.

        Args:
            file_url (str): The URL of the blob to interact with.
                            e.g. "https://mystorage.blob.core.windows.net/mycontainer/myblob.png"

        Raises:
            EnvironmentError: If the blob URL is not properly formatted.
            Exception: If credential initialization fails.
        """
        self.file_url = file_url
        self.credential = None
        self.blob_service_client = None

        # 1. Initialize the ChainedTokenCredential with ManagedIdentityCredential + AzureCliCredential
        try:
            self.credential = ChainedTokenCredential(
                ManagedIdentityCredential(),
                AzureCliCredential()
            )
            logging.debug("[blob] Initialized ChainedTokenCredential with ManagedIdentityCredential and AzureCliCredential.")
        except Exception as e:
            logging.error(f"[blob] Failed to initialize ChainedTokenCredential: {e}")
            raise

        # 2. Parse the blob URL => account_url, container_name, blob_name
        try:
            parsed_url = urlparse(self.file_url)
            self.account_url = f"{parsed_url.scheme}://{parsed_url.netloc}"   # e.g. https://mystorage.blob.core.windows.net
            self.container_name = parsed_url.path.split("/")[1]              # e.g. 'mycontainer'
            # Blob name is everything after "/{container_name}/"
            self.blob_name = unquote(parsed_url.path[len(f"/{self.container_name}/"):])
            logging.debug(f"[blob][{self.blob_name}] Parsed blob URL successfully.")
        except Exception as e:
            logging.error(f"[blob] Invalid blob URL '{self.file_url}': {e}")
            raise EnvironmentError(f"Invalid blob URL '{self.file_url}': {e}")

        # 3. Initialize the BlobServiceClient
        try:
            self.blob_service_client = BlobServiceClient(
                account_url=self.account_url, 
                credential=self.credential
            )
            logging.debug(f"[blob][{self.blob_name}] Initialized BlobServiceClient.")
        except Exception as e:
            logging.error(f"[blob][{self.blob_name}] Failed to initialize BlobServiceClient: {e}")
            raise

    def download_blob(self) -> bytes:
        """
        Downloads the *default* blob (self.blob_name in self.container_name) from Azure Blob Storage.

        Returns:
            bytes: The content of the blob as bytes.

        Raises:
            Exception: If downloading the blob fails (with one retry).
        """
        blob_client = self.blob_service_client.get_blob_client(
            container=self.container_name, 
            blob=self.blob_name
        )
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
            raise Exception(error_message)

        return data

    def list_blobs(self, container_name: str = None):
        """
        Lists blobs in the specified container. If no container is provided, 
        uses the default container_name from the parsed file_url.

        Returns:
            Iterable[BlobProperties]: An iterable of blob properties within that container.
        """
        container_name = container_name or self.container_name
        container_client = self.blob_service_client.get_container_client(container_name)

        logging.debug(f"[blob][list_blobs] Listing blobs in container: {container_name}")
        return container_client.list_blobs()

    def delete_blob(self, blob_name: str = None, container_name: str = None):
        """
        Deletes the specified blob from the specified container. If no blob_name is provided, 
        defaults to self.blob_name; likewise for container_name.

        Args:
            blob_name (str, optional): The name of the blob to delete.
            container_name (str, optional): The container name. Defaults to parsed self.container_name.
        """
        _container_name = container_name or self.container_name
        _blob_name = blob_name or self.blob_name

        blob_client = self.blob_service_client.get_blob_client(_container_name, _blob_name)

        logging.debug(f"[blob][{_blob_name}] Attempting to delete blob in container '{_container_name}'.")
        try:
            blob_client.delete_blob()
            logging.info(f"[blob][{_blob_name}] Blob deleted successfully.")
        except Exception as e:
            logging.error(f"[blob][{_blob_name}] Failed to delete blob: {e}")
            raise

    def upload_blob(self, data: bytes, blob_name: str = None, container_name: str = None, overwrite: bool = True):
        """
        Uploads bytes to the specified container/blob. If none provided, uses self.blob_name/container_name.
        Useful if you want to reuse this client to upload multiple blobs to the same storage account.

        Args:
            data (bytes): The content to upload.
            blob_name (str, optional): The blob name. Defaults to self.blob_name.
            container_name (str, optional): The container name. Defaults to self.container_name.
            overwrite (bool, optional): Whether to overwrite if the blob exists. Defaults to True.
        """
        _container_name = container_name or self.container_name
        _blob_name = blob_name or self.blob_name

        blob_client = self.blob_service_client.get_blob_client(_container_name, _blob_name)

        logging.debug(f"[blob][{_blob_name}] Attempting to upload blob in container '{_container_name}'. Overwrite={overwrite}")
        try:
            blob_client.upload_blob(data, overwrite=overwrite)
            logging.info(f"[blob][{_blob_name}] Blob uploaded successfully.")
        except Exception as e:
            logging.error(f"[blob][{_blob_name}] Failed to upload blob: {e}")
            raise
