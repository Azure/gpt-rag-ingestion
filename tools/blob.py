from azure.storage.blob import ContainerClient, BlobServiceClient
from azure.identity import ManagedIdentityCredential, AzureCliCredential, ChainedTokenCredential
from azure.core.exceptions import ResourceNotFoundError, AzureError
from urllib.parse import urlparse, unquote
import logging
import os
import time

class BlobClient:
    def __init__(self, blob_url, credential=None):
        """
        Initialize BlobClient with a specific blob URL.
        
        :param blob_url: URL of the blob (e.g., "https://mystorage.blob.core.windows.net/mycontainer/myblob.png")
        :param credential: Credential for authentication (optional)
        """
        # 1. Generate the credential in case it is not provided 
        self.credential = self._get_credential(credential)
        self.file_url = blob_url
        self.blob_service_client = None

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

    def _get_credential(self, credential):
        """
        Get the appropriate credential for authentication.
        
        :param credential: Credential for authentication (optional)
        :return: Credential object
        """
        if credential is None:
            try:
                credential = ChainedTokenCredential(
                    ManagedIdentityCredential(),
                    AzureCliCredential()
                )
                logging.debug("[blob] Initialized ChainedTokenCredential with ManagedIdentityCredential and AzureCliCredential.")
            except Exception as e:
                logging.error(f"[blob] Failed to initialize ChainedTokenCredential: {e}")
                raise
        else:
            logging.debug("[blob] Initialized BlobClient with provided credential.")
        return credential

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
            raise Exception(error_message)

        return data

class BlobContainerClient:
    def __init__(self, storage_account_base_url, container_name, credential=None):
        """
        Initialize BlobContainerClient with the storage account base URL and container name.
        
        :param storage_account_base_url: Base URL of the storage account (e.g., "https://mystorage.blob.core.windows.net")
        :param container_name: Name of the container
        :param credential: Credential for authentication (optional)
        """
        try:
            self.credential = self._get_credential(credential)
            self.container_client = ContainerClient(
                account_url=storage_account_base_url,
                container_name=container_name,
                credential=self.credential
            )
            # Verify the container exists
            self.container_client.get_container_properties()
            logging.debug(f"[blob] Connected to container '{container_name}'.")
        except ResourceNotFoundError:
            logging.error(f"[blob] Container '{container_name}' does not exist.")
            raise
        except AzureError as e:
            logging.error(f"[blob] Failed to connect to container: {e}")
            raise


    def _get_credential(self, credential):
        """
        Get the appropriate credential for authentication.
        
        :param credential: Credential for authentication (optional)
        :return: Credential object
        """
        if credential is None:
            try:
                credential = ChainedTokenCredential(
                    ManagedIdentityCredential(),
                    AzureCliCredential()
                )
                logging.debug("[blob] Initialized ChainedTokenCredential with ManagedIdentityCredential and AzureCliCredential.")
            except Exception as e:
                logging.error(f"[blob] Failed to initialize ChainedTokenCredential: {e}")
                raise
        else:
            logging.debug("[blob] Initialized BlobClient with provided credential.")
        return credential

    def upload_blob(self, blob_name, file_path, overwrite=False):
        """
        Upload a local file to a blob within the container.
        
        :param blob_name: Name of the blob
        :param file_path: Path to the local file to upload
        :param overwrite: Whether to overwrite the blob if it already exists
        """
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=overwrite)
            logging.info(f"[blob] Uploaded '{file_path}' as blob '{blob_name}'.")
        except AzureError as e:
            logging.info(f"[blob] Failed to upload blob '{blob_name}': {e}")

    def download_blob(self, blob_name, download_file_path):
        """
        Download a blob from the container to a local file.
        
        :param blob_name: Name of the blob
        :param download_file_path: Path to the local file where the blob will be downloaded
        """
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            with open(download_file_path, "wb") as download_file:
                download_stream = blob_client.download_blob()
                download_file.write(download_stream.readall())
            logging.info(f"[blob] Downloaded blob '{blob_name}' to '{download_file_path}'.")
        except ResourceNotFoundError:
            logging.info(f"[blob] Blob '{blob_name}' not found in container '{self.container_client.container_name}'.")
        except AzureError as e:
            logging.info(f"[blob] Failed to download blob '{blob_name}': {e}")

    def delete_blob(self, blob_name):
        """
        Delete a blob from the container.
        
        :param blob_name: Name of the blob to delete
        """
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_client.delete_blob()
            logging.info(f"[blob] Deleted blob '{blob_name}' from container '{self.container_client.container_name}'.")
        except ResourceNotFoundError:
            logging.info(f"[blob] Blob '{blob_name}' not found in container '{self.container_client.container_name}'.")
        except AzureError as e:
            logging.info(f"[blob] Failed to delete blob '{blob_name}': {e}")

    def list_blobs(self):
        """
        List all blobs in the container.
        
        :return: List of blob names
        """
        try:
            blobs = self.container_client.list_blobs()
            blob_names = [blob.name for blob in blobs]
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(f"Blobs in container '{self.container_client.container_name}':")
                for name in blob_names:
                    logging.debug(f" - {name}")
            return blob_names
        except AzureError as e:
            logging.info(f"[blob] Failed to list blobs: {e}")
            return []

# Example usage
if __name__ == "__main__":
    # Replace these variables with your actual values
    STORAGE_ACCOUNT_URL = "https://mystorage.blob.core.windows.net"
    CONTAINER_NAME = "mycontainer"
    CREDENTIAL = os.getenv("AZURE_STORAGE_KEY")  # Or use another method for credentials

    try:
        # Initialize BlobContainerClient
        container_client = BlobContainerClient(
            storage_account_base_url=STORAGE_ACCOUNT_URL,
            container_name=CONTAINER_NAME,
            credential=CREDENTIAL
        )

        # Upload a blob
        container_client.upload_blob(
            blob_name="example_blob.txt",
            file_path="/path/to/local/example_blob.txt",
            overwrite=True
        )

        # List blobs
        container_client.list_blobs()

        # Download a blob
        container_client.download_blob(
            blob_name="example_blob.txt",
            download_file_path="/path/to/downloaded/example_blob.txt"
        )

        # Delete a blob
        container_client.delete_blob(blob_name="example_blob.txt")

    except Exception as e:
        logging.info(f"[blob] An error occurred: {e}")
