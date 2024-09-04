from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from urllib.parse import urlparse, unquote
import logging
import time

class BlobStorageClient:

    def download_blob(self, file_url):        
            parsed_url = urlparse(file_url)
            account_url = parsed_url.scheme + "://" + parsed_url.netloc
            container_name = parsed_url.path.split("/")[1]
            # blob_name = parsed_url.path.split("/")[2]
            blob_name = unquote(parsed_url.path.split("/")[2])

            logging.info(f"[blob][{blob_name}] Connecting to blob.")

            credential = DefaultAzureCredential()
            blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
            blob_error = None

            data = ""

            try:
                data = blob_client.download_blob().readall()
            except Exception as e:
                logging.info(f"[blob][{blob_name}] Connection error, retrying in 10 seconds...")
                time.sleep(10)
                try:
                    data = blob_client.download_blob().readall()
                except Exception as e:
                    blob_error = e

            if blob_error:
                error_message = f"Blob client error when reading from blob storage. {blob_error}"
                logging.info(f"[blob][{blob_name}] {error_message}")
            
            return data	