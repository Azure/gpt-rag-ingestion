import logging
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

class Transformer:
    def __init__(self, transform_config):
        self.transform_config = transform_config

    def transform(self, blob: BlobClient):
        file_extension = blob.blob_name.split('.')[-1]
        logging.info(f"[transform] File extension: {file_extension}")

        # Download the blob content
        downloader = blob.download_blob()
        blob_content = downloader.readall()
        blob_size = len(blob_content)
        logging.info(f"[transform] Blob size: {blob_size} bytes")
        logging.info(f"[transform] extract_config {self.transform_config}")

        # Retrieve operations based on file extension
        operations = self.transform_config.get('transform', {}).get(file_extension, [])
        if not operations:
            operations = self.transform_config.get('transform', {}).get('default', [])

        # Execute each operation
        for operation in operations:
            op_name = operation['operation']
            if op_name == 'max_pages':
                self.max_pages(blob_content, operation.get('parameters', {}))
            elif op_name == 'print_screen':
                self.print_screen(blob_content)
            elif op_name == 'move':
                self.move(blob.blob_name, blob_content, operation.get('output_format', 'same'))

    def max_pages(self, blob_content, parameters):
        num_max_pages = parameters.get('num_max_pages', 10)
        logging.info(f"[max_pages] Limiting to {num_max_pages} pages")
        # Implement logic to limit the number of pages in the blob_content

    def print_screen(self, blob_content):
        logging.info("[print_screen] Printing screen of the content")
        # Implement logic to print screen of the blob_content

    def move(self, blob_name, blob_content, output_format):
        logging.info(f"[move] Moving content with output format: {output_format}")
        
        # Retrieve environment variables
        connection_string = os.getenv('AzureWebJobsRagStorageConnection')
        container_name = os.getenv('STORAGE_CONTAINER')
        
        if not connection_string or not container_name:
            logging.error("[move] Missing environment variables for storage connection")
            return
        
        # Connect to the storage account
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        
        # Upload the blob
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(blob_content, overwrite=True)
        
        logging.info(f"[move] Blob {blob_name} moved to container {container_name}")