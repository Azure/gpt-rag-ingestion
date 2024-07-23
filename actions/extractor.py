import logging
import json
import os
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

RAW_DATA_CONTAINER = 'rawdata'

class Extractor:
    def __init__(self, extract_config):
        self.extract_config = extract_config

    def extract(self):
        logging.info(f"[extractor] extract_config {self.extract_config}")

        # Iterate over each extraction type in the extract_config
        for extract_type in self.extract_config.get('extract', []):
            type_name = extract_type['type']
            if type_name == 'graph_api':
                logging.info(f"[extractor] extract_config extracting type: {type_name}")                
                self.graph_api(extract_type['query'], extract_type['output_format'], type_name)

    def graph_api(self, query, output_format, extract_type):
        logging.info(f"[graph_api] Performing query: {query}")
        
        # Dummy implementation of the Graph API query
        result = {
            "data": "This is a dummy result for the query."
        }
        
        # Convert result to the specified output format
        if output_format == 'json':
            content = json.dumps(result)
        elif output_format == 'txt':
            content = result['data']
        
        # Store the result in the blob storage
        self.store_blob(content, output_format, extract_type)

    def store_blob(self, content, output_format, extract_type):
        # Retrieve environment variables
        connection_string = os.getenv('AzureWebJobsRagStorageConnection')
        if not connection_string:
            logging.error("[store_blob] Missing environment variable for storage connection")
            return
        
        # Connect to the storage account
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(RAW_DATA_CONTAINER)

        try:
            # Create the container if it doesn't exist
            container_client.create_container()
            logging.info(f"[store_blob] Created container '{RAW_DATA_CONTAINER}'")
        except Exception as e:
            if "ContainerAlreadyExists" in str(e):
                logging.info(f"[store_blob] Container '{RAW_DATA_CONTAINER}' already exists. Proceeding with blob upload.")
            else:
                logging.error(f"[store_blob] Error creating container: {e}")
                return
        
        # Count the number of blobs in the container
        blob_count = len(list(container_client.list_blobs()))
        
        # temporary, remove this after testing
        if blob_count >= 10:
            logging.info(f"[store_blob] Container '{RAW_DATA_CONTAINER}' already has 10 or more blobs. Skipping storage.")
            return
        
        # Generate blob name based on timestamp and extract_type
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        blob_name = f"{extract_type}_{timestamp}.{output_format}"
        
        # Upload the blob
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(content, overwrite=True)
        
        logging.info(f"[store_blob] Blob {blob_name} stored in container '{RAW_DATA_CONTAINER}'")

