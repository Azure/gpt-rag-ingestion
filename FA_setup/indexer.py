import logging
import time
import os
import requests
from typing import Optional
import json

# set up logging configuration globally
# logging.getLogger("azure").setLevel(logging.WARNING)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

########################################################
# Constants
########################################################

document_chunking_func_key = os.getenv("DOCUMENT_CHUNKING_FUNC_KEY")
search_api_version = "2024-11-01-preview"
azure_search_admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
search_service_name = os.getenv("AZURE_SEARCH_SERVICE_NAME")

########################################################
# Create index
########################################################

def create_indexer_body(
    indexer_name: str,
    search_index_name: str,
    search_index_interval: str = "P1D",
    datasource_name: str = None,
):
    # Endpoint URL
    endpoint = f"https://{search_service_name}.search.windows.net/indexers/{indexer_name}?api-version={search_api_version}"

    # Headers
    headers = {"Content-Type": "application/json", "api-key": azure_search_admin_key}
    body = {
        "name": indexer_name,
        "dataSourceName": datasource_name,
        "targetIndexName": f"{search_index_name}",
        "skillsetName": f"{search_index_name}-skillset-chunking",
        "schedule": {"interval": f"{search_index_interval}"},
        "fieldMappings": [
            {
            "sourceFieldName": "metadata_storage_name",
            "targetFieldName": "title",
            "mappingFunction": None
            }
        ],
        "outputFieldMappings": [],
        "cache": None,
        "encryptionKey": None
    }
    # First, try to delete the existing indexer if it exists
    try:
        delete_response = requests.delete(endpoint, headers=headers)
        print(f"Delete existing indexer response: {delete_response.status_code}")
    except Exception as e:
        print(f"Error deleting existing indexer: {e}")

    # Create the new indexer
    try:
        response = requests.put(endpoint, headers=headers, json=body)

        if response.status_code in [200, 201]:
            print("Indexer created successfully!")
            print(json.dumps(response.json(), indent=2))
        else:
            print(f"Error creating indexer. Status code: {response.status_code}")
            print(f"Error message: {response.text}")

    except Exception as e:
        print(f"Error creating indexer: {e}")

if __name__ == "__main__":
    indexer_name = "financial-indexer-test"
    search_index_name = "financial-index-test"
    datasource_name = "financial-datasource-test"
    create_indexer_body(indexer_name, search_index_name, datasource_name)
    