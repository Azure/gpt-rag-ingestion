import logging
import time
import os
import requests
from typing import Optional
import json
from index import create_index_body
from datasource import create_datasource
from skillset import create_skillset
from indexer import create_indexer_body


# set up logging configuration globally
# logging.getLogger("azure").setLevel(logging.WARNING)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

########################################################
# Variables & Constants
########################################################

document_chunking_func_key = os.getenv("DOCUMENT_CHUNKING_FUNCTION_KEY")
cognitive_service_key = os.getenv("COGNITIVE_SERVICES_KEY")
storage_connection_string = os.getenv("STORAGE_CONNECTION_STRING")
search_api_version = "2024-11-01-preview"
azure_search_admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
search_service_name = os.getenv("AZURE_SEARCH_SERVICE_NAME")
azure_openai_api_key = os.getenv("AZURE_OPENAI_API_KEY")
function_endpoint = "https://document-chunking-az-func.azurewebsites.net"


########################################################
# set up the search indexing pipeline
########################################################


def setup_search_indexing(
    search_service_name: str,
    datasource_name: str,
    container_name: str,
    index_name: str,
    indexer_name: str,
    function_endpoint: str = function_endpoint,
    search_api_version: str = search_api_version,
    storage_connection_string: str = storage_connection_string,
) -> None:
    """
    Sets up the complete search indexing pipeline including datasource, index, skillset, and indexer.

    Args:
        search_service_name: Name of the Azure Search service
        datasource_name: Name for the datasource to be created
        storage_connection_string: Connection string for the storage account
        container_name: Name of the container to index
        index_name: Name for the search index
        indexer_name: Name for the indexer
        function_endpoint: Endpoint URL for the Azure Function
        search_api_version: API version for Azure Search service (default: 2023-07-01-Preview)
    """
    try:
        # Step 1: Create datasource
        logging.info(f"Step 1: Creating datasource {datasource_name}...")
        create_datasource(
            search_service=search_service_name,
            datasource_name=datasource_name,
            storage_connection_string=storage_connection_string,
            container_name=container_name,
        )

        # Step 2: Create index
        logging.info(f"Step 2: Creating index {index_name}...")
        create_index_body(
            index_name=index_name,
            search_api_version=search_api_version,
        )

        # Step 3: Create skillset
        logging.info(f"Step 3: Creating skillset for {index_name}...")
        create_skillset(
            search_index_name=index_name,
            function_endpoint=function_endpoint,
        )

        # Step 4: Create indexer
        logging.info(f"Step 4: Creating indexer {indexer_name}...")
        create_indexer_body(
            indexer_name=indexer_name,
            search_index_name=index_name,
            datasource_name=datasource_name,
        )

        logging.info("Search indexing setup completed successfully!")

    except Exception as e:
        logging.error(f"Error during search indexing setup: {str(e)}")
        raise


if __name__ == "__main__":
    index_name = "ragindex-test"  # name of the search index
    datasource_name = "ragindex-test-datasource"  # name of the datasource (in Azure Search)
    container_name = (
        "documents"  # name of the storage container (in Azure Blob Storage)
    )
    indexer_name = "ragindex-test-indexer"  # name of the indexer we want to create

    setup_search_indexing(
        search_service_name=search_service_name,
        datasource_name=datasource_name,
        container_name=container_name,
        index_name=index_name,
        indexer_name=indexer_name,
    )
