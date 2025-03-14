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

document_chunking_func_key = os.getenv("DOCUMENT_CHUNKING_FUNCTION_KEY")
cognitive_service_key = os.getenv("COGNITIVE_SERVICES_KEY")
storage_connection_string = os.getenv("STORAGE_CONNECTION_STRING")
search_api_version = "2024-11-01-preview"
azure_search_admin_key = os.getenv("AZURE_SEARCH_ADMIN_KEY")
search_service_name = os.getenv("AZURE_SEARCH_SERVICE_NAME")

########################################################
# Create Data Source in AI Search
########################################################


def create_datasource(
    search_service: str,
    datasource_name: str,
    storage_connection_string: str,
    container_name: str,
    subfolder=None,
    search_api_version: str = "2024-11-01-preview",
    max_retries: int = 3,
    initial_delay: float = 3.0,
    deletion_delay: float = 8.0,
):
    """
    Creates or recreates a datasource for Azure Cognitive Search with retry logic.
    """
    logging.info(f"Starting datasource operation for '{datasource_name}'")

    headers = {
        "Content-Type": "application/json",
        "api-key": os.getenv("AZURE_SEARCH_ADMIN_KEY"),
    }

    # First check if datasource exists
    check_endpoint = f"https://{search_service}.search.windows.net/datasources/{datasource_name}?api-version={search_api_version}"
    try:
        logging.info(f"Checking if datasource '{datasource_name}' exists...")
        check_response = requests.get(check_endpoint, headers=headers)

        if check_response.status_code == 200:
            logging.info(
                f"Datasource '{datasource_name}' exists. Checking for associated indexers..."
            )

            # Get all indexers
            indexers_endpoint = f"https://{search_service}.search.windows.net/indexers?api-version={search_api_version}"
            indexers_response = requests.get(indexers_endpoint, headers=headers)

            if indexers_response.status_code == 200:
                indexers = indexers_response.json().get("value", [])
                for indexer in indexers:
                    if indexer.get("dataSourceName") == datasource_name:
                        indexer_name = indexer.get("name")
                        logging.info(
                            f"Found associated indexer '{indexer_name}'. Resetting it..."
                        )

                        # Reset the indexer
                        reset_endpoint = f"https://{search_service}.search.windows.net/indexers/{indexer_name}/reset?api-version={search_api_version}"
                        reset_response = requests.post(reset_endpoint, headers=headers)

                        if reset_response.status_code in [200, 204]:
                            logging.info(f"Successfully reset indexer '{indexer_name}'")
                        else:
                            logging.warning(
                                f"Failed to reset indexer '{indexer_name}': {reset_response.text}"
                            )

            # Now delete the datasource
            logging.info(f"Deleting datasource '{datasource_name}'...")
            delete_response = requests.delete(check_endpoint, headers=headers)
            if delete_response.status_code in [200, 204]:
                logging.info(
                    f"Successfully deleted existing datasource '{datasource_name}'"
                )
                logging.info(
                    f"Waiting {deletion_delay} seconds before creating new datasource..."
                )
                time.sleep(deletion_delay)
            else:
                logging.warning(
                    f"Unexpected status code while deleting datasource: {delete_response.status_code}"
                )
        elif check_response.status_code == 404:
            logging.info(
                f"Datasource '{datasource_name}' not found. Proceeding with creation."
            )
        else:
            logging.warning(
                f"Unexpected status code while checking datasource: {check_response.status_code}"
            )

    except requests.exceptions.ConnectionError:
        logging.error(
            f"Connection error while checking datasource. Please verify your network connection."
        )
        raise
    except Exception as e:
        logging.error(f"Error checking datasource existence: {str(e)}")
        raise

    # Create the datasource
    body = {
        "name": datasource_name,
        "description": f"Datastore for {datasource_name}",
        "type": "azureblob",
        "dataDeletionDetectionPolicy": {
            "@odata.type": "#Microsoft.Azure.Search.NativeBlobSoftDeleteDeletionDetectionPolicy"  # Fixed typo here
        },
        "credentials": {"connectionString": storage_connection_string},
        "container": {
            "name": container_name,
            "query": f"{subfolder}/" if subfolder else "",
        },
    }

    create_endpoint = f"https://{search_service}.search.windows.net/datasources?api-version={search_api_version}"

    # Retry logic with exponential backoff
    retry_count = 0
    current_delay = initial_delay

    while retry_count <= max_retries:
        try:
            logging.info(
                f"Creating datasource '{datasource_name}' (Attempt {retry_count + 1}/{max_retries + 1})..."
            )
            response = requests.post(create_endpoint, headers=headers, json=body)

            if response.status_code in [200, 201]:
                logging.info(f"Successfully created datasource '{datasource_name}'")
                logging.info(f"Response: {response.json()}")
                return response
            elif response.status_code == 429:  # Too Many Requests
                if retry_count < max_retries:
                    logging.warning(
                        f"Rate limit hit. Retrying in {current_delay} seconds..."
                    )
                    time.sleep(current_delay)
                    current_delay *= 2  # Exponential backoff
                    retry_count += 1
                    continue
                else:
                    logging.error("Max retries reached for rate limiting")
                    raise Exception("Rate limit exceeded after maximum retries")
            else:
                logging.error(f"Failed to create datasource '{datasource_name}'")
                logging.error(f"Status code: {response.status_code}")
                logging.error(f"Error response: {response.text}")
                raise Exception(f"Failed to create datasource: {response.text}")

        except requests.exceptions.ConnectionError:
            if retry_count < max_retries:
                logging.warning(
                    f"Connection error. Retrying in {current_delay} seconds..."
                )
                time.sleep(current_delay)
                current_delay *= 2
                retry_count += 1
                continue
            else:
                logging.error("Max retries reached for connection errors")
                raise
        except requests.exceptions.Timeout:
            if retry_count < max_retries:
                logging.warning(
                    f"Request timed out. Retrying in {current_delay} seconds..."
                )
                time.sleep(current_delay)
                current_delay *= 2
                retry_count += 1
                continue
            else:
                logging.error("Max retries reached for timeouts")
                raise
        except Exception as e:
            logging.error(f"Unexpected error while creating datasource: {str(e)}")
    try:
        logging.info(f"Creating datasource '{datasource_name}'...")
        response = requests.post(create_endpoint, headers=headers, json=body)

        if response.status_code in [200, 201]:
            logging.info(f"Successfully created datasource '{datasource_name}'")
            logging.info(f"Response: {response.json()}")
        else:
            logging.error(f"Failed to create datasource '{datasource_name}'")
            logging.error(f"Status code: {response.status_code}")
            logging.error(f"Error response: {response.text}")

    except requests.exceptions.ConnectionError:
        logging.error(
            f"Connection error while creating datasource. Please verify your network connection."
        )
        raise
    except Exception as e:
        logging.error(f"Error creating datasource: {str(e)}")
        raise

    return response


if __name__ == "__main__":

    datasource_name = "ragindex-test-datasource"
    container_name = "ragindex-test"

    create_datasource(
        search_service=search_service_name,
        datasource_name=datasource_name,
        storage_connection_string=storage_connection_string,
        container_name=container_name,
    )
