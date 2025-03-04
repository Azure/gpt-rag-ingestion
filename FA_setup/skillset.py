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
azure_openai_api_key = os.getenv("AZURE_OPENAI_API_KEY")

########################################################
# Delete skillset
########################################################

def delete_skillset(skillset_name: str,
                   service_name: str = search_service_name,
                   api_version: str = search_api_version,
                   admin_key: str = azure_search_admin_key):
    """
    Deletes an existing skillset.

    Args:
        skillset_name (str): Name of the skillset to delete
    """
    endpoint = f"https://{service_name}.search.windows.net/skillsets/{skillset_name}?api-version={api_version}"
    headers = {"Content-Type": "application/json", "api-key": admin_key}

    try:
        logging.info(f"Attempting to delete skillset '{skillset_name}'...")
        response = requests.delete(endpoint, headers=headers)

        if response.status_code == 204:
            logging.info(f"Skillset '{skillset_name}' deleted successfully!")
        elif response.status_code == 404:
            logging.info(f"Skillset '{skillset_name}' does not exist.")
        else:
            logging.warning(f"Unexpected status code while deleting skillset: {response.status_code}")
            logging.warning(f"Response: {response.text}")

    except requests.exceptions.ConnectionError:
        logging.error("Connection error while deleting skillset. Please verify your network connection.")
        raise
    except Exception as e:
        logging.error(f"Error deleting skillset: {str(e)}")
        raise

    return response

########################################################
# Create skillset
########################################################

def create_skillset(search_index_name: str,
                    function_endpoint: str,
                    function_key: str = document_chunking_func_key,
                    service_name: str = search_service_name,
                    api_version: str = search_api_version,
                    admin_key: str = azure_search_admin_key,
                    cognitive_services_key: str = cognitive_service_key):
    """
    Creates a skillset for document processing and key phrase extraction.

    Args:
        search_index_name (str): Name of the search index
        function_endpoint (str): Endpoint URL for the document chunking function
    """

    if not function_key:
        logging.error(
            "Function key not found. Please set the DOCUMENT_CHUNKING_FUNCTION_KEY environment variable."
        )
        raise ValueError(
            "Function key not found. Please set the DOCUMENT_CHUNKING_FUNCTION_KEY environment variable."
        )
    
    if not cognitive_services_key:
        logging.error(
            "Cognitive services key not found. Please set the COGNITIVE_SERVICES_KEY environment variable."
        )
        raise ValueError(
            "Cognitive services key not found. Please set the COGNITIVE_SERVICES_KEY environment variable."
        )
    skillset_name = f"{search_index_name}-skillset-chunking"

    logging.info(f"Starting skillset operation for '{skillset_name}'")
    logging.info(f"Using search service: {service_name}")
    logging.info(f"Using API version: {api_version}")

    # Delete existing skillset if it exists
    logging.info(f"Checking if skillset '{skillset_name}' exists...")
    delete_skillset(skillset_name, service_name, api_version, admin_key)

    # Endpoint URL
    endpoint = f"https://{service_name}.search.windows.net/skillsets/{skillset_name}?api-version={api_version}"
    logging.info(f"Endpoint URL: {endpoint}")

    # Headers
    headers = {"Content-Type": "application/json", "api-key": admin_key}

    # If skillset doesn't exist, create it
    logging.info(f"Starting skillset creation process...")
    logging.info(f"Initializing skillset configuration...")
    start_time = time.time()

    body = {
        "name": skillset_name,
        "description": "SKillset to do document chunking",
        "skills": [
            {
                "@odata.type": "#Microsoft.Skills.Custom.WebApiSkill",
                "name": "docint-processing",
                "description": "Process content with document intelligence markdown notation",
                "context": "/document",
                "uri": f"{function_endpoint}/api/document-chunking?code={function_key}",
                "httpMethod": "POST",
                "timeout": "PT3M50S",
                "batchSize": 1,
                "inputs": [
                    {
                        "name": "documentUrl",
                        "source": "/document/metadata_storage_path",
                        "inputs": [],
                    },
                    {
                        "name": "documentContent",
                        "source": "/document/content",
                        "inputs": [],
                    },
                    {
                        "name": "documentSasToken",
                        "source": "/document/metadata_storage_sas_token",
                        "inputs": [],
                    },
                    {
                        "name": "documentContentType",
                        "source": "/document/metadata_content_type",
                        "inputs": [],
                    },
                ],
                "outputs": [{"name": "chunks", "targetName": "chunks"}],
                "httpHeaders": {},
            }
        ],
        "cognitiveServices": {
            "@odata.type": "#Microsoft.Azure.Search.CognitiveServicesByKey",
            "key": cognitive_services_key,
        },
        "indexProjections": {
            "selectors": [
                {
                    "targetIndexName": f"{search_index_name}",
                    "parentKeyFieldName": "parent_id",
                    "sourceContext": "/document/chunks/*",
                    "mappings": [
                    {
                        "name": "text_vector",
                        "source": "/document/chunks/*/vector",
                        "inputs": []
                    },
                    {
                        "name": "chunk",
                        "source": "/document/chunks/*/content",
                        "inputs": []
                    },
                    {
                        "name": "title",
                        "source": "/document/chunks/*/title",
                        "inputs": []
                    },
                    {
                        "name": "url",
                        "source": "/document/chunks/*/url",
                        "inputs": []
                    },
                    {
                        "name": "file_name",
                        "source": "/document/chunks/*/title",
                        "inputs": []
                    },
                    {
                        "name": "document_id",
                        "source": "/document/document_id",
                        "inputs": [],
                    },
                    {
                        "name": "date_last_modified",
                        "source": "/document/metadata_storage_last_modified",
                            "inputs": [],
                        },
                    ],
                },
            ],
            "parameters": {"projectionMode": "skipIndexingParentDocuments"},
        },
    }

    response_time = time.time() - start_time
    logging.info(f"Skillset configuration prepared in {round(response_time,2)} seconds")

    # send a put request to create the skillset
    try:
        logging.info(f"Sending request to create skillset '{skillset_name}'...")
        response = requests.put(endpoint, headers=headers, json=body)

        if response.status_code == 200:
            logging.info(f"Skillset '{skillset_name}' created successfully!")
            logging.info(f"Creation time: {round(response_time,2)} seconds")
            logging.info(f"Skillset details: {response.json()}")
        elif response.status_code == 201:
            logging.info(
                f"Skillset '{skillset_name}' created successfully! (Status: 201)"
            )
            logging.info(f"Creation time: {round(response_time,2)} seconds")
            logging.info(f"Skillset details: {response.json()}")
        else:
            logging.error(f"Failed to create skillset '{skillset_name}'")
            logging.error(f"Status code: {response.status_code}")
            logging.error(f"Error response: {response.text}")
            logging.error("Please check your configuration and try again.")

    except requests.exceptions.ConnectionError:
        logging.error(
            f"Connection error while creating skillset. Please verify your network connection."
        )
        raise
    except requests.exceptions.Timeout:
        logging.error(f"Request timed out while creating skillset. Please try again.")
        raise
    except Exception as e:
        logging.error(f"Unexpected error while creating skillset: {str(e)}")
        raise

    return response

if __name__ == "__main__":
    search_index_name = "financial-index-test"
    function_endpoint = "https://document-chunking-az-func.azurewebsites.net"
    create_skillset(search_index_name, function_endpoint)
