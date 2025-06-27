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
# Create index
########################################################




def create_index_body(
    index_name: str,
    search_api_version: str = "2024-11-01-preview",
):
    """
    Creates or recreates an Azure AI search

    Args:
        index_name: str, the name of the index to create or recreate
    """

    logging.info(f"Starting index creation for '{index_name}'")

    # Azure cognitive search settings
    service_name = search_service_name
    api_version = search_api_version

    # Endpoint URL
    endpoint = f"https://{service_name}.search.windows.net/indexes/{index_name}?api-version={api_version}"
    logging.info(f"Endpoint URL: {endpoint}")

    # Headers
    headers = {
        "Content-Type": "application/json",
        "api-key": azure_search_admin_key,
    }

    # first check if the index exists
    try:
        logging.info(f"Checking if index '{index_name}' exists...")
        check_response = requests.get(endpoint, headers=headers)

        if check_response.status_code == 200:
            logging.info(f"Index '{index_name}' already exists. Deleting it...")
            delete_response = requests.delete(endpoint, headers=headers)

            if delete_response.status_code in [200, 204]:
                logging.info(f"Successfully deleted index '{index_name}'")
            else:
                logging.warning(
                    f"Unexpected response while deleting index '{index_name}': {delete_response.status_code}"
                )
        elif check_response.status_code == 404:
            logging.info(f"Index '{index_name}' does not exist. Creating it...")
        else:
            logging.warning(
                f"Unexpected response while checking index '{index_name}': {check_response.status_code}"
            )

    except requests.exceptions.ConnectionError:
        logging.warning(
            f"Connection error while checking index '{index_name}.' Please verify your network connection and try again."
        )
        raise

    except Exception as e:
        logging.error(f"Error checking or deleting index '{index_name}': {e}")
        raise e

    # Create index body
    logging.info(f"Creating index '{index_name}'...")

    start_time = time.time()

    body = {
        "name": index_name,
        "fields": [
            {
                "name": "id",
                "type": "Edm.String",
                "searchable": True,
                "filterable": True,
                "retrievable": True,
                "stored": True,
                "sortable": True,
                "facetable": True,
                "key": True,
                "analyzer": "keyword",
                "synonymMaps": [],
            },
            {
                "name": "parent_id",
                "type": "Edm.String",
                "searchable": False,
                "filterable": True,
                "retrievable": True,
                "stored": True,
                "sortable": True,
                "facetable": True,
                "key": False,
                "synonymMaps": [],
            },
            {
                "name": "metadata_storage_path",
                "type": "Edm.String",
                "searchable": False,
                "filterable": False,
                "retrievable": True,
                "stored": True,
                "sortable": False,
                "facetable": False,
                "key": False,
                "synonymMaps": [],
            },
            {
                "name": "metadata_storage_name",
                "type": "Edm.String",
                "searchable": False,
                "filterable": False,
                "retrievable": True,
                "stored": True,
                "sortable": False,
                "facetable": False,
                "key": False,
                "synonymMaps": [],
            },
            {
                "name": "chunk_id",
                "type": "Edm.Int32",
                "searchable": False,
                "filterable": True,
                "retrievable": True,
                "stored": True,
                "sortable": True,
                "facetable": True,
                "key": False,
                "synonymMaps": [],
            },
            {
                "name": "content",
                "type": "Edm.String",
                "searchable": True,
                "filterable": True,
                "retrievable": True,
                "stored": True,
                "sortable": True,
                "facetable": True,
                "key": False,
                "analyzer": "standard.lucene",
                "synonymMaps": [],
            },
            {
                "name": "page",
                "type": "Edm.Int32",
                "searchable": False,
                "filterable": True,
                "retrievable": True,
                "stored": True,
                "sortable": True,
                "facetable": True,
                "key": False,
                "synonymMaps": [],
            },
            {
                "name": "offset",
                "type": "Edm.Int64",
                "searchable": False,
                "filterable": False,
                "retrievable": True,
                "stored": True,
                "sortable": True,
                "facetable": True,
                "key": False,
                "synonymMaps": [],
            },
            {
                "name": "length",
                "type": "Edm.Int32",
                "searchable": False,
                "filterable": False,
                "retrievable": True,
                "stored": True,
                "sortable": True,
                "facetable": True,
                "key": False,
                "synonymMaps": [],
            },
            {
                "name": "title",
                "type": "Edm.String",
                "searchable": True,
                "filterable": True,
                "retrievable": True,
                "stored": True,
                "sortable": True,
                "facetable": True,
                "key": False,
                "analyzer": "standard.lucene",
                "synonymMaps": [],
            },
            {
                "name": "category",
                "type": "Edm.String",
                "searchable": True,
                "filterable": True,
                "retrievable": True,
                "stored": True,
                "sortable": True,
                "facetable": True,
                "key": False,
                "analyzer": "standard.lucene",
                "synonymMaps": [],
            },
            {
                "name": "filepath",
                "type": "Edm.String",
                "searchable": False,
                "filterable": False,
                "retrievable": True,
                "stored": True,
                "sortable": True,
                "facetable": True,
                "key": False,
                "synonymMaps": [],
            },
            {
                "name": "url",
                "type": "Edm.String",
                "searchable": False,
                "filterable": False,
                "retrievable": True,
                "stored": True,
                "sortable": True,
                "facetable": True,
                "key": False,
                "synonymMaps": [],
            },
            {
                "name": "vector",
                "type": "Collection(Edm.Single)",
                "searchable": True,
                "filterable": False,
                "retrievable": True,
                "stored": True,
                "sortable": False,
                "facetable": False,
                "key": False,
                "dimensions": 1536,
                "vectorSearchProfile": "myHnswProfile",
                "synonymMaps": [],
            },
            {
                "name": "keyPhrases",
                "type": "Collection(Edm.String)",
                "searchable": True,
                "filterable": False,
                "retrievable": True,
                "stored": True,
                "sortable": False,
                "facetable": False,
                "key": False,
                "analyzer": "standard.lucene",
                "synonymMaps": [],
            },
            {
                "name": "organization_id",
                "type": "Edm.String",
                "searchable": True,
                "filterable": True,
                "retrievable": True,
                "stored": True,
                "sortable": False,
                "facetable": False,
                "key": False,
                "analyzer": "standard.lucene",
                "synonymMaps": [],
            },
            {
                "name": "date_uploaded",
                "type": "Edm.DateTimeOffset",
                "searchable": False,
                "filterable": True,
                "retrievable": True,
                "stored": True,
                "sortable": True,
                "facetable": True,
                "key": False,
                "synonymMaps": [],
            },
            
                {
                "name": "date_last_modified",
                "type": "Edm.DateTimeOffset",
                "searchable": False,
                "filterable": True,
                "retrievable": True,
                "stored": True,
                "sortable": True,
                "facetable": True,
                "key": False,
                "synonymMaps": [],
            },
        ],
        "scoringProfiles": [
            {
                "name": f"{index_name}-scoring-profile",
                "functionAggregation": "sum",
                "text": {"weights": {"content": 4, "keyPhrases": 5, "title": 7}},
                "functions": [
                    {
                        "fieldName": "date_last_modified",
                        "interpolation": "linear",
                        "type": "freshness",
                        "boost": 10,
                        "freshness": {
                            "boostingDuration": "P183D",
                        },
                    },
                ],
            }
        ],
        "defaultScoringProfile": f"{index_name}-scoring-profile",
        "corsOptions": {"allowedOrigins": ["*"], "maxAgeInSeconds": 60},
        "suggesters": [],
        "analyzers": [],
        "normalizers": [],
        "tokenizers": [],
        "tokenFilters": [],
        "charFilters": [],
        "similarity": {"@odata.type": "#Microsoft.Azure.Search.BM25Similarity"},
        "semantic": {
            "configurations": [
                {
                    "name": "my-semantic-config",
                    "prioritizedFields": {
                        "prioritizedContentFields": [{"fieldName": "content"}],
                        "prioritizedKeywordsFields": [{"fieldName": "category"}],
                        "titleField": {"fieldName": "title"},
                    },
                }
            ]
        },
        "vectorSearch": {
            "algorithms": [
                {
                    "name": "myHnswConfig",
                    "kind": "hnsw",
                    "hnswParameters": {
                        "metric": "cosine",
                        "m": 4,
                        "efConstruction": 400,
                        "efSearch": 500,
                    },
                }
            ],
            "profiles": [
                {
                    "name": "myHnswProfile",
                    "algorithm": "myHnswConfig",
                    "vectorizer": "vector-ce-vectorizer",
                }
            ],
            "vectorizers": [
                {
                    "name": "vector-ce-vectorizer",
                    "kind": "azureOpenAI",
                    "azureOpenAIParameters": {
                        "resourceUri": os.getenv("AZURE_OPENAI_ENDPOINT"),
                        "deploymentId": "text-embedding-3-small",
                        "apiKey": os.getenv("AZURE_OPENAI_API_KEY"),
                        "modelName": "text-embedding-3-small",
                    },
                }
            ],
            "compressions": [],
        },
    }
    response_time = time.time() - start_time
    logging.info(f"Index configuration prepared in {round(response_time,2)} seconds")

    # Create index
    try:
        logging.info(f"Creating index '{index_name}'...")
        response = requests.put(endpoint, headers=headers, json=body)

        if response.status_code in [200, 201]:
            logging.info(f"Index '{index_name}' created successfully")
            logging.info(f"Creation time: {round(time.time() - start_time, 2)} seconds")
        else:
            logging.error(f"Failed to create index '{index_name}'")
            logging.error(f"Status code: {response.status_code}")
            logging.error(f"Response: {response.text}")
            logging.error("Please check your configuration and try again.")
            raise Exception(f"Failed to create index '{index_name}'")

    except requests.exceptions.ConnectionError:
        logging.error(
            f"Connection error while creating index. Please verify your network connection and try again."
        )
        raise
    except requests.exceptions.Timeout:
        logging.error(f"Timeout error while creating index. Please try again.")
        raise
    except Exception as e:
        logging.error(f"Error creating index '{index_name}': {e}")
        raise e

    return response


if __name__ == "__main__":
    index_name = "ragindex-test"
    create_index_body(index_name)
