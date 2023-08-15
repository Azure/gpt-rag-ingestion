import logging
import os
import time
import requests
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

FUNCTION_APP_NAME = os.environ.get("FUNCTION_APP_NAME")
FUNCTION_ENDPOINT = f"https://{FUNCTION_APP_NAME}.azurewebsites.net"
SEARCH_SERVICE = os.environ.get("SEARCH_SERVICE")
SEARCH_ANALYZER_NAME=os.environ.get('SEARCH_ANALYZER_NAME')
SEARCH_API_VERSION = os.environ.get("SEARCH_API_VERSION")
SEARCH_INDEX_INTERVAL= os.environ.get("SEARCH_INDEX_INTERVAL")
SEARCH_INDEX_NAME = os.environ.get("SEARCH_INDEX_NAME")
STORAGE_CONTAINER = os.environ.get("STORAGE_CONTAINER")
STORAGE_CONTAINER_CHUNKS = os.environ.get("STORAGE_CONTAINER_CHUNKS")

def get_secret(secretName):
    keyVaultName = os.environ["AZURE_KEY_VAULT_NAME"]
    KVUri = f"https://{keyVaultName}.vault.azure.net"
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    logging.info(f"Retrieving {secretName} secret from {keyVaultName}.")   
    retrieved_secret = client.get_secret(secretName)
    return retrieved_secret.value

FUNCTION_KEY = get_secret('ingestionKey')
STORAGE_CONNECTION_STRING = get_secret('storageConnectionString')

def call_search_api(resource_type, resource_name, method, body=None):
    azureSearchKey = get_secret('azureSearchKey')
    headers = {
        'Content-Type': 'application/json',
        'api-key': azureSearchKey
    }
    search_endpoint = f"https://{SEARCH_SERVICE}.search.windows.net/{resource_type}/{resource_name}?api-version={SEARCH_API_VERSION}"
    response = None
    try:
        if method not in ["get", "put"]:
            logging.error(f"Invalid method {method} ")
        if method == "get":
            response = requests.get(search_endpoint, headers=headers)
        elif method == "put":
            response = requests.put(search_endpoint, headers=headers, json=body)
        if response is not None:
            status_code = response.status_code
            if status_code >= 400:
                logging.error(f"Error when calling search API {method} {resource_type} {resource_name}. Code: {status_code}. Reason: {response.reason}")
            else:
                logging.info(f"Successfully called search API {method} {resource_type} {resource_name}. Code: {status_code}.")                
    except Exception as e:
        error_message = str(e)
        logging.error(f"Error when calling search API {method} {resource_type} {resource_name}. Error: {error_message}")
    return response


def execute_setup():
    logging.info("Setting up search.")

    ###########################################################################
    # 00 Creating blob containers (if needed)
    ###########################################################################
    logging.info("01 Creating containers step.")    
    start_time = time.time()
    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
    # Create documents container
    container_client = blob_service_client.get_container_client(STORAGE_CONTAINER)
    if not container_client.exists():
        # Create the container
        container_client.create_container()
        logging.info(f"Container '{STORAGE_CONTAINER}' created successfully.")
    else:
        logging.info(f"Container '{STORAGE_CONTAINER}' already exists.")
    # Create chunks container
    container_client = blob_service_client.get_container_client(STORAGE_CONTAINER_CHUNKS)
    if not container_client.exists():
        # Create the container
        container_client.create_container()
        logging.info(f"Container '{STORAGE_CONTAINER_CHUNKS}' created successfully.")
    else:
        logging.info(f"Container '{STORAGE_CONTAINER_CHUNKS}' already exists.")        
    response_time = time.time() - start_time
    logging.info(f"01 Create containers step. {round(response_time,2)} seconds")

    ###########################################################################
    # 01 Creating datasources
    ###########################################################################    
    logging.info("02 Creating datastores step.")
    start_time = time.time()

    body = {
        "description": "Input documents",
        "type": "azureblob",
        "credentials": {
            "connectionString": STORAGE_CONNECTION_STRING
        },
        "container": {
            "name": STORAGE_CONTAINER
        }
    }
    call_search_api("datasources", f"{SEARCH_INDEX_NAME}-datasource", "put", body)
    
    body = {
        "description": "Document chunks",
        "type": "azureblob",
        "credentials": {
            "connectionString": STORAGE_CONNECTION_STRING
        },
        "container": {
            "name": f"{STORAGE_CONTAINER_CHUNKS}"
        }   
    }
    call_search_api("datasources", f"{SEARCH_INDEX_NAME}-datasource-chunks", "put", body)

    response_time = time.time() - start_time
    logging.info(f"02 Create datastores step. {round(response_time,2)} seconds")

    ###########################################################################
    # 02 Creating skillset
    ###########################################################################
    logging.info("02 Creating skillsets step.")
    start_time = time.time()

    body = { 
        "name": f"{SEARCH_INDEX_NAME}-skillset-chunking",
        "description":"SKillset to do document chunking",
        "skills":[ 
            { 
                "@odata.type":"#Microsoft.Skills.Custom.WebApiSkill",
                "name":"document-chunking",
                "description":"Extract chunks from documents.",
                "uri":f"{FUNCTION_ENDPOINT}/api/document-chunking?code={FUNCTION_KEY}",
                "httpMethod":"POST",
                "timeout":"PT230S",
                "context":"/document",
                "batchSize":1,
                "inputs":[ 
                    { 
                        "name":"documentContent",
                        "source":"/document/content"
                    },
                    {
                        "name":"documentUrl",
                        "source":"/document/metadata_storage_path"
                    },
                    {
                        "name":"documentUrlencoded",
                        "source":"/document/metadata_storage_path_encoded"
                    },
                    { 
                        "name":"documentSasToken",
                        "source":"/document/metadata_storage_sas_token"
                    },
                    { 
                        "name":"documentContentType",
                        "source":"/document/metadata_content_type"
                    }
                ],
                "outputs":[ 
                    {
                        "name":"chunks",
                        "targetName":"chunks"
                    }
                ]
            }
        ],
        "knowledgeStore" : {
            "storageConnectionString": STORAGE_CONNECTION_STRING,
            "projections": [
                {
                    "tables": [],
                    "objects": [
                        {
                                "storageContainer": f"{STORAGE_CONTAINER_CHUNKS}",
                                "generatedKeyName": "chunk_id",
                                "source": "/document/chunks/*"
                        }
                    ],
                    "files": []
                }
            ]
        }
    }
    call_search_api("skillsets", f"{SEARCH_INDEX_NAME}-skillset-chunking", "put", body)
    response_time = time.time() - start_time
    logging.info(f"02 Create skillset step. {round(response_time,2)} seconds")

    ###########################################################################
    # 03 Creating indexes
    ###########################################################################
    logging.info(f"03 Creating indexes step.")
    start_time = time.time()

    body = {
        "name": f"{SEARCH_INDEX_NAME}-source-documents",
        "fields": [
            {
                "name": "metadata_storage_path_encoded",
                "type": "Edm.String",
                "searchable": False,
                "sortable": False,
                "key": True,                               
                "filterable": False,
                "facetable": False
            },            {
                "name": "metadata_storage_path",
                "type": "Edm.String",
                "searchable": False,
                "sortable": False,     
                "filterable": False,
                "facetable": False
            },
            {
                "name": "metadata_storage_name",
                "type": "Edm.String",
                "searchable": False,
                "sortable": False,
                "filterable": False,
                "facetable": False
            },
            {
                "name": "chunks",
                "type": "Collection(Edm.ComplexType)",
                "fields": [
                    {
                        "name": "content",
                        "type": "Edm.String",
                        "searchable": True,
                        "retrievable": True
                    },
                    {
                        "name": "category",
                        "type": "Edm.String",
                        "filterable": False,
                        "searchable": False,
                        "retrievable": True
                    },
                    {
                        "name": "contentVector",
                        "type": "Collection(Edm.Double)",
                        "searchable": False,
                        "searchable": False,
                        "retrievable": True
                    }
                ]
            } 
        ],
        "corsOptions": {
            "allowedOrigins": [
                "*"
            ],
            "maxAgeInSeconds": 60
        }
    }
    response = call_search_api("indexes", f"{SEARCH_INDEX_NAME}-source-documents", "put", body)

    body = {
        "name":  f"{SEARCH_INDEX_NAME}",
        "fields": [
            {
                "name": "metadata_storage_path_encoded",
                "type": "Edm.String",
                "searchable": False,
                "sortable": False,
                "key": True,                               
                "filterable": False,
                "facetable": False
            },
            {
                "name": "metadata_storage_path",
                "type": "Edm.String",
                "searchable": False,
                "sortable": False,     
                "filterable": False,
                "facetable": False
            },
            {
                "name": "metadata_storage_name",
                "type": "Edm.String",
                "searchable": False,
                "sortable": False,
                "filterable": False,
                "facetable": False
            },            
            {
                "name": "chunk_id",
                "type": "Edm.Int32",
                "searchable": False,
                "retrievable": True
            },
            {
                "name": "content",
                "type": "Edm.String",
                "searchable": True,
                "retrievable": True,
                "analyzer": SEARCH_ANALYZER_NAME
            },
            {
                "name": "offset",
                "type": "Edm.Int64",
                "filterable": False,
                "searchable": False,
                "retrievable": True
            },
            {
                "name": "length",
                "type": "Edm.Int32",
                "filterable": False,
                "searchable": False,
                "retrievable": True
            },
            {
                "name": "title",
                "type": "Edm.String",
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "analyzer": SEARCH_ANALYZER_NAME
            },
            {
                "name": "category",
                "type": "Edm.String",
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "analyzer": SEARCH_ANALYZER_NAME
            },
            {
                "name": "filepath",
                "type": "Edm.String",
                "filterable": False,
                "searchable": False,
                "retrievable": True
            },
            {
                "name": "url",
                "type": "Edm.String",
                "filterable": False,
                "searchable": False,
                "retrievable": True
            },
            {
                "name": "contentVector",
                "type": "Collection(Edm.Single)",
                "searchable": True,
                "retrievable": True,
                "dimensions": 1536,
                "vectorSearchConfiguration": "my-vector-config"
            } 
        ],
        "corsOptions": {
            "allowedOrigins": [
                "*"
            ],
            "maxAgeInSeconds": 60
        },
        "vectorSearch": {
            "algorithmConfigurations": [
                {
                    "name": "my-vector-config",
                    "kind": "hnsw",
                    "hnswParameters": {
                        "m": 4,
                        "efConstruction": 400,
                        "efSearch": 500,
                        "metric": "cosine"
                    }
                }
            ]
        },
        "semantic": {
            "configurations": [
                {
                    "name": "my-semantic-config",
                    "prioritizedFields": {
                        "prioritizedContentFields": [
                            {
                                "fieldName": "content"
                            }
                        ],
                        "prioritizedKeywordsFields": [
                            {
                                "fieldName": "category"
                            }
                        ]
                    }
                }
            ]
        }
    }
    response = call_search_api("indexes", f"{SEARCH_INDEX_NAME}", "put", body)

    response_time = time.time() - start_time
    logging.info(f"03 Create indexes step. {round(response_time,2)} seconds")

    ###########################################################################
    # 04 Creating indexers
    ###########################################################################
    logging.info("04 Creating indexer step.")
    start_time = time.time()
    body = {
        "dataSourceName" : f"{SEARCH_INDEX_NAME}-datasource",
        "targetIndexName" : f"{SEARCH_INDEX_NAME}-source-documents",
        "skillsetName" : f"{SEARCH_INDEX_NAME}-skillset-chunking",
        "schedule" : { "interval" : f"{SEARCH_INDEX_INTERVAL}"},
        "fieldMappings" : [
            {
            "sourceFieldName" : "metadata_storage_path",
            "targetFieldName" : "metadata_storage_path_encoded",
            "mappingFunction" : { "name" : "base64Encode" }
            }
        ],
        "outputFieldMappings" : [
        ],
        "parameters":
        {
            "batchSize": 1,
            "maxFailedItems":-1,
            "maxFailedItemsPerBatch":-1,
            "configuration": 
            {
                "dataToExtract": "contentAndMetadata"    
            }
        }
        }    
    call_search_api("indexers", f"{SEARCH_INDEX_NAME}-indexer-chunk-documents", "put", body)

    body = {
        "dataSourceName" : f"{SEARCH_INDEX_NAME}-datasource-chunks",
        "targetIndexName" : f"{SEARCH_INDEX_NAME}",
        "schedule" : { "interval" : f"{SEARCH_INDEX_INTERVAL}"},
        "fieldMappings" : [
            {
            "sourceFieldName" : "metadata_storage_path",
            "targetFieldName" : "metadata_storage_path_encoded",
            "mappingFunction" : { "name" : "base64Encode" }
            }
        ],        
        "parameters":
        {
            "batchSize": 1,
            "maxFailedItems":-1,
            "maxFailedItemsPerBatch":-1,
            "configuration": 
            {
                "dataToExtract": "contentAndMetadata",
                "parsingMode": "json"
            }
        }
        }
    call_search_api("indexers", f"{SEARCH_INDEX_NAME}-indexer-chunks", "put", body)

    response_time = time.time() - start_time
    logging.info(f"04 Create indexers step. {round(response_time,2)} seconds")

    logging.info(f"DONE")