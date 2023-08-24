import logging
import time
import requests
import argparse
from tenacity import retry, wait_fixed, stop_after_delay
import azure.core.exceptions
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.mgmt.web import WebSiteManagementClient
from azure.mgmt.storage import StorageManagementClient
logging.getLogger('azure').setLevel(logging.WARNING)

def call_search_api(search_service, search_api_version, resource_type, resource_name, method, credential, body=None):
    # get the token
    token = credential.get_token("https://search.azure.com/.default").token

    headers = {
        "Authorization": f"Bearer {token}",
        'Content-Type': 'application/json'
        # 'api-key': SEARCH_API_KEY
    }
    search_endpoint = f"https://{search_service}.search.windows.net/{resource_type}/{resource_name}?api-version={search_api_version}"
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

@retry(stop=stop_after_delay(20*60), wait=wait_fixed(60), before_sleep=lambda _: logging.info('Will attempt again in a minute as the function may not yet be available for use...'))
def get_function_key(subscription_id, resource_group, function_app_name):
    credential = DefaultAzureCredential(logging_enable=True)
    web_mgmt_client = WebSiteManagementClient(credential, subscription_id, logging_enable=True)    
    keys = web_mgmt_client.web_apps.list_function_keys(resource_group, function_app_name, 'document_chunking')
    function_key = keys.additional_properties["default"]
    return function_key

def execute_setup(subscription_id, resource_group, function_app_name):
    
    logging.info(f"Getting function app {function_app_name} properties.") 
    credential = DefaultAzureCredential()
    web_mgmt_client = WebSiteManagementClient(credential, subscription_id)
    function_app_settings = web_mgmt_client.web_apps.list_application_settings(resource_group, function_app_name)
    function_endpoint = f"https://{function_app_name}.azurewebsites.net"
    search_service = function_app_settings.properties["SEARCH_SERVICE"]
    search_analyzer_name= function_app_settings.properties["SEARCH_ANALYZER_NAME"]
    search_api_version = function_app_settings.properties["SEARCH_API_VERSION"]
    search_index_interval = function_app_settings.properties["SEARCH_INDEX_INTERVAL"]
    search_index_name = function_app_settings.properties["SEARCH_INDEX_NAME"]
    storage_container = function_app_settings.properties["STORAGE_CONTAINER"]
    storage_container_chunks = function_app_settings.properties["STORAGE_CONTAINER_CHUNKS"]
    storage_account_name = function_app_settings.properties["STORAGE_ACCOUNT_NAME"]

    # create a code to print all variables above
    logging.info(f"Function endpoint: {function_endpoint}")
    logging.info(f"Search service: {search_service}")
    logging.info(f"Search analyzer name: {search_analyzer_name}")
    logging.info(f"Search API version: {search_api_version}")
    logging.info(f"Search index interval: {search_index_interval}")
    logging.info(f"Search index name: {search_index_name}")
    logging.info(f"Storage container: {storage_container}")
    logging.info(f"Storage container chunks: {storage_container_chunks}")
    logging.info(f"Storage account name: {storage_account_name}")

    logging.info(f"Getting function app {function_app_name} key.")
    function_key = get_function_key(subscription_id, resource_group, function_app_name)

    logging.info(f"Getting {function_app_name} storage connection string.")
    storage_client = StorageManagementClient(credential, subscription_id)
    keys = storage_client.storage_accounts.list_keys(resource_group, storage_account_name)
    storage_connection_string = f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName={storage_account_name};AccountKey={keys.keys[0].value}"
    # logging.info(f"Storage account connection string: {storage_connection_string}")

    ###########################################################################
    # 00 Creating blob containers (if needed)
    ###########################################################################
    logging.info("01 Creating containers step.")    
    start_time = time.time()
    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)
    # Create documents container
    container_client = blob_service_client.get_container_client(storage_container)
    try:
        if not container_client.exists():
            # Create the container
            container_client.create_container()
            logging.info(f"Container '{storage_container}' created successfully.")
        else:
            logging.info(f"Container '{storage_container}' already exists.")
    except azure.core.exceptions.ClientAuthenticationError as e:
        error_message = str(e)
        logging.error(f"Error connecting with storage account, you may need to restart the computer. Error: {error_message}")
        exit()

    # Create chunks container
    container_client = blob_service_client.get_container_client(storage_container_chunks)
    if not container_client.exists():
        # Create the container
        container_client.create_container()
        logging.info(f"Container '{storage_container_chunks}' created successfully.")
    else:
        logging.info(f"Container '{storage_container_chunks}' already exists.")        
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
            "connectionString": storage_connection_string
        },
        "container": {
            "name": storage_container
        }
    }
    call_search_api(search_service, search_api_version, "datasources", f"{search_index_name}-datasource", "put", credential, body)
    
    body = {
        "description": "Document chunks",
        "type": "azureblob",
        "credentials": {
            "connectionString": storage_connection_string
        },
        "container": {
            "name": f"{storage_container_chunks}"
        }   
    }
    call_search_api(search_service, search_api_version, "datasources", f"{search_index_name}-datasource-chunks", "put", credential, body)

    response_time = time.time() - start_time
    logging.info(f"02 Create datastores step. {round(response_time,2)} seconds")

    ###########################################################################
    # 02 Creating skillset
    ###########################################################################
    logging.info("02 Creating skillsets step.")
    start_time = time.time()

    body = { 
        "name": f"{search_index_name}-skillset-chunking",
        "description":"SKillset to do document chunking",
        "skills":[ 
            { 
                "@odata.type":"#Microsoft.Skills.Custom.WebApiSkill",
                "name":"document-chunking",
                "description":"Extract chunks from documents.",
                "uri":f"{function_endpoint}/api/document-chunking?code={function_key}",
                "httpMethod":"POST",
                "timeout":"PT230S",
                "context":"/document",
                "batchSize":1,
                "inputs":[ 
                    {
                        "name":"documentUrl",
                        "source":"/document/metadata_storage_path"
                    },
                    {
                        "name":"documentContent",
                        "source":"/document/content"
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
            "storageConnectionString": storage_connection_string,
            "projections": [
                {
                    "tables": [],
                    "objects": [
                        {
                                "storageContainer": f"{storage_container_chunks}",
                                "generatedKeyName": "chunk_id",
                                "source": "/document/chunks/*"
                        }
                    ],
                    "files": []
                }
            ]
        }
    }
    call_search_api(search_service, search_api_version, "skillsets", f"{search_index_name}-skillset-chunking", "put", credential,body)
    response_time = time.time() - start_time
    logging.info(f"02 Create skillset step. {round(response_time,2)} seconds")

    ###########################################################################
    # 03 Creating indexes
    ###########################################################################
    logging.info(f"03 Creating indexes step.")
    start_time = time.time()

    body = {
        "name": f"{search_index_name}-source-documents",
        "fields": [
            {
                "name": "id",
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
                "name": "chunks",
                "type": "Collection(Edm.ComplexType)",
                "fields": [
                    {
                        "name": "content",
                        "type": "Edm.String",
                        "searchable": False,
                        "retrievable": True
                    },
                    {
                        "name": "category",
                        "type": "Edm.String",
                        "searchable": False,
                        "retrievable": True
                    },
                    {
                        "name": "filepath",
                        "type": "Edm.String",
                        "searchable": False,
                        "retrievable": True
                    },
                    {
                        "name": "chunk_id",
                        "type": "Edm.Int32",
                        "searchable": False,
                        "retrievable": True
                    },
                    {
                        "name": "page",
                        "type": "Edm.Int32",
                        "searchable": False,
                        "retrievable": True
                    },
                    {
                        "name": "offset",
                        "type": "Edm.Int32",
                        "searchable": False,
                        "retrievable": True
                    },
                    {
                        "name": "length",
                        "type": "Edm.Int32",
                        "searchable": False,
                        "retrievable": True
                    },
                    {
                        "name": "title",
                        "type": "Edm.String",
                        "searchable": False,
                        "retrievable": True
                    },
                    {
                        "name": "url",
                        "type": "Edm.String",
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
    response = call_search_api(search_service, search_api_version, "indexes", f"{search_index_name}-source-documents", "put", credential, body)

    body = {
        "name":  f"{search_index_name}",
        "fields": [
            {
                "name": "id",
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
                "analyzer": search_analyzer_name
            },
            {
                "name": "page",
                "type": "Edm.Int32",
                "searchable": False,
                "retrievable": True
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
                "analyzer": search_analyzer_name
            },
            {
                "name": "category",
                "type": "Edm.String",
                "filterable": True,
                "searchable": True,
                "retrievable": True,
                "analyzer": search_analyzer_name
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
    response = call_search_api(search_service, search_api_version, "indexes", f"{search_index_name}", "put", credential, body)

    response_time = time.time() - start_time
    logging.info(f"03 Create indexes step. {round(response_time,2)} seconds")

    ###########################################################################
    # 04 Creating indexers
    ###########################################################################
    logging.info("04 Creating indexer step.")
    start_time = time.time()
    body = {
        "dataSourceName" : f"{search_index_name}-datasource",
        "targetIndexName" : f"{search_index_name}-source-documents",
        "skillsetName" : f"{search_index_name}-skillset-chunking",
        "schedule" : { "interval" : f"{search_index_interval}"},
        "fieldMappings" : [
            {
            "sourceFieldName" : "metadata_storage_path",
            "targetFieldName" : "id"
            }
        ],
        "outputFieldMappings" : [
        ],
        "parameters":
        {
            "batchSize": 1,
            "maxFailedItems":-1,
            "maxFailedItemsPerBatch":-1,
            "base64EncodeKeys": True,
            "configuration": 
            {
                "dataToExtract": "contentAndMetadata"
            }
        }
        }    
    call_search_api(search_service, search_api_version, "indexers", f"{search_index_name}-indexer-chunk-documents", "put", credential, body)

    body = {
        "dataSourceName" : f"{search_index_name}-datasource-chunks",
        "targetIndexName" : f"{search_index_name}",
        "schedule" : { "interval" : f"{search_index_interval}"},
        "fieldMappings" : [
            {
            "sourceFieldName" : "metadata_storage_path",
            "targetFieldName" : "id"
            }
        ],        
        "parameters":
        {
            "batchSize": 1,
            "maxFailedItems":-1,
            "maxFailedItemsPerBatch":-1,
            "base64EncodeKeys": True,            
            "configuration": 
            {
                "dataToExtract": "contentAndMetadata",
                "parsingMode": "json"
            }
        }
        }
    call_search_api(search_service, search_api_version, "indexers", f"{search_index_name}-indexer-chunks", "put", credential, body)

    response_time = time.time() - start_time
    logging.info(f"04 Create indexers step. {round(response_time,2)} seconds")



def main(subscription_id=None, resource_group=None, function_app_name=None):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f"Starting setup.")

    if subscription_id is None:
        subscription_id = input("Enter subscription ID: ")
    if resource_group is None:
        resource_group = input("Enter resource group: ")
    if function_app_name is None:
        function_app_name = input("Enter chunking function app name: ")

    start_time = time.time()

    execute_setup(subscription_id, resource_group, function_app_name)

    response_time = time.time() - start_time
    logging.info(f"Finished setup. {round(response_time,2)} seconds")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to do the data ingestion setup for Azure Cognitive Search.')
    parser.add_argument('-s', '--subscription_id', help='Subscription ID')
    parser.add_argument('-r', '--resource_group', help='Resource group')
    parser.add_argument('-f', '--function_app_name', help='Chunking function app name')
    args = parser.parse_args()

    main(subscription_id=args.subscription_id, resource_group=args.resource_group, function_app_name=args.function_app_name)    