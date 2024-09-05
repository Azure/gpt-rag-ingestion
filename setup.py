import logging
import time
import requests
import argparse
import json
import azure.core.exceptions
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.mgmt.web import WebSiteManagementClient
from azure.mgmt.storage import StorageManagementClient

logging.getLogger('azure').setLevel(logging.WARNING)


def call_search_api(search_service, search_api_version, resource_type, resource_name, method, credential, body=None):
    """
    Calls the Azure Search API with the specified parameters.

    Args:
        search_service (str): The name of the Azure Search service.
        search_api_version (str): The version of the Azure Search API to use.
        resource_type (str): The type of resource to access (e.g. "indexes", "docs").
        resource_name (str): The name of the resource to access.
        method (str): The HTTP method to use (either "get" or "put").
        credential (TokenCredential): An instance of a TokenCredential class that can provide an access token.
        body (dict, optional): The JSON payload to include in the request body (for "put" requests).

    Returns:
        None

    Raises:
        ValueError: If the specified HTTP method is not "get" or "put".
        HTTPError: If the response status code is 400 or greater.

    """    
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
        if method not in ["get", "put", "delete"]:
            logging.warn(f"Invalid method {method} ")

        # get and put processing
        if method == "get":
            response = requests.get(search_endpoint, headers=headers)
        elif method == "put":
            response = requests.put(search_endpoint, headers=headers, json=body)

        if response is not None:
            status_code = response.status_code
            if status_code >= 400:
                logging.error(f"Error when calling search API {method} {resource_type} {resource_name}. Code: {status_code}")
                logging.error(f"Error when calling search API Reason: {response.reason}")
                response_text_dict = json.loads(response.text)
                logging.error(f"Error when calling search API {method} {resource_type} {resource_name}. Message: {response_text_dict['error']['message']}")                
            else:
                logging.info(f"Successfully called search API {method} {resource_type} {resource_name}. Code: {status_code}.")

        # delete processing
        if method == "delete":
            response = requests.delete(search_endpoint, headers=headers)
            status_code = response.status_code
            logging.info(f"Successfully called search API {method} {resource_type} {resource_name}. Code: {status_code}.")

    except Exception as e:
        error_message = str(e)
        logging.error(f"Error when calling search API {method} {resource_type} {resource_name}. Error: {error_message}")


def get_function_key(subscription_id, resource_group, function_app_name, credential):
    """
    Returns an API key for the given function.

    Parameters:
    subscription_id (str): The subscription ID.
    resource_group (str): The resource group name.
    function_app_name (str): The name of the function app.
    credential (str): The credential to use.

    Returns:
    str: A unique key for the function.
    """    
    logging.info(f"Obtaining function key after creating or updating its value.")
    accessToken = f"Bearer {credential.get_token('https://management.azure.com/.default').token}"
    # Get key
    requestUrl = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Web/sites/{function_app_name}/functions/document_chunking/keys/mykey?api-version=2022-03-01"
    requestHeaders = {
        "Authorization": accessToken,
        "Content-Type": "application/json"
    }
    data = {
        'properties': {}
    }
    response = requests.put(requestUrl, headers=requestHeaders, data=json.dumps(data))
    response_json = json.loads(response.content.decode('utf-8'))
    try:
        function_key = response_json['properties']['value']
    except Exception as e:
        function_key = None
        logging.error(f"Error when getting function key. Details: {str(e)}.")        
    return function_key

def approve_private_link_connections(accessToken, subscription_id, resource_group, service_name, service_type, api_version):
    """
    Approves private link service connections for a given service.

    Args:
        accessToken (str): The access token used for authorization.
        subscription_id (str): The subscription ID.
        resource_group (str): The resource group name.
        service_name (str): The name of the service.
        service_type (str): The type of the service.
        api_version (str): The API version.

    Returns:
        None: This function does not return anything.
    """
    requestUrl = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/{service_type}/{service_name}/privateEndpointConnections?api-version={api_version}"
    requestHeaders = {
        "Authorization": accessToken,
        "Content-Type": "application/json"
    }
    response = requests.get(requestUrl, headers=requestHeaders)
    responseJson = json.loads(response.content)
    for connection in responseJson["value"]:
        status = connection['properties']['privateLinkServiceConnectionState']['status']
        logging.info(f"Checking connection {connection['name']}. Status {status}.")
        if status == "Pending":
            requestUrl = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/{service_type}/{service_name}/privateEndpointConnections/{connection['name']}?api-version={api_version}"
            requestBody = {
                "properties": {
                    "privateLinkServiceConnectionState": {
                        "status": "Approved",
                        "description": "Approved by setup script"
                    }
                }
            }
            requestBodyJson = json.dumps(requestBody)
            requestHeaders = {
                "Authorization": accessToken,
                "Content-Type": "application/json"
            }
            response = requests.put(requestUrl, data=requestBodyJson, headers=requestHeaders)
            logging.info(f"Approving private link service connection {connection['name']}. Code {response.status_code}. Message: {response.reason}.")

def approve_search_shared_private_access(subscription_id, resource_group, function_app_name, storage_account_name, credential):
    """
    Approves Shared Private Access requests made by AI Search to get private endpoints to access storage account and the ingestion function.

    Args:
        subscription_id (str): The subscription ID.
        resource_group (str): The resource group name.
        function_app_name (str): The name of the function app.
        storage_account_name (str): The name of the storage account.
        credential (DefaultAzureCredential): The credential object used to authenticate with Azure.

    Returns:
        None: This function does not return anything.
    """    
    try: 
        logging.info(f"Approving Search private link service connection if needed.")
        # Replace with your access token
        logging.info(f"Approving Search private link service connection if needed.")
        accessToken = f"Bearer {credential.get_token('https://management.azure.com/.default').token}"

        # First the storage private link connections
        approve_private_link_connections(accessToken, subscription_id, resource_group, storage_account_name, 'Microsoft.Storage/storageAccounts', '2023-01-01')
        
        # Second the function app private link connections
        approve_private_link_connections(accessToken, subscription_id, resource_group, function_app_name, 'Microsoft.Web/sites', '2023-01-01')

    except Exception as e:
        error_message = str(e)
        logging.error(f"Error when approving private link service connection. Please do it manually. Error: {error_message}")


def execute_setup(subscription_id, resource_group, function_app_name, search_principal_id, azure_search_use_mis, enable_managed_identities, enable_env_credentials):
    """
    This function performs the necessary steps to set up the ingestion sub components, such as creating the required datastores and indexers.
    
    Args:
        subscription_id (str): The subscription ID of the Azure subscription to use.
        resource_group (str): The name of the resource group containing the solution resources.
        function_app_name (str): The name of the function app to use.
        search_principal_id (str): The principal ID of the search managed identity.
        azure_search_use_mis (bool): Whether to use Search Service Managed Identity to Connect to data ingestion function
        enable_managed_identities (bool, optional): Whether to use VM's managed identities to run the setup, defaults to False.
        enable_env_credentials (bool): Whether to use environment credentials to run the setup.

    Returns:
        None
    """    
    logging.info(f"Getting function app {function_app_name} properties.") 
    credential = DefaultAzureCredential(logging_enable=True, exclude_managed_identity_credential=not enable_managed_identities, exclude_environment_credential=not enable_env_credentials)
    web_mgmt_client = WebSiteManagementClient(credential, subscription_id)
    function_app_settings = web_mgmt_client.web_apps.list_application_settings(resource_group, function_app_name)
    function_endpoint = f"https://{function_app_name}.azurewebsites.net"
    search_service = function_app_settings.properties["SEARCH_SERVICE"]
    search_analyzer_name= function_app_settings.properties["SEARCH_ANALYZER_NAME"]
    search_api_version = function_app_settings.properties.get("SEARCH_API_VERSION", "2023-10-01-Preview")
    search_api_version = '2023-10-01-Preview' # enforced, to support indexProjections and also if the version is lower than 2023-10-01-Preview it wont work with MIS authResourceId parameter.   
    search_index_interval = function_app_settings.properties["SEARCH_INDEX_INTERVAL"]
    search_index_name = function_app_settings.properties["SEARCH_INDEX_NAME"]
    storage_container = function_app_settings.properties["STORAGE_CONTAINER"]
    storage_account_name = function_app_settings.properties["STORAGE_ACCOUNT_NAME"]
    network_isolation = True if function_app_settings.properties["NETWORK_ISOLATION"].lower() == "true" else False

    logging.info(f"Function endpoint: {function_endpoint}")
    logging.info(f"Search service: {search_service}")
    logging.info(f"Search analyzer name: {search_analyzer_name}")
    logging.info(f"Search API version: {search_api_version}")
    logging.info(f"Search index interval: {search_index_interval}")
    logging.info(f"Search index name: {search_index_name}")
    logging.info(f"Storage container: {storage_container}")
    logging.info(f"Storage account name: {storage_account_name}")

    
    ###########################################################################
    # Get function key to be used later when creating the skillset
    ########################################################################### 
    function_key = get_function_key(subscription_id, resource_group, function_app_name, credential)
    if function_key is None:
            logging.error(f"Could not get function key. Please make sure the function {function_app_name}/document_chunking is deployed before running this script.")
            exit(1) 

    ###########################################################################
    # Approve Search Shared Private Links (if needed)
    ########################################################################### 
    logging.info("00 Approving search shared private links.")  
    approve_search_shared_private_access(subscription_id, resource_group, function_app_name, storage_account_name, credential)

    ###########################################################################
    # 01 Creating blob containers (if needed)
    ###########################################################################
    logging.info("01 Creating containers step.")    
    
    logging.info(f"Getting {storage_account_name} storage connection string.")
    storage_client = StorageManagementClient(credential, subscription_id)
    keys = storage_client.storage_accounts.list_keys(resource_group, storage_account_name)
    storage_connection_string = f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName={storage_account_name};AccountKey={keys.keys[0].value}"
    
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
        exit(1)
    except azure.core.exceptions.HttpResponseError as e:
        error_message = str(e)
        logging.error(f"Error when creating container. {error_message}")
        logging.error(f"If you are in a network isolation scenario please run the script when connected to the solution vnet.")
        exit(1)
     
    response_time = time.time() - start_time
    logging.info(f"01 Create containers step. {round(response_time,2)} seconds")

    ###########################################################################
    # 02 Creating AI Search datasource
    ###########################################################################    
    logging.info("02 Creating datastores step.")
    start_time = time.time()

    body = {
        "description": "Input documents",
        "type": "azureblob",
        "dataDeletionDetectionPolicy" : {
        "@odata.type" :"#Microsoft.Azure.Search.NativeBlobSoftDeleteDeletionDetectionPolicy"
        },
        "credentials": {
            "connectionString": storage_connection_string
        },
        "container": {
            "name": storage_container
        }
    }
    call_search_api(search_service, search_api_version, "datasources", f"{search_index_name}-datasource", "put", credential, body)
    response_time = time.time() - start_time
    logging.info(f"02 Create datastores step. {round(response_time,2)} seconds")

    ###########################################################################
    # 03 Creating indexes
    ###########################################################################
    logging.info(f"03 Creating indexes step.")
    start_time = time.time()

    body = {
        "name":  f"{search_index_name}",
        "fields": [
            {
                "name": "id",
                "type": "Edm.String",
                "key": True,
                "analyzer": "keyword",                
                "searchable": True,
                "retrievable": True
            },    
            {
                "name": "parent_id",
                "type": "Edm.String",
                "key": False,
                "searchable": False,
                "retrievable": True
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
                "name": "summary",
                "type": "Edm.String",
                "filterable": False,
                "searchable": True,
                "retrievable": True
            },            
            {
                "name": "relatedImages",
                "type": "Collection(Edm.String)",
                "filterable": False,
                "searchable": False,
                "retrievable": True
            },
            {
                "name": "relatedFiles",
                "type": "Collection(Edm.String)",
                "filterable": False,
                "searchable": False,
                "retrievable": True
            },
            {
                "name": "security_id",
                "type": "Collection(Edm.String)",
                "searchable": False,
                "retrievable": True,
                "filterable": True
            },
            {
                "name": "contentVector",
                "type": "Collection(Edm.Single)",
                "searchable": True,
                "retrievable": True,
                "dimensions": 1536,
                "vectorSearchProfile": "myHnswProfile"
            }
        ],
        "corsOptions": {
            "allowedOrigins": [
                "*"
            ],
            "maxAgeInSeconds": 60
        },
        "vectorSearch": {
            "profiles": [
            {
                "name": "myHnswProfile",
                "algorithm": "myHnswConfig"
            }
            ],
            "algorithms": [
            {
                "name": "myHnswConfig",
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

    call_search_api(search_service, search_api_version, "indexes", f"{search_index_name}", "put", credential, body)
    response_time = time.time() - start_time
    logging.info(f"03 Create indexes step. {round(response_time,2)} seconds")


    ###########################################################################
    # 04 Creating AI Search skillsets
    ###########################################################################
    logging.info("04 Creating skillsets step.")
    start_time = time.time()

    body = { 
        "name": f"{search_index_name}-skillset-chunking",
        "description":"SKillset to do document chunking",
        "skills":[ 
            { 
                "@odata.type":"#Microsoft.Skills.Custom.WebApiSkill",
                "name":"document-chunking",
                "description":"Extract chunks from documents.",
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
        "indexProjections": {
            "selectors": [
                {
                    "targetIndexName":f"{search_index_name}",
                    "parentKeyFieldName": "parent_id",
                    "sourceContext": "/document/chunks/*",
                    "mappings": [
                        {
                        "name": "chunk_id",
                        "source": "/document/chunks/*/chunk_id",
                        "inputs": []
                        },
                        {
                            "name": "offset",
                            "source": "/document/chunks/*/offset",
                            "inputs": []
                        },
                        {
                            "name": "length",
                            "source": "/document/chunks/*/length",
                            "inputs": []
                        },
                        {
                            "name": "page",
                            "source": "/document/chunks/*/page",
                            "inputs": []
                        },
                        {
                            "name": "title",
                            "source": "/document/chunks/*/title",
                            "inputs": []
                        },
                        {
                            "name": "category",
                            "source": "/document/chunks/*/category",
                            "inputs": []
                        },
                        {
                            "name": "url",
                            "source": "/document/chunks/*/url",
                            "inputs": []
                        },
                        {
                            "name": "relatedImages",
                            "source": "/document/chunks/*/relatedImages",
                            "inputs": []
                        },
                        {
                            "name": "relatedFiles",
                            "source": "/document/chunks/*/relatedFiles",
                            "inputs": []
                        },
                        {
                            "name": "filepath",
                            "source": "/document/chunks/*/filepath",
                            "inputs": []
                        },
                        {
                            "name": "content",
                            "source": "/document/chunks/*/content",
                            "inputs": []
                        },
                        {
                            "name": "summary",
                            "source": "/document/chunks/*/summary",
                            "inputs": []
                        },                        
                        {
                            "name": "contentVector",
                            "source": "/document/chunks/*/contentVector",
                            "inputs": []
                        },
                        { 
                            "name": "security_id",
                            "source": "/document/chunks/*/security_id",
                            "inputs": []                            
                        },
                        {
                            "name": "metadata_storage_path",
                            "source": "/document/metadata_storage_path",
                            "inputs": []
                        },
                        {
                            "name": "metadata_storage_name",
                            "source": "/document/metadata_storage_name",
                            "inputs": []
                        }                    
                    ]
                }
            ],
            "parameters": {
                "projectionMode": "skipIndexingParentDocuments"
            }
        }
    }
    if azure_search_use_mis:
        body['skills'][0]['uri'] = f"{function_endpoint}/api/document-chunking"
        body['skills'][0]['authResourceId'] = f"api://{search_principal_id}"
    else:
        body['skills'][0]['uri'] = f"{function_endpoint}/api/document-chunking?code={function_key}"
        
    # first delete to enforce web api skillset to be updated
    call_search_api(search_service, search_api_version, "skillsets", f"{search_index_name}-skillset-chunking", "delete", credential)        

    call_search_api(search_service, search_api_version, "skillsets", f"{search_index_name}-skillset-chunking", "put", credential, body)
    
    response_time = time.time() - start_time
    logging.info(f"04 Create skillset step. {round(response_time,2)} seconds")

    ###########################################################################
    # 05 Creating indexers
    ###########################################################################
    logging.info("05 Creating indexer step.")
    start_time = time.time()
    body = {
        "dataSourceName" : f"{search_index_name}-datasource",
        "targetIndexName" : f"{search_index_name}",
        "skillsetName" : f"{search_index_name}-skillset-chunking",
        "schedule" : { "interval" : f"{search_index_interval}"},
        "fieldMappings" : [
            {
                "sourceFieldName" : "metadata_storage_path",
                "targetFieldName" : "id",
                "mappingFunction" : {
                    "name" : "fixedLengthEncode"
                }
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
                "dataToExtract": "allMetadata"
            }
        }
    }
    if network_isolation: body['parameters']['configuration']['executionEnvironment'] = "private"
    call_search_api(search_service, search_api_version, "indexers", f"{search_index_name}-indexer-chunk-documents", "put", credential, body)

    response_time = time.time() - start_time
    logging.info(f"05 Create indexers step. {round(response_time,2)} seconds")

def main(subscription_id=None, resource_group=None, function_app_name=None, search_principal_id='', azure_search_use_mis=False, enable_managed_identities=False, enable_env_credentials=False):
    """
    Sets up a chunking function app in Azure.

    Args:
        subscription_id (str): The subscription ID to use. If not provided, the user will be prompted to enter it.
        resource_group (str): The resource group to use. If not provided, the user will be prompted to enter it.
        function_app_name (str): The name of the chunking function app. If not provided, the user will be prompted to enter it.
        search_principal_id (str): Entra ID of the search managed identity.         
        azure_search_use_mis (bool): Whether to use Search Service Managed Identity to Connect to data ingestion function
        enable_managed_identities (bool, optional): Whether to use VM's managed identities to run the setup, defaults to False.
        enable_env_credentials (bool, optional): Whether to use environment credentials to run the setup, defaults to False.
    """   
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f"Starting setup.")
    if subscription_id is None:
        subscription_id = input("Enter subscription ID: ")
    if resource_group is None:
        resource_group = input("Enter resource group: ")
    if function_app_name is None:
        function_app_name = input("Enter chunking function app name: ")

    start_time = time.time()

    execute_setup(subscription_id, resource_group, function_app_name, search_principal_id, azure_search_use_mis, enable_managed_identities, enable_env_credentials)

    response_time = time.time() - start_time
    logging.info(f"Finished setup. {round(response_time,2)} seconds")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to do the data ingestion setup for Azure Cognitive Search.')
    parser.add_argument('-s', '--subscription_id', help='Subscription ID')
    parser.add_argument('-r', '--resource_group', help='Resource group')
    parser.add_argument('-f', '--function_app_name', help='Chunking function app name')
    parser.add_argument('-a', '--search_principal_id', default='none', help='Entra ID of the search service')
    parser.add_argument('-m', '--azure_search_use_mis', help='Use Search Service Managed Identity to Connect to data ingestion function')
    parser.add_argument('-i', '--enable_managed_identities', action='store_true', default=False, help='Use VM\'s managed identities for the setup')
    parser.add_argument('-e', '--enable_env_credentials', action='store_true', default=False, help='Use environment credentials for the setup')    
    args = parser.parse_args()

    # format search_use_mis to boolean
    search_use_mis = args.azure_search_use_mis.lower() == "true" if args.azure_search_use_mis not in [None, ""] else False

    main(subscription_id=args.subscription_id, resource_group=args.resource_group, function_app_name=args.function_app_name, search_principal_id=args.search_principal_id, 
        azure_search_use_mis=search_use_mis, enable_managed_identities=args.enable_managed_identities, enable_env_credentials=args.enable_env_credentials)    