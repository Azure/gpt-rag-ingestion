import logging
import time
import requests
import argparse
import json
from azure.mgmt.web import WebSiteManagementClient
from azure.identity import DefaultAzureCredential
from azure.identity import ManagedIdentityCredential, AzureCliCredential, ChainedTokenCredential
from azure.core.exceptions import ClientAuthenticationError, HttpResponseError
# Set up logging configuration globally
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

        # delete processing
        if method == "delete":
            response = requests.delete(search_endpoint, headers=headers)
            status_code = response.status_code
            logging.info(f"Successfully called search API {method} {resource_type} {resource_name}. Code: {status_code}.")

        if response is not None:
            status_code = response.status_code
            if status_code >= 400:
                logging.warning(f"{status_code} code when calling search API {method} {resource_type} {resource_name}. Reason: {response.reason}.")
                response_text_dict = json.loads(response.text)
                logging.warning(f"{status_code} code when calling search API {method} {resource_type} {resource_name}. Message: {response_text_dict['error']['message']}")                
            else:
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
        "properties": {
            "name": "mykey"  # Omit the 'value' field to let Azure generate the key
        }
    }

    response = requests.put(requestUrl, headers=requestHeaders, json=data)
    response_json = json.loads(response.content.decode('utf-8'))
    # print(response_json)
    try:
        function_key = response_json['properties']['value']
    except Exception as e:
        function_key = None
        logging.error(f"Error when getting function key. Details: {str(e)}.")        
    return function_key

def approve_private_link_connections(access_token, subscription_id, resource_group, service_name, service_type, api_version):
    """
    Approves private link service connections for a given service.

    Args:
        access_token (str): The access token used for authorization.
        subscription_id (str): The subscription ID.
        resource_group (str): The resource group name.
        service_name (str): The name of the service.
        service_type (str): The type of the service (e.g., 'Microsoft.Storage/storageAccounts').
        api_version (str): The API version.

    Returns:
        None

    Raises:
        HTTPError: If any of the HTTP requests fail.
    """
    request_url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/{service_type}/{service_name}/privateEndpointConnections?api-version={api_version}"
    request_headers = {
        "Authorization": access_token,
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(request_url, headers=request_headers)
        response.raise_for_status()
        response_json = response.json()

        if 'value' not in response_json:
            logging.error(f"Unexpected response structure when fetching private link connections. Response content: {response.content}")
            raise Exception("Unexpected response structure.")

        for connection in response_json["value"]:
            status = connection['properties']['privateLinkServiceConnectionState']['status']
            logging.info(f"Checking connection '{connection['name']}'. Status: {status}.")
            if status.lower() == "pending":
                connection_name = connection['name']
                approve_url = f"{request_url}/{connection_name}/approve?api-version={api_version}"
                request_body = {
                    "properties": {
                        "privateLinkServiceConnectionState": {
                            "status": "Approved",
                            "description": "Approved by setup script"
                        }
                    }
                }

                approve_response = requests.post(approve_url, headers=request_headers, json=request_body)
                if approve_response.status_code in [200, 202]:
                    logging.info(f"Approved private endpoint connection '{connection_name}' for service '{service_name}'.")
                else:
                    logging.error(f"Failed to approve private endpoint connection '{connection_name}' for service '{service_name}'. Status Code: {approve_response.status_code}, Response: {approve_response.text}")
                    approve_response.raise_for_status()

    except requests.HTTPError as http_err:
        logging.error(f"HTTP error occurred when approving private link connections: {http_err}. Response: {response.text}")
        raise
    except Exception as e:
        logging.error(f"Error occurred when approving private link connections: {e}")
        raise

def approve_search_shared_private_access(subscription_id, resource_group, function_app_name, storage_account_name, openai_service_name, credential):
    """
    Approves Shared Private Access requests for private endpoints for AI Search, storage account, function app, and Azure OpenAI Service.

    Args:
        subscription_id (str): The subscription ID.
        resource_group (str): The resource group name.
        function_app_name (str): The name of the function app.
        storage_account_name (str): The name of the storage account.
        openai_service_name (str): The name of the Azure OpenAI service.

    Returns:
        None

    Raises:
        Exception: If approval fails.
    """    
    try:
        logging.info("Approving Shared Private Access requests for storage, function app, and Azure OpenAI Service if needed.")
        
        # Obtain the access token
        try:
            token_response = credential.get_token("https://management.azure.com/.default")
            access_token = f"Bearer {token_response.token}"
            logging.info("Obtained access token successfully.")
        except ClientAuthenticationError as e:
            logging.error(f"Authentication failed when obtaining access token: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error when obtaining access token: {e}")
            raise
        
        # Approve private link connection for storage account
        try:
            approve_private_link_connections(
                access_token, 
                subscription_id, 
                resource_group, 
                storage_account_name, 
                'Microsoft.Storage/storageAccounts', 
                '2023-05-01'
            )
            logging.info(f"Approved private link connections for Storage Account: {storage_account_name}.")
        except Exception as e:
            logging.error(f"Failed to approve private link connections for Storage Account '{storage_account_name}': {e}")
            raise
        
        # Approve private link connection for function app
        try:
            approve_private_link_connections(
                access_token, 
                subscription_id, 
                resource_group, 
                function_app_name, 
                'Microsoft.Web/sites', 
                '2023-01-01'
            )
            logging.info(f"Approved private link connections for Function App: {function_app_name}.")
        except Exception as e:
            logging.error(f"Failed to approve private link connections for Function App '{function_app_name}': {e}")
            raise

        # Approve private link connection for Azure OpenAI Service
        try:
            approve_private_link_connections(
                access_token, 
                subscription_id, 
                resource_group, 
                openai_service_name, 
                'Microsoft.CognitiveServices/accounts', 
                '2023-05-01'
            )
            logging.info(f"Approved private link connections for Azure OpenAI Service: {openai_service_name}.")
        except Exception as e:
            logging.error(f"Failed to approve private link connections for Azure OpenAI Service '{openai_service_name}': {e}")
            raise
    
    except Exception as e:
        error_message = str(e)
        logging.error(f"Error when approving private link service connection. Please do it manually. Error: {error_message}")
        raise


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
    # credential = DefaultAzureCredential()
    credential = ChainedTokenCredential(
        ManagedIdentityCredential(),
        AzureCliCredential()
    )
    web_mgmt_client = WebSiteManagementClient(credential, subscription_id)
    function_app_settings = web_mgmt_client.web_apps.list_application_settings(resource_group, function_app_name)
    function_endpoint = f"https://{function_app_name}.azurewebsites.net"
    azure_openai_service_name = function_app_settings.properties["AZURE_OPENAI_SERVICE_NAME"]
    search_service = function_app_settings.properties["AZURE_SEARCH_SERVICE"]
    search_analyzer_name= function_app_settings.properties["SEARCH_ANALYZER_NAME"]
    search_api_version = function_app_settings.properties.get("SEARCH_API_VERSION", "2024-07-01") 
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


    # NL2SQL Elements
    storage_container_nl2sql = "nl2sql"
    search_index_name_nl2sql_queries = "nl2sql-queries"
    search_index_name_nl2sql_tables = "nl2sql-tables"
    search_index_name_nl2sql_columns = "nl2sql-columns"

    logging.info(f"NL2SQL Storage container: {storage_container_nl2sql}")
    logging.info(f"NL2SQL Search index name (queries): {search_index_name_nl2sql_queries}")
    logging.info(f"NL2SQL Search index name (tables): {search_index_name_nl2sql_tables}")
    logging.info(f"NL2SQL Search index name (columns): {search_index_name_nl2sql_columns}")    

    
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
    logging.info("Approving search shared private links.")  
    approve_search_shared_private_access(subscription_id, resource_group, function_app_name, storage_account_name, azure_openai_service_name, credential)

    ###########################################################################
    # Creating blob containers
    ###########################################################################
    # Note: this step was removed since the storage account and container are already created by azd provision

    ###############################################################################
    # Creating AI Search datasource
    ###############################################################################
    
    def create_datasource(search_service, search_api_version, datasource_name, storage_connection_string, container_name, credential, subfolder=None):
        body = {
            "description": f"Datastore for {datasource_name}",
            "type": "azureblob",
            "dataDeletionDetectionPolicy": {
                "@odata.type": "#Microsoft.Azure.Search.NativeBlobSoftDeleteDeletionDetectionPolicy"
            },
            "credentials": {
                "connectionString": storage_connection_string
            },
            "container": {
                "name": container_name,
                "query": f"{subfolder}/" if subfolder else ""  # Adding subfolder path if provided
            }
        }
        call_search_api(search_service, search_api_version, "datasources", f"{datasource_name}-datasource", "put", credential, body)

    logging.info("Creating datasources step.")
    start_time = time.time()

    # Define storage connection string without account key
    storage_connection_string = f"ResourceId=/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Storage/storageAccounts/{storage_account_name}/;"

    # Creating main datasource
    create_datasource(search_service, search_api_version, f"{search_index_name}", storage_connection_string, storage_container, credential)

    # Creating NL2SQL datasource in different subfolders
    nl2sql_subfolders = {
        "queries": search_index_name_nl2sql_queries,
        "tables": search_index_name_nl2sql_tables,
        "columns": search_index_name_nl2sql_columns
    }

    for subfolder, index_name in nl2sql_subfolders.items():
        create_datasource(search_service, search_api_version, index_name, storage_connection_string, "nl2sql", credential, subfolder=subfolder)

    response_time = time.time() - start_time
    logging.info(f"Create datastores step. {round(response_time, 2)} seconds")


    ###############################################################################
    # Creating indexes
    ###############################################################################

    def create_index_body(index_name, fields, content_field_name, keyword_field_name, vector_dimensions, vector_profile_name="myHnswProfile", vector_algorithm_name="myHnswConfig"):
        body = {
            "name": index_name,
            "fields": fields,
            "corsOptions": {
                "allowedOrigins": ["*"],
                "maxAgeInSeconds": 60
            },
            "vectorSearch": {
                "profiles": [
                    {
                        "name": vector_profile_name,
                        "algorithm": vector_algorithm_name
                    }
                ],
                "algorithms": [
                    {
                        "name": vector_algorithm_name,
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
                                    "fieldName": content_field_name
                                }
                            ],
                            "prioritizedKeywordsFields": [
                                {
                                    "fieldName": keyword_field_name
                                }
                            ]
                        }
                    }
                ]
            }
        }
        return body


    logging.info("Creating indexes.")
    start_time = time.time()

    # Common vector search configurations
    vector_profile_name = "myHnswProfile"
    vector_algorithm_name = "myHnswConfig"

    # Define index configurations
    indices = [
        {
            "index_name": search_index_name,  # RAG index
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
                    "name": "metadata_storage_last_modified",
                    "type": "Edm.DateTimeOffset",
                    "searchable": False,
                    "sortable": True,
                    "retrievable": True,
                    "filterable": True
                },
                {
                    "name": "metadata_security_id",
                    "type": "Collection(Edm.String)",
                    "searchable": False,
                    "retrievable": True,
                    "filterable": True
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
                    "name": "source",
                    "type": "Edm.String",
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
                    "vectorSearchProfile": vector_profile_name
                }
            ],
            "content_field_name": "content",
            "keyword_field_name": "category",
            "vector_dimensions": 1536
        },
        {
            "index_name": search_index_name_nl2sql_queries,
            "fields": [
                {
                    "name": "id",
                    "type": "Edm.String",
                    "key": True,
                    "searchable": False,
                    "filterable": False,
                    "sortable": False,
                    "facetable": False
                },
                {
                    "name": "question",
                    "type": "Edm.String",
                    "searchable": True,
                    "filterable": False,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": False,
                    "analyzer": "standard.lucene"
                },
                {
                    "name": "query",
                    "type": "Edm.String",
                    "searchable": False,
                    "filterable": False,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": False
                },
                {
                    "name": "selected_tables",
                    "type": "Collection(Edm.String)",
                    "searchable": False,
                    "filterable": False,
                    "retrievable": True
                },
                {
                    "name": "selected_columns",
                    "type": "Collection(Edm.String)",
                    "searchable": False,
                    "filterable": False,
                    "retrievable": True
                },
                {
                    "name": "reasoning",
                    "type": "Edm.String",
                    "searchable": True,
                    "filterable": False,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": False
                },
                {
                    "name": "contentVector",
                    "type": "Collection(Edm.Single)",
                    "searchable": True,
                    "retrievable": True,
                    "dimensions": 1536,
                    "vectorSearchProfile": vector_profile_name
                }
            ],
            "content_field_name": "question",
            "keyword_field_name": "question",
            "vector_dimensions": 1536
        },
        {
            "index_name": search_index_name_nl2sql_tables,
            "fields": [
                {
                    "name": "id",
                    "type": "Edm.String",
                    "key": True,
                    "searchable": False,
                    "filterable": False,
                    "sortable": False,
                    "facetable": False
                },
                {
                    "name": "table_name",
                    "type": "Edm.String",
                    "searchable": True,
                    "filterable": False,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": False,
                    "analyzer": "standard.lucene"
                },
                {
                    "name": "description",
                    "type": "Edm.String",
                    "searchable": True,
                    "filterable": False,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": False,
                    "analyzer": "standard.lucene"
                },
                {
                    "name": "contentVector",
                    "type": "Collection(Edm.Single)",
                    "searchable": True,
                    "retrievable": True,
                    "dimensions": 1536,
                    "vectorSearchProfile": vector_profile_name
                }
            ],
            "content_field_name": "description",
            "keyword_field_name": "description",
            "vector_dimensions": 1536
        },
        {
            "index_name": search_index_name_nl2sql_columns,
            "fields": [
                {
                    "name": "id",
                    "type": "Edm.String",
                    "key": True,
                    "searchable": False,
                    "filterable": False,
                    "sortable": False,
                    "facetable": False
                },
                {
                    "name": "table_name",
                    "type": "Edm.String",
                    "searchable": True,
                    "filterable": True,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": False,
                    "analyzer": "standard.lucene"
                },
                {
                    "name": "column_name",
                    "type": "Edm.String",
                    "searchable": True,
                    "filterable": False,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": False,
                    "analyzer": "standard.lucene"
                },
                {
                    "name": "description",
                    "type": "Edm.String",
                    "searchable": True,
                    "filterable": False,
                    "retrievable": True,
                    "sortable": False,
                    "facetable": False,
                    "analyzer": "standard.lucene"
                },
                {
                    "name": "contentVector",
                    "type": "Collection(Edm.Single)",
                    "searchable": True,
                    "retrievable": True,
                    "dimensions": 1536,
                    "vectorSearchProfile": vector_profile_name
                }
            ],
            "content_field_name": "description",
            "keyword_field_name": "description",
            "vector_dimensions": 1536
        }
    ]

    # Iterate over each index configuration and create the index
    for index in indices:
        body = create_index_body(
            index_name=index["index_name"],
            fields=index["fields"],
            content_field_name=index["content_field_name"],
            keyword_field_name=index["keyword_field_name"],
            vector_dimensions=index["vector_dimensions"],
            vector_profile_name=vector_profile_name,
            vector_algorithm_name=vector_algorithm_name
        )
        # Delete existing index if it exists
        call_search_api(search_service, search_api_version, "indexes", index["index_name"], "delete", credential)
        # Create the index
        call_search_api(search_service, search_api_version, "indexes", index["index_name"], "put", credential, body)

    response_time = time.time() - start_time
    logging.info(f"Indexes created in {round(response_time, 2)} seconds")

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
                            "name": "source",
                            "source": "/document/chunks/*/source",
                            "inputs": []
                        },                                                      
                        {
                            "name": "contentVector",
                            "source": "/document/chunks/*/contentVector",
                            "inputs": []
                        },
                        {
                            "name": "metadata_storage_last_modified",
                            "source": "/document/metadata_storage_last_modified",
                            "inputs": []
                        },
                        {
                            "name": "metadata_storage_name",
                            "source": "/document/metadata_storage_name",
                            "inputs": []
                        },
                        {
                            "name": "metadata_storage_path",
                            "source": "/document/metadata_storage_path",
                            "inputs": []
                        },                        
                        {
                            "name": "metadata_security_id", 
                            "source": "/document/metadata_security_id",
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

    # creating skill sets for the NL2SQL indexes

    def create_embedding_skillset(skillset_name, resource_uri, deployment_id, model_name, input_field, output_field, dimensions=1536):
        skill = {
            "@odata.type": "#Microsoft.Skills.Text.AzureOpenAIEmbeddingSkill",
            "name": f"{skillset_name}-embedding-skill",
            "description": f"Generates embeddings for {input_field}.",
            "resourceUri": resource_uri,
            "deploymentId": deployment_id,
            "modelName": model_name,
            "dimensions": dimensions,
            "context":"/document",            
            "inputs": [
                {
                    "name": "text",
                    "source": f"/document/{input_field}"
                }
            ],
            "outputs": [
                {
                    "name": "embedding",
                    "targetName": output_field
                }
            ]
        }

        skillset_body = {
            "name": skillset_name,
            "description": f"Skillset for generating embeddings for {skillset_name} index.",
            "skills": [skill]
        }

        return skillset_body

    # Configuration parameters
    resource_uri = f"https://{azure_openai_service_name}.openai.azure.com/"
    deployment_id = "text-embedding-ada-002"  # Example deployment ID
    model_name = "text-embedding-ada-002"
 
    # Define skillsets configurations
    skillsets = [
        {
            "skillset_name": "queries-skillset",
            "input_field": "question",
            "output_field": "contentVector"
        },
        {
            "skillset_name": "tables-skillset",
            "input_field": "description",
            "output_field": "contentVector"
        },
        {
            "skillset_name": "columns-skillset",
            "input_field": "description",
            "output_field": "contentVector"
        }
    ]

    # Iterate and create skillsets
    for skillset in skillsets:
        body = create_embedding_skillset(
            skillset_name=skillset["skillset_name"],
            resource_uri=resource_uri,
            deployment_id=deployment_id,
            model_name=model_name,
            input_field=skillset["input_field"],
            output_field=skillset["output_field"],
            dimensions=1536
        )

        # Delete existing skillset if it exists
        call_search_api(search_service, search_api_version, "skillsets", skillset["skillset_name"], "delete", credential)

        # Create the new skillset
        call_search_api(search_service, search_api_version, "skillsets", skillset["skillset_name"], "put", credential, body)

        logging.info(f"Skillset '{skillset['skillset_name']}' created successfully.")




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

    # creating indexers for the NL2SQL indexes
    def create_indexer_body(indexer_name, index_name, data_source_name, skillset_name, field_mappings=None, indexing_parameters=None):
        body = {
            "name": indexer_name,
            "dataSourceName": data_source_name,
            "targetIndexName": index_name,
            "skillsetName": skillset_name,
            "schedule": {
                "interval": "PT2H"  # Adjust as needed
            },
            "fieldMappings": field_mappings if field_mappings else [],
            "outputFieldMappings": [
                {
                    "sourceFieldName": "/document/contentVector",
                    "targetFieldName": "contentVector"
                }
            ],
            "parameters":
            {
                "configuration": {
                    "parsingMode": "json"
                }
            }            
        }
        if indexing_parameters:
            body["parameters"] = indexing_parameters
        return body

    # Define field mappings for the 'queries-indexer'
    field_mappings_queries = [
        {
            "sourceFieldName" : "metadata_storage_path",
            "targetFieldName" : "id",
            "mappingFunction" : {
                "name" : "fixedLengthEncode"
            }
        },      
        {
            "sourceFieldName": "question",
            "targetFieldName": "question"
        },
        {
            "sourceFieldName": "query",
            "targetFieldName": "query"
        },
        {
            "sourceFieldName": "selected_tables",
            "targetFieldName": "selected_tables"
        },
        {
            "sourceFieldName": "selected_columns",
            "targetFieldName": "selected_columns"
        },
        {
            "sourceFieldName": "reasoning",
            "targetFieldName": "reasoning"
        }
    ]

    # Define field mappings for the 'tables-indexer'
    field_mappings_tables = [
        {
            "sourceFieldName" : "metadata_storage_path",
            "targetFieldName" : "id",
            "mappingFunction" : {
                "name" : "fixedLengthEncode"
            }
        },      
        {
            "sourceFieldName": "table_name",
            "targetFieldName": "table_name"
        },
        {
            "sourceFieldName": "description",
            "targetFieldName": "description"
        }
    ]

    # Define field mappings for the 'columns-indexer'
    field_mappings_columns = [
        {
            "sourceFieldName" : "metadata_storage_path",
            "targetFieldName" : "id",
            "mappingFunction" : {
                "name" : "fixedLengthEncode"
            }
        },
        {
            "sourceFieldName": "table_name",
            "targetFieldName": "table_name"
        },
        {
            "sourceFieldName": "column_name",
            "targetFieldName": "column_name"
        },
        {
            "sourceFieldName": "description",
            "targetFieldName": "description"
        }        
    ]


    # Define indexing parameters for the 'queries-indexer'
    indexing_parameters = {
        "configuration": {
            "parsingMode": "json"
        }
    }

    # Define indexers configurations
    indexers = [
        {
            "indexer_name": "queries-indexer",
            "index_name": f"{search_index_name_nl2sql_queries}",
            "data_source_name": f"{search_index_name_nl2sql_queries}-datasource",
            "skillset_name": "queries-skillset",
            "field_mappings": field_mappings_queries,
            "indexing_parameters": indexing_parameters
        },
        {
            "indexer_name": "tables-indexer",
            "index_name": f"{search_index_name_nl2sql_tables}",
            "data_source_name": f"{search_index_name_nl2sql_tables}-datasource",
            "skillset_name": "tables-skillset",
            "field_mappings": field_mappings_tables,
            "indexing_parameters": indexing_parameters
        },
        {
            "indexer_name": "columns-indexer",
            "index_name": f"{search_index_name_nl2sql_columns}",
            "data_source_name": f"{search_index_name_nl2sql_columns}-datasource",
            "skillset_name": "columns-skillset",
            "field_mappings": field_mappings_columns,
            "indexing_parameters": indexing_parameters
        }
    ]

    # Iterate and create indexers
    for indexer in indexers:
        body = create_indexer_body(
            indexer_name=indexer["indexer_name"],
            index_name=indexer["index_name"],
            data_source_name=indexer["data_source_name"],
            skillset_name=indexer["skillset_name"],
            field_mappings=indexer["field_mappings"]
        )


        # Delete existing indexer if it exists
        call_search_api(search_service, search_api_version, "indexers", indexer["indexer_name"], "delete", credential)

        # Create the new indexer
        call_search_api(search_service, search_api_version, "indexers", indexer["indexer_name"], "put", credential, body)

        logging.info(f"Indexer '{indexer['indexer_name']}' created successfully.")

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
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')    
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
    