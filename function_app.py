import logging
import jsonschema
import json
import os
import time
import azure.functions as func
import datetime
from chunking import DocumentChunker
from json import JSONEncoder
from connectors import SharePointDataExtractor
from tools import KeyVaultClient


import logging
import json
import os
import time
import azure.functions as func

###############################################################################
# Pipeline Functions
###############################################################################

app = func.FunctionApp()

        
###################################################################################
# Orchestator function (HTTP Triggered by AI Search)
###################################################################################

        
@app.route(route="orc", auth_level=func.AuthLevel.FUNCTION)
async def orc(req: func.HttpRequest) -> func.HttpResponse:
    req_body = req.get_json()

    # Get input parameters
    conversation_id = req_body.get('conversation_id')
    question = req_body.get('question')

    # Get client principal information
    client_principal_id = req_body.get('client_principal_id')
    client_principal_name = req_body.get('client_principal_name') 
    if not client_principal_id or client_principal_id == '':
        client_principal_id = '00000000-0000-0000-0000-000000000000'
        client_principal_name = 'anonymous'    
    client_principal = {
        'id': client_principal_id,
        'name': client_principal_name
    }

    # Call orchestrator
    if question:
        result = "{oi}"
        return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)
    else:
        return func.HttpResponse('{"error": "no question found in json input"}', mimetype="application/json", status_code=200)



###############################################################################
# Pipeline Functions
###############################################################################

# app = func.FunctionApp()

# @app.route(route="sharepoint-connector", auth_level=func.AuthLevel.FUNCTION)
# async def sharepoint_connector(req: func.HttpRequest) -> func.HttpResponse:
# @app.function_name(name="sharepoint_connector")
# @app.schedule(
#     schedule="0 */5 * * * *",  # Every 5 minutes
#     arg_name="sharepoint_connector",           # Specify the name of the parameter
#     run_on_startup=True
# )
# async def sharepoint_connector(timer: func.TimerRequest) -> None:
    # logging.info("[sharepoint_connector] Triggered sharepoint connector function.")
    # return func.HttpResponse("{oi}", mimetype="application/json")
    # connector_enabled=os.getenv("SHAREPOINT_CONNECTOR_ENABLED", "false")

    # if connector_enabled.lower() != "true":
    #     logging.info("[sharepoint_connector] SharePoint connector is disabled. Set SHAREPOINT_CONNECTOR_ENABLED to 'true' to enable the connector.")

    # else:
    #     # initialize variables from environment
    #     tenant_id = os.getenv("SHAREPOINT_TENANT_ID")
    #     client_id = os.getenv("SHAREPOINT_CLIENT_ID")
    #     site_domain = os.getenv("SHAREPOINT_SITE_DOMAIN")
    #     site_name = os.getenv("SHAREPOINT_SITE_NAME")
    #     folder_path = os.getenv("SHAREPOINT_SITE_FOLDER", "/")
    #     file_formats = os.getenv("SHAREPOINT_FILES_FORMAT", "pdf,docx").split(",")
    #     # initialize secret from Key Vault
    #     keyvault_client = KeyVaultClient()
    #     client_secret = await keyvault_client.get_secret("sharepointClientSecret")

    #     # check we have variables and secret neeeded
    #     missing_env_vars = [var for var, value in {
    #         "SHAREPOINT_TENANT_ID": tenant_id,
    #         "SHAREPOINT_CLIENT_ID": client_id,
    #         "SHAREPOINT_SITE_DOMAIN": site_domain,
    #         "SHAREPOINT_SITE_NAME": site_name
    #     }.items() if not value]
    
    #     if missing_env_vars:
    #         logging.error(f"[sharepoint_connector] SharePoint connector variables are not properly configured. Missing environment variables: {', '.join(missing_env_vars)}. Please set all required environment variables.")
    
    #     if not client_secret:
    #         logging.error("[sharepoint_connector] SharePoint connector secret is not properly configured. Missing secret: sharepointClientSecret. Please set the required secret in Key Vault.")
    
    #     if not missing_env_vars and client_secret:
    #         # Instantiate the SharePointDataExtractor class with credentials
    #         extractor = SharePointDataExtractor(
    #             tenant_id=tenant_id,
    #             client_id=client_id,
    #             client_secret = client_secret       
    #         )

    #         # Authenticate with Microsoft Graph
    #         extractor.msgraph_auth()

    #         try:
    #             # Call the retrieve_sharepoint_files_content method5
    #             files_content = extractor.retrieve_sharepoint_files_content(
    #                 site_domain=site_domain,
    #                 site_name=site_name,
    #                 folder_path=folder_path,
    #                 file_formats=file_formats
    #             )

    #             # If files_content is not None, iterate over files and print the file name
    #             if files_content:
    #                 for file_data in files_content:
    #                     file_name = file_data.get("name")
    #                     if file_name:
    #                         logging.info(f"File Name: {file_name}")
    #             else:
    #                 logging.info("No files retrieved from SharePoint.")

    #         except Exception as e:
    #             logging.error(f"Error in sharepoint_connector: {e}")