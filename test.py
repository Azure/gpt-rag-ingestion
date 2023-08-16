from azure.identity import AzureCliCredential
from azure.mgmt.web import WebSiteManagementClient

credential = AzureCliCredential()
subscription_id = "SUBSCRIPTION_ID" #Add your subscription ID
resource_group = "RESOURCE_GROUP_ID" #Add your resource group
function_app_name = "FUNCTION_APP_NAME" #Add your function app name

# Create the web management client
web_mgmt_client = WebSiteManagementClient(credential, subscription_id)

# Get function app details
function_app_details = app_service_client.web_apps.get(resource_group, function_app_name)
print(function_app_details)

# # Get function app configuration details
# function_app_config = app_service_client.web_apps.get_configuration(resource_group, function_app_name)
# print(function_app_config)

# List function app application settings
function_app_settings = app_service_client.web_apps.list_application_settings(resource_group, function_app_name)
print(function_app_settings)

keys = storage_client.storage_accounts.list_keys(RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME)

print(f"Primary key for storage account: {keys.keys[0].value}")

conn_string = f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={keys.keys[0].value}"

print(f"Connection string: {conn_string}")