import os

AZURE_WEBSITE_INSTANCE_ID = "WEBSITE_INSTANCE_ID"
AZURE_CONTAINER_NAME = "CONTAINER_APP_NAME"
AZURE_WEBJOBS_SCRIPT_ROOT = "AzureWebJobsScriptRoot"

def is_azure_environment():
    """Check if the function app is running on the cloud"""
    return (AZURE_CONTAINER_NAME in os.environ
            or AZURE_WEBSITE_INSTANCE_ID in os.environ)