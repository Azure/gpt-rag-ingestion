"""
SharePoint Indexer

This script indexes files from a SharePoint folder into Azure AI Search 
and removes deleted files from the index. It uses asynchronous workflows for 
efficient operation.

Features:
- File Indexing: Reads SharePoint files and uploads metadata to Azure AI Search.
- File Purging: Removes metadata for deleted files to maintain index consistency.
- Asynchronous Execution: Utilizes `asyncio` for network efficiency.

Prerequisites:
1. Environment Variables (set in `.env` or environment):
   - General:
     - SHAREPOINT_CONNECTOR_ENABLED: 'true' to enable the connector (default: 'false').
     - SHAREPOINT_TENANT_ID, SHAREPOINT_CLIENT_ID: For SharePoint authentication.
     - SHAREPOINT_CLIENT_SECRET_NAME: Azure Key Vault secret name (default: 'sharepointClientSecret').
     - AZURE_SEARCH_SHAREPOINT_INDEX_NAME: Name of the Azure AI Search index (default: 'ragindex').
   - SharePoint Config:
     - SHAREPOINT_SITE_DOMAIN, SHAREPOINT_SITE_NAME: SharePoint site details.
     - SHAREPOINT_SITE_FOLDER: Folder path (default: '/').
     - SHAREPOINT_FILES_FORMAT: Comma-separated list of file formats (e.g., 'pdf,docx').

2. Azure Config:
   - Azure Key Vault: Contains the SharePoint client secret.
   - Azure AI Search: Preconfigured with an appropriate schema.

Usage:
- Run the script: `python run.py`.

"""

import logging
import os
import asyncio
from dotenv import load_dotenv
from connectors import SharepointFilesIndexer, SharepointDeletedFilesPurger

load_dotenv(override=True)

# -------------------------------
# Logging configuration
# -------------------------------
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
log_level = getattr(logging, log_level, logging.INFO)
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
suppress_loggers = [
    'azure',
    'azure.core',
    'azure.core.pipeline',
    'azure.core.pipeline.policies.http_logging_policy',
    'azsdk-python-search-documents',
    'azsdk-python-identity',
    'azure.ai.openai',  # Assuming 'aoai' refers to Azure OpenAI
    'azure.identity',
    'azure.storage',
    'azure.ai.*',  # Wildcard-like suppression for any azure.ai sub-loggers
    # Add any other specific loggers if necessary
]
for logger_name in suppress_loggers:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.WARNING)
    logger.propagate = False  


# -------------------------------
# Main Method
# -------------------------------

def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Index sharepoint files
    try:
        indexer = SharepointFilesIndexer()
        asyncio.run(indexer.run())
    except Exception as e:
        logging.error(f"[main] An unexpected error occurred: {e}")

    # Purge deleted files
    try:
        purger = SharepointDeletedFilesPurger()
        asyncio.run(purger.run())
    except Exception as e:
        logging.error(f"[main] An unexpected error occurred: {e}")


# -------------------------------
# Entry Point
# -------------------------------

if __name__ == "__main__":
    main()