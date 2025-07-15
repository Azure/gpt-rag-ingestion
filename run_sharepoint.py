"""
SharePoint Indexer

Loads configuration from environment, then:

 • Streams file metadata from the specified SharePoint site and drive  
 • Downloads and chunks any new or changed files  
 • Indexes those chunks into your Azure AI Search index  
 • (Optional) Purges search entries for files deleted in SharePoint  

Required environment variables (or put them in a `.env` file):

  • SHAREPOINT_CONNECTOR_ENABLED        — 'true' to run  
  • SHAREPOINT_TENANT_ID                — Azure AD tenant ID  
  • SHAREPOINT_CLIENT_ID                — Azure AD app (client) ID  
  • SHAREPOINT_CLIENT_SECRET_NAME       — Key Vault secret name (default: 'sharepointClientSecret')  
  • AZURE_SEARCH_SHAREPOINT_INDEX_NAME — Search index name (default: 'ragindex')  
  • SHAREPOINT_SITE_DOMAIN              — e.g. 'contoso.sharepoint.com'  
  • SHAREPOINT_SITE_NAME                — e.g. 'Documents'  
  • SHAREPOINT_DRIVE_ID                 — Drive/library identifier  
  • SHAREPOINT_SUBFOLDERS_NAMES         — comma-separated subfolders under the drive root (optional)  
  • SHAREPOINT_FILES_FORMAT             — comma-separated extensions to include (e.g., 'pdf,docx')  

Usage:

    python run_sharepoint.py
"""


import logging
import os
import asyncio
from dotenv import load_dotenv
from connectors import SharePointDocumentIngestor, SharePointDeletedItemsCleaner

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
        indexer = SharePointDocumentIngestor()
        asyncio.run(indexer.run())
    except Exception as e:
        logging.error(f"[main] An unexpected error occurred: {e}")

    # Purge deleted files
    try:
        purger = SharePointDeletedItemsCleaner()
        asyncio.run(purger.run())
    except Exception as e:
        logging.error(f"[main] An unexpected error occurred: {e}")


# -------------------------------
# Entry Point
# -------------------------------

if __name__ == "__main__":
    main()