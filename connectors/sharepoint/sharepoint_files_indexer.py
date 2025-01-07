import logging
import os
import asyncio
from connectors import SharePointDataReader
from tools import KeyVaultClient
from tools import AISearchClient
from typing import Any, Dict, List, Optional
from chunking import DocumentChunker
from chunking import ChunkerFactory

class SharepointFilesIndexer:
    def __init__(self):
        # Initialize configuration from environment variables
        self.connector_enabled = os.getenv("SHAREPOINT_CONNECTOR_ENABLED", "false").lower() == "true"
        self.tenant_id = os.getenv("SHAREPOINT_TENANT_ID")
        self.client_id = os.getenv("SHAREPOINT_CLIENT_ID")
        self.site_domain = os.getenv("SHAREPOINT_SITE_DOMAIN")
        self.site_name = os.getenv("SHAREPOINT_SITE_NAME")
        self.folder_path = os.getenv("SHAREPOINT_SITE_FOLDER", "/")
        self.sharepoint_client_secret_name = os.getenv("KEYVAULT_SHAREPOINT_SECRET_NAME", "sharepointClientSecret")
        self.index_name = os.getenv("AZURE_SEARCH_SHAREPOINT_INDEX_NAME", "ragindex")
        self.file_formats = os.getenv("SHAREPOINT_FILES_FORMAT")
        if not self.file_formats:
            self.file_formats = ChunkerFactory.get_supported_extensions()
        self.keyvault_client: Optional[KeyVaultClient] = None
        self.client_secret: Optional[str] = None
        self.sharepoint_data_reader: Optional[SharePointDataReader] = None
        self.search_client: Optional[AISearchClient] = None

    async def initialize_clients(self) -> bool:
        """Initialize KeyVaultClient, retrieve secrets, and initialize SharePointDataReader and AISearchClient."""
        # Initialize Key Vault Client and retrieve SharePoint client secret
        try:
            self.keyvault_client = KeyVaultClient()
            self.client_secret = await self.keyvault_client.get_secret(self.sharepoint_client_secret_name)
            logging.debug("[sharepoint_files_indexer] Retrieved sharepointClientSecret secret from Key Vault.")
        except Exception as e:
            logging.error(f"[sharepoint_files_indexer] Failed to retrieve secret from Key Vault: {e}")
            return False
        finally:
            if self.keyvault_client:
                await self.keyvault_client.close()
        
        # Check for missing environment variables
        required_vars = {
            "SHAREPOINT_TENANT_ID": self.tenant_id,
            "SHAREPOINT_CLIENT_ID": self.client_id,
            "SHAREPOINT_SITE_DOMAIN": self.site_domain,
            "SHAREPOINT_SITE_NAME": self.site_name,
        }

        missing_env_vars = [var for var, value in required_vars.items() if not value]

        if missing_env_vars:
            logging.error(
                f"[sharepoint_files_indexer] Missing environment variables: {', '.join(missing_env_vars)}. "
                "Please set all required environment variables."
            )
            return False

        if not self.client_secret:
            logging.error(
                "[sharepoint_files_indexer] SharePoint connector secret is not properly configured. "
                "Missing secret: sharepointClientSecret. Please set the required secret in Key Vault."
            )
            return False

        # Initialize SharePointDataReader
        try:
            self.sharepoint_data_reader = SharePointDataReader(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            self.sharepoint_data_reader._msgraph_auth()
            logging.debug("[sharepoint_files_indexer] Authenticated with Microsoft Graph successfully.")
        except Exception as e:
            logging.error(f"[sharepoint_files_indexer] Authentication failed: {e}")
            return False

        # Initialize AISearchClient
        try:
            self.search_client = AISearchClient()
            logging.debug("[sharepoint_files_indexer] Initialized AISearchClient successfully.")
        except ValueError as ve:
            logging.error(f"[sharepoint_files_indexer] AISearchClient initialization failed: {ve}")
            return False
        except Exception as e:
            logging.error(f"[sharepoint_files_indexer] Unexpected error during AISearchClient initialization: {e}")
            return False

        return True

    async def delete_existing_chunks(self, existing_chunks: Dict[str, Any], file_name: str) -> None:
        """Delete existing document chunks from the search index."""
        chunk_ids = [doc['id'] for doc in existing_chunks.get('documents', []) if 'id' in doc]
        if not chunk_ids:
            logging.warning(f"[sharepoint_files_indexer] No valid 'id's found for existing chunks of '{file_name}'. Skipping deletion.")
            return
        try:
            await self.search_client.delete_documents(index_name=self.index_name, key_field="id", key_values=chunk_ids)
            logging.debug(f"[sharepoint_files_indexer] Deleted {len(chunk_ids)} existing chunks for '{file_name}'.")
        except Exception as e:
            logging.error(f"[sharepoint_files_indexer] Failed to delete existing chunks for '{file_name}': {e}")

    async def index_file(self, data: Dict[str, Any]) -> None:
        """Index a single file's metadata into the search index."""
        try:
            await self.search_client.index_document(index_name=self.index_name, document=data)
            logging.debug(f"[sharepoint_files_indexer] Indexed file '{data['fileName']}' successfully.")
        except Exception as e:
            logging.error(f"[sharepoint_files_indexer] Failed to index file '{data['fileName']}': {e}")

    async def process_file(self, file: Dict[str, Any], semaphore: asyncio.Semaphore) -> None:
        """Process and index a single SharePoint file."""
        async with semaphore:
            file_name = file.get("name")
            if not file_name:
                logging.warning("[sharepoint_files_indexer] File name is missing. Skipping file.")
                return

            sharepoint_id = file.get("id")
            document_bytes = file.get("content")
            document_url = file.get("source")
            last_modified_datetime = file.get("last_modified_datetime")
            read_access_entity = file.get("read_access_entity")                  

            logging.info(f"[sharepoint_files_indexer] Processing File: {file_name}. Last Modified: {last_modified_datetime}")

            data = {
                "sharepointId": sharepoint_id,
                "fileName": file_name,
                "documentBytes": document_bytes,
                "documentUrl": document_url
            }

            # Fetch existing chunks related to the file
            try:
                existing_chunks = await self.search_client.search_documents(
                    index_name=self.index_name,
                    search_text="*",
                    filter_str=f"parent_id eq '{sharepoint_id}' and source eq 'sharepoint'",
                    select_fields=['id', 'metadata_storage_last_modified', 'metadata_storage_name'],
                    top=0
                )
            except Exception as e:
                logging.error(f"[sharepoint_files_indexer] Failed to search existing chunks for '{file_name}': {e}")
                return

            if existing_chunks.get('count', 0) == 0:
                logging.debug(f"[sharepoint_files_indexer] No existing chunks found for '{file_name}'. Proceeding to index.")
            else:
                indexed_last_modified_str = existing_chunks['documents'][0].get('metadata_storage_last_modified')

                if not indexed_last_modified_str:
                    logging.warning(
                        f"[sharepoint_files_indexer] 'metadata_storage_last_modified' not found for existing chunks of '{file_name}'. "
                        "Deleting existing chunks and proceeding to re-index."
                    )
                    await self.delete_existing_chunks(existing_chunks, file_name)
                else:
                    # Compare modification times
                    if last_modified_datetime <= indexed_last_modified_str:
                        logging.info(f"[sharepoint_files_indexer] '{file_name}' has not been modified since last indexing. Skipping.")
                        return  # Skip indexing as no changes detected
                    else:
                        # If the file has been modified, delete existing chunks and re-index
                        logging.debug(f"[sharepoint_files_indexer] '{file_name}' has been modified. Deleting existing chunks and re-indexing.")
                        await self.delete_existing_chunks(existing_chunks, file_name)

            # Chunk and index document
            chunks, errors, warnings = DocumentChunker().chunk_documents(data)

            if warnings:
                for warning in warnings:
                    logging.warning(f"[sharepoint_files_indexer] Warning when chunking {file_name}: {warning.get('message', 'No message')}")

            if errors:
                for error in errors:
                    logging.error(f"[sharepoint_files_indexer] Skipping {file_name}. Error: {error.get('message', 'No message')}")
                return  # Skip this file

            # Ingest the chunks into the index
            for chunk in chunks:
                chunk["id"] = f"{sharepoint_id}_{chunk.get('chunk_id', 'unknown')}"
                chunk["parent_id"] = sharepoint_id
                chunk["metadata_storage_path"] = document_url
                chunk["metadata_storage_name"] = file_name
                chunk["metadata_storage_last_modified"] = last_modified_datetime
                chunk["metadata_security_id"] = read_access_entity
                chunk["source"] = "sharepoint"

                try:
                    await self.search_client.index_document(self.index_name, chunk)
                except Exception as e:
                    logging.error(f"[sharepoint_files_indexer] Failed to index chunk for '{file_name}': {e}")

            logging.info(f"[sharepoint_files_indexer] Indexed {file_name} chunks.")

    async def run(self) -> None:
        """Main method to run the SharePoint files indexing process."""
        logging.info("[sharepoint_files_indexer] Started sharepoint files index run.")

        if not self.connector_enabled:
            logging.info(
                "[sharepoint_files_indexer] SharePoint connector is disabled. "
                "Set SHAREPOINT_CONNECTOR_ENABLED to 'true' to enable the connector."
            )
            return

        # Initialize clients and configurations
        if not await self.initialize_clients():
            return

        # Retrieve SharePoint files content
        try:
            files = self.sharepoint_data_reader.retrieve_sharepoint_files_content(
                site_domain=self.site_domain,
                site_name=self.site_name,
                folder_path=self.folder_path,
                file_formats=self.file_formats,
            )
            number_files = len(files) if files else 0
            logging.info(f"[sharepoint_files_indexer] Retrieved {number_files} files from SharePoint.")
        except Exception as e:
            logging.error(f"[sharepoint_files_indexer] Failed to retrieve files: {e}")
            return

        if not files:
            logging.info("[sharepoint_files_indexer] No files retrieved from SharePoint.")
            await self.search_client.close()
            return

        semaphore = asyncio.Semaphore(10)  # Limit concurrent file processing

        # Create tasks to process all files in parallel
        tasks = [self.process_file(file, semaphore) for file in files]
        await asyncio.gather(*tasks)

        # Close the AISearchClient
        try:
            await self.search_client.close()
            logging.debug("[sharepoint_files_indexer] Closed AISearchClient successfully.")
        except Exception as e:
            logging.error(f"[sharepoint_files_indexer] Failed to close AISearchClient: {e}")

        logging.info("[sharepoint_files_indexer] SharePoint connector finished.")

# Example usage
# To run the indexer, you would typically do the following in an async context:

# import asyncio
# 
# if __name__ == "__main__":
#     indexer = SharepointFilesIndexer()
#     asyncio.run(indexer.run())
