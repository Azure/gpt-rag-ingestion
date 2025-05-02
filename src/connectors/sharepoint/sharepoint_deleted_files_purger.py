import logging
import os
import asyncio
import aiohttp
from collections import defaultdict
from tools import KeyVaultClient
from tools import AISearchClient
from typing import Any, Dict, List, Optional


class SharepointDeletedFilesPurger:
    def __init__(self):
        # Initialize configuration from environment variables
        self.connector_enabled = os.getenv("SHAREPOINT_CONNECTOR_ENABLED", "false").lower() == "true"
        self.tenant_id = os.getenv("SHAREPOINT_TENANT_ID")
        self.client_id = os.getenv("SHAREPOINT_CLIENT_ID")
        self.client_secret_name = os.getenv("SHAREPOINT_CLIENT_SECRET_NAME", "sharepointClientSecret")
        self.index_name = os.getenv("AZURE_SEARCH_SHAREPOINT_INDEX_NAME", "ragindex")
        self.site_domain = os.getenv("SHAREPOINT_SITE_DOMAIN")
        self.site_name = os.getenv("SHAREPOINT_SITE_NAME")
        
        self.keyvault_client: Optional[KeyVaultClient] = None
        self.client_secret: Optional[str] = None
        self.search_client: Optional[AISearchClient] = None
        self.site_id: Optional[str] = None
        self.access_token: Optional[str] = None

    async def initialize_clients(self) -> bool:
        """Initialize KeyVaultClient, retrieve secrets, and initialize AISearchClient."""
        # Initialize Key Vault Client and retrieve SharePoint client secret
        try:
            self.keyvault_client = KeyVaultClient()
            self.client_secret = await self.keyvault_client.get_secret(self.client_secret_name)
            logging.debug("[sharepoint_purge_deleted_files] Retrieved sharepointClientSecret secret from Key Vault.")
        except Exception as e:
            logging.error(f"[sharepoint_purge_deleted_files] Failed to retrieve secret from Key Vault: {e}")
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
            "AZURE_SEARCH_SHAREPOINT_INDEX_NAME": self.index_name
        }

        missing_env_vars = [var for var, value in required_vars.items() if not value]

        if missing_env_vars:
            logging.error(
                f"[sharepoint_purge_deleted_files] Missing environment variables: {', '.join(missing_env_vars)}. "
                "Please set all required environment variables."
            )
            return False

        if not self.client_secret:
            logging.error(
                "[sharepoint_purge_deleted_files] SharePoint connector secret is not properly configured. "
                "Missing secret: sharepointClientSecret. Please set the required secret in Key Vault."
            )
            return False

        # Initialize AISearchClient
        try:
            self.search_client = AISearchClient()
            logging.debug("[sharepoint_purge_deleted_files] Initialized AISearchClient successfully.")
        except ValueError as ve:
            logging.error(f"[sharepoint_purge_deleted_files] AISearchClient initialization failed: {ve}")
            return False
        except Exception as e:
            logging.error(f"[sharepoint_purge_deleted_files] Unexpected error during AISearchClient initialization: {e}")
            return False

        return True

    async def get_graph_access_token(self) -> Optional[str]:
        """Obtain access token for Microsoft Graph API."""
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default"
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(token_url, headers=headers, data=data) as resp:
                    if resp.status == 200:
                        token_response = await resp.json()
                        access_token = token_response.get("access_token")
                        logging.debug("[sharepoint_purge_deleted_files] Successfully obtained access token for Microsoft Graph API.")
                        return access_token
                    else:
                        error_response = await resp.text()
                        logging.error(f"[sharepoint_purge_deleted_files] Failed to obtain access token: {resp.status} - {error_response}")
                        return None
            except Exception as e:
                logging.error(f"[sharepoint_purge_deleted_files] Exception while obtaining access token: {e}")
                return None

    async def get_site_id(self) -> Optional[str]:
        """Retrieve the SharePoint site ID using Microsoft Graph API."""
        access_token = await self.get_graph_access_token()
        if not access_token:
            return None

        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_domain}:/sites/{self.site_name}?$select=id"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        site_id = data.get("id", None)
                        if site_id:
                            logging.info("[sharepoint_purge_deleted_files] Successfully retrieved site ID.")
                            return site_id
                        else:
                            logging.error("[sharepoint_purge_deleted_files] 'id' field not found in site response.")
                            return None
                    else:
                        error_response = await resp.text()
                        logging.error(f"[sharepoint_purge_deleted_files] Failed to retrieve site ID: {resp.status} - {error_response}")
                        return None
            except Exception as e:
                logging.error(f"[sharepoint_purge_deleted_files] Exception while retrieving site ID: {e}")
                return None

    async def check_parent_id_exists(self, parent_id: Any, headers: Dict[str, str], semaphore: asyncio.Semaphore) -> bool:
        """Check if a SharePoint parent ID exists."""
        check_url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drive/items/{parent_id}"
        async with semaphore:
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(check_url, headers=headers) as resp:
                        if resp.status == 200:
                            logging.debug(f"[sharepoint_purge_deleted_files] SharePoint ID {parent_id} exists.")
                            return True
                        elif resp.status == 404:
                            logging.debug(f"[sharepoint_purge_deleted_files] SharePoint ID {parent_id} does not exist.")
                            return False
                        else:
                            error_text = await resp.text()
                            logging.error(f"[sharepoint_purge_deleted_files] Error checking SharePoint ID {parent_id}: {resp.status} - {error_text}")
                            return False
                except Exception as e:
                    logging.error(f"[sharepoint_purge_deleted_files] Exception while checking SharePoint ID {parent_id}: {e}")
                    return False  # Assume it doesn't exist if there's an error

    async def purge_deleted_files(self) -> None:
        """Main method to purge deleted SharePoint files from Azure Search index."""
        logging.info("[sharepoint_purge_deleted_files] Started SharePoint purge connector function.")

        if not self.connector_enabled:
            logging.info(
                "[sharepoint_purge_deleted_files] SharePoint purge connector is disabled. "
                "Set SHAREPOINT_CONNECTOR_ENABLED to 'true' to enable the connector."
            )
            return

        # Initialize clients and configurations
        if not await self.initialize_clients():
            return

        # Obtain the site_id
        self.site_id = await self.get_site_id()
        if not self.site_id:
            logging.error("[sharepoint_purge_deleted_files] Unable to retrieve site_id. Aborting operation.")
            return

        # Obtain access token for item checks
        self.access_token = await self.get_graph_access_token()
        if not self.access_token:
            logging.error("[sharepoint_purge_deleted_files] Cannot proceed without access token.")
            await self.search_client.close()
            return

        headers = {
            "Authorization": f"Bearer {self.access_token}"
        }

        # Retrieve all documents with sharepoint_id != null from Azure Search
        logging.info("[sharepoint_purge_deleted_files] Retrieving documents from Azure Search index.")
        try:
            search_results = await self.search_client.search_documents(
                index_name=self.index_name,
                search_text="*",
                filter_str="parent_id ne null and source eq 'sharepoint'",
                select_fields=["parent_id", "id", "metadata_storage_name"],
                top=0
            )
        except Exception as e:
            logging.error(f"[sharepoint_purge_deleted_files] Failed to retrieve documents from Azure Search: {e}")
            await self.search_client.close()
            return

        documents = search_results.get("documents", [])
        logging.info(f"[sharepoint_purge_deleted_files] Retrieved {len(documents)} SharePoint document chunks.")

        if not documents:
            logging.info("[sharepoint_purge_deleted_files] No document chunks to purge. Exiting function.")
            await self.search_client.close()
            return

        # Map parent_id to a list of document ids
        sharepoint_to_doc_ids = defaultdict(list)
        for doc in documents:
            if "parent_id" in doc and "id" in doc:
                sharepoint_to_doc_ids[doc["parent_id"]].append(doc["id"])

        parent_ids = list(sharepoint_to_doc_ids.keys())
        logging.info(f"[sharepoint_purge_deleted_files] Checking existence of {len(parent_ids)} SharePoint document(s).")

        semaphore = asyncio.Semaphore(10)  # Limit concurrent requests

        # Create tasks to check if parent IDs exist
        existence_tasks = [
            self.check_parent_id_exists(parent_id, headers, semaphore) for parent_id in parent_ids
        ]
        existence_results = await asyncio.gather(*existence_tasks)

        # Identify all document IDs to delete for non-existing parent_ids
        doc_ids_to_delete = []
        for parent_id, exists in zip(parent_ids, existence_results):
            if not exists:
                doc_ids_to_delete.extend(sharepoint_to_doc_ids[parent_id])

        logging.info(f"[sharepoint_purge_deleted_files] {len(doc_ids_to_delete)} document chunks identified for purging.")

        if doc_ids_to_delete:
            batch_size = 100
            for i in range(0, len(doc_ids_to_delete), batch_size):
                batch = doc_ids_to_delete[i:i + batch_size]
                try:
                    await self.search_client.delete_documents(
                        index_name=self.index_name,
                        key_field="id",
                        key_values=batch
                    )
                    logging.info(f"[sharepoint_purge_deleted_files] Purging batch of {len(batch)} documents from Azure Search.")
                except Exception as e:
                    logging.error(f"[sharepoint_purge_deleted_files] Failed to purge batch starting at index {i}: {e}")
        else:
            logging.info("[sharepoint_purge_deleted_files] No documents to purge.")

        # Close the AISearchClient
        try:
            await self.search_client.close()
            logging.debug("[sharepoint_purge_deleted_files] Closed AISearchClient successfully.")
        except Exception as e:
            logging.error(f"[sharepoint_purge_deleted_files] Failed to close AISearchClient: {e}")

        logging.info("[sharepoint_purge_deleted_files] Completed SharePoint purge connector function.")

    async def run(self) -> None:
        """Run the purge process."""
        await self.purge_deleted_files()


# Example usage
# To run the purge process, you would typically do the following in an async context:

# import asyncio
# 
# if __name__ == "__main__":
#     purger = SharepointDeletedFilesPurger()
#     asyncio.run(purger.run())
