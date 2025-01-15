import os
import logging
from azure.search.documents.aio import SearchClient
from azure.search.documents.models import SearchMode
from azure.core.exceptions import AzureError
from azure.identity.aio import ManagedIdentityCredential, AzureCliCredential, ChainedTokenCredential
from typing import Any, Dict, List, Optional


class AISearchClient:
    """
    AISearchClient provides methods to index documents into an Azure AI Search index
    using Managed Identity or Azure CLI credentials for authentication.
    """

    def __init__(self):
        self.search_service_name = os.getenv("AZURE_SEARCH_SERVICE")
        if not self.search_service_name:
            logging.error("[aisearch] AZURE_SEARCH_SERVICE environment variable not set.")
            raise ValueError("AZURE_SEARCH_SERVICE environment variable not set.")

        self.endpoint = f"https://{self.search_service_name}.search.windows.net"

        # Initialize the ChainedTokenCredential
        try:
            self.credential = ChainedTokenCredential(
                ManagedIdentityCredential(),
                AzureCliCredential()
            )
            logging.debug("[aisearch] Initialized ChainedTokenCredential with ManagedIdentity and AzureCliCredential.")
        except Exception as e:
            logging.error(f"[aisearch] Failed to initialize credentials: {e}")
            raise

        self.clients = {}  # Cache SearchClient instances per index

    async def get_search_client(self, index_name: str) -> SearchClient:
        """
        Retrieves a cached SearchClient for the specified index or creates a new one if not cached.

        Parameters:
            index_name (str): The name of the Azure AI Search index.

        Returns:
            SearchClient: An instance of SearchClient for the specified index.
        """
        if index_name not in self.clients:
            try:
                self.clients[index_name] = SearchClient(
                    endpoint=self.endpoint,
                    index_name=index_name,
                    credential=self.credential
                )
                logging.debug(f"[aisearch] Initialized SearchClient for index '{index_name}'.")
            except Exception as e:
                logging.error(f"[aisearch] Failed to initialize SearchClient for index '{index_name}': {e}")
                raise
        return self.clients[index_name]

    async def index_document(self, index_name: str, document: dict):
        """
        Indexes a single document into the specified Azure AI Search index.

        Parameters:
            index_name (str): The name of the Azure AI Search index.
            document (dict): The JSON document to be indexed.
        """
        client = await self.get_search_client(index_name)

        try:
            result = await client.upload_documents(documents=[document])
            if result[0].succeeded:
                logging.info(f"[aisearch] Successfully indexed document into '{index_name}'.")
            else:
                error_messages = "; ".join([error["error"] for error in result[0].error_messages])
                logging.error(f"[aisearch] Failed to index document into '{index_name}': {error_messages}")
        except AzureError as e:
            logging.error(f"[aisearch] AzureError while indexing document into '{index_name}': {e}")
        except Exception as e:
            logging.error(f"[aisearch] Unexpected error while indexing document into '{index_name}': {e}")

    async def delete_document(self, index_name: str, key_field: str, key_value: str):
        """
        Deletes a document from the specified Azure AI Search index.

        Parameters:
            index_name (str): The name of the Azure AI Search index.
            key_field (str): The name of the key field in the index.
            key_value (str): The value of the key field for the document to delete.
        """
        client = await self.get_search_client(index_name)

        try:
            result = await client.delete_documents(key_field, [key_value])
            logging.info(f"[aisearch] Successfully deleted document with {key_field}='{key_value}' from '{index_name}'.")
        except AzureError as e:
            logging.error(f"[aisearch] AzureError while deleting document from '{index_name}': {e}")
        except Exception as e:
            logging.error(f"[aisearch] Unexpected error while deleting document from '{index_name}': {e}")

    async def delete_documents(self, index_name: str, key_field: str, key_values: List[str]):
        """
        Deletes multiple documents from the specified Azure AI Search index.

        Parameters:
            index_name (str): The name of the Azure AI Search index.
            key_field (str): The name of the key field in the index.
            key_values (List[str]): A list of key values identifying the documents to delete.
        """
        if not key_values:
            logging.warning("[aisearch] No key values provided for deletion.")
            return

        client = await self.get_search_client(index_name)

        try:
            # Prepare the delete actions
            actions = [{"@search.action": "delete", key_field: key_value} for key_value in key_values]

            # Azure AI Search supports batch operations, but there might be limits on batch size.
            # Here, we assume that the list is within acceptable limits. For very large lists, consider batching.
            result = await client.upload_documents(documents=actions)

            # Check results
            succeeded = 0
            failed = 0
            for res in result:
                if res.succeeded:
                    succeeded += 1
                else:
                    failed += 1
                    error_messages = "; ".join([error["error"] for error in res.error_messages])
                    logging.error(f"[aisearch] Failed to delete a document: {error_messages}")

            logging.info(f"[aisearch] Deleted {succeeded} documents from '{index_name}'.")
            if failed > 0:
                logging.warning(f"[aisearch] Failed to delete {failed} documents from '{index_name}'. Check logs for details.")
        except AzureError as e:
            logging.error(f"[aisearch] AzureError while deleting documents from '{index_name}': {e}")
        except Exception as e:
            logging.error(f"[aisearch] Unexpected error while deleting documents from '{index_name}': {e}")

    async def search_documents(
        self,
        index_name: str,
        search_text: str = "*",
        filter_field: Optional[str] = None,
        filter_value: Optional[Any] = None,
        filter_operator: str = "eq",
        select_fields: Optional[List[str]] = None,
        top: int = 10,
        skip: int = 0,
        order_by: Optional[str] = None,
        filter_str: Optional[str] = None  # <-- Add this
    ) -> Dict[str, Any]:
        client = await self.get_search_client(index_name)
        try:
            # Construct the filter string only if filter_str is not provided
            if filter_str is None and filter_field and filter_value is not None:
                if isinstance(filter_value, str):
                    escaped_value = filter_value.replace("'", "''")
                    filter_str = f"{filter_field} {filter_operator} '{escaped_value}'"
                else:
                    filter_str = f"{filter_field} {filter_operator} {filter_value}"

            search_kwargs = {
                "search_text": search_text,
                "filter": filter_str,
                "order_by": order_by,
                "search_mode": SearchMode.ALL,
                "skip": skip
            }

            if select_fields:
                search_kwargs["select"] = select_fields

            if top > 0:
                search_kwargs["top"] = top
            else:
                search_kwargs["top"] = 1000

            results = await client.search(**search_kwargs)
            documents = []
            async for result in results:
                documents.append(result)
                if top > 0 and len(documents) >= top:
                    break

            return {
                "count": len(documents),
                "documents": documents
            }

        except AzureError as e:
            logging.error(f"[aisearch] AzureError while searching documents in '{index_name}': {e}")
            return {"count": 0, "documents": [], "error": str(e)}
        except Exception as e:
            logging.error(f"[aisearch] Unexpected error while searching documents in '{index_name}': {e}")
            return {"count": 0, "documents": [], "error": str(e)}

    async def close(self):
        """
        Closes all SearchClient instances and the credential.
        """
        for index_name, client in self.clients.items():
            await client.close()
            logging.debug(f"[aisearch] Closed SearchClient for index '{index_name}'.")
        self.clients.clear()

        # Close the ChainedTokenCredential if it has a close method
        if hasattr(self.credential, "close"):
            await self.credential.close()
            logging.debug("[aisearch] Closed ChainedTokenCredential.")
