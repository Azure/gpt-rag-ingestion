import logging
from collections import Counter
from contextlib import AsyncExitStack
from math import isfinite
from azure.search.documents.aio import SearchClient
from azure.search.documents.models import IndexingResult, SearchMode
from azure.core.exceptions import AzureError, HttpResponseError
from azure.identity.aio import ManagedIdentityCredential, AzureCliCredential, ChainedTokenCredential
from typing import Any, AsyncIterator, Dict, List, Optional
from dependencies import get_config
from tools.credentials import get_azure_client_id
from telemetry import audit

# Elevated-read header – bypasses permission filtering for service-side queries.
_ELEVATED_HEADERS = {"x-ms-enable-elevated-read": "true"}
_QUERY_SOURCE_AUTHORIZATION_HEADER = "x-ms-query-source-authorization"
_ELEVATED_API_VERSION = "2025-11-01-preview"

app_config_client = get_config()


def confirmed_result_keys(key_values: List[str], result: List[IndexingResult]) -> List[str]:
    """Return only requested keys confirmed without excess duplicate responses."""
    requested = Counter(key_values)
    remaining = requested.copy()
    response_counts = Counter(res.key for res in result)
    confirmed = []
    for res in result:
        if remaining[res.key] <= 0 or response_counts[res.key] > requested[res.key]:
            logging.error("[aisearch] Search returned an unexpected result key.")
            continue
        remaining[res.key] -= 1
        if res.succeeded is True:
            confirmed.append(res.key)
    return confirmed


def require_confirmed_batch(documents: List[Dict[str, Any]], result: List[IndexingResult]) -> None:
    """Reject a worker batch unless Search confirms every requested document."""
    keys = [document["id"] for document in documents]
    if len(confirmed_result_keys(keys, result)) != len(keys):
        raise AzureError("Search did not confirm every requested document operation.")


def search_retry_delay(error: HttpResponseError, default: float) -> float:
    """Honor SDK retry headers without treating seconds as milliseconds."""
    headers = error.response.headers if error.response is not None else {}
    milliseconds = headers.get("retry-after-ms")
    raw = milliseconds if milliseconds is not None else headers.get("Retry-After")
    if not raw:
        return default
    try:
        seconds = float(raw) / 1000.0 if milliseconds is not None else float(raw)
    except (TypeError, ValueError):
        logging.warning("[aisearch] Invalid retry delay header; using exponential backoff.")
        return default
    if not isfinite(seconds) or seconds < 0:
        logging.warning("[aisearch] Non-finite or negative retry delay; using exponential backoff.")
        return default
    return max(default, seconds)


class AISearchClient:
    """
    AISearchClient provides methods to index documents into an Azure AI Search index
    using Managed Identity or Azure CLI credentials for authentication.
    """

    def __init__(self):
        self.search_service_name = app_config_client.get("SEARCH_SERVICE_NAME")
        if not self.search_service_name:
            logging.error("[aisearch] SEARCH_SERVICE_NAME environment variable not set.")
            raise ValueError("SEARCH_SERVICE_NAME environment variable not set.")

        self.endpoint = f"https://{self.search_service_name}.search.windows.net"

        # Initialize the ChainedTokenCredential
        client_id = get_azure_client_id(app_config_client)
        self.credential = ChainedTokenCredential(
            ManagedIdentityCredential(client_id=client_id),
            AzureCliCredential()
        )
        logging.debug("[aisearch] Initialized ChainedTokenCredential with ManagedIdentity and AzureCliCredential.")

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
            self.clients[index_name] = SearchClient(
                endpoint=self.endpoint,
                index_name=index_name,
                credential=self.credential,
                api_version=_ELEVATED_API_VERSION,
            )
            logging.debug(f"[aisearch] Initialized SearchClient for index '{index_name}'.")
        return self.clients[index_name]

    async def index_document(self, index_name: str, document: dict) -> bool:
        """
        Indexes a single document into the specified Azure AI Search index.

        Parameters:
            index_name (str): The name of the Azure AI Search index.
            document (dict): The JSON document to be indexed.
        """
        client = await self.get_search_client(index_name)

        try:
            result = await client.upload_documents(documents=[document])
            audit.record_search_batch_result(
                operation="upload_documents",
                documents=[document],
                result=result,
                source_type=index_name,
            )
            if (len(result) == 1 and result[0].succeeded is True
                    and result[0].key == document.get("id")):
                logging.info(f"[aisearch] Successfully indexed document into '{index_name}'.")
                return True
            else:
                logging.error("[aisearch] Search did not confirm the document upload.")
                return False
        except AzureError:
            logging.error("[aisearch] Search upload failed.")
            return False

    async def delete_document(self, index_name: str, key_field: str, key_value: str):
        """
        Deletes a document from the specified Azure AI Search index.

        Parameters:
            index_name (str): The name of the Azure AI Search index.
            key_field (str): The name of the key field in the index.
            key_value (str): The value of the key field for the document to delete.
        """
        outcome = await self.delete_documents(index_name, key_field, [key_value])
        if outcome["deleted"] != 1 or outcome["failed"]:
            raise AzureError("Search did not confirm the requested document deletion.")

    async def delete_documents(self, index_name: str, key_field: str, key_values: List[str]) -> Dict[str, int]:
        """
        Deletes multiple documents from the specified Azure AI Search index.

        Parameters:
            index_name (str): The name of the Azure AI Search index.
            key_field (str): The name of the key field in the index.
            key_values (List[str]): A list of key values identifying the documents to delete.
        """
        if not key_values:
            logging.warning("[aisearch] No key values provided for deletion.")
            return {"deleted": 0, "failed": 0}

        client = await self.get_search_client(index_name)

        try:
            documents = [{key_field: key_value} for key_value in key_values]
            result = await client.delete_documents(documents=documents)

            audit.record_search_batch_result(
                operation="delete_documents",
                documents=documents,
                result=result,
                source_type=index_name,
                key_field=key_field,
            )

            succeeded = len(confirmed_result_keys(key_values, result))
            failed = len(key_values) - succeeded
            logging.info(f"[aisearch] Deleted {succeeded} documents from '{index_name}'.")
            if failed > 0:
                logging.warning(f"[aisearch] Failed to delete {failed} documents from '{index_name}'. Check logs for details.")
            return {"deleted": succeeded, "failed": failed}
        except AzureError:
            logging.error("[aisearch] Search deletion failed.")
            return {"deleted": 0, "failed": len(key_values)}

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
        filter_str: Optional[str] = None,
        use_elevated_read: bool = True,
        query_source_authorization: Optional[str] = None,
    ) -> Dict[str, Any]:
        client = await self.get_search_client(index_name)
        try:
            if use_elevated_read and query_source_authorization:
                raise ValueError(
                    "Elevated read and query-source authorization are mutually exclusive."
                )

            # Construct the filter string only if filter_str is not provided
            if filter_str is None and filter_field and filter_value is not None:
                if isinstance(filter_value, str):
                    escaped_value = filter_value.replace("'", "''")
                    filter_str = f"{filter_field} {filter_operator} '{escaped_value}'"
                else:
                    filter_str = f"{filter_field} {filter_operator} {filter_value}"

            headers: Dict[str, str] = {}
            if use_elevated_read:
                headers = _ELEVATED_HEADERS
            elif query_source_authorization:
                headers = {
                    _QUERY_SOURCE_AUTHORIZATION_HEADER: (
                        f"Bearer {query_source_authorization}"
                    )
                }

            search_kwargs = {
                "search_text": search_text,
                "headers": headers,
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
            logging.error("[aisearch] Search query failed (%s)", type(e).__name__)
            return {"count": 0, "documents": [], "error": "Azure AI Search query failed."}
        except Exception as e:
            logging.error("[aisearch] Unexpected Search query failure (%s)", type(e).__name__)
            return {"count": 0, "documents": [], "error": "Azure AI Search query failed."}

    async def iter_documents(
        self, index_name: str, select_fields: List[str],
    ) -> AsyncIterator[Dict[str, Any]]:
        """Stream every page for service-side maintenance; never return a partial success."""
        client = await self.get_search_client(index_name)
        results = await client.search(
            search_text="*", select=select_fields, headers=_ELEVATED_HEADERS,
            search_mode=SearchMode.ALL,
        )
        async for document in results:
            yield document

    async def close(self):
        """
        Closes all SearchClient instances and the credential.
        """
        clients = list(self.clients.values())
        self.clients.clear()
        async with AsyncExitStack() as cleanup:
            if hasattr(self.credential, "close"):
                cleanup.push_async_callback(self.credential.close)
            for client in reversed(clients):
                cleanup.push_async_callback(client.close)
        logging.debug("[aisearch] Closed Search clients and credential.")
