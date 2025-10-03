import logging
import os
import asyncio
import aiohttp
from collections import defaultdict
from typing import Any, Dict, List, Optional
from tools import KeyVaultClient, AISearchClient, SharePointMetadataStreamer
from dependencies import get_config

app_config_client = get_config()

# ANSI color codes for logging
DEBUG_COLOR   = "\033[36m"  # Cyan
INFO_COLOR    = "\033[35m"  # Magenta
WARNING_COLOR = "\033[33m"  # Yellow
ERROR_COLOR   = "\033[31m"  # Red
RESET_COLOR   = "\033[0m"

class SharePointDeletedItemsCleaner:
    def __init__(self):
        # Load environment
        self.connector_enabled = app_config_client.get("SHAREPOINT_CONNECTOR_ENABLED", default="false").lower() == "true"
        self.tenant_id = app_config_client.get("SHAREPOINT_TENANT_ID")
        self.client_id = app_config_client.get("SHAREPOINT_CLIENT_ID")
        self.client_secret_name = app_config_client.get("SHAREPOINT_CLIENT_SECRET_NAME", default="sharepointClientSecret")
        self.index_name = app_config_client.get("SEARCH_RAG_INDEX_NAME", default="ragindex")
        self.site_domain = app_config_client.get("SHAREPOINT_SITE_DOMAIN")
        self.site_name = app_config_client.get("SHAREPOINT_SITE_NAME")
        self.sub_site_name = app_config_client.get("SHAREPOINT_SUB_SITE_NAME", default=None, allow_none=True)
        self.drive_name = app_config_client.get("SHAREPOINT_DRIVE_NAME")

        self.keyvault_client: Optional[KeyVaultClient] = None
        self.client_secret: Optional[str] = None
        self.search_client: Optional[AISearchClient] = None
        self.sharepoint_data_reader: Optional[SharePointMetadataStreamer] = None
        self.site_id: Optional[str] = None      # Graph site ID
        self.drive_id: Optional[str] = None     # Will be resolved from drive_name
        self.access_token: Optional[str] = None # Graph access token

    async def initialize_clients(self, session: aiohttp.ClientSession) -> bool:
        logging.debug(DEBUG_COLOR + "[sharepoint_purge] Initializing clients..." + RESET_COLOR)
        # Retrieve SharePoint client secret
        try:
            self.keyvault_client = KeyVaultClient()
            self.client_secret = await self.keyvault_client.get_secret(self.client_secret_name)
            logging.info(INFO_COLOR + "[sharepoint_purge] Retrieved SharePoint client secret." + RESET_COLOR)
        except Exception as e:
            logging.error(ERROR_COLOR + f"[sharepoint_purge] Secret retrieval failed: {e}" + RESET_COLOR)
            return False
        finally:
            if self.keyvault_client:
                await self.keyvault_client.close()

        # Check required env vars
        required = {
            "SHAREPOINT_TENANT_ID": self.tenant_id,
            "SHAREPOINT_CLIENT_ID": self.client_id,
            "SHAREPOINT_SITE_DOMAIN": self.site_domain,
            "SHAREPOINT_SITE_NAME": self.site_name,
            "SHAREPOINT_DRIVE_NAME": self.drive_name,
            "SEARCH_RAG_INDEX_NAME": self.index_name,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            logging.error(ERROR_COLOR + f"[sharepoint_purge] Missing env vars: {', '.join(missing)}" + RESET_COLOR)
            return False

        # Initialize Azure Search client
        try:
            self.search_client = AISearchClient()
            logging.info(INFO_COLOR + "[sharepoint_purge] Initialized AISearchClient." + RESET_COLOR)
        except Exception as e:
            logging.error(ERROR_COLOR + f"[sharepoint_purge] AISearchClient init failed: {e}" + RESET_COLOR)
            return False

        # Initialize SharePointMetadataStreamer to resolve drive_id from drive_name
        try:
            self.sharepoint_data_reader = SharePointMetadataStreamer(
                tenant_id=self.tenant_id,
                client_id=self.client_id,
                client_secret=self.client_secret,
            )
            self.sharepoint_data_reader._msgraph_auth()
            self.site_id, self.drive_id = self.sharepoint_data_reader._get_site_and_drive_ids(
                self.site_domain, self.site_name, self.sub_site_name, self.drive_name
            )
            logging.info(INFO_COLOR + f"[sharepoint_purge] Resolved site_id: {self.site_id}, drive_id: {self.drive_id}" + RESET_COLOR)
        except Exception as e:
            logging.error(ERROR_COLOR + f"[sharepoint_purge] Failed to resolve site and drive IDs: {e}" + RESET_COLOR)
            return False

        return True

    async def get_graph_access_token(self, session: aiohttp.ClientSession) -> Optional[str]:
        logging.debug(DEBUG_COLOR + "[sharepoint_purge] Requesting Graph access token..." + RESET_COLOR)
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default",
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        try:
            async with session.post(token_url, headers=headers, data=data) as resp:
                if resp.status == 200:
                    token_json = await resp.json()
                    logging.info(INFO_COLOR + "[sharepoint_purge] Obtained Graph access token." + RESET_COLOR)
                    return token_json.get("access_token")
                text = await resp.text()
                logging.error(ERROR_COLOR + f"[sharepoint_purge] Token request failed {resp.status}: {text}" + RESET_COLOR)
                return None
        except Exception as e:
            logging.error(ERROR_COLOR + f"[sharepoint_purge] Exception requesting token: {e}" + RESET_COLOR)
            return None

    async def _fetch_all_indexed_docs(self) -> List[Dict[str, Any]]:
        logging.info(WARNING_COLOR + "[sharepoint_purge] Fetching indexed documents..." + RESET_COLOR)
        docs: List[Dict[str, Any]] = []
        top, skip = 1000, 0

        while True:
            resp = await self.search_client.search_documents(
                index_name=self.index_name,
                search_text="*",
                filter_str="parent_id ne null and source eq 'sharepoint'",
                select_fields=["id", "parent_id", "metadata_storage_name"],
                top=top,
                skip=skip,
            )
            batch = resp.get("documents", [])
            docs.extend(batch)
            logging.debug(DEBUG_COLOR + f"[sharepoint_purge] Fetched {len(docs)} docs so far" + RESET_COLOR)
            if len(batch) < top:
                break
            skip += top

        logging.info(INFO_COLOR + f"[sharepoint_purge] Retrieved {len(docs)} documents from index." + RESET_COLOR)
        return docs

    async def check_sharepoint_item_exists(
        self,
        sharepoint_item_id: Any,
        headers: Dict[str, str],
        semaphore: asyncio.Semaphore,
        session: aiohttp.ClientSession,
        retries: int = 3
    ) -> bool:
        if not self.drive_id:
            logging.error(ERROR_COLOR + f"[sharepoint_purge] SHAREPOINT_DRIVE_NAME could not be resolved to drive_id; cannot check item existence." + RESET_COLOR)
            return False

        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives/{self.drive_id}/items/{sharepoint_item_id}"
        for attempt in range(retries):
            async with semaphore:
                try:
                    async with session.get(url, headers=headers) as resp:
                        if resp.status == 200:
                            logging.debug(DEBUG_COLOR + f"[sharepoint_purge] Exists: {sharepoint_item_id}" + RESET_COLOR)
                            return True
                        if resp.status == 404:
                            logging.warning(WARNING_COLOR + f"[sharepoint_purge] Not found: {sharepoint_item_id}" + RESET_COLOR)
                            return False
                        if resp.status == 429:
                            ra = int(resp.headers.get("Retry-After", "1"))
                            logging.warning(WARNING_COLOR + f"[sharepoint_purge] 429 on {sharepoint_item_id}, retry after {ra}s" + RESET_COLOR)
                            await asyncio.sleep(ra)
                            continue
                        text = await resp.text()
                        logging.error(ERROR_COLOR + f"[sharepoint_purge] Unexpected status {resp.status} on {sharepoint_item_id}: {text}" + RESET_COLOR)
                        return False
                except Exception as e:
                    backoff = 2 ** attempt
                    logging.error(ERROR_COLOR + f"[sharepoint_purge] Exception on {sharepoint_item_id}: {e}, retrying in {backoff}s" + RESET_COLOR)
                    await asyncio.sleep(backoff)
        logging.warning(WARNING_COLOR + f"[sharepoint_purge] Giving up on {sharepoint_item_id} after {retries} attempts" + RESET_COLOR)
        return False

    async def purge_deleted_files(self) -> None:
        logging.info(INFO_COLOR + "[sharepoint_purge] Starting purge process." + RESET_COLOR)
        if not self.connector_enabled:
            logging.info(INFO_COLOR + "[sharepoint_purge] Connector disabled." + RESET_COLOR)
            return

        session = aiohttp.ClientSession()
        try:
            # Initialize clients (which now resolves drive_id from drive_name)
            if not await self.initialize_clients(session):
                return

            # Get access token for Graph API calls
            token = await self.get_graph_access_token(session)
            if not token:
                return
            self.access_token = token

            docs = await self._fetch_all_indexed_docs()
            if not docs:
                return

            item_id_to_index_ids = defaultdict(list)
            for doc in docs:
                sid = doc.get("parent_id")
                iid = doc.get("id")
                if sid and iid:
                    item_id_to_index_ids[sid].append(iid)

            ids = list(item_id_to_index_ids.keys())
            logging.info(INFO_COLOR + f"[sharepoint_purge] Checking existence for {len(ids)} SharePoint items..." + RESET_COLOR)

            headers = {"Authorization": f"Bearer {self.access_token}"}
            sem = asyncio.Semaphore(10)
            results = await asyncio.gather(*(self.check_sharepoint_item_exists(sid, headers, sem, session) for sid in ids))
            logging.info(INFO_COLOR + f"[sharepoint_purge] Completed existence checks for {len(results)} items." + RESET_COLOR)

            to_delete = [idx for sid, exists in zip(ids, results) if not exists for idx in item_id_to_index_ids[sid]]
            logging.info(INFO_COLOR + f"[sharepoint_purge] {len(to_delete)} index documents to purge." + RESET_COLOR)
            if not to_delete:
                return

            batch_size = 100
            for i in range(0, len(to_delete), batch_size):
                batch = to_delete[i : i + batch_size]
                try:
                    await self.search_client.delete_documents(
                        index_name=self.index_name,
                        key_field="id",
                        key_values=batch
                    )
                    logging.info(INFO_COLOR + f"[sharepoint_purge] Purged batch of {len(batch)} docs." + RESET_COLOR)
                except Exception as e:
                    logging.error(ERROR_COLOR + f"[sharepoint_purge] Failed to purge batch at offset {i}: {e}" + RESET_COLOR)

        finally:
            await session.close()
            logging.debug(DEBUG_COLOR + "[sharepoint_purge] Closed aiohttp ClientSession." + RESET_COLOR)
            if self.search_client:
                await self.search_client.close()
                logging.debug(DEBUG_COLOR + "[sharepoint_purge] Closed AISearchClient." + RESET_COLOR)
            logging.info(INFO_COLOR + "[sharepoint_purge] Purge process complete." + RESET_COLOR)

    async def run(self) -> None:
        await self.purge_deleted_files()