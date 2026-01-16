import asyncio
import logging
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple
from urllib.parse import quote, urlencode
from uuid import UUID

import aiohttp
import msal
from aiohttp import ClientResponseError

from jobs.sharepoint_ingestion_config import LOG_SCOPE, SharePointConfig
from tools import KeyVaultClient


class SharePointGraphClient:
    def __init__(self, cfg: SharePointConfig, key_vault: KeyVaultClient) -> None:
        self.cfg = cfg
        self._kv = key_vault
        self._graph_base = "https://graph.microsoft.com/v1.0"
        self._graph_beta_base = "https://graph.microsoft.com/beta"
        self._graph_token: Optional[str] = None
        self._client_secret: Optional[str] = None
        # Hidden/system SharePoint lists that frequently back lookup columns but are not readable via Graph list APIs.
        self._hidden_lookup_list_ids: set[str] = {
            "appprincipals",
            "userinfo",
            "taxonomyhiddenlist",
            "taxonomy_x0020_hidden_x0020_list",
            "workflowtasks",
            "workflowhistory",
            "reusable content",
            "reusablecontent",
        }

    async def ensure_token(self) -> None:
        if self._graph_token:
            return

        self._client_secret = await self._kv.get_secret(self.cfg.client_secret_name)

        def _acquire() -> str:
            app = msal.ConfidentialClientApplication(
                client_id=self.cfg.client_id,
                authority=f"https://login.microsoftonline.com/{self.cfg.tenant_id}",
                client_credential=self._client_secret,
            )
            token = app.acquire_token_silent(["https://graph.microsoft.com/.default"], account=None)
            if not token:
                token = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
            if not token or "access_token" not in token:
                raise RuntimeError("MS Graph token acquisition failed")
            return token["access_token"]

        self._graph_token = await asyncio.to_thread(_acquire)

    async def get_site_id(self, session: aiohttp.ClientSession, site_domain: str, site_name: str) -> Optional[str]:
        url = f"{self._graph_base}/sites/{site_domain}:/sites/{site_name}?$select=id"
        data = await self._gget(session, url)
        return data.get("id")

    async def get_collection_id(self, session: aiohttp.ClientSession, site_id: str, collection_name: str) -> Optional[str]:
        """LEGACY: resolve list id via display name or internal name."""
        url = f"{self._graph_base}/sites/{site_id}/lists?$select=id,displayName,name"
        data = await self._gget(session, url)
        for lst in data.get("value", []):
            if lst.get("displayName") == collection_name or lst.get("name") == collection_name:
                return lst.get("id")
        return None

    async def get_list_metadata(
        self,
        session: aiohttp.ClientSession,
        site_id: str,
        collection_id: str,
    ) -> Dict[str, Any]:
        """Fetch lightweight metadata for a SharePoint list (display name + base web URL)."""
        url = f"{self._graph_base}/sites/{site_id}/lists/{collection_id}?$select=displayName,name,webUrl"
        return await self._gget(session, url)

    async def get_lookup_columns(
        self,
        session: aiohttp.ClientSession,
        site_id: str,
        collection_id: str,
    ) -> List[Dict[str, Any]]:
        """Return column metadata for lookup fields in a list."""
        url = f"{self._graph_base}/sites/{site_id}/lists/{collection_id}/columns?$select=name,displayName,lookup"
        data = await self._gget(session, url)
        columns: List[Dict[str, Any]] = []
        for col in data.get("value", []):
            lookup_info = col.get("lookup") or {}
            if lookup_info.get("listId") and lookup_info.get("columnName"):
                columns.append(col)
        return columns

    async def get_lookup_field_value(
        self,
        session: aiohttp.ClientSession,
        site_id: str,
        target_list_id: str,
        target_item_id: str,
        target_field_name: str,
    ) -> Optional[Any]:
        """Fetch a specific field from a target list item for lookup expansion."""
        if self._is_hidden_lookup_list(target_list_id):
            logging.debug(
                f"{LOG_SCOPE} lookup field fetch skipped (hidden list) for {target_list_id} item {target_item_id}"
            )
            return None
        url = (
            f"{self._graph_base}/sites/{site_id}/lists/{target_list_id}/items/{target_item_id}"
            f"?$expand=fields($select={target_field_name})"
        )
        try:
            data = await self._gget(session, url)
        except ClientResponseError as exc:
            if exc.status in (400, 404):
                logging.debug(
                    f"{LOG_SCOPE} lookup field fetch failed ({exc.status}) for list {target_list_id} item {target_item_id}"
                )
                return None
            raise

        fields = data.get("fields", {})
        return fields.get(target_field_name)

    async def get_drive_item(
        self,
        session: aiohttp.ClientSession,
        site_id: str,
        collection_id: str,
        item_id: str,
    ) -> Optional[Dict[str, Any]]:
        url = f"{self._graph_base}/sites/{site_id}/lists/{collection_id}/items/{item_id}/driveItem"
        try:
            return await self._gget(session, url)
        except ClientResponseError as exc:
            if exc.status in (400, 404):
                logging.debug(
                    f"{LOG_SCOPE} driveItem lookup failed ({exc.status}) for list item {item_id} in list {collection_id}"
                )
                return None
            raise

    async def download_drive_item(
        self,
        session: aiohttp.ClientSession,
        drive_item: Dict[str, Any],
    ) -> bytes:
        download_url = drive_item.get("@microsoft.graph.downloadUrl")
        if download_url:
            delay = 1.0
            for attempt in range(6):
                try:
                    async with session.get(download_url) as response:
                        if response.status == 429:
                            retry = int(response.headers.get("Retry-After", "1"))
                            logging.warning(f"{LOG_SCOPE} driveItem download throttled; retry in {retry}s")
                            await asyncio.sleep(retry)
                            continue
                        response.raise_for_status()
                        return await response.read()
                except Exception as exc:  # noqa: BLE001
                    backoff = min(2 ** attempt, 30)
                    logging.warning(f"{LOG_SCOPE} driveItem download retry in {backoff}s: {exc}")
                    await asyncio.sleep(backoff)
            raise RuntimeError("driveItem download failed after retries")

        item_id = drive_item.get("id")
        drive_id = drive_item.get("parentReference", {}).get("driveId")
        if item_id and drive_id:
            url = f"{self._graph_base}/drives/{drive_id}/items/{item_id}/content"
            return await self._gbytes(session, url)

        raise RuntimeError("driveItem missing download references")

    async def iter_items(
        self,
        session: aiohttp.ClientSession,
        site_id: str,
        collection_id: str,
        select_fields: Optional[List[str]] = None,
        filter_expression: Optional[str] = None,
        page_size: int = 200,
        site_name: str = "",
        collection_name: str = "",
    ) -> AsyncIterator[Dict[str, Any]]:
        base = f"{self._graph_base}/sites/{site_id}/lists/{collection_id}/items"

        if select_fields:
            expand = f"fields($select={','.join(select_fields)})"
        else:
            expand = "fields"

        params = {"$expand": expand, "$top": str(page_size)}
        if filter_expression:
            params["$filter"] = filter_expression
            logging.info(
                f"[{self.cfg.indexer_name}][DISCOVERY] Using OData filter | siteName={site_name} "
                f"collection={collection_name} filter=\"{filter_expression}\""
            )
        else:
            logging.info(
                f"[{self.cfg.indexer_name}][DISCOVERY] No OData filter applied | "
                f"siteName={site_name} collection={collection_name}"
            )

        url = f"{base}?{urlencode(params)}"
        page_no = 0
        total_items = 0
        while url:
            data = await self._gget(session, url)
            values = data.get("value", [])
            page_no += 1
            total_items += len(values)
            logging.info(
                f"[{self.cfg.indexer_name}][DISCOVERY] Page {page_no} fetched | itemsInPage={len(values)} "
                f"totalSoFar={total_items} hasNextLink={bool(data.get('@odata.nextLink'))}"
            )
            for item in values:
                yield item
            url = data.get("@odata.nextLink")

        logging.info(
            f"[{self.cfg.indexer_name}][DISCOVERY] Discovery complete | siteName={site_name} "
            f"collection={collection_name} totalItemsDiscovered={total_items} totalPages={page_no}"
        )

    async def iter_item_ids(
        self,
        session: aiohttp.ClientSession,
        site_id: str,
        collection_id: str,
        *,
        page_size: int = 200,
        site_name: str = "",
        collection_name: str = "",
    ) -> AsyncIterator[str]:
        base = f"{self._graph_base}/sites/{site_id}/lists/{collection_id}/items"
        params = {"$select": "id", "$top": str(page_size)}
        url = f"{base}?{urlencode(params)}"
        page_no = 0
        total_emitted = 0
        while url:
            data = await self._gget(session, url)
            values = data.get("value", [])
            page_no += 1
            logging.info(
                f"[{self.cfg.indexer_name}][PURGER] Item-id page {page_no} | siteName={site_name} collection={collection_name} "
                f"itemsInPage={len(values)} hasNextLink={bool(data.get('@odata.nextLink'))}"
            )
            for entry in values:
                if "id" in entry:
                    total_emitted += 1
                    yield str(entry["id"])
            url = data.get("@odata.nextLink")

        logging.info(
            f"[{self.cfg.indexer_name}][PURGER] Item-id enumeration complete | siteName={site_name} "
            f"collection={collection_name} totalItems={total_emitted} totalPages={page_no}"
        )

    async def list_attachments_meta(
        self,
        session: aiohttp.ClientSession,
        site_id: str,
        collection_id: str,
        item_id: str,
        collection_name: str,
    ) -> List[Dict[str, Any]]:
        try:
            url = f"{self._graph_base}/sites/{site_id}/lists/{collection_id}/items/{item_id}/attachments"
            data = await self._gget(session, url)
            values = data.get("value", [])
            if isinstance(values, list) and values:
                return [
                    {
                        "name": attachment.get("name"),
                        "downloadUrl": f"{url}/{attachment.get('id')}/$value",
                    }
                    for attachment in values
                    if attachment.get("name") and attachment.get("id")
                ]
        except ClientResponseError as exc:
            if exc.status not in (400, 404):
                raise

        safe_collection = quote(collection_name, safe="")
        safe_item = quote(str(item_id), safe="")
        path = f"/Lists/{safe_collection}/Attachments/{safe_item}"
        url2 = f"{self._graph_base}/sites/{site_id}/drive/root:{path}:/children"
        try:
            data2 = await self._gget(session, url2)
        except ClientResponseError as exc:
            if exc.status in (400, 404):
                return []
            raise

        attachments: List[Dict[str, Any]] = []
        for entry in data2.get("value", []):
            if "file" in entry:
                attachments.append(
                    {
                        "name": entry.get("name"),
                        "downloadUrl": f"{self._graph_base}/sites/{site_id}/drive/items/{entry.get('id')}/content",
                        "lastModifiedDateTime": entry.get("fileSystemInfo", {}).get("lastModifiedDateTime"),
                        "webUrl": entry.get("webUrl"),
                        "contentType": entry.get("file", {}).get("mimeType"),
                    }
                )
        return attachments

    async def get_item_permission_object_ids(
        self,
        session: aiohttp.ClientSession,
        site_id: str,
        collection_id: str,
        item_id: str,
    ) -> List[str]:
        """Return a normalized list of object IDs that have explicit permissions on an item.

        This is kept for backward compatibility. Prefer get_item_permission_principal_ids.
        """
        user_ids, group_ids = await self.get_item_permission_principal_ids(
            session=session,
            site_id=site_id,
            collection_id=collection_id,
            item_id=item_id,
        )
        # Preserve previous behavior: a single combined list.
        return list(dict.fromkeys([*user_ids, *group_ids]))

    async def get_item_permission_principal_ids(
        self,
        session: aiohttp.ClientSession,
        site_id: str,
        collection_id: str,
        item_id: str,
    ) -> Tuple[List[str], List[str]]:
        """Return (user_object_ids, group_object_ids) that have explicit permissions on an item."""
        # The permissions resource for list items currently exists only in the beta Graph surface.
        url = (
            f"{self._graph_beta_base}/sites/{site_id}/lists/{collection_id}/items/{item_id}/permissions"
            "?$select=id,grantedToIdentitiesV2,grantedTo"
        )
        try:
            data = await self._gget(session, url)
        except ClientResponseError as exc:
            if exc.status in (400, 401, 403, 404, 501):
                logging.debug(
                    f"{LOG_SCOPE} permissions lookup failed ({exc.status}) for list {collection_id} item {item_id}"
                )
                return ([], [])
            raise

        user_ids: List[str] = []
        group_ids: List[str] = []
        seen_users: set[str] = set()
        seen_groups: set[str] = set()

        for perm in data.get("value", []):
            identities = perm.get("grantedToIdentitiesV2") or perm.get("grantedToIdentities") or []
            for identity in identities:
                kind, obj_id = self._extract_identity_principal(identity)
                if kind == "user" and obj_id and obj_id not in seen_users:
                    seen_users.add(obj_id)
                    user_ids.append(obj_id)
                elif kind == "group" and obj_id and obj_id not in seen_groups:
                    seen_groups.add(obj_id)
                    group_ids.append(obj_id)

            granted_to = perm.get("grantedToV2") or perm.get("grantedTo")
            if isinstance(granted_to, dict):
                kind, obj_id = self._extract_identity_principal(granted_to)
                if kind == "user" and obj_id and obj_id not in seen_users:
                    seen_users.add(obj_id)
                    user_ids.append(obj_id)
                elif kind == "group" and obj_id and obj_id not in seen_groups:
                    seen_groups.add(obj_id)
                    group_ids.append(obj_id)

        return (user_ids, group_ids)

    @staticmethod
    def _is_guid(value: str) -> bool:
        try:
            UUID(str(value))
            return True
        except Exception:  # noqa: BLE001
            return False

    @classmethod
    def _extract_identity_id(cls, identity: Dict[str, Any]) -> Optional[str]:
        """Return Entra object IDs only (users/groups)."""
        for key in ("user", "group"):
            obj = identity.get(key)
            if isinstance(obj, dict):
                obj_id = obj.get("id")
                if obj_id and cls._is_guid(str(obj_id)):
                    return str(obj_id)

        direct = identity.get("id")
        if direct and cls._is_guid(str(direct)):
            return str(direct)
        return None

    @classmethod
    def _extract_identity_principal(cls, identity: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        """Return (kind, object_id) where kind is 'user' or 'group'.

        Graph permission payloads typically provide a nested identity set, e.g.:
        {"user": {"id": "..."}} or {"group": {"id": "..."}}.

        If the payload does not identify the kind, return (None, None).
        """
        user_obj = identity.get("user")
        if isinstance(user_obj, dict):
            obj_id = user_obj.get("id")
            if obj_id and cls._is_guid(str(obj_id)):
                return ("user", str(obj_id))

        group_obj = identity.get("group")
        if isinstance(group_obj, dict):
            obj_id = group_obj.get("id")
            if obj_id and cls._is_guid(str(obj_id)):
                return ("group", str(obj_id))

        return (None, None)

    def _is_hidden_lookup_list(self, list_id: str) -> bool:
        normalized = (list_id or "").strip().lower()
        return bool(normalized and normalized in self._hidden_lookup_list_ids)

    async def _gget(self, session: aiohttp.ClientSession, url: str, **kwargs: Any) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {self._graph_token}"}
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))

        delay = 1.0
        for _ in range(6):
            try:
                async with session.get(url, headers=headers, **kwargs) as response:
                    if response.status == 429:
                        retry = int(response.headers.get("Retry-After", "1"))
                        logging.warning(f"{LOG_SCOPE} 429 {url}; sleeping {retry}s")
                        await asyncio.sleep(retry)
                        continue

                    if 200 <= response.status < 300:
                        return await response.json()

                    if 400 <= response.status < 500:
                        text = await response.text()
                        raise ClientResponseError(
                            request_info=response.request_info,
                            history=response.history,
                            status=response.status,
                            message=text,
                            headers=response.headers,
                        )

                    text = await response.text()
                    logging.warning(f"{LOG_SCOPE} {response.status} on {url}; retry in {delay}s: {text[:200]}")
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30)

            except ClientResponseError as exc:
                if 400 <= exc.status < 500 and exc.status != 429:
                    raise
                logging.warning(f"{LOG_SCOPE} HTTP error; retry in {delay}s: {exc}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
            except Exception as exc:  # noqa: BLE001
                logging.warning(f"{LOG_SCOPE} GET backoff {delay}s on {url}: {exc}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

        raise RuntimeError(f"GET failed after retries: {url}")

    async def _gbytes(self, session: aiohttp.ClientSession, url: str) -> bytes:
        headers = {"Authorization": f"Bearer {self._graph_token}"}
        for attempt in range(6):
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 429:
                        retry = int(response.headers.get("Retry-After", "1"))
                        await asyncio.sleep(retry)
                        continue
                    response.raise_for_status()
                    return await response.read()
            except Exception as exc:  # noqa: BLE001
                backoff = min(2**attempt, 30)
                logging.warning(f"{LOG_SCOPE} GET-bytes backoff {backoff}s: {exc}")
                await asyncio.sleep(backoff)
        raise RuntimeError(f"GET-bytes failed after retries: {url}")

    async def download_attachment(self, session: aiohttp.ClientSession, url: str) -> bytes:
        return await self._gbytes(session, url)
