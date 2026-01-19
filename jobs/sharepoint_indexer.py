import asyncio
import inspect
import json
import logging
import random
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

import aiohttp
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError, ServiceRequestError
from azure.identity.aio import AzureCliCredential, ChainedTokenCredential, ManagedIdentityCredential
from azure.search.documents.aio import SearchClient as AsyncSearchClient
from azure.storage.blob import ContentSettings
from azure.storage.blob.aio import BlobServiceClient
from openai import RateLimitError

from chunking import DocumentChunker
from dependencies import get_config
from .sharepoint_graph_client import SharePointGraphClient
from .sharepoint_ingestion_config import (
    LOG_SCOPE,
    LIST_TYPE_DOCUMENT_LIBRARY,
    LIST_TYPE_GENERIC_LIST,
    RunStats,
    SharePointConfig,
    _as_dt,
    _chunk,
    _get_setting_float,
    _get_setting_int,
    _is_strictly_newer,
    _make_chunk_key,
    _make_chunk_key_prefix,
    _make_parent_key,
    _sanitize_key_part,
    _utc_now,
)
from tools import AzureOpenAIClient, KeyVaultClient, CosmosDBClient

# -----------------------------
# Main indexer
# -----------------------------
@dataclass
class LookupFieldMetadata:
    target_list_id: str
    target_field_name: str
    allow_multiple: bool


class SharePointIndexer:
    """
    Indexes SharePoint items and attachments into the shared AI Search index used by the Blob job.
    - Each item becomes a single document in `content` + embedding in `contentVector`, source='sharepoint'.
    - Each attachment becomes chunked documents via DocumentChunker with parent_id=<item>/<attachment>.
    - Blob-like logging to Blob Storage (`jobs` container) with per-item logs and run summaries.
    """

    def __init__(self, cfg: Optional[SharePointConfig] = None):
        self.cfg = cfg or SharePointConfig.from_app_config()
        self._app = get_config()

        # Cosmos configuration (lazy-loaded)
        self._cosmos_client = CosmosDBClient()
        self._cosmos_datasource_container = self._app.get("COSMOS_DATASOURCES_CONTAINER", "datasources")
        self._cosmos_site_configs: Optional[List[Dict[str, Any]]] = None
        self._cosmos_sites_loaded = False

        # Clients
        self._credential = None
        self._blob_service = None
        self._search_client = None
        self._kv = None
        self._aoai = None

        # Graph
        self._graph_client: Optional[SharePointGraphClient] = None
        self._lookup_columns_cache: Dict[str, Dict[str, LookupFieldMetadata]] = {}
        self._lookup_value_cache: Dict[Tuple[str, str, str], Optional[str]] = {}
        self._list_nav_url_cache: Dict[str, str] = {}

        # Allowed attachment extensions (defaults if unset)
        formats = (self.cfg.files_format or "pdf,docx,pptx")
        self._allowed_exts = {f.strip().lower().lstrip('.') for f in formats.split(',') if f and f.strip()}

        # AOAI limits
        aoai_max_concurrency = int(self._app.get("AOAI_MAX_CONCURRENCY", 2, type=int))
        self._aoai_sem = asyncio.Semaphore(aoai_max_concurrency)
        self._aoai_backoff_cap = float(self._app.get("AOAI_BACKOFF_MAX_SECONDS", 60, type=float))
        self._aoai_transient_tries = int(self._app.get("AOAI_MAX_TRANSIENT_ATTEMPTS", 8, type=int))
        self._aoai_rate_limit_tries = int(self._app.get("AOAI_MAX_RATE_LIMIT_ATTEMPTS", 8, type=int))

        # Storage logging gate (decided lazily)
        self._storage_writable = None

        # Timeouts (tunable via env)
        self._item_timeout_s = _get_setting_int("INDEXER_ITEM_TIMEOUT_SECONDS", 600)
        self._http_total_timeout_s = _get_setting_float("HTTP_TOTAL_TIMEOUT_SECONDS", 120.0)
        self._blob_op_timeout_s = _get_setting_float("BLOB_OP_TIMEOUT_SECONDS", 20.0)
        self._collection_gather_timeout_s = _get_setting_float("LIST_GATHER_TIMEOUT_SECONDS", 7200.0)  # 2 hours for large collections
        self._run_summary_timeout_s = _get_setting_float("RUN_SUMMARY_TIMEOUT_SECONDS", 60.0)
        self._run_summary_total_timeout_s = _get_setting_float("RUN_SUMMARY_TOTAL_TIMEOUT_SECONDS", 90.0)

    def _log_event(self, level: int, event: str, **fields: Any) -> None:
        """Emit a structured log line that is easy to query via App Insights."""
        payload: Dict[str, Any] = {"event": event}
        for key, value in fields.items():
            if value is None:
                continue
            if isinstance(value, datetime):
                payload[key] = value.isoformat()
            else:
                payload[key] = value
        try:
            message = json.dumps(payload, ensure_ascii=False)
        except TypeError:
            safe_payload = {k: str(v) for k, v in payload.items()}
            message = json.dumps(safe_payload, ensure_ascii=False)
        logging.log(level, f"[{self.cfg.indexer_name}] {message}")

    # ---------- lifecycle ----------
    async def _ensure_clients(self):
        if not self._credential:
            client_id = self._app.get("AZURE_CLIENT_ID", None, allow_none=True)
            self._credential = ChainedTokenCredential(
                AzureCliCredential(),
                ManagedIdentityCredential(client_id=client_id)
            )

        if not self._blob_service and self.cfg.storage_account_name:
            try:
                self._blob_service = BlobServiceClient(
                    f"https://{self.cfg.storage_account_name}.blob.core.windows.net",
                    credential=self._credential
                )
            except Exception:
                logging.warning("[sp-ingest] storage client init failed; disabling storage logs", exc_info=True)
                self._blob_service = None
                self._storage_writable = False

        # Search / KV / AOAI as you already do
        if not self._search_client:
            self._search_client = AsyncSearchClient(
                endpoint=self.cfg.search_endpoint,
                index_name=self.cfg.search_index_name,
                credential=self._credential,
            )
        if not self._kv:
            self._kv = KeyVaultClient()
        if not self._graph_client and self._kv:
            self._graph_client = SharePointGraphClient(self.cfg, self._kv)
        if not self._aoai:
            self._aoai = AzureOpenAIClient()

        # One-time probe for storage write perms
        await self._init_storage_logging_guard()

        if self._graph_client:
            await self._graph_client.ensure_token()

    async def _init_storage_logging_guard(self) -> None:
        """Decide once whether blob logging is writable. If not, disable all storage writes."""
        # Allow a hard override (useful in tests or to suppress any storage I/O)
        disable_logs = str(self._app.get("DISABLE_STORAGE_LOGS", "", allow_none=True) or "").strip().lower()
        if disable_logs in {"1", "true", "yes"}:
            self._storage_writable = False
            logging.info("[sp-ingest] storage logs disabled by env (DISABLE_STORAGE_LOGS)")
            return

        # No account configured or client failed to init
        if not self.cfg.storage_account_name or not self._blob_service:
            self._storage_writable = False
            logging.info("[sp-ingest] storage logs disabled (no storage account or client)")
            return

        cc = self._blob_service.get_container_client(self.cfg.jobs_log_container)

        try:
            # Check if container exists (403 here means no access)
            exists = await cc.exists()
            if not exists:
                # Try to create if we can — still counts as "not writing in the storage when we lack perms"
                await cc.create_container()

            # Try a zero-byte write + delete as a true "write" probe
            import uuid
            probe_name = f"{self.cfg.indexer_name}/_probe_{uuid.uuid4().hex}.tmp"
            await cc.upload_blob(name=probe_name, data=b"", overwrite=True)
            try:
                await cc.delete_blob(probe_name)
            except Exception:
                # Not critical — but we tried; avoid failing the run
                logging.debug("[sp-ingest] probe blob delete failed (ignoring)", exc_info=True)

            self._storage_writable = True
            logging.info("[sp-ingest] storage logs enabled")
        except Exception as e:
            # Any auth/perm/network error → disable storage logging
            self._storage_writable = False
            logging.warning(f"[sp-ingest] storage logs disabled (probe failed): {e}")

    async def _close_clients(self):
        try:
            if self._search_client:
                await self._search_client.close()
        except Exception:
            logging.debug("[sp-ingest] ignoring error closing search client", exc_info=True)
        try:
            if self._blob_service:
                await self._blob_service.close()
        except Exception:
            logging.debug("[sp-ingest] ignoring error closing blob service", exc_info=True)
        try:
            if self._kv:
                await self._kv.close()
        except Exception:
            logging.debug("[sp-ingest] ignoring error closing keyvault", exc_info=True)
        try:
            if self._credential and hasattr(self._credential, "close"):
                res = self._credential.close()
                if inspect.isawaitable(res):
                    await res
        except Exception:
            logging.debug("[sp-ingest] ignoring error closing credential", exc_info=True)

    async def _hydrate_site_configs_from_cosmos(self) -> None:
        """Load SharePoint site configurations from Cosmos DB once per run."""
        if self._cosmos_sites_loaded:
            return

        self._cosmos_sites_loaded = True

        if not self._cosmos_client:
            logging.warning("%s CosmosDBClient not initialized; skipping Cosmos site configs", LOG_SCOPE)
            return

        try:
            documents = await self._cosmos_client.list_documents(self._cosmos_datasource_container)
        except Exception as exc:  # noqa: BLE001
            logging.warning(
                "%s Failed to load datasources from Cosmos container '%s': %s",
                LOG_SCOPE,
                self._cosmos_datasource_container,
                exc,
            )
            return

        site_docs: List[Dict[str, Any]] = []
        for doc in documents:
            if isinstance(doc, dict) and (doc.get("type") or "").lower() == "sharepoint_site":
                site_docs.append(doc)

        if not site_docs:
            logging.warning(
                "%s No sharepoint_site documents found in Cosmos container '%s'",
                LOG_SCOPE,
                self._cosmos_datasource_container,
            )
            return

        self._cosmos_site_configs = site_docs
        logging.info(
            "%s Loaded %d SharePoint site config(s) from Cosmos container '%s'",
            LOG_SCOPE,
            len(site_docs),
            self._cosmos_datasource_container,
        )

    # ---------- collections spec parsing ----------
    def _parse_collections(self) -> List[Dict[str, Any]]:
        """Normalize collections from Cosmos docs only."""
        site_payloads: List[Dict[str, Any]] = []
        if self._cosmos_site_configs:
            site_payloads.extend(self._cosmos_site_configs)

        if not site_payloads:
            logging.warning("%s No SharePoint site configuration available from Cosmos DB.", LOG_SCOPE)
            return []

        out: List[Dict[str, Any]] = []
        for site_idx, site_obj in enumerate(site_payloads):
            if not isinstance(site_obj, dict):
                logging.warning("%s site[%d] ignored: not an object", LOG_SCOPE, site_idx)
                continue

            lists = site_obj.get("lists")
            if not isinstance(lists, list):
                logging.warning("%s site[%d] ignored: missing 'lists' array", LOG_SCOPE, site_idx)
                continue

            site_domain = (site_obj.get("siteDomain") or "").strip()
            site_name = (site_obj.get("siteName") or "").strip()
            if not (site_domain and site_name):
                logging.warning("%s site[%d] ignored: missing siteDomain/siteName", LOG_SCOPE, site_idx)
                continue

            site_category = (site_obj.get("category") or "").strip()

            for i, item in enumerate(lists):
                if not isinstance(item, dict):
                    logging.warning("%s %s/%s lists[%d] ignored: not an object", LOG_SCOPE, site_domain, site_name, i)
                    continue

                # Accept listId (preferred) or listName (legacy, requires Graph lookup)
                list_id = (item.get("listId") or "").strip()
                list_name = (item.get("listName") or "").strip()
                if not list_id and not list_name:
                    logging.warning("%s %s/%s lists[%d] ignored: missing listId or listName", LOG_SCOPE, site_domain, site_name, i)
                    continue

                fields = item.get("includeFields")
                if isinstance(fields, list):
                    fields = [str(f).strip() for f in fields if str(f).strip()]
                    fields = list(dict.fromkeys(fields))
                else:
                    fields = None

                exclude = item.get("excludeFields")
                if isinstance(exclude, list):
                    exclude = [str(x).strip() for x in exclude if str(x).strip()]
                elif isinstance(exclude, str) and exclude.strip():
                    exclude = [exclude.strip()]
                else:
                    exclude = []

                category = (item.get("category") or site_category or "").strip()

                list_type_raw = (item.get("listType") or LIST_TYPE_GENERIC_LIST).strip()
                list_type_lower = list_type_raw.lower()
                if list_type_lower == LIST_TYPE_DOCUMENT_LIBRARY.lower():
                    list_type = LIST_TYPE_DOCUMENT_LIBRARY
                elif list_type_lower == LIST_TYPE_GENERIC_LIST.lower():
                    list_type = LIST_TYPE_GENERIC_LIST
                else:
                    logging.warning(
                        "%s %s/%s lists[%d] has invalid listType '%s'; defaulting to '%s'",
                        LOG_SCOPE,
                        site_domain,
                        site_name,
                        i,
                        list_type_raw,
                        LIST_TYPE_GENERIC_LIST,
                    )
                    list_type = LIST_TYPE_GENERIC_LIST

                out.append(
                    {
                        "siteDomain": site_domain,
                        "siteName": site_name,
                        "listId": list_id or None,
                        "listName": list_name or None,
                        "filter": (item.get("filter") or "").strip() or None,
                        "fields": fields,
                        "exclude": exclude,
                        "category": category,
                        "listType": list_type,
                    }
                )

        if not out:
            logging.warning("%s No valid list specs found after parsing site configurations.", LOG_SCOPE)

        return out

    async def _get_lookup_columns_map(
        self,
        session: aiohttp.ClientSession,
        site_id: str,
        collection_id: str,
    ) -> Dict[str, LookupFieldMetadata]:
        cache_key = f"{site_id}:{collection_id}"
        if cache_key in self._lookup_columns_cache:
            return self._lookup_columns_cache[cache_key]

        if not self._graph_client:
            return {}

        lookup_map: Dict[str, LookupFieldMetadata] = {}
        try:
            columns = await self._graph_client.get_lookup_columns(session, site_id, collection_id)
        except Exception:
            logging.warning(
                f"[{self.cfg.indexer_name}] Failed to load lookup columns for list {collection_id}",
                exc_info=True,
            )
            self._lookup_columns_cache[cache_key] = lookup_map
            return lookup_map

        for col in columns:
            field_name = (col.get("name") or "").strip()
            lookup_info = col.get("lookup") or {}
            if not field_name:
                continue
            target_list_id = (lookup_info.get("listId") or "").strip()
            target_field_name = (lookup_info.get("columnName") or "").strip()
            if not (target_list_id and target_field_name):
                continue
            lookup_map[field_name] = LookupFieldMetadata(
                target_list_id=target_list_id,
                target_field_name=target_field_name,
                allow_multiple=bool(lookup_info.get("allowMultipleValues")),
            )

        self._lookup_columns_cache[cache_key] = lookup_map
        if lookup_map:
            logging.info(
                f"[{self.cfg.indexer_name}] Loaded {len(lookup_map)} lookup column(s) for list {collection_id}"
            )
        return lookup_map

    async def _resolve_lookup_fields_for_item(
        self,
        session: aiohttp.ClientSession,
        site_id: str,
        lookup_columns: Dict[str, LookupFieldMetadata],
        item_fields: Dict[str, Any],
        include_fields: Optional[List[str]],
        exclude_fields: Iterable[str],
    ) -> Dict[str, Any]:
        if not lookup_columns:
            return {}

        include_set = set(include_fields or [])
        exclude_set = set(exclude_fields or [])
        resolved: Dict[str, Any] = {}

        for field_name, meta in lookup_columns.items():
            if exclude_set and field_name in exclude_set:
                continue
            if include_set and field_name not in include_set:
                continue

            lookup_ids = self._extract_lookup_ids(item_fields, field_name)
            if not lookup_ids:
                continue

            values: List[str] = []
            for lookup_id in lookup_ids:
                cache_key = (meta.target_list_id, lookup_id, meta.target_field_name)
                if cache_key in self._lookup_value_cache:
                    lookup_value = self._lookup_value_cache[cache_key]
                else:
                    lookup_value = await self._graph_client.get_lookup_field_value(
                        session,
                        site_id,
                        meta.target_list_id,
                        lookup_id,
                        meta.target_field_name,
                    )
                    self._lookup_value_cache[cache_key] = lookup_value

                if lookup_value is None:
                    continue
                if isinstance(lookup_value, list):
                    values.extend(str(v) for v in lookup_value if v is not None)
                else:
                    values.append(str(lookup_value))

            if not values:
                continue

            resolved_value: Any
            if meta.allow_multiple:
                resolved_value = values
            else:
                resolved_value = values[0]

            resolved[field_name] = resolved_value
            resolved[f"{field_name}__lookupIds"] = lookup_ids

        return resolved

    def _extract_lookup_ids(self, fields: Dict[str, Any], field_name: str) -> List[str]:
        candidates: List[Any] = []
        for suffix in ("LookupId", "LookupIds", "Id"):
            value = fields.get(f"{field_name}{suffix}")
            if value is not None:
                candidates = self._normalize_lookup_value(value)
                if candidates:
                    return candidates

        raw_value = fields.get(field_name)
        if raw_value is not None:
            return self._normalize_lookup_value(raw_value)

        return []

    def _normalize_lookup_value(self, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(v) for v in value if v not in (None, "")]
        if isinstance(value, (set, tuple)):
            return [str(v) for v in value if v not in (None, "")]
        if value == "":
            return []
        return [str(value)]

    async def _get_list_navigation_base_url(
        self,
        session: aiohttp.ClientSession,
        site_id: str,
        site_domain: str,
        site_name: str,
        collection_id: str,
        configured_list_name: str,
    ) -> str:
        cache_key = f"{site_id}:{collection_id}"
        if cache_key in self._list_nav_url_cache:
            return self._list_nav_url_cache[cache_key]

        base_url = ""
        list_name = configured_list_name or ""

        if self._graph_client:
            try:
                metadata = await self._graph_client.get_list_metadata(session, site_id, collection_id)
                list_name = metadata.get("displayName") or metadata.get("name") or list_name
                base_url = metadata.get("webUrl") or ""
            except Exception:
                logging.debug(
                    f"[{self.cfg.indexer_name}] Failed to load list metadata for list {collection_id}",
                    exc_info=True,
                )

        if not base_url and list_name:
            safe_list = quote(list_name, safe="")
            base_url = f"https://{site_domain}/sites/{site_name}/Lists/{safe_list}"

        self._list_nav_url_cache[cache_key] = base_url
        return base_url

    def _build_item_web_url(self, base_url: str, item_id: str, fallback_url: str) -> str:
        if base_url:
            trimmed = base_url.rstrip("/")
            return f"{trimmed}/DispForm.aspx?ID={item_id}"
        return fallback_url or ""

    async def _get_body_lastmod_by_id(self, parent_id: str) -> Optional[datetime]:
        """Fast + robust: get the body doc (chunk 0) by its key instead of searching."""
        if not self._search_client:
            raise RuntimeError("Search client not initialized")
        key = _make_chunk_key(parent_id, 0)
        try:
            doc = await self._search_client.get_document(key=key)
            dt = doc.get("metadata_storage_last_modified")
            if isinstance(dt, datetime):
                result = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            elif isinstance(dt, str):
                result = _as_dt(dt)
            else:
                result = None
            logging.debug(f"[{self.cfg.indexer_name}][FRESHNESS] Body doc found in index | parentId={parent_id} key={key} existingLastMod={result.isoformat() if result else 'None'}")
            return result
        except ResourceNotFoundError:
            logging.debug(f"[{self.cfg.indexer_name}][FRESHNESS] Body doc NOT found in index | parentId={parent_id} key={key} reason=ResourceNotFoundError")
            return None
        except Exception as e:
            logging.warning(f"[{self.cfg.indexer_name}][FRESHNESS] Failed to get body doc | parentId={parent_id} key={key} error={str(e)}", exc_info=True)
            return None

    # ---------- search helpers for freshness ----------
    async def _get_latest_mod_for_parent(self, parent_id: str) -> Optional[datetime]:
        """
        DEPRECATED: This method has pagination issues with large lists (>1000 items).
        Use _get_body_lastmod_by_id() instead for direct, efficient lookup.
        
        Returns the latest (max) metadata_storage_last_modified for any doc with this parent_id,
        or None if nothing is indexed yet.
        
        ⚠️ WARNING: Only fetches top=1000 results, causing bugs with large SharePoint lists.
        """
        if not self._search_client:
            raise RuntimeError("Search client not initialized")

        # Escape single quotes for OData filter
        sanitized = parent_id.replace("'", "''")
        latest: Optional[datetime] = None

        try:
            results = await self._search_client.search(
                search_text="*",
                filter=f"parent_id eq '{sanitized}'",
                select=["metadata_storage_last_modified"],
                top=1000,
            )
            async for page in results.by_page():
                async for doc in page:
                    dt = doc.get("metadata_storage_last_modified")
                    if isinstance(dt, datetime):
                        dtp = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
                    elif isinstance(dt, str):
                        dtp = _as_dt(dt)
                    else:
                        continue
                    if latest is None or dtp > latest:
                        latest = dtp
        except Exception:
            logging.warning("[sp-ingest] failed to read latest mod time from index for %s", parent_id, exc_info=True)

        return latest

    async def _needs_reindex(self, parent_id: str, incoming_last_mod: datetime) -> bool:
        """
        Check if a document needs reindexing by comparing incoming last modified date
        with the existing document in the index.
        
        Uses direct document lookup (get_body_lastmod_by_id) instead of search to:
        - Avoid pagination issues with large lists (>1000 items)
        - Improve performance (1 get operation vs search with pagination)
        - Reduce costs (fewer RUs consumed)
        """
        # Use direct document lookup - much faster and no pagination limits
        existing = await self._get_body_lastmod_by_id(parent_id)
        needs_reindex = _is_strictly_newer(incoming_last_mod, existing)
        
        # Debug log with structured data for KQL
        if existing:
            delta_ms = (incoming_last_mod - existing).total_seconds() * 1000
            logging.debug(
                f"[{self.cfg.indexer_name}][FRESHNESS] Freshness check | "
                f"parentId={parent_id} "
                f"incomingLastMod={incoming_last_mod.isoformat()} "
                f"existingLastMod={existing.isoformat()} "
                f"deltaMs={delta_ms:.0f} "
                f"needsReindex={needs_reindex} "
                f"reason={'newer' if needs_reindex else 'not-newer-or-equal'} "
                f"method=direct-lookup"
            )
        else:
            logging.debug(
                f"[{self.cfg.indexer_name}][FRESHNESS] Freshness check | "
                f"parentId={parent_id} "
                f"incomingLastMod={incoming_last_mod.isoformat()} "
                f"existingLastMod=None "
                f"needsReindex={needs_reindex} "
                f"reason=first-time "
                f"method=direct-lookup"
            )
        
        return needs_reindex



    # ---------- search ops ----------
    async def _with_backoff(self, func, **kw):
        delay = 1.0
        for _ in range(8):
            try:
                return await func(**kw)
            except HttpResponseError as e:
                ra = None
                try:
                    ra = e.response.headers.get("retry-after-ms") or e.response.headers.get("Retry-After")
                except Exception:
                    pass
                if ra:
                    try:
                        delay = max(delay, float(ra) / 1000.0)
                    except Exception:
                        pass
                logging.warning(f"[sp-ingest] search backoff {delay}s: {e}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
            except ServiceRequestError as e:
                logging.warning(f"[sp-ingest] network error; retry in {delay}s: {e}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def _delete_parent_docs(self, parent_id: str):
        sanitized = parent_id.replace("'", "''")
        ids: List[Dict[str, str]] = []
        
        logging.debug(f"[{self.cfg.indexer_name}][INDEX-DELETE] Starting delete operation | parentId={parent_id}")
        
        results = await self._search_client.search(
            search_text="*",
            filter=f"parent_id eq '{sanitized}'",
            select=["id"],
            top=1000,
        )
        async for page in results.by_page():
            async for doc in page:
                if doc.get("id"):
                    ids.append({"id": doc["id"]})
        
        logging.debug(f"[{self.cfg.indexer_name}][INDEX-DELETE] Found documents to delete | parentId={parent_id} documentsFound={len(ids)}")
        
        deleted_count = 0
        for batch in _chunk(ids, self.cfg.batch_size):
            if batch:
                await self._with_backoff(self._search_client.delete_documents, documents=batch)
                deleted_count += len(batch)
                logging.debug(f"[{self.cfg.indexer_name}][INDEX-DELETE] Batch deleted | parentId={parent_id} batchSize={len(batch)} totalDeleted={deleted_count}")
        
        if deleted_count > 0:
            logging.debug(f"[{self.cfg.indexer_name}][INDEX-DELETE] Completed | parentId={parent_id} totalDocumentsDeleted={deleted_count}")
        else:
            logging.debug(f"[{self.cfg.indexer_name}][INDEX-DELETE] No documents to delete | parentId={parent_id}")

    async def _upload_docs(self, docs: List[Dict[str, Any]]):
        if not docs:
            logging.debug(f"[{self.cfg.indexer_name}][INDEX-UPLOAD] No documents to upload")
            return
            
        total_docs = len(docs)
        uploaded_count = 0
        
        # Log sample of first document for debugging
        first_doc_preview = {k: v for k, v in docs[0].items() if k not in ['contentVector', 'captionVector', 'content']}
        if 'content' in docs[0]:
            first_doc_preview['contentLength'] = len(docs[0].get('content', ''))
        if 'contentVector' in docs[0]:
            first_doc_preview['contentVectorDimensions'] = len(docs[0].get('contentVector', []))
            
        logging.debug(f"[{self.cfg.indexer_name}][INDEX-UPLOAD] Starting upload | totalDocuments={total_docs} firstDocPreview={first_doc_preview}")
        
        for batch_idx, batch in enumerate(_chunk(docs, self.cfg.batch_size), 1):
            if batch:
                batch_ids = [d.get('id', 'unknown') for d in batch]
                logging.debug(f"[{self.cfg.indexer_name}][INDEX-UPLOAD] Uploading batch | batchNumber={batch_idx} batchSize={len(batch)} documentIds={batch_ids}")
                
                await self._with_backoff(self._search_client.upload_documents, documents=batch)
                uploaded_count += len(batch)
                
                logging.debug(f"[{self.cfg.indexer_name}][INDEX-UPLOAD] Batch uploaded | batchNumber={batch_idx} batchSize={len(batch)} totalUploaded={uploaded_count}/{total_docs}")
        
        logging.debug(f"[{self.cfg.indexer_name}][INDEX-UPLOAD] Upload completed | totalDocumentsUploaded={uploaded_count}")


    # ---------- content builders ----------
    async def _get_security_principals_for_item(
        self,
        *,
        session: aiohttp.ClientSession,
        site_id: str,
        collection_id: str,
        item_id: str,
    ) -> tuple[List[str], List[str]]:
        if not self._graph_client:
            return ([], [])

        try:
            user_ids, group_ids = await self._graph_client.get_item_permission_principal_ids(
                session=session,
                site_id=site_id,
                collection_id=collection_id,
                item_id=item_id,
            )

            before_users = len(user_ids)
            before_groups = len(group_ids)
            user_ids = self._normalize_acl_ids(user_ids, max_values=32)
            group_ids = self._normalize_acl_ids(group_ids, max_values=32)
            if len(user_ids) != before_users or len(group_ids) != before_groups:
                logging.warning(
                    f"[{self.cfg.indexer_name}][SECURITY] Truncated/deduped ACLs | itemId={item_id} "
                    f"users={before_users}->{len(user_ids)} groups={before_groups}->{len(group_ids)}"
                )

            if user_ids or group_ids:
                logging.debug(
                    f"[{self.cfg.indexer_name}][SECURITY] Resolved permissions | itemId={item_id} users={len(user_ids)} groups={len(group_ids)}"
                )
            return (user_ids, group_ids)
        except Exception as exc:  # noqa: BLE001
            logging.warning(
                f"[{self.cfg.indexer_name}][SECURITY] Failed to resolve permissions | itemId={item_id} error={exc}"
            )
            return ([], [])

    @staticmethod
    def _normalize_acl_ids(values: List[str], *, max_values: int = 32) -> List[str]:
        """Normalize ACL ID lists for Azure AI Search permission trimming.

        - Removes empty values
        - De-duplicates while preserving order
        - Truncates to `max_values` (Azure AI Search limitation)
        """
        normalized: List[str] = []
        seen: set[str] = set()
        for value in values or []:
            s = str(value).strip()
            if not s or s in seen:
                continue
            seen.add(s)
            normalized.append(s)
        return normalized[:max_values]

    def _fields_to_text(self, fields: Dict[str, Any], exclude: Optional[Iterable[str]] = None) -> str:
        exclude_set = set(exclude or [])
        parts = []
        for k, v in fields.items():
            if k in exclude_set:
                continue  # skip excluded keys in content only
            if isinstance(v, dict):
                vv = json.dumps(v, ensure_ascii=False)
            elif isinstance(v, list):
                vv = ", ".join(str(x) for x in v)
            else:
                vv = str(v)
            parts.append(f"{k}: {vv}")
        return "\n".join(parts).strip()

    async def _embed(self, text: str) -> List[float]:
        """
        Generate embeddings with:
        - a shared concurrency gate (self._aoai_sem) to reduce rate-limit pressure
        - bounded retries for RateLimitError (429), honoring Retry-After headers
        - bounded retries for other transient errors (network, 5xx)
        """
        text_preview = text[:100] if text else ""
        logging.debug(f"[{self.cfg.indexer_name}][EMBEDDING] Starting embedding generation | textLength={len(text)} textPreview={text_preview}...")
        
        async with self._aoai_sem:
            backoff = 1.0
            transient_tries = 0
            rate_limit_tries = 0

            while True:
                try:
                    # Let THIS loop handle all backoff logic, so disable the wrapper's extra retry
                    result = await asyncio.to_thread(self._aoai.get_embeddings, text, False)
                    logging.debug(f"[{self.cfg.indexer_name}][EMBEDDING] Success | textLength={len(text)} vectorDimensions={len(result)} rateLimitRetries={rate_limit_tries} transientRetries={transient_tries}")
                    return result

                except RateLimitError as e:
                    rate_limit_tries += 1
                    # Honor headers if present; fall back to exponential backoff + jitter
                    wait_s = None
                    try:
                        hdrs = getattr(e, "response", None) and e.response.headers or {}
                        if hdrs:
                            if "retry-after-ms" in hdrs:
                                wait_s = max(float(hdrs["retry-after-ms"]) / 1000.0, 0.5)
                            elif "Retry-After" in hdrs:
                                wait_s = max(float(hdrs["Retry-After"]), 0.5)
                    except Exception:
                        pass

                    if wait_s is None:
                        wait_s = backoff

                    jitter = random.uniform(0, max(0.25 * wait_s, 0.1))
                    sleep_s = min(wait_s + jitter, self._aoai_backoff_cap)
                    logging.warning(
                        f"[{self.cfg.indexer_name}][EMBEDDING] Rate limit (429) | "
                        f"attempt={rate_limit_tries}/{self._aoai_rate_limit_tries} "
                        f"retryAfterSeconds={sleep_s:.2f} backoffCap={self._aoai_backoff_cap} textLength={len(text)}"
                    )
                    if rate_limit_tries >= self._aoai_rate_limit_tries:
                        logging.error(
                            f"[{self.cfg.indexer_name}][EMBEDDING] Max rate limit retries exhausted | "
                            f"attempts={rate_limit_tries} textLength={len(text)}"
                        )
                        raise
                    await asyncio.sleep(sleep_s)
                    backoff = min(backoff * 2, self._aoai_backoff_cap)

                except (ServiceRequestError, TimeoutError, OSError) as e:
                    transient_tries += 1
                    jitter = random.uniform(0, max(0.25 * backoff, 0.1))
                    sleep_s = min(backoff + jitter, self._aoai_backoff_cap)
                    logging.warning(
                        f"[{self.cfg.indexer_name}][EMBEDDING] Transient error | "
                        f"errorType={type(e).__name__} attempt={transient_tries}/{self._aoai_transient_tries} "
                        f"retryAfterSeconds={sleep_s:.2f} textLength={len(text)} error={str(e)}"
                    )
                    if transient_tries >= self._aoai_transient_tries:
                        logging.error(
                            f"[{self.cfg.indexer_name}][EMBEDDING] Max transient retries exhausted | "
                            f"errorType={type(e).__name__} attempts={transient_tries} textLength={len(text)}"
                        )
                        raise
                    await asyncio.sleep(sleep_s)
                    backoff = min(backoff * 2, self._aoai_backoff_cap)

                except Exception as e:
                    # Unknown / non-transient → bubble up (keeps your current error reporting for truly fatal cases)
                    logging.error(f"[{self.cfg.indexer_name}][EMBEDDING] Fatal error | errorType={type(e).__name__} textLength={len(text)} error={str(e)}", exc_info=True)
                    raise

    def _doc_for_item(
        self,
        parent_id: str,
        key_id: str,
        title: str,
        web_url: str,
        last_mod: datetime,
        content: str,
        content_vec: List[float],
        category: str = "",
        security_user_ids: Optional[List[str]] = None,
        security_group_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        chunk_id = 0
        doc_id = _make_chunk_key(parent_id, chunk_id)
        return {
            "id": doc_id,
            "parent_id": parent_id,
            "metadata_storage_path": parent_id,
            "metadata_storage_name": key_id,
            "metadata_storage_last_modified": last_mod,
            "metadata_security_user_ids": list(security_user_ids or []),
            "metadata_security_group_ids": list(security_group_ids or []),
            "chunk_id": chunk_id,
            "page": 0,
            "offset": 0,
            "length": len(content or ""),
            "title": title or "",
            "url": web_url or "",
            "content": content or "",
            "contentVector": content_vec or [],
            "captionVector": [],
            "relatedFiles": [],
            "relatedImages": [],
            "summary": "",
            "category": category or "",    # <---
            "filepath": "",
            "imageCaptions": "",
            "source": "sharepoint-list",
        }

    def _doc_for_attachment_chunk(
        self,
        parent_id: str,
        chunk: Dict[str, Any],
        file_name: str,
        web_url: str,
        last_mod: datetime,
        category: str = "",
        security_user_ids: Optional[List[str]] = None,
        security_group_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        cid = int(chunk.get("chunk_id", 0))
        return {
            "id": _make_chunk_key(parent_id, cid),
            "parent_id": parent_id,
            "metadata_storage_path": parent_id,
            "metadata_storage_name": file_name,
            "metadata_storage_last_modified": last_mod,
            "metadata_security_user_ids": list(security_user_ids or []),
            "metadata_security_group_ids": list(security_group_ids or []),
            "chunk_id": cid,
            "page": int(chunk.get("page", 0)),
            "offset": int(chunk.get("offset", 0)),
            "length": int(chunk.get("length", len(chunk.get("content", "")))),
            "title": chunk.get("title", ""),
            "url": web_url or chunk.get("url", ""),
            "content": chunk.get("content", ""),
            "contentVector": chunk.get("contentVector", []),
            "captionVector": chunk.get("captionVector", []),
            "relatedFiles": chunk.get("relatedFiles", []),
            "relatedImages": chunk.get("relatedImages", []),
            "summary": chunk.get("summary", ""),
            "category": (category or chunk.get("category", "")),  # <---
            "filepath": "",
            "imageCaptions": chunk.get("imageCaptions", ""),
            "source": "sharepoint-list",
        }

    async def _process_document_library_file(
        self,
        session: aiohttp.ClientSession,
        *,
        site_id: str,
        collection_id: str,
        site_domain: str,
        site_name: str,
        collection_name: str,
        item: Dict[str, Any],
        item_id: str,
        category: str,
        security_user_ids: Optional[List[str]],
        security_group_ids: Optional[List[str]],
        stats: RunStats,
        stats_lock: asyncio.Lock,
    ) -> Dict[str, Any]:
        result = {"chunks": 0, "had_candidate": False}

        drive_item = await self._graph_client.get_drive_item(session, site_id, collection_id, item_id)
        if not drive_item:
            logging.debug(
                f"[{self.cfg.indexer_name}][DOC-LIB] Drive item missing | itemId={item_id} collection={collection_name}"
            )
            return result

        if "file" not in drive_item:
            logging.debug(
                f"[{self.cfg.indexer_name}][DOC-LIB] Entry is not a file (likely folder) | itemId={item_id} collection={collection_name}"
            )
            return result

        file_name = drive_item.get("name") or item.get("fields", {}).get("FileLeafRef") or f"{item_id}.bin"
        ext = file_name.rsplit(".", 1)[-1].lower() if "." in file_name else ""
        if self._allowed_exts and ext not in self._allowed_exts:
            async with stats_lock:
                stats.att_skipped_ext_not_allowed += 1
            logging.debug(
                f"[{self.cfg.indexer_name}][DOC-LIB] Extension not allowed | itemId={item_id} fileName={file_name} ext={ext}"
            )
            return result

        file_last_mod = _as_dt(
            drive_item.get("fileSystemInfo", {}).get("lastModifiedDateTime")
            or drive_item.get("lastModifiedDateTime")
            or item.get("lastModifiedDateTime")
        )
        file_parent = _make_parent_key(site_domain, site_name, collection_id, item_id, file_name)
        need_file = await self._needs_reindex(file_parent, file_last_mod)
        if not need_file:
            async with stats_lock:
                stats.att_skipped_not_newer += 1
            logging.debug(
                f"[{self.cfg.indexer_name}][DOC-LIB] Skipped (not newer) | itemId={item_id} fileName={file_name}"
            )
            return result

        async with stats_lock:
            stats.att_candidates += 1

        file_web_url = drive_item.get("webUrl") or item.get("webUrl") or ""
        logging.debug(
            f"[{self.cfg.indexer_name}][DOC-LIB] Processing file | itemId={item_id} fileName={file_name} parent={file_parent}"
        )

        bytes_ = await self._graph_client.download_drive_item(session, drive_item)
        data = {
            "documentBytes": bytes_,
            "fileName": file_name,
            "documentContentType": drive_item.get("file", {}).get("mimeType", ""),
            "documentUrl": file_web_url,
        }
        chunker = DocumentChunker()
        chunks, errors, warnings = await asyncio.to_thread(chunker.chunk_documents, data)
        logging.debug(
            f"[{self.cfg.indexer_name}][DOC-LIB] Chunked file | itemId={item_id} fileName={file_name} chunks={len(chunks)} warnings={len(warnings)}"
        )
        if errors:
            raise RuntimeError(f"chunker errors for {file_name}: {errors}")

        await self._delete_parent_docs(file_parent)
        docs = [
            self._doc_for_attachment_chunk(
                file_parent,
                chunk,
                file_name,
                file_web_url,
                file_last_mod,
                category=category,
                security_user_ids=security_user_ids,
                security_group_ids=security_group_ids,
            )
            for chunk in chunks
        ]
        await self._upload_docs(docs)

        async with stats_lock:
            stats.att_uploaded_chunks += len(docs)

        result.update({
            "chunks": len(docs),
            "had_candidate": True,
            "fileName": file_name,
            "webUrl": file_web_url,
        })
        return result

    async def _ensure_log_container(self):
        if not self._storage_writable:
            return
        try:
            cc = self._blob_service.get_container_client(self.cfg.jobs_log_container)
            await cc.create_container()
        except Exception:
            pass

    async def _write_file_log(self, blob_name: str, payload: Dict[str, Any]):
        if not self._storage_writable:
            return
        await self._ensure_log_container()
        cc = self._blob_service.get_container_client(self.cfg.jobs_log_container)
        try:
            await asyncio.wait_for(
                cc.upload_blob(
                    name=f"{self.cfg.indexer_name}/files/{blob_name}",
                    data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
                    overwrite=True,
                    content_settings=ContentSettings(content_type="application/json"),
                ),
                timeout=self._blob_op_timeout_s,
            )
        except Exception:
            # Do not raise; just log locally and continue
            logging.exception(f"[{self.cfg.indexer_name}] failed to write file log {blob_name}")

    async def _write_run_summary(self, run_id: str, summary: Dict[str, Any]):
        # If we positively know storage is not writable, skip. If it's None, still try.
        if self._storage_writable is False:
            logging.warning(f"[{self.cfg.indexer_name}] run summary skipped (storage not writable)")
            return

        await self._ensure_log_container()
        cc = self._blob_service.get_container_client(self.cfg.jobs_log_container)

        stage = (summary.get("status") or "").strip().lower()
        base = f"{self.cfg.indexer_name}/runs/{run_id}"
        canonical_name = f"{base}.json"                  # best-effort overwrite target
        stage_name = f"{base}.{stage}.json" if stage else f"{base}.snapshot.json"
        latest_name = f"{self.cfg.indexer_name}/runs/latest.json"

        payload = json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8")

        async def _put_and_verify(blob_name: str, overwrite: bool) -> bool:
            bclient = cc.get_blob_client(blob_name)
            backoff = 1.0
            for attempt in range(8):
                try:
                    await asyncio.wait_for(
                        bclient.upload_blob(
                            data=payload,
                            overwrite=overwrite,
                            content_settings=ContentSettings(content_type="application/json"),
                        ),
                        timeout=self._blob_op_timeout_s,
                    )
                    # Verify content matches what we just wrote
                    dl = await asyncio.wait_for(bclient.download_blob(), timeout=self._blob_op_timeout_s)
                    txt = (await asyncio.wait_for(dl.readall(), timeout=self._blob_op_timeout_s)).decode("utf-8", "ignore")
                    try:
                        on_blob = json.loads(txt)
                    except Exception:
                        on_blob = {}

                    ok = (
                        on_blob.get("runId") == summary.get("runId")
                        and on_blob.get("status") == summary.get("status")
                        and on_blob.get("itemsProcessed") == summary.get("itemsProcessed")
                        and on_blob.get("success") == summary.get("success")
                        and on_blob.get("failed") == summary.get("failed")
                        and on_blob.get("totalChunksUploaded") == summary.get("totalChunksUploaded")
                    )
                    if ok:
                        logging.info(f"[{self.cfg.indexer_name}] run summary verified: {blob_name}")
                        return True

                    logging.warning(
                        f"[{self.cfg.indexer_name}] run summary mismatch on {blob_name} "
                        f"(attempt {attempt+1}); retrying in {backoff:.1f}s"
                    )
                except Exception as e:
                    logging.warning(
                        f"[{self.cfg.indexer_name}] run summary write failed for {blob_name} "
                        f"(attempt {attempt+1}): {e}; retry in {backoff:.1f}s"
                    )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
            return False

        logging.info(
            f"[{self.cfg.indexer_name}] write_run_summary: status={stage or 'n/a'} "
            f"stage_name={stage_name} canonical={canonical_name} latest={latest_name}"
        )

        # 1) Always write stage-specific blob (authoritative snapshot for this call)
        wrote_stage = await _put_and_verify(stage_name, overwrite=True)
        if not wrote_stage:
            logging.error(f"[{self.cfg.indexer_name}] failed to write stage run summary: {stage_name}")

        # 2) Best-effort update canonical and latest
        ok_canonical = await _put_and_verify(canonical_name, overwrite=True)
        ok_latest = await _put_and_verify(latest_name, overwrite=True)

        # 3) Pointer always points to the stage blob; if overwriting canonical pointer is blocked, fall back to stage-suffixed pointer
        try:
            pointer = {
                "runId": summary.get("runId"),
                "status": summary.get("status"),
                "blobName": stage_name,
                "note": "Authoritative snapshot for this stage. Canonical and latest are best-effort."
            }
            pointer_payload = json.dumps(pointer, ensure_ascii=False, indent=2).encode("utf-8")
            pointer_name = f"{base}.pointer.json"
            await asyncio.wait_for(
                cc.upload_blob(
                    name=pointer_name,
                    data=pointer_payload,
                    overwrite=True,
                    content_settings=ContentSettings(content_type="application/json"),
                ),
                timeout=self._blob_op_timeout_s,
            )
            logging.info(f"[{self.cfg.indexer_name}] pointer updated -> {pointer_name} -> {stage_name}")
        except Exception as e:
            # Fall back to a stage-suffixed pointer to avoid overwrites on immutable containers
            try:
                stage_suffix = (stage or "snapshot")
                fallback_pointer = f"{base}.pointer.{stage_suffix}.json"
                await asyncio.wait_for(
                    cc.upload_blob(
                        name=fallback_pointer,
                        data=pointer_payload,
                        overwrite=True,
                        content_settings=ContentSettings(content_type="application/json"),
                    ),
                    timeout=self._blob_op_timeout_s,
                )
                logging.info(f"[{self.cfg.indexer_name}] pointer fallback -> {fallback_pointer}")
            except Exception:
                logging.debug(f"[{self.cfg.indexer_name}] pointer write skipped: {e}", exc_info=True)

        if not wrote_stage or not ok_latest:
            logging.error(
                f"[{self.cfg.indexer_name}] RUN-SUMMARY-FALLBACK: "
                f"wrote_stage={wrote_stage}, ok_canonical={ok_canonical}, ok_latest={ok_latest}"
            )
    # ---------- public entry ----------
    async def run(self) -> None:
        await self._ensure_clients()
        await self._hydrate_site_configs_from_cosmos()

        collections = self._parse_collections()
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        start_iso = _utc_now()

        stats = RunStats()
        stats_lock = asyncio.Lock()

        summary = {
            "indexerType": self.cfg.indexer_name,
            "runId": run_id,
            "runStartedAt": start_iso,
            "runFinishedAt": None,
            "collections": len(collections),
            "itemsProcessed": 0,
            "success": 0,
            "failed": 0,
            "totalChunksUploaded": 0,
            "status": "started",
        }
        await self._write_run_summary_safely(run_id, summary)
        self._log_event(
            logging.INFO,
            "RUN-START",
            runId=run_id,
            collections=len(collections),
            sitesWithCollections=len({(c.get("siteDomain"), c.get("siteName")) for c in collections}),
        )

        try:
            if collections:
                timeout = aiohttp.ClientTimeout(total=self._http_total_timeout_s)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    sem = asyncio.Semaphore(self.cfg.max_concurrency)
                    results = await asyncio.gather(
                        *(self._process_collection(session, spec, run_id, sem, stats, stats_lock) for spec in collections),
                        return_exceptions=True
                    )

                # Aggregate collection-level results to top-level summary (best-effort)
                for r in results:
                    if isinstance(r, dict):
                        summary["itemsProcessed"]      += r.get("items", 0)
                        summary["success"]             += r.get("success", 0)
                        summary["failed"]              += r.get("failed", 0)
                        summary["totalChunksUploaded"] += r.get("chunks", 0)
                    else:
                        logging.error("[%s] Collection task failed: %r", self.cfg.indexer_name, r, exc_info=True)

            # Add richer stats and flush a "finishing" snapshot
            summary.update({
                "itemsDiscovered": stats.items_discovered,
                "candidateItems": stats.items_candidates,
                "indexedItems": stats.items_indexed,
                "skippedNoChange": stats.items_skipped_nochange,
                "failed": max(summary["failed"], stats.items_failed),
                "documentLibraryStats": {
                    "candidates": stats.att_candidates,
                    "skippedNotNewer": stats.att_skipped_not_newer,
                    "skippedExtNotAllowed": stats.att_skipped_ext_not_allowed,
                    "uploadedChunks": max(summary["totalChunksUploaded"], stats.att_uploaded_chunks),
                },
            })
            summary["status"] = "finishing"
            await self._write_run_summary_safely(run_id, summary)

        except asyncio.CancelledError:
            summary["status"] = "cancelled"
            summary["runFinishedAt"] = _utc_now()
            await self._write_run_summary_safely(run_id, summary)
            self._log_event(logging.WARNING, "RUN-CANCELLED", runId=run_id)
            logging.info("[%s] Run cancelled: runId=%s", self.cfg.indexer_name, run_id)
            raise
        except Exception as exc:
            logging.exception("[%s] run() failed", self.cfg.indexer_name)
            summary["error"] = str(exc)
            summary["status"] = "failed"
            self._log_event(logging.ERROR, "RUN-ERROR", runId=run_id, error=str(exc))
        finally:
            # Final, authoritative "finished" (or failed/cancelled) snapshot
            summary.setdefault("documentLibraryStats", {})
            summary.update({
                "itemsDiscovered": stats.items_discovered,
                "candidateItems": stats.items_candidates,
                "indexedItems": stats.items_indexed,
                "skippedNoChange": stats.items_skipped_nochange,
                "failed": max(summary.get("failed", 0), stats.items_failed),
                "itemsProcessed": max(summary.get("itemsProcessed", 0), stats.items_discovered),
                "success": max(summary.get("success", 0), stats.items_indexed),
                "totalChunksUploaded": max(
                    summary.get("totalChunksUploaded", 0),
                    stats.body_docs_uploaded + stats.att_uploaded_chunks
                ),
                "documentLibraryStats": {
                    "candidates": stats.att_candidates,
                    "skippedNotNewer": stats.att_skipped_not_newer,
                    "skippedExtNotAllowed": stats.att_skipped_ext_not_allowed,
                    "uploadedChunks": stats.att_uploaded_chunks,
                },
                "runFinishedAt": _utc_now(),
            })
            if summary.get("status") not in {"failed", "cancelled"}:
                summary["status"] = "finished"

            await self._write_run_summary_safely(run_id, summary)
            duration_seconds = None
            try:
                start_dt = _as_dt(summary.get("runStartedAt"))
                finish_dt = _as_dt(summary.get("runFinishedAt"))
                if start_dt and finish_dt:
                    duration_seconds = max((finish_dt - start_dt).total_seconds(), 0.0)
            except Exception:
                duration_seconds = None

            self._log_event(
                logging.INFO,
                "RUN-COMPLETE",
                runId=run_id,
                status=summary.get("status"),
                collectionsSeen=summary.get("collections", 0),
                itemsDiscovered=stats.items_discovered,
                itemsIndexed=stats.items_indexed,
                itemsFailed=stats.items_failed,
                skippedNoChange=stats.items_skipped_nochange,
                documentLibraryCandidates=stats.att_candidates,
                documentLibraryChunks=stats.att_uploaded_chunks,
                totalChunksUploaded=summary.get("totalChunksUploaded", 0),
                durationSeconds=duration_seconds,
            )
            logging.info("[%s] Summary(final): %s", self.cfg.indexer_name, json.dumps(summary))

            # Compact end-of-run summary for easy scanning in logs
            logging.info(
                "[%s] Run complete: runId=%s collections=%s items=%s success=%s failed=%s chunks=%s",
                self.cfg.indexer_name,
                run_id,
                summary.get("collections", 0),
                summary.get("itemsProcessed", 0),
                summary.get("success", 0),
                summary.get("failed", 0),
                summary.get("totalChunksUploaded", 0),
            )

            await self._close_clients()

    async def _process_collection(
        self,
        session: aiohttp.ClientSession,

        spec: Dict[str, Any],
        run_id: str,
        sem: asyncio.Semaphore,
        stats: RunStats,
        stats_lock: asyncio.Lock
    ) -> Dict[str, int]:
        site_domain = spec.get("siteDomain")
        site_name = spec.get("siteName")
        list_id = spec.get("listId")
        list_name = spec.get("listName")

        fields_from_spec: Optional[List[str]] = spec.get("fields") or None
        exclude_from_spec: List[str] = spec.get("exclude") or []       
        exclude_set = set(exclude_from_spec)
        category_from_spec: str = spec.get("category") or ""                   
        if fields_from_spec and not isinstance(fields_from_spec, list):
            fields_from_spec = None

        filter_from_spec: Optional[str] = (spec.get("filter") or "").strip() or None

        list_type_value = (spec.get("listType") or LIST_TYPE_GENERIC_LIST).strip()
        if list_type_value.lower() == LIST_TYPE_DOCUMENT_LIBRARY.lower():
            list_type = LIST_TYPE_DOCUMENT_LIBRARY
        else:
            list_type = LIST_TYPE_GENERIC_LIST
        is_document_library = list_type == LIST_TYPE_DOCUMENT_LIBRARY

        if not all([site_domain, site_name]):
            logging.warning(f"[sp-ingest] invalid spec (missing site info): {spec}")
            return {"items": 0, "success": 0, "failed": 0, "chunks": 0}
        
        if not list_id and not list_name:
            logging.warning(f"[sp-ingest] invalid spec (missing listId or listName): {spec}")
            return {"items": 0, "success": 0, "failed": 0, "chunks": 0}

        site_id = await self._graph_client.get_site_id(session, site_domain, site_name)
        if not site_id:
            logging.error(f"[sp-ingest] Could not resolve site id for {site_domain}/{site_name}")
            return {"items": 0, "success": 0, "failed": 0, "chunks": 0}
        
        # Use listId directly if provided, otherwise fall back to legacy name lookup
        if list_id:
            collection_id = list_id
            collection_label = list_name or list_id  # For logging
            logging.info(f"[sp-ingest] Using listId directly: {list_id}")
        else:
            collection_id = await self._graph_client.get_collection_id(session, site_id, list_name)
            collection_label = list_name
            if not collection_id:
                logging.error(f"[sp-ingest] Could not resolve collection '{list_name}' (consider using listId instead)")
                return {"items": 0, "success": 0, "failed": 0, "chunks": 0}
            logging.warning(f"[sp-ingest] Using legacy listName lookup for '{list_name}' → {collection_id} (recommend switching to listId)")

        list_nav_base_url = await self._get_list_navigation_base_url(
            session=session,
            site_id=site_id,
            site_domain=site_domain,
            site_name=site_name,
            collection_id=collection_id,
            configured_list_name=collection_label or "",
        )

        lookup_columns = await self._get_lookup_columns_map(session, site_id, collection_id)

        count_items = 0
        success = 0
        failed = 0
        chunks_uploaded = 0
        processed = 0

        async def worker(item: Dict[str, Any]):
            nonlocal success, failed, chunks_uploaded, processed
            async with sem:
                # count the item as discovered as soon as we start
                async with stats_lock:
                    stats.items_discovered += 1
                item_id = str(item.get("id"))
                fields = item.get("fields", {}) or {}
                content_fields = {k: v for k, v in fields.items()}
                lookup_enrichment = await self._resolve_lookup_fields_for_item(
                    session=session,
                    site_id=site_id,
                    lookup_columns=lookup_columns,
                    item_fields=fields,
                    include_fields=fields_from_spec,
                    exclude_fields=exclude_set,
                )
                content_fields.update(lookup_enrichment)
                if fields_from_spec:
                    include_set = set(fields_from_spec)
                    content_fields = {k: v for k, v in content_fields.items() if k in include_set}
                title = (fields.get("Title") or fields.get("Name") or f"Item {item_id}")
                fallback_web_url = item.get("webUrl") or fields.get("url") or ""
                web_url = self._build_item_web_url(list_nav_base_url, item_id, fallback_web_url)
                last_mod = _as_dt(item.get("lastModifiedDateTime") or fields.get("Modified"))
                parent_item_id = _make_parent_key(site_domain, site_name, collection_id, item_id)
                security_user_ids, security_group_ids = await self._get_security_principals_for_item(
                    session=session,
                    site_id=site_id,
                    collection_id=collection_id,
                    item_id=item_id,
                )

                # Structured log at the start of item processing
                logging.debug(
                    f"[{self.cfg.indexer_name}][ITEM-START] Processing item | "
                    f"itemId={item_id} "
                    f"parentId={parent_item_id} "
                    f"title={title} "
                    f"lastModified={last_mod.isoformat()} "
                    f"hasAttachmentsFlag={bool(fields.get('Attachments'))} "
                    f"fieldCount={len(fields)}"
                )

                file_log = {
                    "indexerType": self.cfg.indexer_name,
                    "collection": f"{site_domain}/{site_name}/{collection_label}",
                    "itemId": item_id,
                    "parent_id": parent_item_id,
                    "runId": run_id,
                    "startedAt": _utc_now(),
                    "chunksIds": _make_chunk_key_prefix(parent_item_id),
                }
                file_log["listType"] = list_type

                chunks_for_item = 0
                body_uploaded = False
                item_had_candidate = False  # whether body or any attachment needed reindex

                try:
                    async def _do() -> Dict[str, Any]:
                        local_chunks_for_item = 0
                        local_body_uploaded = False
                        local_item_had_candidate = False
                        # ---- BODY (item) ----
                        logging.debug(f"[{self.cfg.indexer_name}][BODY] Checking body freshness | itemId={item_id} parentId={parent_item_id}")
                        
                        existing_body_mod = await self._get_body_lastmod_by_id(parent_item_id)
                        need_body = _is_strictly_newer(last_mod, existing_body_mod)
                        
                        if need_body:
                            local_item_had_candidate = True
                            logging.debug(f"[{self.cfg.indexer_name}][BODY] Needs indexing | itemId={item_id} reason={'first-time' if existing_body_mod is None else 'newer'}")
                            
                            content = self._fields_to_text(content_fields, exclude=exclude_from_spec)
                            logging.debug(f"[{self.cfg.indexer_name}][BODY] Generated content | itemId={item_id} contentLength={len(content)} excludedFields={exclude_from_spec}")
                            
                            emb = await self._embed(content) if content else []

                            body_doc = self._doc_for_item(
                                parent_id=parent_item_id,
                                key_id=item_id,
                                title=title,
                                web_url=web_url,
                                last_mod=last_mod,
                                content=content,
                                content_vec=emb,
                                category=category_from_spec,
                                security_user_ids=security_user_ids,
                                security_group_ids=security_group_ids,
                            )

                            # Only delete & re-upload if we actually need to refresh
                            await self._delete_parent_docs(parent_item_id)
                            await self._upload_docs([body_doc])
                            local_chunks_for_item += 1
                            local_body_uploaded = True
                            logging.debug(f"[{self.cfg.indexer_name}][BODY] Successfully indexed | itemId={item_id} parentId={parent_item_id}")
                        else:
                            logging.debug(f"[{self.cfg.indexer_name}][BODY] Skipped (not newer) | itemId={item_id} incomingLastMod={last_mod.isoformat()} existingLastMod={existing_body_mod.isoformat() if existing_body_mod else 'None'}")

                        file_log.update({
                            "incomingLastMod": last_mod.isoformat(),
                            "existingLastMod": (existing_body_mod.isoformat() if existing_body_mod else None),
                            "freshnessReason": (
                                "first-time" if existing_body_mod is None else
                                f"newer-by-ms={(last_mod - existing_body_mod).total_seconds()*1000:.0f}"
                            )
                        })

                        # ---- ATTACHMENTS ----
                        att_present = bool(fields.get("Attachments"))
                        logging.debug(
                            f"[{self.cfg.indexer_name}][ATTACHMENT] Checking attachments | "
                            f"itemId={item_id} parentId={parent_item_id} hasAttachmentsFlag={att_present}"
                        )

                        if is_document_library:
                            doclib_result = await self._process_document_library_file(
                                session=session,
                                site_id=site_id,
                                collection_id=collection_id,
                                site_domain=site_domain,
                                site_name=site_name,
                                collection_name=collection_label,
                                item=item,
                                item_id=item_id,
                                category=category_from_spec,
                                security_user_ids=security_user_ids,
                                security_group_ids=security_group_ids,
                                stats=stats,
                                stats_lock=stats_lock,
                            )
                            local_chunks_for_item += int(doclib_result.get("chunks", 0))
                            if doclib_result.get("had_candidate"):
                                local_item_had_candidate = True
                            if doclib_result.get("fileName"):
                                file_log.setdefault("documentLibraryFileName", doclib_result.get("fileName"))
                                file_log.setdefault("documentLibraryUrl", doclib_result.get("webUrl"))
                        else:
                            logging.debug(
                                f"[{self.cfg.indexer_name}][ATTACHMENT] Attachment processing disabled for generic lists | "
                                f"itemId={item_id} collection={collection_label}"
                            )

                        # ---- LOG + COUNTERS ----
                        did_upload_any = (local_body_uploaded or local_chunks_for_item > 0)
                        file_log.update({
                            "status": "success" if did_upload_any else "skipped-no-change",
                            "finishedAt": _utc_now(),
                            "chunks": local_chunks_for_item
                        })
                        await self._write_file_log(f"{_sanitize_key_part(parent_item_id)}.json", file_log)

                        attach_chunks = max(local_chunks_for_item - (1 if local_body_uploaded else 0), 0)
                        
                        # Structured completion log
                        logging.debug(
                            f"[{self.cfg.indexer_name}][ITEM-COMPLETE] Item processing completed | "
                            f"itemId={item_id} "
                            f"parentId={parent_item_id} "
                            f"bodyUploaded={local_body_uploaded} "
                            f"attachmentChunks={attach_chunks} "
                            f"totalChunks={local_chunks_for_item} "
                            f"hadCandidate={local_item_had_candidate} "
                            f"status={'uploaded' if did_upload_any else 'skipped-no-change'}"
                        )

                        return {
                            "chunks": local_chunks_for_item,
                            "uploaded": did_upload_any,
                            "had_candidate": local_item_had_candidate,
                            "body_uploaded": local_body_uploaded,
                        }

                    result = await asyncio.wait_for(_do(), timeout=self._item_timeout_s)
                    # Update shared counters only after the item finishes
                    processed += 1
                    chunks_uploaded += int(result.get("chunks", 0))
                    if result.get("had_candidate"):
                        async with stats_lock:
                            stats.items_candidates += 1
                    if result.get("uploaded"):
                        success += 1
                        async with stats_lock:
                            stats.items_indexed += 1
                    else:
                        async with stats_lock:
                            stats.items_skipped_nochange += 1
                    if result.get("body_uploaded"):
                        async with stats_lock:
                            stats.body_docs_uploaded += 1

                    # Cumulative totals log (restores previous behavior)
                    total_attach_chunks = max(int(result.get("chunks", 0)) - (1 if result.get("body_uploaded") else 0), 0)
                    self._log_event(
                        logging.INFO,
                        "ITEM-COMPLETE",
                        runId=run_id,
                        collection=collection_label,
                        site=f"{site_domain}/{site_name}",
                        listType=list_type,
                        itemId=item_id,
                        parentId=parent_item_id,
                        status="uploaded" if result.get("uploaded") else "skipped-no-change",
                        bodyUploaded=bool(result.get("body_uploaded")),
                        attachmentChunks=total_attach_chunks,
                        totalChunks=int(result.get("chunks", 0)),
                        hadCandidate=bool(result.get("had_candidate")),
                        category=category_from_spec,
                        webUrl=web_url,
                    )
                    logging.info(
                        f"[{self.cfg.indexer_name}] {site_name}/{collection_label} item {item_id}: "
                        f"body={'1' if result.get('body_uploaded') else '0'}, attachment-chunks={total_attach_chunks}. "
                        f"Totals — items: {processed}, docs: {chunks_uploaded}"
                    )

                except asyncio.TimeoutError:
                    # Item took too long; record and continue
                    processed += 1
                    file_log.update({
                        "status": "error",
                        "error": f"timeout after {self._item_timeout_s}s",
                        "finishedAt": _utc_now()
                    })
                    await self._write_file_log(f"{_sanitize_key_part(parent_item_id)}.json", file_log)
                    failed += 1
                    async with stats_lock:
                        stats.items_failed += 1
                    self._log_event(
                        logging.ERROR,
                        "ITEM-TIMEOUT",
                        runId=run_id,
                        collection=collection_label,
                        site=f"{site_domain}/{site_name}",
                        itemId=item_id,
                        parentId=parent_item_id,
                        timeoutSeconds=self._item_timeout_s,
                    )

                except Exception as e:
                    logging.exception(f"[{self.cfg.indexer_name}] item {item_id} failed")
                    processed += 1
                    file_log.update({"status": "error", "error": str(e), "finishedAt": _utc_now()})
                    await self._write_file_log(f"{_sanitize_key_part(parent_item_id)}.json", file_log)
                    failed += 1
                    async with stats_lock:
                        stats.items_failed += 1
                    self._log_event(
                        logging.ERROR,
                        "ITEM-ERROR",
                        runId=run_id,
                        collection=collection_label,
                        site=f"{site_domain}/{site_name}",
                        itemId=item_id,
                        parentId=parent_item_id,
                        error=str(e),
                    )

        async def _run_one_pass(use_spec_filter: bool) -> int:
            local_count_items = 0
            _tasks: List[asyncio.Task] = []

            async for item in self._graph_client.iter_items(
                session=session,
                site_id=site_id,
                collection_id=collection_id,
                select_fields=fields_from_spec,
                filter_expression=(filter_from_spec if use_spec_filter else None),
                site_name=site_name,
                collection_name=collection_label,
            ):
                local_count_items += 1
                _tasks.append(asyncio.create_task(worker(item)))

            if _tasks:
                done, pending = await asyncio.wait(_tasks, timeout=self._collection_gather_timeout_s)
                logging.info(
                    f"[{self.cfg.indexer_name}] collection pass wait done={len(done)} pending={len(pending)} (timeout={self._collection_gather_timeout_s}s)"
                )
                if pending:
                    logging.warning(
                        f"[{self.cfg.indexer_name}] collection pass timeout waiting for {len(pending)} item task(s); cancelling"
                    )
                    for t in pending:
                        t.cancel()
                    # Ensure cancellations are observed
                    await asyncio.gather(*pending, return_exceptions=True)
            return local_count_items

        # Pass: use only the per-collection filter supplied in the config
        count_items = await _run_one_pass(use_spec_filter=True)

        if count_items == 0 and filter_from_spec:
            logging.warning(
                f"[{self.cfg.indexer_name}] {site_domain}/{site_name}/{collection_label}: "
                f"no items matched with collection filter; skipping fallback run"
            )

        # Structured log for observability
        self._log_event(
            logging.INFO,
            "COLLECTION-COMPLETE",
            runId=run_id,
            collection=f"{site_domain}/{site_name}/{collection_label}",
            listType=list_type,
            items=count_items,
            success=success,
            failed=failed,
            chunksUploaded=chunks_uploaded,
        )

        return {
            "collectionKey": f"{site_domain}/{site_name}/{collection_label}",
            "items": count_items,
            "success": success,
            "failed": failed,
            "chunks": chunks_uploaded,
        }

    async def _write_run_summary_safely(self, run_id: str, summary: Dict[str, Any]) -> None:
        """Wrapper that bounds the total time spent persisting the run summary.
        Ensures the function can always proceed to print the final summary even if storage is slow or blocked.
        """
        try:
            await asyncio.wait_for(self._write_run_summary(run_id, summary), timeout=self._run_summary_total_timeout_s)
        except Exception as e:
            logging.warning(
                f"[{self.cfg.indexer_name}] run summary write skipped/timeout after {self._run_summary_total_timeout_s}s: {e}"
            )

