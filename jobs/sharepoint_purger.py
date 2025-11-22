import asyncio
import inspect
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
from azure.core.exceptions import HttpResponseError, ServiceRequestError
from azure.identity.aio import AzureCliCredential, ChainedTokenCredential, ManagedIdentityCredential
from azure.search.documents.aio import SearchClient as AsyncSearchClient
from azure.storage.blob import ContentSettings
from azure.storage.blob.aio import BlobServiceClient

from .sharepoint_graph_client import SharePointGraphClient
from .sharepoint_ingestion_config import (
	SharePointConfig,
	_as_dt,
	_chunk,
	_get_setting_float,
	_utc_now,
)
from dependencies import get_config
from tools import KeyVaultClient


PURGE_SCOPE = "[sp-purge]"


@dataclass
class PurgeRunStats:
	collections_seen: int = 0
	items_checked: int = 0
	docs_scanned: int = 0
	docs_deleted: int = 0
	docs_failed_delete: int = 0
	pages_scanned: int = 0


class SharePointPurger:
	"""Deletes AI Search docs whose backing SharePoint collection items no longer exist."""

	def __init__(self, cfg: Optional[SharePointConfig] = None) -> None:
		self.cfg = cfg or SharePointConfig.from_app_config()
		self._app = get_config()
		if not getattr(self.cfg, "indexer_name", None) or "indexer" in self.cfg.indexer_name:
			self.cfg.indexer_name = self._app.get("SP_LISTS_PURGER_NAME", "sharepoint-purger")

		self._credential: Optional[ChainedTokenCredential] = None
		self._blob_service: Optional[BlobServiceClient] = None
		self._search_client: Optional[AsyncSearchClient] = None
		self._kv: Optional[KeyVaultClient] = None
		self._graph_client: Optional[SharePointGraphClient] = None

		self._storage_writable: Optional[bool] = None
		self._blob_op_timeout_s = _get_setting_float("BLOB_OP_TIMEOUT_SECONDS", 20.0)
		self._run_summary_total_timeout_s = _get_setting_float("RUN_SUMMARY_TOTAL_TIMEOUT_SECONDS", 90.0)
		self._http_total_timeout_s = _get_setting_float("HTTP_TOTAL_TIMEOUT_SECONDS", 120.0)
		self._search_page_size = int(self._app.get("SEARCH_SCAN_PAGE_SIZE", 1000, type=int))

		self._collection_items_cache: Dict[str, Optional[Set[str]]] = {}
		self._allowed_collection_keys: Optional[Set[str]] = None

	def _log_event(self, level: int, event: str, **fields: Any) -> None:
		"""Emit structured log lines for App Insights queries."""
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
	async def _ensure_clients(self) -> None:
		if not self._credential:
			client_id = self._app.get("AZURE_CLIENT_ID", None, allow_none=True)
			self._credential = ChainedTokenCredential(
				AzureCliCredential(),
				ManagedIdentityCredential(client_id=client_id),
			)

		if not self._blob_service and self.cfg.storage_account_name:
			try:
				self._blob_service = BlobServiceClient(
					f"https://{self.cfg.storage_account_name}.blob.core.windows.net",
					credential=self._credential,
				)
			except Exception:
				logging.warning(f"{PURGE_SCOPE} storage client init failed; disabling storage logs", exc_info=True)
				self._blob_service = None
				self._storage_writable = False

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
			await self._graph_client.ensure_token()

		await self._init_storage_logging_guard()

	async def _close_clients(self) -> None:
		try:
			if self._search_client:
				await self._search_client.close()
		except Exception:
			logging.debug(f"{PURGE_SCOPE} ignoring error closing search client", exc_info=True)
		try:
			if self._blob_service:
				await self._blob_service.close()
		except Exception:
			logging.debug(f"{PURGE_SCOPE} ignoring error closing blob service", exc_info=True)
		try:
			if self._kv:
				await self._kv.close()
		except Exception:
			logging.debug(f"{PURGE_SCOPE} ignoring error closing key vault client", exc_info=True)
		try:
			if self._credential and hasattr(self._credential, "close"):
				res = self._credential.close()
				if inspect.isawaitable(res):
					await res
		except Exception:
			logging.debug(f"{PURGE_SCOPE} ignoring error closing credential", exc_info=True)

	async def _init_storage_logging_guard(self) -> None:
		disable_logs = str(self._app.get("DISABLE_STORAGE_LOGS", "", allow_none=True) or "").strip().lower()
		if disable_logs in {"1", "true", "yes"}:
			self._storage_writable = False
			logging.info(f"{PURGE_SCOPE} storage logs disabled via env override")
			return

		if not self.cfg.storage_account_name or not self._blob_service:
			self._storage_writable = False
			logging.info(f"{PURGE_SCOPE} storage logs disabled (no storage account or client)")
			return

		cc = self._blob_service.get_container_client(self.cfg.jobs_log_container)
		try:
			exists = await cc.exists()
			if not exists:
				await cc.create_container()

			import uuid

			probe_blob = f"{self.cfg.indexer_name}/_probe_{uuid.uuid4().hex}.tmp"
			await cc.upload_blob(name=probe_blob, data=b"", overwrite=True)
			try:
				await cc.delete_blob(probe_blob)
			except Exception:
				logging.debug(f"{PURGE_SCOPE} probe delete failed (ignored)", exc_info=True)

			self._storage_writable = True
			logging.info(f"{PURGE_SCOPE} storage logs enabled")
		except Exception as exc:  # noqa: BLE001
			self._storage_writable = False
			logging.warning(f"{PURGE_SCOPE} storage logs disabled (probe failed): {exc}")

	# ---------- search helpers ----------
	async def _with_backoff(self, func, **kwargs):
		delay = 1.0
		for _ in range(8):
			try:
				return await func(**kwargs)
			except HttpResponseError as exc:
				retry = exc.response.headers.get("retry-after-ms") if exc.response else None
				if retry is None and exc.response:
					retry = exc.response.headers.get("Retry-After")
				if retry:
					try:
						delay = max(delay, float(retry) / 1000.0)
					except Exception:
						pass
				logging.warning(f"{PURGE_SCOPE} search backoff {delay}s: {exc}")
				await asyncio.sleep(delay)
				delay = min(delay * 2, 30)
			except ServiceRequestError as exc:
				logging.warning(f"{PURGE_SCOPE} network error; retry in {delay}s: {exc}")
				await asyncio.sleep(delay)
				delay = min(delay * 2, 30)

	async def _delete_docs_by_id(self, run_id: str, docs: List[Dict[str, Any]]) -> Tuple[int, int]:
		deleted = 0
		failed = 0
		if not docs or not self._search_client:
			return deleted, failed

		for batch in _chunk(docs, self.cfg.batch_size):
			try:
				payload = [{"id": item["id"]} for item in batch]
				results = await self._with_backoff(self._search_client.delete_documents, documents=payload)
				result_map: Dict[str, Any] = {}
				if isinstance(results, list):
					for res in results:
						key = getattr(res, "key", None)
						if key is None and isinstance(res, dict):
							key = res.get("key")
						result_map[key] = res
				for item in batch:
					key = item.get("id")
					res = result_map.get(key)
					if res is not None and isinstance(res, dict):
						succeeded = res.get("succeeded", True)
						status_code = res.get("status_code")
						error_message = res.get("error_message")
					else:
						succeeded = getattr(res, "succeeded", True) if res is not None else True
						status_code = getattr(res, "status_code", None) if res is not None else None
						error_message = getattr(res, "error_message", None) if res is not None else None
					if succeeded:
						deleted += 1
						status = "deleted"
						log_level = logging.INFO
					else:
						failed += 1
						status = "delete-failed"
						log_level = logging.ERROR
					self._log_event(
						log_level,
						"ITEM-COMPLETE",
						runId=run_id,
						collection=item.get("collection"),
						site=item.get("site"),
						itemId=item.get("itemId"),
						parentId=item.get("parentId"),
						docId=key,
						status=status,
						reason="missing-sharepoint-item",
						deleteStatusCode=status_code,
						errorMessage=error_message,
					)
			except Exception as exc:  # noqa: BLE001
				failed += len(batch)
				logging.exception(f"{PURGE_SCOPE} delete_documents failed for batch")
				for item in batch:
					self._log_event(
						logging.ERROR,
						"ITEM-COMPLETE",
						runId=run_id,
						collection=item.get("collection"),
						site=item.get("site"),
						itemId=item.get("itemId"),
						parentId=item.get("parentId"),
						docId=item.get("id"),
						status="delete-failed",
						reason="missing-sharepoint-item",
						errorMessage=str(exc),
					)
		return deleted, failed

	# ---------- graph helpers ----------
	async def _load_collection_item_set(
		self,
		session: aiohttp.ClientSession,
		collection_key: str,
	) -> Optional[Set[str]]:
		if collection_key in self._collection_items_cache:
			return self._collection_items_cache[collection_key]

		if not self._graph_client:
			raise RuntimeError("Graph client not initialized")

		try:
			domain, site, collection_id = collection_key.split("/", 2)
		except ValueError:
			logging.warning(f"{PURGE_SCOPE} invalid collection key: {collection_key}")
			self._collection_items_cache[collection_key] = set()
			return set()

		site_id = await self._graph_client.get_site_id(session, domain, site)
		if not site_id:
			logging.error(f"{PURGE_SCOPE} could not resolve site id for {domain}/{site}")
			self._collection_items_cache[collection_key] = None
			return None

		item_ids: Set[str] = set()
		async for item_id in self._graph_client.iter_item_ids(
			session,
			site_id,
			collection_id,
			page_size=200,
			site_name=site,
			collection_name=collection_id,
		):
			item_ids.add(item_id)

		self._collection_items_cache[collection_key] = item_ids
		logging.info(f"{PURGE_SCOPE} loaded {len(item_ids)} existing item ids for {collection_key}")
		return item_ids

	# ---------- storage helpers ----------
	async def _ensure_log_container(self) -> None:
		if not self._storage_writable or not self._blob_service:
			return
		try:
			cc = self._blob_service.get_container_client(self.cfg.jobs_log_container)
			await cc.create_container()
		except Exception:
			pass

	async def _write_run_summary(self, run_id: str, summary: Dict[str, Any]) -> None:
		if self._storage_writable is False or not self._blob_service:
			logging.warning(f"[{self.cfg.indexer_name}] run summary skipped (storage not writable)")
			return

		await self._ensure_log_container()
		cc = self._blob_service.get_container_client(self.cfg.jobs_log_container)

		stage = (summary.get("status") or "").strip().lower()
		base = f"{self.cfg.indexer_name}/runs/{run_id}"
		stage_name = f"{base}.{stage}.json" if stage else f"{base}.snapshot.json"
		canonical = f"{base}.json"
		latest = f"{self.cfg.indexer_name}/runs/latest.json"

		payload = json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8")

		async def _put_and_verify(name: str, overwrite: bool) -> bool:
			blob = cc.get_blob_client(name)
			backoff = 1.0
			for attempt in range(8):
				try:
					await asyncio.wait_for(
						blob.upload_blob(
							data=payload,
							overwrite=overwrite,
							content_settings=ContentSettings(content_type="application/json"),
						),
						timeout=self._blob_op_timeout_s,
					)
					dl = await asyncio.wait_for(blob.download_blob(), timeout=self._blob_op_timeout_s)
					txt = (await asyncio.wait_for(dl.readall(), timeout=self._blob_op_timeout_s)).decode("utf-8", "ignore")
					try:
						on_blob = json.loads(txt)
					except Exception:
						on_blob = {}
					ok = (
						on_blob.get("runId") == summary.get("runId")
						and on_blob.get("status") == summary.get("status")
						and on_blob.get("docsScanned") == summary.get("docsScanned")
						and on_blob.get("docsDeleted") == summary.get("docsDeleted")
					)
					if ok:
						logging.info(f"[{self.cfg.indexer_name}] run summary verified: {name}")
						return True
					logging.warning(
						f"[{self.cfg.indexer_name}] run summary mismatch on {name} (attempt {attempt + 1}); retry in {backoff:.1f}s"
					)
				except Exception as exc:  # noqa: BLE001
					logging.warning(
						f"[{self.cfg.indexer_name}] run summary write failed for {name} (attempt {attempt + 1}): {exc}; "
						f"retry in {backoff:.1f}s"
					)
				await asyncio.sleep(backoff)
				backoff = min(backoff * 2, 30)
			return False

		wrote_stage = await _put_and_verify(stage_name, overwrite=True)
		if not wrote_stage:
			logging.error(f"[{self.cfg.indexer_name}] failed to write stage run summary: {stage_name}")

		await _put_and_verify(canonical, overwrite=True)
		await _put_and_verify(latest, overwrite=True)

		try:
			pointer = {
				"runId": summary.get("runId"),
				"status": summary.get("status"),
				"blobName": stage_name,
				"note": "Authoritative snapshot for this stage. Canonical and latest are best-effort.",
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
		except Exception:
			logging.debug(f"[{self.cfg.indexer_name}] pointer write skipped", exc_info=True)

	async def _write_run_summary_safely(self, run_id: str, summary: Dict[str, Any]) -> None:
		try:
			await asyncio.wait_for(self._write_run_summary(run_id, summary), timeout=self._run_summary_total_timeout_s)
		except Exception as exc:  # noqa: BLE001
			logging.warning(
				f"[{self.cfg.indexer_name}] run summary write skipped/timeout after {self._run_summary_total_timeout_s}s: {exc}"
			)

	# ---------- purge core ----------
	@staticmethod
	def _extract_collection_and_item_from_path(path: str) -> Optional[Tuple[str, str, str]]:
		if not path:
			return None
		parts = path.strip("/").split("/")
		if len(parts) < 4:
			return None
		domain, site, collection, item_id = parts[:4]
		collection_key = f"{domain}/{site}/{collection}"
		parent_path = "/" + "/".join(parts)
		return collection_key, item_id, parent_path

	async def _scan_and_purge(self, stats: PurgeRunStats, run_id: str) -> None:
		if not self._search_client:
			raise RuntimeError("Search client not initialized")
		if not self._graph_client:
			raise RuntimeError("Graph client not initialized")

		timeout = aiohttp.ClientTimeout(total=self._http_total_timeout_s)
		async with aiohttp.ClientSession(timeout=timeout) as session:
			select_fields = ["id", "metadata_storage_path", "filepath"]
			filter_expr = "source eq 'sharepoint-list'"

			try:
				results = await self._search_client.search(
					search_text="*",
					filter=filter_expr,
					select=select_fields,
					include_total_count=True,
					top=self._search_page_size,
				)
			except HttpResponseError as exc:
				logging.warning(f"{PURGE_SCOPE} filter on 'source' failed; retrying without filter: {exc}")
				results = await self._search_client.search(
					search_text="*",
					select=select_fields,
					include_total_count=True,
					top=self._search_page_size,
				)

			try:
				total = await results.get_count()
				if total is not None:
					logging.info(f"{PURGE_SCOPE} expected docs with source=sharepoint-list: {total}")
			except Exception:
				pass

			pending_delete_docs: List[Dict[str, Any]] = []
			seen_collections: Set[str] = set()

			async for page in results.by_page():
				stats.pages_scanned += 1
				async for doc in page:
					stats.docs_scanned += 1
					path = doc.get("metadata_storage_path") or doc.get("filepath") or ""
					parsed = self._extract_collection_and_item_from_path(path)
					if not parsed:
						continue

					collection_key, item_id, parent_path = parsed

					if self._allowed_collection_keys and collection_key not in self._allowed_collection_keys:
						continue

					if collection_key not in seen_collections:
						seen_collections.add(collection_key)
						stats.collections_seen = len(seen_collections)

					if collection_key not in self._collection_items_cache:
						cache_value = await self._load_collection_item_set(session, collection_key)
						if cache_value is None:
							self._collection_items_cache[collection_key] = None
							continue

					existing_ids = self._collection_items_cache.get(collection_key)
					if not existing_ids:
						continue

					stats.items_checked += 1
					if item_id not in existing_ids:
						did = doc.get("id")
						if did:
							key_parts = collection_key.split("/")
							site_value = "/".join(key_parts[:2]) if len(key_parts) >= 2 else collection_key
							pending_delete_docs.append(
								{
									"id": did,
									"collection": collection_key,
									"site": site_value,
									"itemId": item_id,
									"parentId": parent_path or path,
								}
							)

				if pending_delete_docs:
					deleted, failed = await self._delete_docs_by_id(run_id, pending_delete_docs)
					stats.docs_deleted += deleted
					stats.docs_failed_delete += failed
					pending_delete_docs.clear()

			if pending_delete_docs:
				deleted, failed = await self._delete_docs_by_id(run_id, pending_delete_docs)
				stats.docs_deleted += deleted
				stats.docs_failed_delete += failed

	# ---------- public entry ----------
	async def run(self) -> None:
		await self._ensure_clients()
		run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
		start_iso = _utc_now()

		stats = PurgeRunStats()
		summary = {
			"indexerType": self.cfg.indexer_name,
			"runId": run_id,
			"runStartedAt": start_iso,
			"runFinishedAt": None,
			"status": "started",
			"collectionsSeen": 0,
			"itemsChecked": 0,
			"docsScanned": 0,
			"docsDeleted": 0,
			"docsFailedDelete": 0,
			"pagesScanned": 0,
		}
		await self._write_run_summary_safely(run_id, summary)
		self._log_event(
			logging.INFO,
			"RUN-START",
			runId=run_id,
			collectionsSeen=0,
			itemsChecked=0,
			docsScanned=0,
			docsDeleted=0,
			docsFailedDelete=0,
			pagesScanned=0,
		)

		try:
			await self._scan_and_purge(stats, run_id)
			summary.update(
				{
					"collectionsSeen": stats.collections_seen,
					"itemsChecked": stats.items_checked,
					"docsScanned": stats.docs_scanned,
					"docsDeleted": stats.docs_deleted,
					"docsFailedDelete": stats.docs_failed_delete,
					"pagesScanned": stats.pages_scanned,
					"status": "finishing",
				}
			)
			await self._write_run_summary_safely(run_id, summary)

		except asyncio.CancelledError:
			summary["status"] = "cancelled"
			summary["runFinishedAt"] = _utc_now()
			await self._write_run_summary_safely(run_id, summary)
			self._log_event(logging.WARNING, "RUN-CANCELLED", runId=run_id)
			logging.info("[%s] Run cancelled: runId=%s", self.cfg.indexer_name, run_id)
			raise
		except Exception as exc:
			logging.exception("[%s] purger run() failed", self.cfg.indexer_name)
			summary["error"] = "see function logs for traceback"
			summary["status"] = "failed"
			self._log_event(logging.ERROR, "RUN-ERROR", runId=run_id, error=str(exc))
		finally:
			summary.update(
				{
					"collectionsSeen": stats.collections_seen,
					"itemsChecked": stats.items_checked,
					"docsScanned": stats.docs_scanned,
					"docsDeleted": stats.docs_deleted,
					"docsFailedDelete": stats.docs_failed_delete,
					"pagesScanned": stats.pages_scanned,
					"runFinishedAt": _utc_now(),
				}
			)
			if summary.get("status") not in {"failed", "cancelled"}:
				summary["status"] = "finished"

			await self._write_run_summary_safely(run_id, summary)
			duration_seconds = None
			try:
				start_dt = _as_dt(summary.get("runStartedAt")) if summary.get("runStartedAt") else None
				finish_dt = _as_dt(summary.get("runFinishedAt")) if summary.get("runFinishedAt") else None
				if start_dt and finish_dt:
					duration_seconds = max((finish_dt - start_dt).total_seconds(), 0.0)
			except Exception:
				duration_seconds = None
			self._log_event(
				logging.INFO,
				"RUN-COMPLETE",
				runId=run_id,
				status=summary.get("status"),
				collectionsSeen=stats.collections_seen,
				itemsChecked=stats.items_checked,
				docsScanned=stats.docs_scanned,
				docsDeleted=stats.docs_deleted,
				docsFailedDelete=stats.docs_failed_delete,
				pagesScanned=stats.pages_scanned,
				durationSeconds=duration_seconds,
			)
			logging.info(
				"[%s] Purge complete: runId=%s collections=%s itemsChecked=%s scanned=%s deleted=%s failedDeletes=%s pages=%s",
				self.cfg.indexer_name,
				run_id,
				stats.collections_seen,
				stats.items_checked,
				stats.docs_scanned,
				stats.docs_deleted,
				stats.docs_failed_delete,
				stats.pages_scanned,
			)
			await self._close_clients()