# connectors/blob_storage_indexer.py
import asyncio
import time
import inspect
import base64
import dataclasses
import json
import logging
import os
import re
import ast
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from azure.identity.aio import AzureCliCredential, ManagedIdentityCredential, ChainedTokenCredential
from azure.core.exceptions import HttpResponseError, ServiceRequestError
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.blob import ContentSettings
from azure.search.documents.aio import SearchClient as AsyncSearchClient

from dependencies import get_config
from chunking import DocumentChunker

# -----------------------------------------------------------------------------
# Custom Exception
# -----------------------------------------------------------------------------
class IndexStateScanLimitExceeded(Exception):
    """Raised when index state scan hits the skip=100k limit, indicating incomplete snapshot."""
    pass


# -----------------------------------------------------------------------------
# Configuration wrapper
# -----------------------------------------------------------------------------
@dataclasses.dataclass
class BlobIndexerConfig:
    # Storage
    storage_account_name: str
    source_container: str
    jobs_log_container: str = "jobs"
    blob_prefix: str = ""  # optional prefix filter

    # Search
    search_endpoint: str = ""  # e.g., https://<svc>.search.windows.net
    search_index_name: str = ""  # e.g., ragindex-<...>

    # Behavior
    max_concurrency: int = 8
    batch_size: int = 500  # AI Search recommended batch size
    indexer_name: str = "blob-storage-indexer"

    # Optional: allow base64 pass-through into chunker, if you change input later
    input_is_base64: bool = False

    @staticmethod
    def from_app_config():
        app = get_config()
        return BlobIndexerConfig(
            search_endpoint=app.get("SEARCH_SERVICE_QUERY_ENDPOINT", ""),
            storage_account_name=app.get("STORAGE_ACCOUNT_NAME", ""),
            source_container=app.get("DOCUMENTS_STORAGE_CONTAINER", "documents"),
            jobs_log_container=app.get("JOBS_LOG_CONTAINER", "jobs"),
            blob_prefix=app.get("BLOB_PREFIX", ""),
            search_index_name=app.get("AI_SEARCH_INDEX_NAME", app.get("SEARCH_RAG_INDEX_NAME", "")),
            max_concurrency=int(app.get("INDEXER_MAX_CONCURRENCY", 8)),
            batch_size=int(app.get("INDEXER_BATCH_SIZE", 500)),
            indexer_name=app.get("BLOB_INDEXER_NAME", "blob-storage-indexer"),
            input_is_base64=(app.get("CHUNKER_INPUT_IS_BASE64", "false").lower() in ("true", "1", "yes")),
        )


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
async def _gather_limited(coros: Iterable, limit: int):
    sem = asyncio.Semaphore(limit)

    async def _runner(coro):
        async with sem:
            return await coro

    return await asyncio.gather(*(_runner(c) for c in coros), return_exceptions=True)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_datetime(value: Any) -> Optional[datetime]:
    """
    Normalize a value that may be a datetime or ISO-8601 string (optionally with 'Z')
    into a timezone-aware datetime. Returns None if it cannot be parsed.
    """
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        s = value.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(s)
        except Exception:
            return None
    return None


# -----------------------------------------------------------------------------
# Indexer
# -----------------------------------------------------------------------------
class BlobStorageDocumentIndexer:
    """
    Enumerates blobs and (re)indexes documents into AI Search using your DocumentChunker.
    - Reindexes only when the blob's last_modified > latest indexed `metadata_storage_last_modified`.
    - Deletes existing chunks for a document (by parent_id) before uploading fresh chunks.
    - Writes per-file logs and a per-run summary to a Storage container.
    """

    def __init__(self, cfg: Optional[BlobIndexerConfig] = None):
        self.cfg = cfg or BlobIndexerConfig.from_app_config()
        self._app = get_config()
        self._credential: Optional[ChainedTokenCredential] = None
        self._blob_service: Optional[BlobServiceClient] = None
        self._search_client: Optional[AsyncSearchClient] = None

    def _log_event(self, level: int, event: str, **fields: Any) -> None:
        """Emit structured JSON logs for KQL queries."""
        payload: Dict[str, Any] = {"event": event}
        for key, value in fields.items():
            if value is None:
                continue
            payload[key] = value.isoformat() if isinstance(value, datetime) else value
        try:
            message = json.dumps(payload, ensure_ascii=False)
        except TypeError:
            safe_payload = {k: str(v) for k, v in payload.items()}
            message = json.dumps(safe_payload, ensure_ascii=False)
        logging.log(level, f"[{self.cfg.indexer_name}] {message}")

    # ---------- Clients ----------
    async def _ensure_clients(self):
        if not self._credential:
            client_id = os.environ.get("AZURE_CLIENT_ID", None)
            self._credential = ChainedTokenCredential(
                AzureCliCredential(),
                ManagedIdentityCredential(client_id=client_id)
            )
        if not self._blob_service:
            acc = self.cfg.storage_account_name
            self._blob_service = BlobServiceClient(
                f"https://{acc}.blob.core.windows.net", credential=self._credential
            )
        if not self._search_client:
            self._search_client = AsyncSearchClient(
                endpoint=self.cfg.search_endpoint,
                index_name=self.cfg.search_index_name,
                credential=self._credential,
            )

    # ---------- Public entrypoint ----------
    async def run(self) -> None:
        await self._ensure_clients()
        # create a runId that matches the run summary filename and capture start time (ISO)
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        start_iso = _utc_now()
        logging.info(f"[{self.cfg.indexer_name}] Starting @ {run_id}")
        summary = {
            "indexerType": self.cfg.indexer_name,
            "runId": run_id,
            "runStartedAt": start_iso,
            "runFinishedAt": None,
            "sourceContainer": self.cfg.source_container,
            "sourceFiles": 0,
            "candidates": 0,
            "itemsDiscovered": 0,
            "indexedItems": 0,
            "skippedNoChange": 0,
            "success": 0,
            "failed": 0,
            "totalChunksUploaded": 0,
            "status": "started",
        }
        await self._write_run_summary(self.cfg.jobs_log_container, summary, run_id)
        self._log_event(
            logging.INFO,
            "RUN-START",
            runId=run_id,
            sourceContainer=self.cfg.source_container,
            blobPrefix=self.cfg.blob_prefix,
            maxConcurrency=self.cfg.max_concurrency,
        )

        source_files = 0
        items_discovered = 0
        success = 0
        failed = 0
        total_chunks = 0
        skipped_no_change = 0

        try:
            # ensure log containers exist (best-effort)
            await self._ensure_container(self.cfg.jobs_log_container)

            # Load current index snapshot for dedup/TS comparisons
            latest_map = await self._load_latest_index_state()
            logging.info(
                f"[{self.cfg.indexer_name}] Loaded index state for {len(latest_map)} parent_id keys"
            )

            # Enumerate blobs to consider
            container = self._blob_service.get_container_client(self.cfg.source_container)
            to_process: List[Tuple[str, datetime, str]] = []

            async for b in container.list_blobs(name_starts_with=self.cfg.blob_prefix):
                if getattr(b, "size", None) == 0 and b.name.endswith("/"):
                    continue
                source_files += 1
                parent_id = self._make_parent_id(b.name)
                blob_lm: datetime = b.last_modified
                prev_lm: Optional[datetime] = latest_map.get(parent_id)
                if prev_lm is None or blob_lm > prev_lm:
                    to_process.append((
                        b.name,
                        blob_lm,
                        getattr(b, "content_settings", None).content_type if getattr(b, "content_settings", None)
                        else "application/octet-stream"
                    ))

            items_discovered = len(to_process)
            skipped_no_change = max(source_files - items_discovered, 0)

            summary.update({
                "sourceFiles": source_files,
                "candidates": items_discovered,
                "itemsDiscovered": items_discovered,
                "skippedNoChange": skipped_no_change,
                "status": "running",
            })
            await self._write_run_summary(self.cfg.jobs_log_container, summary, run_id)

            logging.info(f"[{self.cfg.indexer_name}] Candidates: {items_discovered}")

            # Process in parallel, pass run_id down
            results = await _gather_limited(
                (self._process_one(name, lm, ctype, run_id) for (name, lm, ctype) in to_process),
                self.cfg.max_concurrency,
            )

            # Summarize
            success = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "success")
            failed = sum(1 for r in results if not isinstance(r, dict) or r.get("status") == "error")
            total_chunks = sum(r.get("chunks", 0) for r in results if isinstance(r, dict))

            summary.update({
                "success": success,
                "indexedItems": success,
                "failed": failed,
                "totalChunksUploaded": total_chunks,
                "status": "finishing",
            })
            await self._write_run_summary(self.cfg.jobs_log_container, summary, run_id)

        except asyncio.CancelledError:
            summary["status"] = "cancelled"
            summary["runFinishedAt"] = _utc_now()
            await self._write_run_summary(self.cfg.jobs_log_container, summary, run_id)
            self._log_event(logging.WARNING, "RUN-CANCELLED", runId=run_id)
            logging.info(f"[{self.cfg.indexer_name}] Run cancelled: runId={run_id}")
            raise
        except Exception as exc:
            logging.exception(f"[{self.cfg.indexer_name}] run() failed")
            summary["error"] = str(exc)
            summary["status"] = "failed"
            await self._write_run_summary(self.cfg.jobs_log_container, summary, run_id)
            self._log_event(logging.ERROR, "RUN-ERROR", runId=run_id, error=str(exc))
        finally:
            summary.update({
                "runFinishedAt": summary.get("runFinishedAt") or _utc_now(),
                "sourceFiles": source_files,
                "candidates": items_discovered,
                "itemsDiscovered": items_discovered,
                "indexedItems": success,
                "success": success,
                "failed": failed,
                "skippedNoChange": skipped_no_change,
                "totalChunksUploaded": total_chunks,
            })
            if summary.get("status") not in {"failed", "cancelled"}:
                summary["status"] = "finished"

            await self._write_run_summary(self.cfg.jobs_log_container, summary, run_id)
            duration_seconds: Optional[float] = None
            try:
                start_dt = _as_datetime(summary.get("runStartedAt"))
                finish_dt = _as_datetime(summary.get("runFinishedAt"))
                if start_dt and finish_dt:
                    duration_seconds = max((finish_dt - start_dt).total_seconds(), 0.0)
            except Exception:
                duration_seconds = None

            self._log_event(
                logging.INFO,
                "RUN-COMPLETE",
                runId=run_id,
                status=summary.get("status"),
                sourceFiles=source_files,
                itemsDiscovered=items_discovered,
                indexedItems=success,
                skippedNoChange=skipped_no_change,
                failed=failed,
                totalChunksUploaded=total_chunks,
                durationSeconds=duration_seconds,
            )
            logging.info(f"[{self.cfg.indexer_name}] Summary: {json.dumps(summary)}")
            await self._close_clients_safely()

    async def _close_clients_safely(self):
        # Gracefully close async clients/credentials to avoid aiohttp SSL shutdown warnings
        try:
            if self._search_client:
                await self._search_client.close()
        except Exception:
            logging.debug("[indexer] ignoring error while closing search client", exc_info=True)
        try:
            if self._blob_service:
                await self._blob_service.close()
        except Exception:
            logging.debug("[indexer] ignoring error while closing blob service", exc_info=True)
        try:
            if self._credential and hasattr(self._credential, "close"):
                res = self._credential.close()
                if inspect.isawaitable(res):
                    await res
        except Exception:
            logging.debug("[indexer] ignoring error while closing credential", exc_info=True)

    # ---------- Core per-blob flow ----------
    async def _process_one(
        self,
        blob_name: str,
        last_modified: datetime,
        content_type: str,
        run_id: str
    ) -> Dict[str, Any]:
        await self._ensure_clients()
        parent_id = self._make_parent_id(blob_name)
        container_client = self._blob_service.get_container_client(self.cfg.source_container)
        blob_client = container_client.get_blob_client(blob_name)
        file_url = f"https://{self.cfg.storage_account_name}.blob.core.windows.net/{self.cfg.source_container}/{blob_name}"
        file_log_key = parent_id.replace("/", "-")

        per_file_log = {
            "indexerType": self.cfg.indexer_name,
            "blob": blob_name,
            "parent_id": parent_id,
            "last_modified": last_modified.astimezone(timezone.utc).isoformat(),
            "startedAt": _utc_now(),
            "runId": run_id,
            "chunksIds": self._make_chunk_key_prefix(parent_id),
        }
        try:
            # Fetch blob metadata to capture security IDs if provided
            security_ids: List[str] = []
            try:
                props = await blob_client.get_blob_properties()
                meta = (getattr(props, "metadata", None) or {})
                # Azure stores metadata keys as lowercase
                raw_val = meta.get("metadata_security_id") or meta.get("metadata-security-id")
                if raw_val:
                    security_ids = self._parse_security_ids(raw_val)
            except Exception as _:
                # Non-fatal: continue without security IDs
                security_ids = []

            download = await blob_client.download_blob()
            file_bytes = await download.readall()

            # If you ever switch to providing Base64 to the chunker, decode here:
            if self.cfg.input_is_base64 and isinstance(file_bytes, (str, bytes)):
                if isinstance(file_bytes, str):
                    file_bytes = base64.b64decode(file_bytes)

            # Prepare data for your chunker
            data = {
                "documentUrl": file_url,
                "documentSasToken": "",  # MI is used; SAS not needed
                "documentContentType": content_type or "application/octet-stream",
                "documentBytes": file_bytes,
                "fileName": os.path.basename(blob_name),
            }

            # Chunk (off-thread to keep event loop snappy if heavy)
            chunks, errors, warnings = await asyncio.to_thread(DocumentChunker().chunk_documents, data)
            if errors:
                raise RuntimeError(f"chunker returned errors: {errors}")

            # Convert chunks -> search docs
            docs = [self._to_search_doc(chunk, parent_id, file_url, blob_name, last_modified, security_ids) for chunk in chunks]

            # Replace existing parent's docs with fresh set
            await self._replace_parent_docs(parent_id, docs)

            per_file_log.update({
                "status": "success",
                "chunks": len(docs),
                "finishedAt": _utc_now(),
            })
            await self._write_file_log(self.cfg.jobs_log_container, f"{file_log_key}.json", per_file_log)
            self._log_event(
                logging.INFO,
                "ITEM-COMPLETE",
                runId=run_id,
                blobName=blob_name,
                parentId=parent_id,
                status="success",
                totalChunks=len(docs),
                contentType=content_type or "application/octet-stream",
                fileUrl=file_url,
                lastModified=last_modified,
            )
            return {"status": "success", "chunks": len(docs)}

        except Exception as e:
            logging.exception(f"[{self.cfg.indexer_name}] Failed processing {blob_name}")
            per_file_log.update({
                "status": "error",
                "error": str(e),
                "finishedAt": _utc_now(),
            })
            await self._write_file_log(self.cfg.jobs_log_container, f"{file_log_key}.json", per_file_log)
            self._log_event(
                logging.ERROR,
                "ITEM-ERROR",
                runId=run_id,
                blobName=blob_name,
                parentId=parent_id,
                status="error",
                contentType=content_type or "application/octet-stream",
                fileUrl=file_url,
                error=str(e),
            )
            return {"status": "error", "error": str(e)}

    def _to_search_doc(
        self,
        chunk: Dict[str, Any],
        parent_id: str,
        file_url: str,
        blob_name: str,
        last_modified: datetime,
        security_ids: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        # Azure Search key must be unique & stable per chunk
        chunk_id = int(chunk.get("chunk_id", 0))
        key = self._make_chunk_key(parent_id, chunk_id)
        return {
            "id": key,
            "parent_id": parent_id,
            "metadata_storage_path": parent_id,
            "metadata_storage_name": os.path.basename(blob_name),
            "metadata_storage_last_modified": last_modified,
            "metadata_security_id": security_ids or [],
            "chunk_id": chunk_id,
            "content": chunk.get("content", ""),
            "imageCaptions": chunk.get("imageCaptions", ""),
            "page": int(chunk.get("page", 0)),
            "offset": int(chunk.get("offset", 0)),
            "length": int(chunk.get("length", len(chunk.get("content", "")))),
            "title": chunk.get("title", ""),
            "category": chunk.get("category", ""),
            "filepath": chunk.get("filepath", parent_id),
            "url": chunk.get("url", file_url),
            "summary": chunk.get("summary", ""),
            "relatedImages": chunk.get("relatedImages", []),
            "relatedFiles": chunk.get("relatedFiles", []),
            "source": "blob",
            "contentVector": chunk.get("contentVector", []),
            "captionVector": chunk.get("captionVector", []),
        }

    # ---------- Index state & (re)write ----------
    async def _load_latest_index_state(self, max_seconds: float = 300.0) -> Dict[str, datetime]:
        """
        Return map parent_id -> latest metadata_storage_last_modified from index.
        Raises IndexStateScanLimitExceeded if skip limit is hit (incomplete snapshot).
        """
        await self._ensure_clients()

        latest: Dict[str, datetime] = {}
        page_size = 1000
        skip = 0
        total_docs_seen = 0
        start = time.monotonic()

        logging.info(
            f"[{self.cfg.indexer_name}] load-state/start page_size={page_size} max_seconds={max_seconds:.1f}"
        )

        while True:
            elapsed = time.monotonic() - start
            if elapsed > max_seconds:
                logging.warning(
                    f"[{self.cfg.indexer_name}] load-state/timeout elapsed={elapsed:.1f}s skip={skip} seen={total_docs_seen}"
                )
                break

            if skip >= 100_000:
                error_msg = (
                    f"Index state scan hit skip limit (skip={skip}, seen={total_docs_seen}). "
                    f"Index has >100k documents. Cannot build complete state snapshot. "
                    f"Please clean up the index or partition the ingestion before retrying."
                )
                logging.error(f"[{self.cfg.indexer_name}] {error_msg}")
                raise IndexStateScanLimitExceeded(error_msg)

            logging.info(
                f"[{self.cfg.indexer_name}] load-state/fetch skip={skip} top={page_size} elapsed={elapsed:.1f}s seen={total_docs_seen}"
            )

            results = await self._search_client.search(
                search_text="*",
                select=["parent_id", "metadata_storage_last_modified"],
                top=page_size,
                skip=skip,
            )

            batch_count = 0
            async for doc in results:
                batch_count += 1
                total_docs_seen += 1

                pid = doc.get("parent_id")
                lm_raw = doc.get("metadata_storage_last_modified")
                lm = _as_datetime(lm_raw)
                if pid and lm:
                    prev = latest.get(pid)
                    if not prev or lm > prev:
                        latest[pid] = lm

            logging.info(
                f"[{self.cfg.indexer_name}] load-state/page-done skip={skip} batch={batch_count} seen={total_docs_seen}"
            )

            if batch_count == 0:
                elapsed = time.monotonic() - start
                logging.info(
                    f"[{self.cfg.indexer_name}] load-state/end skip={skip} seen={total_docs_seen} elapsed={elapsed:.1f}s"
                )
                break

            skip += page_size

        logging.info(
            f"[{self.cfg.indexer_name}] load-state/complete parent-ids={len(latest)} seen={total_docs_seen}"
        )

        return latest


    async def _replace_parent_docs(self, parent_id: str, docs: List[Dict[str, Any]]):
        # Delete existing docs for parent_id then upload fresh docs in batches
        await self._delete_parent_docs(parent_id)
        await self._upload_in_batches(docs)

    async def _delete_parent_docs(self, parent_id: str):
        await self._ensure_clients()

        page_size = 1000
        skip = 0
        total_ids = 0
        max_seconds = 300.0
        start = time.monotonic()
        sanitized = parent_id.replace("'", "''")

        logging.info(
            f"[{self.cfg.indexer_name}] _delete_parent_docs starting for parent_id={parent_id} "
            f"(page_size={page_size}, max_seconds={max_seconds})"
        )

        while True:
            elapsed = time.monotonic() - start
            if elapsed > max_seconds:
                logging.warning(
                    f"[{self.cfg.indexer_name}] _delete_parent_docs TIMEOUT for parent_id={parent_id} "
                    f"after {elapsed:.1f}s (skip={skip}, total_ids={total_ids})"
                )
                break

            if skip >= 100_000:
                logging.warning(
                    f"[{self.cfg.indexer_name}] _delete_parent_docs reached skip limit for parent_id={parent_id} "
                    f"(skip={skip}, total_ids={total_ids}). Stopping delete."
                )
                break

            logging.info(
                f"[{self.cfg.indexer_name}] _delete_parent_docs fetching page for parent_id={parent_id}: "
                f"skip={skip}, top={page_size}, elapsed={elapsed:.1f}s, total_ids={total_ids}"
            )

            results = await self._search_client.search(
                search_text="*",
                filter=f"parent_id eq '{sanitized}'",
                select=["id"],
                top=page_size,
                skip=skip,
            )

            batch_ids: List[Dict[str, str]] = []
            async for doc in results:
                doc_id = doc.get("id")
                if doc_id:
                    batch_ids.append({"id": doc_id})

            batch_count = len(batch_ids)
            logging.info(
                f"[{self.cfg.indexer_name}] _delete_parent_docs page collected "
                f"{batch_count} ids for parent_id={parent_id} (skip={skip})"
            )

            if batch_ids:
                await self._with_backoff(self._search_client.delete_documents, documents=batch_ids)
                total_ids += batch_count

            if batch_count == 0:
                elapsed = time.monotonic() - start
                logging.info(
                    f"[{self.cfg.indexer_name}] _delete_parent_docs finished for parent_id={parent_id} "
                    f"(total_ids_deleted={total_ids}, elapsed={elapsed:.1f}s)"
                )
                break

            skip += page_size

    async def _upload_in_batches(self, docs: List[Dict[str, Any]]):
        if not docs:
            return
        for batch in _chunk(docs, self.cfg.batch_size):
            await self._with_backoff(self._search_client.upload_documents, documents=batch)

    async def _with_backoff(self, func, **kwargs):
        # Respect retry-after-ms if present; exponential fallback
        delay = 1.0
        for attempt in range(8):
            try:
                return await func(**kwargs)
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
                logging.warning(f"[{self.cfg.indexer_name}] backoff {delay}s on {type(e).__name__}: {e}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
            except ServiceRequestError as e:
                logging.warning(f"[{self.cfg.indexer_name}] network error; retrying in {delay}s: {e}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    # ---------- Logging helpers ----------
    async def _ensure_container(self, name: str):
        try:
            cc = self._blob_service.get_container_client(name)
            await cc.create_container()
        except Exception:
            # likely already exists
            pass

    async def _write_file_log(self, container: str, blob_name: str, payload: Dict[str, Any]):
        await self._ensure_clients()
        cc = self._blob_service.get_container_client(container)
        try:
            await cc.upload_blob(
                name=f"{self.cfg.indexer_name}/files/{blob_name}",
                data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
                overwrite=True,
                content_settings=ContentSettings(content_type="application/json"),
            )
        except Exception:
            logging.exception(f"[{self.cfg.indexer_name}] failed to write file log {blob_name}")

    async def _write_run_summary(self, container: str, summary: Dict[str, Any], run_id: str):
        await self._ensure_clients()
        cc = self._blob_service.get_container_client(container)
        name = f"{self.cfg.indexer_name}/runs/{run_id}.json"
        try:
            await cc.upload_blob(
                name=name,
                data=json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8"),
                overwrite=True,
                content_settings=ContentSettings(content_type="application/json"),
            )
        except Exception:
            logging.exception(f"[{self.cfg.indexer_name}] failed to write run summary {name}")

    # ---------- Utilities ----------
    def _make_parent_id(self, blob_name: str) -> str:
        # Unique per file path; keep stable and filterable
        return f"/{self.cfg.source_container}/{blob_name}"

    def _sanitize_key_part(self, s: str) -> str:
        """
        Sanitize a string for use in an Azure AI Search key:
        keep only [A-Za-z0-9_-]; replace others (including '.') with '-'; collapse repeats; trim.
        """
        # Replace disallowed chars (including '.') with '-'
        s = re.sub(r"[^A-Za-z0-9_-]+", "-", s)
        # Collapse multiple '-'
        s = re.sub(r"-+", "-", s)
        # Trim leading/trailing '-'
        return s.strip('-')

    def _make_chunk_key(self, parent_id: str, chunk_id: int) -> str:
        """
        Build a stable, valid key from the file path (parent_id) + chunk number.
        Ensures only allowed characters are present and key length is reasonable.
        """
        # Drop leading slash from parent_id for readability and sanitize
        base = self._sanitize_key_part(parent_id.lstrip('/'))
        if not base:
            base = "doc"
        # Truncate overly long base while preserving uniqueness with a short hash
        if len(base) > 128:
            digest = hashlib.sha1(parent_id.encode('utf-8')).hexdigest()[:10]
            base = f"{base[:100]}-{digest}"
        return f"{base}-c{chunk_id:05d}"

    def _make_chunk_key_prefix(self, parent_id: str) -> str:
        """
        Build the prefix pattern to find all chunks for a given parent_id.
        Example: for '/documents/employee_handbook.pdf' -> 'documents-employee_handbook-pdf-c*'
        """
        base = self._sanitize_key_part(parent_id.lstrip('/'))
        if not base:
            base = "doc"
        if len(base) > 128:
            digest = hashlib.sha1(parent_id.encode('utf-8')).hexdigest()[:10]
            base = f"{base[:100]}-{digest}"
        return f"{base}-c*"

    def _parse_security_ids(self, raw_val: str) -> List[str]:
        """
        Parse metadata_security_id from blob metadata into a clean list of strings.
        Supports:
        - JSON arrays: ["a","b"]
        - Python-style lists with single quotes: ['a', 'b']
        - Comma/semicolon-separated strings: a,b or a; b
        """
        # Try JSON
        try:
            parsed = json.loads(raw_val)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            pass
        # Try Python literal (e.g., "['a', 'b']")
        try:
            lit = ast.literal_eval(raw_val)
            if isinstance(lit, list):
                return [str(x).strip() for x in lit if str(x).strip()]
        except Exception:
            pass
        # Fallback: strip surrounding brackets, split on comma or semicolon
        s = raw_val.strip()
        if s.startswith("[") and s.endswith("]"):
            s = s[1:-1]
        # Remove any surrounding single/double quotes from each token
        tokens = re.split(r"[,;]", s)
        cleaned: List[str] = []
        for t in tokens:
            tt = t.strip()
            if (tt.startswith("'") and tt.endswith("'")) or (tt.startswith('"') and tt.endswith('"')):
                tt = tt[1:-1].strip()
            if tt:
                cleaned.append(tt)
        return cleaned


# -----------------------------------------------------------------------------
# Purger: delete index docs for blobs that no longer exist
# -----------------------------------------------------------------------------
class BlobStorageDeletedItemsCleaner:
    def __init__(self, cfg: Optional[BlobIndexerConfig] = None):
        self.cfg = cfg or BlobIndexerConfig.from_app_config()
        self._credential: Optional[ChainedTokenCredential] = None
        self._blob_service: Optional[BlobServiceClient] = None
        self._search_client: Optional[AsyncSearchClient] = None

    async def _ensure_clients(self):
        if not self._credential:
            client_id = os.environ.get("AZURE_CLIENT_ID", None)
            self._credential = ChainedTokenCredential(
                AzureCliCredential(),
                ManagedIdentityCredential(client_id=client_id)
            )
        if not self._blob_service:
            acc = self.cfg.storage_account_name
            self._blob_service = BlobServiceClient(
                f"https://{acc}.blob.core.windows.net", credential=self._credential
            )
        if not self._search_client:
            self._search_client = AsyncSearchClient(
                endpoint=self.cfg.search_endpoint,
                index_name=self.cfg.search_index_name,
                credential=self._credential,
            )

    async def run(self) -> None:
        await self._ensure_clients()
        start_iso = _utc_now()
        logging.info(f"[blob-storage-purger] Starting @ {start_iso}")

        try:
            # Ensure jobs log container exists
            await self._ensure_container(self.cfg.jobs_log_container)

            # Current set of parent_ids that exist in Storage
            existing: Set[str] = set()
            cont = self._blob_service.get_container_client(self.cfg.source_container)
            async for b in cont.list_blobs(name_starts_with=self.cfg.blob_prefix):
                if getattr(b, "size", None) == 0 and b.name.endswith("/"):
                    continue
                existing.add(f"/{self.cfg.source_container}/{b.name}")

            logging.info(
                f"[blob-storage-purger] Found {len(existing)} parent_ids in blob container "
                f"'{self.cfg.source_container}'"
            )

            # All parent_ids present in index and total chunk documents before
            in_index: Set[str] = set()
            chunk_docs_before: int = 0
            page_size = 1000
            skip = 0
            max_seconds_index_scan = 600.0
            scan_start = time.monotonic()

            logging.info(
                f"[blob-storage-purger] Scanning index for blob documents "
                f"(page_size={page_size}, max_seconds={max_seconds_index_scan})"
            )

            while True:
                elapsed = time.monotonic() - scan_start
                if elapsed > max_seconds_index_scan:
                    logging.warning(
                        f"[blob-storage-purger] Index scan TIMEOUT after {elapsed:.1f}s "
                        f"(skip={skip}, chunk_docs_before={chunk_docs_before})"
                    )
                    break

                if skip >= 100_000:
                    logging.warning(
                        f"[blob-storage-purger] Index scan reached skip limit "
                        f"(skip={skip}, chunk_docs_before={chunk_docs_before}). Stopping scan."
                    )
                    break

                logging.info(
                    f"[blob-storage-purger] Fetching index page: skip={skip}, top={page_size}, "
                    f"elapsed={elapsed:.1f}s, docs_so_far={chunk_docs_before}"
                )

                results = await self._search_client.search(
                    search_text="*",
                    filter="source eq 'blob'",
                    select=["parent_id"],
                    top=page_size,
                    skip=skip,
                )

                batch_count = 0
                async for doc in results:
                    batch_count += 1
                    chunk_docs_before += 1
                    pid = doc.get("parent_id")
                    if pid:
                        in_index.add(pid)

                logging.info(
                    f"[blob-storage-purger] Index page processed: skip={skip}, "
                    f"batch_count={batch_count}, docs_so_far={chunk_docs_before}"
                )

                if batch_count == 0:
                    elapsed = time.monotonic() - scan_start
                    logging.info(
                        f"[blob-storage-purger] Reached end of index results at skip={skip} "
                        f"(total_chunk_docs={chunk_docs_before}, elapsed={elapsed:.1f}s)"
                    )
                    break

                skip += page_size

            # Compute counts at the beginning
            source_parent_count = len(existing)
            indexed_parent_count_before = len(in_index)

            to_purge = sorted(in_index - existing)
            logging.info(
                f"[blob-storage-purger] Will purge {len(to_purge)} parent_id sets "
                f"(source_parent_count={source_parent_count}, index_parent_count_before={indexed_parent_count_before})"
            )

            total_deleted_docs = 0
            max_seconds_per_parent = 300.0

            for parent_id in to_purge:
                sanitized_parent = parent_id.replace("'", "''")
                per_parent_deleted = 0
                skip = 0
                parent_start = time.monotonic()

                logging.info(
                    f"[blob-storage-purger] Deleting documents for parent_id={parent_id} "
                    f"(page_size={page_size}, max_seconds={max_seconds_per_parent})"
                )

                while True:
                    elapsed_parent = time.monotonic() - parent_start
                    if elapsed_parent > max_seconds_per_parent:
                        logging.warning(
                            f"[blob-storage-purger] TIMEOUT deleting docs for parent_id={parent_id} "
                            f"after {elapsed_parent:.1f}s (skip={skip}, per_parent_deleted={per_parent_deleted})"
                        )
                        break

                    if skip >= 100_000:
                        logging.warning(
                            f"[blob-storage-purger] Reached skip limit while deleting for parent_id={parent_id} "
                            f"(skip={skip}, per_parent_deleted={per_parent_deleted}). Stopping deletes for this parent."
                        )
                        break

                    logging.info(
                        f"[blob-storage-purger] Fetching delete page for parent_id={parent_id}: "
                        f"skip={skip}, top={page_size}, elapsed={elapsed_parent:.1f}s, "
                        f"per_parent_deleted={per_parent_deleted}"
                    )

                    result = await self._search_client.search(
                        search_text="*",
                        filter=f"parent_id eq '{sanitized_parent}' and source eq 'blob'",
                        select=["id"],
                        top=page_size,
                        skip=skip,
                    )

                    page_ids: List[Dict[str, str]] = []
                    async for doc in result:
                        doc_id = doc.get("id")
                        if doc_id:
                            page_ids.append({"id": doc_id})

                    batch_count = len(page_ids)
                    logging.info(
                        f"[blob-storage-purger] Collected {batch_count} ids to delete for parent_id={parent_id} "
                        f"(skip={skip})"
                    )

                    if page_ids:
                        await self._with_backoff(self._search_client.delete_documents, documents=page_ids)
                        per_parent_deleted += batch_count
                        total_deleted_docs += batch_count

                    if batch_count == 0:
                        elapsed_parent = time.monotonic() - parent_start
                        logging.info(
                            f"[blob-storage-purger] Finished deleting docs for parent_id={parent_id} "
                            f"(deleted={per_parent_deleted}, elapsed={elapsed_parent:.1f}s)"
                        )
                        break

                    skip += page_size

                # write per-parent log
                await self._write_file_log(
                    self.cfg.jobs_log_container,
                    f"{parent_id.replace('/', '-')}.json",
                    {
                        "indexerType": "blob-storage-purger",
                        "parent_id": parent_id,
                        "deletedChunkDocs": per_parent_deleted,
                        "finishedAt": _utc_now(),
                    },
                )

            # Post-delete: brief, bounded consistency wait; stop when purged parents disappear
            expected_after = max(indexed_parent_count_before - len(to_purge), 0)
            attempts = 3
            delays = [0.75, 1.5, 3.0]
            index_parents_after = None
            max_seconds_check = 120.0

            for i in range(attempts):
                in_index_after: Set[str] = set()
                skip = 0
                check_start = time.monotonic()
                logging.info(
                    f"[blob-storage-purger] Consistency check attempt {i + 1}/{attempts} "
                    f"(page_size={page_size}, max_seconds={max_seconds_check})"
                )

                while True:
                    elapsed_check = time.monotonic() - check_start
                    if elapsed_check > max_seconds_check:
                        logging.warning(
                            f"[blob-storage-purger] Consistency check TIMEOUT on attempt {i + 1} "
                            f"after {elapsed_check:.1f}s (skip={skip}, parents_seen={len(in_index_after)})"
                        )
                        break

                    if skip >= 100_000:
                        logging.warning(
                            f"[blob-storage-purger] Consistency check reached skip limit on attempt {i + 1} "
                            f"(skip={skip}, parents_seen={len(in_index_after)})."
                        )
                        break

                    results_after = await self._search_client.search(
                        search_text="*",
                        filter="source eq 'blob'",
                        select=["parent_id"],
                        top=page_size,
                        skip=skip,
                    )

                    batch_count = 0
                    async for doc in results_after:
                        batch_count += 1
                        pid = doc.get("parent_id")
                        if pid:
                            in_index_after.add(pid)

                    if batch_count == 0:
                        break

                    skip += page_size

                index_parents_after = len(in_index_after)
                logging.info(
                    f"[blob-storage-purger] Consistency check attempt {i + 1} result: "
                    f"index_parents_after={index_parents_after}, expected_after={expected_after}"
                )

                # Condition 1: observed <= expected_after (index settled enough)
                # Condition 2: none of the purged parents remain
                if index_parents_after <= expected_after and not any(p in in_index_after for p in to_purge):
                    logging.info(
                        "[blob-storage-purger] Consistency check satisfied: purged parents are gone "
                        "and parent count is at or below expected."
                    )
                    break

                if i < len(delays):
                    await asyncio.sleep(delays[i])

            if index_parents_after is None:
                index_parents_after = 0

            # summary
            summary = {
                "indexerType": "blob-storage-purger",
                "runStartedAt": start_iso,
                "runFinishedAt": _utc_now(),
                "blobDocumentsCount": source_parent_count,
                "indexParentsCountBefore": indexed_parent_count_before,
                "indexChunkDocumentsBefore": chunk_docs_before,
                "indexParentsPurged": len(to_purge),
                "indexChunkDocumentsDeleted": total_deleted_docs,
                "indexParentsCountAfter": index_parents_after,
            }
            await self._write_run_summary(self.cfg.jobs_log_container, summary)
            logging.info(f"[blob-storage-purger] Summary: {json.dumps(summary)}")
        finally:
            await self._close_clients_safely()

    async def _close_clients_safely(self):
        # Gracefully close async clients/credentials to avoid aiohttp SSL shutdown warnings
        try:
            if self._search_client:
                await self._search_client.close()
        except Exception:
            logging.debug("[purger] ignoring error while closing search client", exc_info=True)
        try:
            if self._blob_service:
                await self._blob_service.close()
        except Exception:
            logging.debug("[purger] ignoring error while closing blob service", exc_info=True)
        try:
            if self._credential and hasattr(self._credential, "close"):
                res = self._credential.close()
                if inspect.isawaitable(res):
                    await res
        except Exception:
            logging.debug("[purger] ignoring error while closing credential", exc_info=True)

    # --- shared helpers (same as indexer; duplicated for clarity) ---
    async def _with_backoff(self, func, **kwargs):
        delay = 1.0
        for attempt in range(8):
            try:
                return await func(**kwargs)
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
                logging.warning(f"[blob-storage-purger] backoff {delay}s on {type(e).__name__}: {e}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def _ensure_container(self, name: str):
        try:
            cc = self._blob_service.get_container_client(name)
            await cc.create_container()
        except Exception:
            pass

    async def _write_file_log(self, container: str, blob_name: str, payload: Dict[str, Any]):
        cc = self._blob_service.get_container_client(container)
        try:
            await cc.upload_blob(
                name=f"blob-storage-purger/files/{blob_name}",
                data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
                overwrite=True,
                content_settings=ContentSettings(content_type="application/json"),
            )
        except Exception:
            logging.exception(f"[blob-storage-purger] failed to write file log {blob_name}")

    async def _write_run_summary(self, container: str, summary: Dict[str, Any]):
        cc = self._blob_service.get_container_client(container)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        name = f"blob-storage-purger/runs/{stamp}.json"
        try:
            await cc.upload_blob(
                name=name,
                data=json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8"),
                overwrite=True,
                content_settings=ContentSettings(content_type="application/json"),
            )
        except Exception:
            logging.exception(f"[blob-storage-purger] failed to write run summary {name}")


# -----------------------------------------------------------------------------
# Local helpers
# -----------------------------------------------------------------------------
def _chunk(items: List[Any], n: int):
    for i in range(0, len(items), n):
        yield items[i: i + n]