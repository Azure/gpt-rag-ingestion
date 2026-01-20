import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Dict

from azure.identity.aio import AzureCliCredential, ManagedIdentityCredential, ChainedTokenCredential
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.blob import ContentSettings
from azure.core.exceptions import ResourceNotFoundError

from dependencies import get_config
from tools import AISearchClient, AzureOpenAIClient


@dataclass
class NL2SQLIndexerConfig:
    storage_account_name: str
    container_name: str = "nl2sql"
    jobs_log_container: str = "jobs"
    max_concurrency: int = 4

    # AI Search index names
    queries_index_name: str = ""
    tables_index_name: str = ""
    measures_index_name: str = ""

    @staticmethod
    def from_app_config():
        app = get_config()
        return NL2SQLIndexerConfig(
            storage_account_name=app.get("STORAGE_ACCOUNT_NAME", ""),
            container_name=app.get("NL2SQL_STORAGE_CONTAINER", "nl2sql"),
            jobs_log_container=app.get("JOBS_LOG_CONTAINER", "jobs"),
            max_concurrency=int(app.get("INDEXER_MAX_CONCURRENCY", 4)),
            queries_index_name=app.get("SEARCH_QUERIES_INDEX_NAME", ""),
            tables_index_name=app.get("SEARCH_TABLES_INDEX_NAME", ""),
            measures_index_name=app.get("SEARCH_MEASURES_INDEX_NAME", ""),
        )


class NL2SQLIndexer:
    """
    Scans the 'nl2sql' container for JSON files under queries/, tables/, measures/ and
    indexes them into the respective Azure AI Search indices. Embeddings are generated
    with Azure OpenAI using:
      - queries: question -> contentVector
      - tables: description -> contentVector
      - measures: description -> contentVector

    Document key ('id') is the blob path (e.g., 'queries/top_5_expensive_products.json').
    """

    def __init__(self, cfg: Optional[NL2SQLIndexerConfig] = None):
        self.cfg = cfg or NL2SQLIndexerConfig.from_app_config()
        self._credential: Optional[ChainedTokenCredential] = None
        self._blob_service: Optional[BlobServiceClient] = None
        self._app = get_config()
        self._ai_search = AISearchClient()

        # quick validation to help early failures
        if not self.cfg.storage_account_name:
            raise ValueError("STORAGE_ACCOUNT_NAME must be set in configuration")
        for name, val in {
            "SEARCH_QUERIES_INDEX_NAME": self.cfg.queries_index_name,
            "SEARCH_TABLES_INDEX_NAME": self.cfg.tables_index_name,
            "SEARCH_MEASURES_INDEX_NAME": self.cfg.measures_index_name,
        }.items():
            if not val:
                logging.warning(f"[nl2sql-indexer] {name} is not configured; corresponding content will fail to index")

    def _log_event(self, level: int, event: str, **fields) -> None:
        """Emit structured logs matching indexer format."""
        payload: Dict = {"event": event}
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
        logging.log(level, f"[nl2sql-indexer] {message}")

    async def _ensure_clients(self):
        if not self._credential:
            client_id = self._app.get("AZURE_CLIENT_ID", None, allow_none=True)
            self._credential = ChainedTokenCredential(
                AzureCliCredential(),
                ManagedIdentityCredential(client_id=client_id),
            )
        if not self._blob_service:
            acc = self.cfg.storage_account_name
            self._blob_service = BlobServiceClient(
                f"https://{acc}.blob.core.windows.net", credential=self._credential
            )

    async def run(self) -> None:
        await self._ensure_clients()
        run_started_at = datetime.now(timezone.utc)
        run_id = run_started_at.strftime("%Y%m%dT%H%M%SZ")
        start_iso = run_started_at.isoformat()
        logging.info(f"[nl2sql-indexer] Starting @ {run_id}")
        self._log_event(
            logging.INFO,
            "RUN-START",
            runId=run_id,
            sourceContainer=self.cfg.container_name,
        )

        total_ok = 0
        total_err = 0
        total_skipped = 0
        total_candidates = 0  # processed = success + failed
        per_kind_counts: Dict[str, Dict[str, int]] = {
            "queries": {"candidates": 0, "success": 0, "failed": 0, "skipped": 0, "vectorsGenerated": 0},
            "tables": {"candidates": 0, "success": 0, "failed": 0, "skipped": 0, "vectorsGenerated": 0},
            "measures": {"candidates": 0, "success": 0, "failed": 0, "skipped": 0, "vectorsGenerated": 0},
        }

        try:
            # best-effort ensure logs container exists
            await self._ensure_container(self.cfg.jobs_log_container)

            container = self._blob_service.get_container_client(self.cfg.container_name)
            blob_names = []
            seen = {"queries": 0, "tables": 0, "measures": 0}
            async for b in container.list_blobs():
                # Only process JSON files inside the three known folders
                n = (b.name or "")
                if n.endswith(".json") and (n.startswith("queries/") or n.startswith("tables/") or n.startswith("measures/")):
                    blob_names.append(n)
                    if n.startswith("queries/"):
                        seen["queries"] += 1
                    elif n.startswith("tables/"):
                        seen["tables"] += 1
                    elif n.startswith("measures/"):
                        seen["measures"] += 1
            logging.info(f"[nl2sql-indexer] Found {len(blob_names)} files (queries={seen['queries']}, tables={seen['tables']}, measures={seen['measures']})")

            sem = asyncio.Semaphore(self.cfg.max_concurrency)

            async def _wrap(name: str):
                async with sem:
                    return await self._process_one(name, run_id)

            results = await asyncio.gather(*(_wrap(n) for n in blob_names), return_exceptions=True)
            for r in results:
                if isinstance(r, dict) and r.get("status") == "success":
                    total_candidates += 1
                    total_ok += 1
                    k = r.get("kind", "unknown")
                    if k in per_kind_counts:
                        per_kind_counts[k]["candidates"] += 1
                        per_kind_counts[k]["success"] += 1
                        vd = r.get("vectorDims")
                        if isinstance(vd, int) and vd > 0:
                            per_kind_counts[k]["vectorsGenerated"] += 1
                elif isinstance(r, dict) and r.get("status") == "skipped":
                    total_skipped += 1
                    k = r.get("kind", "unknown")
                    if k in per_kind_counts:
                        per_kind_counts[k]["skipped"] += 1
                else:
                    total_candidates += 1
                    total_err += 1
                    k = r.get("kind", "unknown") if isinstance(r, dict) else "unknown"
                    if k in per_kind_counts:
                        per_kind_counts[k]["candidates"] += 1
                        per_kind_counts[k]["failed"] += 1

            summary = {
                "indexerType": "nl2sql-indexer",
                "runId": run_id,
                "runStartedAt": start_iso,
                "runFinishedAt": datetime.now(timezone.utc).isoformat(),
                "sourceContainer": self.cfg.container_name,
                "candidates": total_candidates,
                "success": total_ok,
                "failed": total_err,
                "byKind": per_kind_counts,
                "skipped": total_skipped,
            }
            duration_seconds = max((datetime.now(timezone.utc) - run_started_at).total_seconds(), 0.0)
            self._log_event(
                logging.INFO,
                "RUN-COMPLETE",
                runId=run_id,
                status="finished",
                collectionsSeen=3,
                itemsDiscovered=total_candidates + total_skipped,
                itemsIndexed=total_ok,
                itemsFailed=total_err,
                skippedNoChange=total_skipped,
                durationSeconds=duration_seconds,
            )
            await self._write_run_summary(self.cfg.jobs_log_container, summary, run_id)
            logging.info(f"[nl2sql-indexer] Summary: {json.dumps(summary)}")
        finally:
            try:
                await self._ai_search.close()
            except Exception:
                pass
            await self._close_clients_safely()

    async def _process_one(self, blob_name: str, run_id: str):
        await self._ensure_clients()
        container = self._blob_service.get_container_client(self.cfg.container_name)
        blob = container.get_blob_client(blob_name)
        log_key = blob_name.replace("/", "-")
        started = datetime.now(timezone.utc).isoformat()
        per_file_log = {
            "indexerType": "nl2sql-indexer",
            "blob": blob_name,
            "id": blob_name,
            "runId": run_id,
            "startedAt": started,
        }

        try:
            # Fetch current blob properties
            props = await blob.get_blob_properties()
            last_mod = (props.last_modified.astimezone(timezone.utc).isoformat() if props and props.last_modified else None)
            etag = getattr(props, "etag", None)

            # Determine kind/index target and sanitized id up front
            kind = "queries" if blob_name.startswith("queries/") else (
                "tables" if blob_name.startswith("tables/") else (
                    "measures" if blob_name.startswith("measures/") else None
                )
            )
            if not kind:
                raise ValueError("Unknown NL2SQL content kind for blob: " + blob_name)

            index_name = (
                self.cfg.queries_index_name if kind == "queries" else (
                    self.cfg.tables_index_name if kind == "tables" else self.cfg.measures_index_name
                )
            )
            doc_id = self._sanitize_id(blob_name)

            # Check last per-file log to skip unchanged blobs (but only if it still exists in index)
            prev_log = await self._read_previous_log(self.cfg.jobs_log_container, f"{log_key}.json")
            if prev_log and prev_log.get("lastModified") == last_mod and prev_log.get("etag") == etag:
                exists = False
                if index_name:
                    try:
                        exists = await self._exists_in_index(index_name, doc_id)
                    except Exception:
                        # If existence check fails, fall back to indexing to be safe
                        exists = False
                if exists:
                    per_file_log.update({
                        "status": "skipped",
                        "reason": "unchanged",
                        "kind": kind,
                        "index": index_name,
                        "lastModified": last_mod,
                        "etag": etag,
                        "finishedAt": datetime.now(timezone.utc).isoformat(),
                    })
                    await self._write_file_log(self.cfg.jobs_log_container, f"{log_key}.json", per_file_log)
                    return {"status": "skipped", "kind": kind}
                else:
                    logging.info(f"[nl2sql-indexer] Re-indexing '{blob_name}' because it's missing in index '{index_name}' despite unchanged blob.")

            # Download and parse payload only if we decided not to skip
            download = await blob.download_blob()
            data_bytes = await download.readall()
            text = data_bytes.decode("utf-8", errors="replace")
            payload = json.loads(text)

            # Build document and embedding
            aoai = AzureOpenAIClient(document_filename=os.path.basename(blob_name))
            doc = {"id": doc_id}
            if kind == "queries":
                # Expected fields: datasource, question, query, reasoning?
                question = payload.get("question") or ""
                doc.update({
                    "datasource": payload.get("datasource", ""),
                    "question": question,
                    "query": payload.get("query", ""),
                    "reasoning": payload.get("reasoning", ""),
                })
                if not question.strip():
                    raise ValueError("queries JSON missing 'question' text for embeddings")
                emb = aoai.get_embeddings(question)
                doc["contentVector"] = emb
                logging.debug(f"[nl2sql-indexer] Generated query embedding len={len(emb)} for {blob_name}")

            elif kind == "tables":
                # Expected: table, description, datasource, columns[]
                description = payload.get("description") or ""
                doc.update({
                    "table": payload.get("table", ""),
                    "description": description,
                    "datasource": payload.get("datasource", ""),
                    "columns": payload.get("columns", []),
                })
                if not description.strip():
                    raise ValueError("tables JSON missing 'description' text for embeddings")
                emb = aoai.get_embeddings(description)
                doc["contentVector"] = emb
                logging.debug(f"[nl2sql-indexer] Generated table embedding len={len(emb)} for {blob_name}")

            elif kind == "measures":
                # Expected: datasource, name, description, type, source_table, data_type, source_model
                description = payload.get("description") or ""
                doc.update({
                    "datasource": payload.get("datasource", ""),
                    "name": payload.get("name", ""),
                    "description": description,
                    "type": payload.get("type", ""),
                    "source_table": payload.get("source_table", ""),
                    "data_type": payload.get("data_type", ""),
                    "source_model": payload.get("source_model", ""),
                })
                if not description.strip():
                    raise ValueError("measures JSON missing 'description' text for embeddings")
                emb = aoai.get_embeddings(description)
                doc["contentVector"] = emb
                logging.debug(f"[nl2sql-indexer] Generated measure embedding len={len(emb)} for {blob_name}")

            if not index_name:
                raise ValueError(f"No index configured for kind '{kind}'")

            # Index document
            ok = await self._ai_search.index_document(index_name=index_name, document=doc)

            vector_dims = (len(doc["contentVector"]) if isinstance(doc.get("contentVector"), list) else None)
            per_file_log.update({
                "kind": kind,
                "index": index_name,
                "docId": doc_id,
                "vectorDims": vector_dims,
                "lastModified": last_mod,
                "etag": etag,
                "finishedAt": datetime.now(timezone.utc).isoformat(),
            })
            if ok:
                per_file_log["status"] = "success"
                # No separate state file; per-file log carries lastModified/etag for future runs
            else:
                per_file_log["status"] = "error"
            await self._write_file_log(self.cfg.jobs_log_container, f"{log_key}.json", per_file_log)
            return {"status": "success" if ok else "error", "kind": kind, "vectorDims": vector_dims}

        except Exception as e:
            logging.exception(f"[nl2sql-indexer] Failed processing {blob_name}")
            per_file_log.update({
                "status": "error",
                "error": str(e),
                "finishedAt": datetime.now(timezone.utc).isoformat(),
                "kind": kind if 'kind' in locals() else None,
            })
            await self._write_file_log(self.cfg.jobs_log_container, f"{log_key}.json", per_file_log)
            return {"status": "error", "error": str(e), "kind": (kind if 'kind' in locals() else "unknown")}

    @staticmethod
    def _sanitize_id(doc_id: str) -> str:
        """Sanitize IDs to be Azure AI Search-safe: allow letters, digits, _ - = only.
        Replace other characters by '-'.
        """
        allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-=")
        return "".join(ch if ch in allowed else '-' for ch in doc_id)

    async def _ensure_container(self, name: str):
        try:
            cc = self._blob_service.get_container_client(name)
            await cc.create_container()
        except Exception:
            pass

    async def _write_file_log(self, container: str, blob_name: str, payload: dict):
        cc = self._blob_service.get_container_client(container)
        try:
            await cc.upload_blob(
                name=f"nl2sql-indexer/files/{blob_name}",
                data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
                overwrite=True,
                content_settings=ContentSettings(content_type="application/json"),
            )
        except Exception:
            logging.exception(f"[nl2sql-indexer] failed to write file log {blob_name}")

    async def _write_run_summary(self, container: str, summary: dict, run_id: str):
        cc = self._blob_service.get_container_client(container)
        name = f"nl2sql-indexer/runs/{run_id}.json"
        try:
            await cc.upload_blob(
                name=name,
                data=json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8"),
                overwrite=True,
                content_settings=ContentSettings(content_type="application/json"),
            )
        except Exception:
            logging.exception(f"[nl2sql-indexer] failed to write run summary {name}")

    async def _exists_in_index(self, index_name: str, doc_id: str) -> bool:
        """Check if a document with given id exists in the target index."""
        try:
            res = await self._ai_search.search_documents(
                index_name=index_name,
                search_text="*",
                filter_str=f"id eq '{doc_id.replace("'", "''")}'",
                select_fields=["id"],
                top=1,
            )
            return (res or {}).get("count", 0) > 0
        except Exception as e:
            logging.error(f"[nl2sql-indexer] exists check failed for index '{index_name}', id '{doc_id}': {e}")
            return False

    async def _read_previous_log(self, container: str, file_log_name: str) -> Optional[Dict]:
        """Read the previous per-file log JSON if it exists, to fetch lastModified/etag."""
        cc = self._blob_service.get_container_client(container)
        name = f"nl2sql-indexer/files/{file_log_name}"
        try:
            bc = cc.get_blob_client(name)
            stream = await bc.download_blob()
            data = await stream.readall()
            return json.loads(data.decode("utf-8", errors="replace"))
        except ResourceNotFoundError:
            return None
        except Exception:
            logging.exception(f"[nl2sql-indexer] failed to read previous log {name}")
            return None

    async def _close_clients_safely(self):
        try:
            if self._blob_service:
                await self._blob_service.close()
        except Exception:
            pass
        try:
            if self._credential and hasattr(self._credential, "close"):
                res = self._credential.close()
                if asyncio.iscoroutine(res):
                    await res
        except Exception:
            pass
