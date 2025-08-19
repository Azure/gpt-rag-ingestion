import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Set, List

from azure.identity.aio import AzureCliCredential, ManagedIdentityCredential, ChainedTokenCredential
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.blob import ContentSettings

from dependencies import get_config
from tools import AISearchClient


@dataclass
class NL2SQLPurgerConfig:
    storage_account_name: str
    container_name: str = "nl2sql"
    jobs_log_container: str = "jobs"

    queries_index_name: str = ""
    tables_index_name: str = ""
    measures_index_name: str = ""

    @staticmethod
    def from_app_config():
        app = get_config()
        return NL2SQLPurgerConfig(
            storage_account_name=app.get("STORAGE_ACCOUNT_NAME", ""),
            container_name=app.get("NL2SQL_STORAGE_CONTAINER", "nl2sql"),
            jobs_log_container=app.get("JOBS_LOG_CONTAINER", "jobs"),
            queries_index_name=app.get("SEARCH_QUERIES_INDEX_NAME", ""),
            tables_index_name=app.get("SEARCH_TABLES_INDEX_NAME", ""),
            measures_index_name=app.get("SEARCH_MEASURES_INDEX_NAME", ""),
        )


class NL2SQLPurger:
    """
    Purges documents from NL2SQL indices (queries/tables/measures) that no longer
    exist as blobs in the nl2sql container. Assumes 'id' of the search document
    equals the blob path (e.g., 'queries/foo.json').
    """

    def __init__(self, cfg: Optional[NL2SQLPurgerConfig] = None):
        self.cfg = cfg or NL2SQLPurgerConfig.from_app_config()
        self._credential: Optional[ChainedTokenCredential] = None
        self._blob_service: Optional[BlobServiceClient] = None
        self._ai_search = AISearchClient()

        if not self.cfg.storage_account_name:
            raise ValueError("STORAGE_ACCOUNT_NAME must be set in configuration")

    async def _ensure_clients(self):
        if not self._credential:
            client_id = os.environ.get("AZURE_CLIENT_ID", None)
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
        start_iso = datetime.now(timezone.utc).isoformat()
        logging.info("[nl2sql-purger] Starting")

        try:
            await self._ensure_container(self.cfg.jobs_log_container)

            # Gather all existing blob names under nl2sql container
            existing: Set[str] = set()
            cont = self._blob_service.get_container_client(self.cfg.container_name)
            async for b in cont.list_blobs():
                n = (b.name or "")
                if n.endswith(".json") and (n.startswith("queries/") or n.startswith("tables/") or n.startswith("measures/")):
                    existing.add(n)
            # Build sanitized set to match the indexed 'id' format used by the indexer
            sanitized_existing: Set[str] = set(self._sanitize_id(n) for n in existing)

            # Purge for each index independently
            totals = []
            for kind, index_name in (
                ("queries", self.cfg.queries_index_name),
                ("tables", self.cfg.tables_index_name),
                ("measures", self.cfg.measures_index_name),
            ):
                if not index_name:
                    continue
                before = await self._count_index_docs(index_name)
                deleted = await self._purge_one_index(index_name, sanitized_existing)
                after = await self._count_index_docs(index_name)
                logging.info(f"[nl2sql-purger] {kind}: before={before}, deleted={deleted}, after={after}")
                totals.append((kind, deleted, before, after))

            summary = {
                "indexerType": "nl2sql-purger",
                "runStartedAt": start_iso,
                "runFinishedAt": datetime.now(timezone.utc).isoformat(),
                "results": [{"kind": k, "deleted": d, "before": b, "after": a} for (k, d, b, a) in totals],
            }
            await self._write_run_summary(self.cfg.jobs_log_container, summary)
            logging.info(f"[nl2sql-purger] Summary: {json.dumps(summary)}")
        finally:
            await self._close_clients_safely()

    async def _purge_one_index(self, index_name: str, sanitized_existing: Set[str]) -> int:
        """
        For an index, retrieve all doc IDs (paged) and delete those not present (by sanitized id).
        """
        deleted = 0
        try:
            client = await self._ai_search.get_search_client(index_name)
            # iterate pages selecting only 'id'
            results = await client.search(search_text="*", select=["id"], include_total_count=True, top=1000)
            async for page in results.by_page():
                page_ids: List[str] = []
                async for doc in page:
                    doc_id = doc.get("id")
                    if not doc_id:
                        continue
                    # Delete if document is not present in sanitized existing set
                    if doc_id not in sanitized_existing:
                        page_ids.append(doc_id)
                if page_ids:
                    await self._ai_search.delete_documents(index_name=index_name, key_field="id", key_values=page_ids)
                    deleted += len(page_ids)
        except Exception:
            logging.exception(f"[nl2sql-purger] Error purging index {index_name}")
        return deleted

    async def _count_index_docs(self, index_name: str) -> int:
        """Count documents in an index (all docs)."""
        try:
            client = await self._ai_search.get_search_client(index_name)
            results = await client.search(search_text="*", select=["id"], include_total_count=True, top=1000)
            count = 0
            async for page in results.by_page():
                async for doc in page:
                    if doc.get("id"):
                        count += 1
            return count
        except Exception:
            logging.exception(f"[nl2sql-purger] Error counting docs in index {index_name}")
            return 0

    @staticmethod
    def _sanitize_id(doc_id: str) -> str:
        """Sanitize IDs like the indexer: allow letters, digits, _ - =; others to '-'."""
        allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-=")
        return "".join(ch if ch in allowed else '-' for ch in doc_id)

    async def _ensure_container(self, name: str):
        try:
            cc = self._blob_service.get_container_client(name)
            await cc.create_container()
        except Exception:
            pass

    async def _write_run_summary(self, container: str, summary: dict):
        cc = self._blob_service.get_container_client(container)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        name = f"nl2sql-purger/runs/{stamp}.json"
        try:
            await cc.upload_blob(
                name=name,
                data=json.dumps(summary, ensure_ascii=False, indent=2).encode("utf-8"),
                overwrite=True,
                content_settings=ContentSettings(content_type="application/json"),
            )
        except Exception:
            logging.exception(f"[nl2sql-purger] failed to write run summary {name}")

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
