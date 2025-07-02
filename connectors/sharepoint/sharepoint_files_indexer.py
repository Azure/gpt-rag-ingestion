"""
SharePointDocumentIngestor

Orchestrates ingestion of SharePoint files into Azure AI Search:
- Streams metadata via Graph API (without loading all at once).
- For each file: download, check if changed, chunk, and index.
- Tracks total, succeeded, and failed counts, and logs progress every N files.
- Uses in-memory tracking of failures for review.
"""

import logging
import os
import asyncio
from connectors import SharePointMetadataStreamer
from tools import KeyVaultClient, AISearchClient
from typing import Any, Dict, List, Optional
from chunking import DocumentChunker, ChunkerFactory

TEAL   = "\033[38;5;6m"
ORANGE = "\033[38;5;208m"
RESET  = "\033[0m"

class SharePointDocumentIngestor:
    """
    Fetches metadata from SharePoint, downloads file content in-process, chunks documents,
    and indexes them into an Azure Search index, streaming metadata to avoid large memory use.
    Tracks total, success, and failure counts and collects failed file identifiers in memory.
    """
    def __init__(self):
        # Connector settings
        self.connector_enabled = os.getenv("SHAREPOINT_CONNECTOR_ENABLED", "false").lower() == "true"
        self.tenant_id = os.getenv("SHAREPOINT_TENANT_ID")
        self.client_id = os.getenv("SHAREPOINT_CLIENT_ID")
        self.site_domain = os.getenv("SHAREPOINT_SITE_DOMAIN")
        self.site_name = os.getenv("SHAREPOINT_SITE_NAME")
        self.drive_id = os.getenv("SHAREPOINT_DRIVE_ID")

        paths_to_traverse = os.getenv("SHAREPOINT_SUBFOLDERS_NAMES")
        if paths_to_traverse:
            self.paths_to_traverse = [name.strip() for name in paths_to_traverse.split(",")]
        else:
            self.paths_to_traverse = []

        folder_regex = os.getenv("SHAREPOINT_SUBFOLDERS_REGEX")
        if folder_regex:
            self.folder_regex = folder_regex.strip()
        else:
            self.folder_regex = ".*"

        self.sharepoint_client_secret_name = os.getenv("SHAREPOINT_CLIENT_SECRET_NAME", "sharepointClientSecret")
        self.index_name = os.getenv("AZURE_SEARCH_SHAREPOINT_INDEX_NAME", "ragindex")

        env_formats = os.getenv("SHAREPOINT_FILES_FORMAT")
        if env_formats:
            self.file_formats = [fmt.strip() for fmt in env_formats.split(",")]
        else:
            supported = ChunkerFactory.get_supported_extensions()
            self.file_formats = supported.split(",")

        self.keyvault_client: Optional[KeyVaultClient] = None
        self.client_secret: Optional[str] = None
        self.sharepoint_data_reader: Optional[SharePointMetadataStreamer] = None
        self.search_client: Optional[AISearchClient] = None
        self.site_id_reader: Optional[str] = None

        # Tracking attributes
        self.total_files: int = 0
        self.success_count: int = 0
        self.failure_count: int = 0
        self.failed_files: List[str] = []
        # Lock for concurrent updates
        self._lock = asyncio.Lock()

        # How often to log progress
        self.progress_interval = 20

    async def initialize_clients(self) -> bool:
        try:
            self.keyvault_client = KeyVaultClient()
            self.client_secret = await self.keyvault_client.get_secret(self.sharepoint_client_secret_name)
            logging.debug("[sharepoint_files_indexer] Retrieved SharePoint client secret from Key Vault.")
        except Exception as e:
            logging.error(f"[sharepoint_files_indexer] Failed to retrieve secret: {e}")
            return False
        finally:
            if self.keyvault_client:
                await self.keyvault_client.close()

        missing = [name for name, val in {
            "SHAREPOINT_TENANT_ID": self.tenant_id,
            "SHAREPOINT_CLIENT_ID": self.client_id,
            "SHAREPOINT_SITE_DOMAIN": self.site_domain,
            "SHAREPOINT_SITE_NAME": self.site_name,
            "SHAREPOINT_DRIVE_ID": self.drive_id,
        }.items() if not val]

        if missing:
            logging.error(f"[indexer] Missing environment variables: {', '.join(missing)}")
            return False

        # Initialize metadata streamer
        self.sharepoint_data_reader = SharePointMetadataStreamer(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        try:
            # Authenticate and get site ID for reader
            self.sharepoint_data_reader._msgraph_auth()
            self.site_id_reader = self.sharepoint_data_reader._get_site_id(
                self.site_domain, self.site_name)
            logging.debug("[indexer] Authenticated with Microsoft Graph and obtained site ID.")
        except Exception as e:
            logging.error(f"[indexer] Graph authentication or site lookup failed: {e}")
            return False

        # Initialize Azure Search client
        try:
            self.search_client = AISearchClient()
            logging.debug("[indexer] Initialized Azure Search client.")
        except Exception as e:
            logging.error(f"[indexer] Failed to initialize Azure Search client: {e}")
            return False

        return True

    async def delete_existing_chunks(self, existing_chunks: Dict[str, Any], file_name: str) -> None:
        ids = [doc['id'] for doc in existing_chunks.get('documents', []) if 'id' in doc]
        if not ids:
            logging.warning(f"[indexer] No existing chunks to delete for '{file_name}'.")
            return
        try:
            await self.search_client.delete_documents(
                index_name=self.index_name,
                key_field="id",
                key_values=ids,
            )
            logging.debug(f"[indexer] Deleted {len(ids)} chunks for '{file_name}'.")
        except Exception as e:
            logging.error(f"[indexer] Error deleting chunks for '{file_name}': {e}")

    async def process_file(self, file: Dict[str, Any], semaphore: asyncio.Semaphore) -> None:
        """
        Process a single file: download, check unchanged, chunk, and index.
        Tracks success/failure, logs progress.
        """
        async with semaphore:
            file_name = file.get("name")
            sp_id = file.get("id")
            # Track total files
            async with self._lock:
                self.total_files += 1
                current_count = self.total_files

            try:
                if not file_name:
                    raise ValueError("File without a name")

                # Download bytes (blocking call via run_in_executor)
                document_bytes = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.sharepoint_data_reader._get_file_content_bytes,
                    self.site_id_reader,
                    self.drive_id,
                    file
                )
                document_url = file.get("webUrl")
                last_mod = file.get('fileSystemInfo', {}).get('lastModifiedDateTime')
                logging.info(
                    TEAL + f'[indexer] Processing file {file["parentReference"]["path"]}/{file_name}, last modified: {last_mod}' + RESET
                )

                # Check existing chunks
                try:
                    existing = await self.search_client.search_documents(
                        index_name=self.index_name,
                        search_text="*",
                        filter_str=f"parent_id eq '{sp_id}' and source eq 'sharepoint'",
                        select_fields=["id", "metadata_storage_last_modified"],
                        top=0,
                    )
                except Exception as e:
                    raise RuntimeError(f"Failed to search existing chunks: {e}")

                if existing.get('count', 0) > 0:
                    idx_mod = existing['documents'][0].get('metadata_storage_last_modified')
                    if last_mod and idx_mod and last_mod <= idx_mod:
                        logging.info(f"[indexer] '{file_name}' unchanged; skipping indexing.")
                        # Treat skip as success
                        async with self._lock:
                            self.success_count += 1
                        # Log progress if needed
                        if current_count % self.progress_interval == 0:
                            logging.info(f"[indexer] Processed {current_count} files so far: {self.success_count} succeeded, {self.failure_count} failed.")
                        return
                    await self.delete_existing_chunks(existing, file_name)

                # Chunk documents
                chunks, errors, warnings = DocumentChunker().chunk_documents({
                    "sharepointId": sp_id,
                    "fileName": file_name,
                    "documentBytes": document_bytes,
                    "documentUrl": document_url,
                })
                for w in warnings:
                    logging.warning(f"[indexer] Chunk warning for '{file_name}': {w.get('message')}")
                if errors:
                    msgs = [err.get('message') for err in errors]
                    raise RuntimeError(f"Chunk errors: {msgs}")

                # Index each chunk
                for chunk in chunks:
                    chunk_id = f"{sp_id}_{chunk.get('chunk_id', '')}"
                    chunk.update({
                        "id": chunk_id,
                        "parent_id": sp_id,
                        "metadata_storage_path": document_url,
                        "metadata_storage_name": file_name,
                        "metadata_storage_last_modified": last_mod,
                        "source": "sharepoint",
                    })
                    try:
                        await self.search_client.index_document(self.index_name, chunk)
                    except Exception as e:
                        logging.error(f"[indexer] Failed to index chunk {chunk_id}: {e}")
                        # continue with next chunk
                logging.info(f"[indexer] Indexed {len(chunks)} chunks for '{file_name}'.")

                async with self._lock:
                    self.success_count += 1

            except Exception as e:
                name_or_id = file_name or sp_id or "<unknown>"
                logging.warning(f"[indexer] {name_or_id}: {e}")
                async with self._lock:
                    self.failure_count += 1
                    self.failed_files.append(f"{name_or_id}: {e}")

            finally:
                # Log progress every progress_interval files
                if current_count % self.progress_interval == 0:
                    # acquire lock to read counts
                    async with self._lock:
                        s, f = self.success_count, self.failure_count
                    logging.info(f"[indexer] Processed {current_count} files so far: {s} succeeded, {f} failed.")

    async def run(self) -> None:
        logging.info("[indexer] Starting SharePoint files index run.")

        if not self.connector_enabled:
            logging.info("[indexer] Connector disabled. Enable SHAREPOINT_CONNECTOR_ENABLED to proceed.")
            return

        if not await self.initialize_clients():
            return

        metadata_iterator = self.sharepoint_data_reader.stream_file_metadata(
            site_domain=self.site_domain,
            site_name=self.site_name,
            drive_id=self.drive_id,
            folders_names=self.paths_to_traverse,
            folder_regex=self.folder_regex,
            file_formats=self.file_formats,
        )

        # Producer-consumer pattern
        queue: asyncio.Queue = asyncio.Queue(maxsize=20)
        concurrency = 10
        semaphore = asyncio.Semaphore(concurrency)

        async def producer():
            count_meta = 0
            for meta in metadata_iterator:
                await queue.put(meta)
                count_meta += 1
                if count_meta % self.progress_interval == 0:
                    logging.info(f"[indexer] Queued {count_meta} metadata items so far.")
            # send stop signals
            for _ in range(concurrency):
                await queue.put(None)

        async def worker():
            while True:
                file_meta = await queue.get()
                if file_meta is None:
                    break
                await self.process_file(file_meta, semaphore)

        # launch producer and workers
        await asyncio.gather(
            producer(),
            *[worker() for _ in range(concurrency)]
        )

        # Summary logging
        logging.info(ORANGE + f"[indexer] Ingestion summary: total={self.total_files}, succeeded={self.success_count}, failed={self.failure_count}" + RESET)
        if self.failed_files:
            logging.warning(ORANGE + "[indexer] Failed files and errors:" + RESET)
            for entry in self.failed_files:
                logging.warning(f"  - {entry}")
            # Persistence example commented out
            # try:
            #     with open("failed_sharepoint_files.log", "a", encoding="utf-8") as f:
            #         for entry in self.failed_files:
            #             f.write(entry.replace('\n', ' ') + "\n")cle
            #     logging.debug("[indexer] Persisted failed files to failed_sharepoint_files.log")
            # except Exception as persist_e:
            #     logging.error(f"[indexer] Could not persist failed files: {persist_e}")

        # Close search client
        if self.search_client:
            await self.search_client.close()
            logging.debug("[indexer] Closed AISearchClient.")

        logging.info("[indexer] SharePoint indexer run complete.")
