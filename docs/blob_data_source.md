# Blob Data Source

The **Blob Data Source** ingests documents from Azure Blob Storage into Azure AI Search and keeps the index synchronized when files are updated or removed.

## How it Works

* **Indexing**

  * Scans the configured blob container (optionally filtered by `BLOB_PREFIX`)
  * Skips unchanged files
  * For each changed file:

    * Replaces existing chunks (by `parent_id`)
    * Uploads new chunks with stable, search-safe IDs
  * Each chunk document sets `source = "blob"` and, when available in blob metadata, includes `metadata_security_id`

* **Purging**

  * Compares storage parents with index parents (`source == "blob"`)
  * Deletes chunk documents for parents no longer present in storage
  * Includes a brief, bounded consistency wait so post-purge counts reflect the final state

## Scheduling

Jobs are enabled through CRON expressions:

* `CRON_RUN_BLOB_INDEX`: runs the indexing job
* `CRON_RUN_BLOB_PURGE`: runs the purge job
* Leave unset to disable

The scheduler uses `SCHEDULER_TIMEZONE` (IANA format, e.g., `Europe/Berlin`), falling back to the host machine’s timezone if not specified.
On startup, if a CRON is configured, the corresponding job is scheduled and also triggered once immediately.

**Examples:**

* `0 * * * *` → hourly
* `*/15 * * * *` → every 15 minutes
* `0 0 * * *` → daily at midnight

## Settings

* `STORAGE_ACCOUNT_NAME` and `DOCUMENTS_STORAGE_CONTAINER`: source location
* `SEARCH_SERVICE_QUERY_ENDPOINT` and `AI_SEARCH_INDEX_NAME` (or `SEARCH_RAG_INDEX_NAME`): target index
* `BLOB_PREFIX` *(optional)*: restricts the scan scope
* `JOBS_LOG_CONTAINER` *(default: jobs)*: container for logs
* `INDEXER_MAX_CONCURRENCY` and `INDEXER_BATCH_SIZE` *(optional)*: performance tuning; defaults: `8` (concurrency) and `500` (batch size)

> [!NOTE]  
> `INDEXER_MAX_CONCURRENCY` controls how many files are processed in parallel (download → chunk → upload). `INDEXER_BATCH_SIZE` controls how many chunk documents are sent in each upload call to Azure AI Search. Increase these to raise throughput, but watch for throttling (HTTP 429), timeouts, and memory usage; lower them if you see retries or instability. The default batch size (500) follows common guidance to keep batches reasonable (typically ≤ 1000).

## Logs

Both jobs write logs to the configured jobs container. Logs are grouped by job type:

* **Indexer (`blob-storage-indexer`)**

  * Per-file logs and per-run summaries under `files/` and `runs/`
  * Summaries include: `sourceFiles`, `candidates`, `success/failed`, `totalChunksUploaded`

* **Purger (`blob-storage-purger`)**

  * Per-run summaries under `runs/`
  * Summaries include: `blobDocumentsCount`, `indexParentsCountBefore/After`, `indexChunkDocumentsBefore`, `indexParentsPurged`, `indexChunkDocumentsDeleted`
