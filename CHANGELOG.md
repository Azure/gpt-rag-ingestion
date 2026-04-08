# Changelog

All notable changes to this project will be documented in this file.  
This format follows [Keep a Changelog](https://keepachangelog.com/) and adheres to [Semantic Versioning](https://semver.org/).

## [v2.3.1] – 2026-04-08

### Added
- **Processing timings breakdown in dashboard**: Each file processing run now records per-phase timing data (download, analysis, chunking + embeddings, index upload) and stores it in the file log. The admin dashboard detail dialog displays a stacked color bar and a legend with durations for each phase, plus a total. Rate-limit retry wait time (429 backoff) is tracked separately and shown as a sub-item under chunking + embeddings. Run history entries also show a Duration column. This makes it easy to identify bottlenecks when processing large documents.
- **Automatic PDF splitting for large documents**: PDFs exceeding the Azure analysis service page limit (configurable via `MAX_PAGES_PER_ANALYSIS`, default 300) are now automatically split into smaller parts before analysis. Each part is analyzed separately and the markdown results are concatenated with correct absolute page numbering. This prevents `InputPageCountExceeded` errors and is transparent to the rest of the pipeline — same `parent_id`, same chunk keys, same search index behavior. Requires the new `pypdf` dependency.
- **Memory guard before blob download**: Before downloading a blob for processing, the indexer now checks the file size against available container memory (via cgroups + `psutil`). If the estimated peak memory usage would exceed available capacity, processing is skipped with a descriptive error instead of risking an OOM crash that restarts the container. Configurable via `MEMORY_SAFETY_MULTIPLIER` (default 4.0) and `MEMORY_SAFETY_THRESHOLD` (default 0.85).
- **Temp file download for large PDFs**: PDFs larger than 10 MB are now downloaded to a temporary file on disk instead of being held entirely in memory. The auto-split logic operates on these temp files, keeping peak memory usage bounded to one part at a time (~200 MB) instead of the full document (~1.5 GB+).

### Fixed
- **`_as_datetime` NameError crashing every indexer run**: The helper function `_as_datetime` was called in four places within `blob_storage_indexer.py` but was never defined, causing a `NameError` on every run after the retry-tracking feature was added. Added the missing function definition at module level.
- **Orphaned `value` variable causing NameError in memory guard**: A leftover code block from an earlier refactor inside `_check_memory_capacity()` referenced an undefined variable `value`, crashing the memory guard check before any file could be processed. Removed the dead code.
- **Dashboard unresponsive during file processing**: The FastAPI event loop was blocked by synchronous chunking and document iteration calls, making the admin dashboard and health endpoints unresponsive for the entire duration of large file processing (20+ minutes). Wrapped the blocking `list(docs_iter)` calls with `asyncio.to_thread()` so they run in a worker thread without blocking the event loop.
- **Stale error field on successful re-processing**: When a file was re-processed successfully after previous failures, the top-level `error` field in the file log retained the last error message despite `status` being `success`. The field is now explicitly cleared to `null` on success.

---

### Upgrading from Earlier Versions

If you are running an older version of the data ingestion component (e.g., v2.0.6, v2.1.0, v2.2.x) and want to upgrade to v2.3.1, follow the instructions below **before running `azd deploy`**. The required steps depend on your current version. Review each section that applies to your upgrade path.

---

#### Upgrading from v2.0.x or v2.1.x (versions prior to v2.2.0)

These versions predate the document-level security enforcement feature introduced in v2.2.0. The following steps are **required**:

##### 1. Add RBAC Security Fields to Azure AI Search Index

Starting with v2.2.0, the ingestion pipeline writes security metadata to the search index. If your index was created before this version, you must manually add the following fields using the Azure Portal JSON editor or the Azure AI Search REST API:

```json
{
  "name": "metadata_security_user_ids",
  "type": "Collection(Edm.String)",
  "filterable": true,
  "searchable": false,
  "sortable": false,
  "facetable": false
},
{
  "name": "metadata_security_group_ids",
  "type": "Collection(Edm.String)",
  "filterable": true,
  "searchable": false,
  "sortable": false,
  "facetable": false
},
{
  "name": "metadata_security_rbac_scope",
  "type": "Edm.String",
  "filterable": true,
  "searchable": false,
  "sortable": false,
  "facetable": false
}
```

**How to add fields via Azure Portal:**
1. Navigate to your Azure AI Search resource.
2. Go to **Indexes** and select your index (e.g., `ragindex`).
3. Click **Edit JSON** (top toolbar).
4. In the `fields` array, add the three field definitions above.
5. Click **Save**.

> **Note:** Azure AI Search allows adding fields to an existing index, but does not allow modifying or removing fields once they exist.

##### 2. Update Container Port Configuration

Starting with v2.2.1, the container uses port `8080` instead of the previously common port `80`. If your Azure Container App is configured for port 80, you must update it:

1. Navigate to your Azure Container App resource (e.g., `ca-xxxx-dataingest`).
2. Go to **Ingress** and change the **Target port** to `8080`.
3. Go to **Containers** → **Health probes** and update:
   - Liveness probe port: `8080`
   - Readiness probe port: `8080`
   - Startup probe port (if configured): `8080`
4. Save the configuration and wait for a new revision to deploy.

Alternatively, using Azure CLI:
```bash
az containerapp ingress update \
  --name <your-container-app-name> \
  --resource-group <your-resource-group> \
  --target-port 8080
```

---

#### Upgrading from v2.2.0

##### 1. Update Container Port Configuration

If you are on v2.2.0, you still need to update the container port from `80` to `8080` (introduced in v2.2.1). Follow the steps in the previous section.

##### 2. RBAC Role Assignment for Elevated Read

Starting with v2.2.5, the ingestion service uses elevated-read operations to query the index without permission filtering (required when `permissionFilterOption` is enabled). The managed identity running the Container App must have the **Search Index Data Contributor** role on the Azure AI Search resource.

```bash
az role assignment create \
  --assignee <managed-identity-object-id> \
  --role "Search Index Data Contributor" \
  --scope /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Search/searchServices/<search-service>
```

> The `Search Index Data Contributor` role includes the `elevatedOperations/read` RBAC data action required for the `x-ms-enable-elevated-read` header.

---

#### Upgrading from v2.2.1, v2.2.2, v2.2.3, or v2.2.4

##### 1. RBAC Role Assignment for Elevated Read

As noted above, v2.2.5 introduced elevated-read headers. Ensure the **Search Index Data Contributor** role is assigned to the Container App managed identity.

##### 2. (Optional) Configure Vision Deployment

If you use multimodal processing and your primary chat model does not support vision (e.g., `gpt-5-nano`), configure the `VISION_DEPLOYMENT_NAME` setting in Azure App Configuration to point to a vision-capable model (e.g., `gpt-4o-mini`). This was introduced in v2.2.4.

---

#### Upgrading from v2.2.5

##### 1. Content Understanding (No Action Required)

Starting with v2.3.0, the default document analysis path uses **Azure AI Foundry Content Understanding** (`prebuilt-layout`) instead of Document Intelligence, reducing costs by ~69% per page. The `AI_FOUNDRY_ACCOUNT_ENDPOINT` setting is automatically configured during deployment via the infrastructure templates.

> **Note:** If you prefer to continue using Document Intelligence instead of Content Understanding, set `USE_DOCUMENT_INTELLIGENCE=true` in Azure App Configuration.

---

#### Resource Recommendations for Processing Large Files

Large document processing (e.g., 100+ page PDFs, large spreadsheets) can be memory-intensive. Container sizing must account for the **number of concurrent document processes** controlled by `INDEXER_MAX_CONCURRENCY`.

##### Concurrency and Container Sizing

| Setting                  | Default | Description |
|--------------------------|---------|-------------|
| `INDEXER_MAX_CONCURRENCY`| 8       | Maximum documents processed in parallel per indexer run |

Each concurrent process consumes memory proportional to document size. When adjusting concurrency, scale container resources accordingly:

| Concurrency | Recommended CPU | Recommended Memory |
|-------------|-----------------|---------------------|
| 2–4         | 0.5             | 1 GB                |
| 8 (default) | 1.0             | 3 GB                |
| 16+         | 2.0             | 4 GB                |

> **Tip:** For workloads with very large documents (100+ pages), consider reducing `INDEXER_MAX_CONCURRENCY` to 2–4 to avoid memory exhaustion.

##### Container Resource Allocation

The GPT-RAG solution may deploy up to **4 container apps** in the same Container Apps Environment:

| Container App       | CPU    | Memory | Notes |
|---------------------|--------|--------|-------|
| Data Ingestion      | 1.0    | 3 GB   | Recommended for default concurrency (8) |
| Orchestrator        | 0.5    | 1 GB   | |
| Frontend            | 0.5    | 1 GB   | |
| MCP Server          | 0.5    | 1 GB   | Optional, if deployed |
| **Total (all 4)**   | **2.5**| **6 GB** | |

> ⚠️ **Workload Profile Limits:** The **Consumption workload profile** supports up to **4 vCPUs and 8 GB memory per container app**. The recommended allocations above fit within these limits. However, when adjusting resources, consider the **combined total across all container apps** in your environment. If you increase Data Ingestion resources significantly (e.g., for high concurrency), verify the total allocation still fits your workload profile capacity. For extreme resource requirements, consider using a **Dedicated workload profile**.

To update container resources via CLI:
```bash
az containerapp update \
  --name <your-container-app-name> \
  --resource-group <your-resource-group> \
  --cpu 1.0 \
  --memory 3Gi
```

To adjust concurrency in Azure App Configuration:
```
INDEXER_MAX_CONCURRENCY = 4
```

---

#### Post-Deployment Verification

After deployment, verify the running version:

```bash
az containerapp show \
  --name <your-container-app-name> \
  --resource-group <your-resource-group> \
  --query "properties.template.containers[0].image" \
  -o tsv
```

The image tag corresponds to the Git commit SHA. You can map it to a release by checking the repository tags:

```bash
git log --oneline --decorate v2.3.1
```

To validate the ingestion pipeline:
1. Upload a small test file to the documents container.
2. Monitor the ingestion logs via the admin dashboard (`/dashboard`) or Container App logs.
3. Verify the document appears in the search index.

---

#### Summary by Source Version

| Current Version | Port Change | Index Fields | RBAC Role | AI Foundry |
|-----------------|-------------|--------------|-----------|------------|
| v2.0.x          | Required    | Required     | Required  | Auto |
| v2.1.x          | Required    | Required     | Required  | Auto |
| v2.2.0          | Required    | ✓            | Required  | Auto |
| v2.2.1–v2.2.4   | ✓           | ✓            | Required  | Auto |
| v2.2.5          | ✓           | ✓            | ✓         | Auto |

> **AI Foundry** column: "Auto" indicates that Content Understanding is the new default (v2.3.0+) and the required settings are automatically configured during deployment.

---

## [v2.3.0] – 2026-04-07

### Added
- **Per-file retry tracking and automatic block list**: Tracks processing attempts per file via per-file JSON logs. Files exceeding `MAX_FILE_PROCESSING_ATTEMPTS` (default 3) are automatically blocked and skipped in future runs. Applies to both blob storage and SharePoint indexers. Administrators can unblock files via the admin dashboard.
- **Admin dashboard**: React-based frontend served from the same Container App at `/dashboard`, providing paginated and sortable tables for job runs and file logs with search, type filter, and an unblock action for blocked files.
- **Content Understanding integration**: New `ContentUnderstandingClient` using Azure AI Foundry `prebuilt-layout` as the default analysis path in `DocAnalysisChunker`, replacing Document Intelligence Layout with ~69% cost reduction per page.
- **Scheduled log cleanup**: Automatic cleanup of old run-summary blobs via APScheduler (`CRON_RUN_LOG_CLEANUP`, default hourly), configurable max via `MAX_LOG_RUN_FILES` (default 500).

## [v2.2.5] – 2026-03-31

### Fixed
- **Ingestion re-indexes every file when `permissionFilterOption` is enabled**: When the Azure AI Search index has `permissionFilterOption` set to `enabled`, all `search()` and `get_document()` calls returned empty or 404 results because there is no end-user token during service-side ingestion. This caused `_load_latest_index_state()` to return an empty state map, making the indexer treat every blob as new and triggering a full re-index on every run with significant cost implications. Fixed by adding the `x-ms-enable-elevated-read: true` header to all index query operations across blob storage indexer, SharePoint indexer, SharePoint purger, NL2SQL purger, and the AI Search client utility. Also pinned `api_version` to `2025-11-01-preview` on all `SearchClient` instances, which is required for the elevated-read header to be recognized by the service. Requires the `Search Index Data Contributor` role (which includes the `elevatedOperations/read` RBAC data action).

## [v2.2.4] – 2026-03-30

### Added
- **Vision deployment configuration (`VISION_DEPLOYMENT_NAME`)**: Added a new optional App Configuration setting `VISION_DEPLOYMENT_NAME` that specifies the Azure OpenAI deployment to use for multimodal (image + text) requests such as figure caption generation. When set, `get_completion()` automatically routes vision requests to this deployment, allowing the use of a vision-capable model (e.g., `gpt-4o-mini`) separately from the primary chat model. Falls back to `CHAT_DEPLOYMENT_NAME` if not configured.

### Fixed
- **Empty image captions when chat model lacks vision support**: When `CHAT_DEPLOYMENT_NAME` pointed to a model without vision capabilities (e.g., `gpt-5-nano`), `get_completion()` returned `None` silently for multimodal requests, producing empty `imageCaptions` in the search index. Added a guard in both `AzureOpenAIClient.get_completion()` (logs a warning with `finish_reason` and model name) and `MultimodalChunker._generate_caption_for_figure()` (falls back to `"No caption available."`) to prevent empty captions from propagating to the index.

## [v2.2.3] – 2026-03-24

### Changed
- **Default chunk overlap increased to 200 tokens**: Changed the default value of `TOKEN_OVERLAP` from `100` to `200` across all chunkers (doc_analysis, json, langchain, nl2sql, transcription), improving context continuity between chunks during document ingestion.
- **Cron fallback defaults for blob ingestion jobs**: Added cron fallback defaults when `CRON_RUN_BLOB_INDEX` and `CRON_RUN_BLOB_PURGE` are not configured: blob indexing now runs hourly (`0 * * * *`) and blob purge runs at 10 minutes past each hour (`10 * * * *`).

### Fixed
- **Multimodal image captions not generated**: The `get_completion()` method in `AzureOpenAIClient` did not accept the `image_base64` parameter passed by the multimodal chunker, causing a `TypeError` on every caption generation call. The exception was caught silently and all image captions defaulted to "No caption available." Added vision support to `get_completion()` by accepting an optional `image_base64` parameter and constructing multimodal messages (text + image) using the OpenAI vision API format when an image is provided.
- **Azure OpenAI API compatibility with newer models**: Replaced `max_tokens` with `max_completion_tokens` in the chat completions API call, fixing a 400 error (`unsupported_parameter`) when using newer models (e.g., GPT-4o) that reject the deprecated parameter.

## [v2.2.2] – 2026-02-04
### Fixed
- Fixed Docker builds on ARM-based machines by explicitly setting the target platform to `linux/amd64`, preventing Azure Container Apps deployment failures.
### Changed
- Pinned the Docker base image to `mcr.microsoft.com/devcontainers/python:3.12-bookworm` to ensure stable package verification behavior across environments.
- Bumped `aiohttp` to `3.13.3`.

## [v2.2.1] – 2026-01-19
### Fixed
- Improved reliability of large spreadsheet ingestion (which generate thousands of embedding calls prone to transient rate limits) by adding robust retry with exponential backoff for Azure OpenAI calls (handles 429/Retry-After and is configurable via `OPENAI_RETRY_*` and `OPENAI_SDK_MAX_RETRIES`).
- Standardized on the container best practice of using a non-privileged port (`8080`) instead of a privileged port (`80`), reducing the risk of runtime/permission friction and improving stability of long-running ingestion workloads.

## [v2.2.0] – 2026-01-15
### Added
- Document-level security enforcement for GPT-RAG using Azure AI Search native ACL/RBAC trimming with end-user identity propagation via `x-ms-query-source-authorization`.
	Includes permission-aware indexing metadata (`userIds`, `groupIds`, `rbacScope`), safe-by-default behavior for requests without a valid user token, and optional elevated-read debugging support.

## [v2.1.0] – 2025-12-15
### Added
- Support for SharePoint Lists
### Changed
- Improved robustness of Blob Storage indexing
- Enhanced data ingestion logging

## [v2.0.5] – 2025-10-02
### Fixed
- Fixed SharePoint ingestion re-indexing unchanged files

## [v2.0.4] – 2025-08-31
### Changed
- Standardized resource group variable as `AZURE_RESOURCE_GROUP`. [#365](https://github.com/Azure/GPT-RAG/issues/365)

## [v2.0.3] – 2025-08-18
### Added
- NL2SQL Ingestion.

## [v2.0.2] – 2025-08-17
### Fixed
- Resolved issue with using Azure Container Apps under a private endpoint in AI Search as a custom web skill.

## [v2.0.1] – 2025-08-08
### Fixed
- Corrected v2.0.0 deployment issues.

## [v2.0.0] – 2025-07-22
### Changed
- Major architecture refactor to support the vNext architecture.

## [v1.0.0] 
- Original version.
