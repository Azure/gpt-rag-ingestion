# Changelog

## [Unreleased]

### Added

- **Repository-local release skill.** Added a reusable Copilot release workflow that discovers the authoritative version from published tags, GitHub releases, and tracked version files; enforces `release/X.Y.Z` branches from `develop` with pull requests to `main`; keeps SemVer, changelog, `VERSION`, tag, and release title consistent; sanitizes public notes; and blocks all tag, release, package, image, deployment, and Azure publication actions until explicit human approval.

- **Fail-closed Foundry Toolbox retrieval boundary (`POST /retrieve`) for the hosted-agent path ([Azure/GPT-RAG#596](https://github.com/Azure/GPT-RAG/issues/596)).** Retrieval now accepts only a cryptographically validated delegated-user bearer supplied by a Toolbox `UserEntraToken` identity-passthrough flow; caller/model-supplied user IDs, group IDs, and index names are schema violations. The token must match the separately configured `HOSTED_RETRIEVAL_TOKEN_AUDIENCE` and is passed unchanged to Azure AI Search through `x-ms-query-source-authorization`, allowing native user, group, and RBAC-scope trimming instead of application-authored ACL filters. The configured `SEARCH_RAG_INDEX_NAME` is the only query target, elevated read is disabled, Search failures return HTTP 502, and response strings and result counts are bounded. The endpoint is disabled by default and returns HTTP 503 unless both `HOSTED_RETRIEVAL_ENABLED` and `HOSTED_RETRIEVAL_INV_002_VALIDATED` are true; operators may set the evidence gate only after ADR-0001 INV-002 passes two-user negative authorization in the isolated topology. Existing AI Search callers retain elevated-read behavior by default, so classic ingestion behavior is unchanged.

## [v2.5.0] - 2026-07-20

### Added

- **Always-emitted ingestion audit base events ([Azure/GPT-RAG#571](https://github.com/Azure/GPT-RAG/issues/571), [PR #261](https://github.com/Azure/gpt-rag-ingestion/pull/261)).** Operators now receive metadata-only audit events for every ingestion run and confirmed document outcome even with provenance disabled. The service emits `ingestion.run.started/completed/failed/cancelled` and `ingestion.document.indexed/rejected/deleted` through the existing OpenTelemetry/Application Insights pipeline. Every run has exactly one started and one terminal event, cancellation is preserved, and document events are emitted only from confirmed Azure AI Search results. Audit emission is bounded and best-effort, so telemetry failures do not turn successful ingestion work into failures.
- **Opt-in provenance and governance controls.** `INGESTION_PROVENANCE_ENABLED` defaults to `false`, so base audit events are always emitted while richer provenance fields remain absent unless explicitly enabled. `INGESTION_REQUIRE_GOVERNANCE_METADATA` defaults to `false`; when enabled with provenance, strict governance fails closed for documents that do not explicitly provide both classification and right-to-use, and it never substitutes configured defaults as evidence. Enabling strict governance while provenance is disabled fails startup. Non-strict provenance uses `INGESTION_DEFAULT_CLASSIFICATION=unclassified` and `INGESTION_DEFAULT_RIGHT_TO_USE=not_asserted` when documents omit those values.

### Changed

- **Shared orchestrator v3.8.0 contract.** The ingestion service vendors the same `audit-event-v1` logical schema and Application Insights wire schema used by `gpt-rag-orchestrator` v3.8.0. The pinned SHA-256 hashes are `825db8ef40a81e2c19e5d80d37c565b6b47fc9a6540e9881d35cc12b8fde5aab` for the logical schema and `066c8f5408610ab839d5121d06ca5bc59e8797e551d5c47c875c5ba52f7e0588` for the wire schema. Exported records use queryable `customEvents.name` values under `gptrag.audit.ingestion.*`, and logical root events retain the required parent property through the contract root sentinel.

### Privacy and retention

- **Opaque hashes are limited traceability aids.** `source_uri_id` and `source_version_id` are unsalted SHA-256 references rather than raw paths, URLs, or filenames. They are deterministic and may be dictionary-matched for small candidate sets, so they are not secrets, proof of source or custody, or evidence of regulatory compliance. Checksums and audit metadata can also reveal document relationships; protect Application Insights with access controls appropriate for the ingested content.
- **`delete_after` expresses policy intent, not automatic purge.** Setting `delete_after` does not schedule or trigger deletion. Only a confirmed Azure AI Search deletion emits `ingestion.document.deleted`; downstream retention enforcement remains an operator responsibility.

### Validation

- The exact reviewed PR head, `5cfbd180bdf2fd1dcd816dbee483894a43c22cac`, was merged after all reported CI checks completed successfully.
- Independent final review found no blockers and validated actual Azure Monitor exporter envelopes against the pinned Application Insights wire schema for all seven event types.
- All 47 focused audit contract, sanitizer, and emitter tests passed, including logical/wire hash pinning, strict-governance failure paths, sanitization, correlation, cancellation, and exported event names and parent properties.

### Added

- **Governance baseline and audit-event trail for ingestion (Azure/GPT-RAG#571).** Introduces the ingestion side of the shared, versioned `audit-event-v1` contract also implemented by `gpt-rag-orchestrator`: a new `contracts/` directory vendors the schema and its SHA-256 pin byte-for-byte, and a new `telemetry.audit` module emits `ingestion.run.started/completed/failed/cancelled` (exactly one started and one terminal event per run, with `asyncio.CancelledError` preserved and re-raised) and `ingestion.document.indexed/rejected/deleted` (derived from confirmed Azure AI Search batch results, never fabricated). Reuses the existing OpenTelemetry/Application Insights pipeline; emission is bounded and best-effort and can never turn a successful index, delete, or run into a failure.
- **Optional provenance fields, off by default.** New `INGESTION_PROVENANCE_ENABLED` (default `false`), `INGESTION_REQUIRE_GOVERNANCE_METADATA` (default `false`), `INGESTION_DEFAULT_CLASSIFICATION` (default `unclassified`), and `INGESTION_DEFAULT_RIGHT_TO_USE` (default `not_asserted`) flags control whether `provenance_id`, `source_uri_id`, `source_version_id`, `content_checksum_sha256`, `ingested_at`, `ingest_run_id`, `data_classification`, `right_to_use`, `retention_class`, and `delete_after` are attached to document events. `source_uri_id`/`source_version_id` are opaque SHA-256 references — never the raw path, URL, or filename. Setting `INGESTION_REQUIRE_GOVERNANCE_METADATA=true` while `INGESTION_PROVENANCE_ENABLED=false` fails startup with an actionable error; strict mode never substitutes the configured defaults for a document that didn't explicitly supply its own classification and right-to-use. See the README's new "Governance and audit events" section for the full flag matrix and known evidence-gap caveats.

### Fixed

- **Application Insights audit event wire shape.** Ingestion audit records now populate `customEvents.name` as `gptrag.audit.ingestion.*` and encode a missing logical `parent_event_id` with the contract's root sentinel, so all seven event types remain queryable and root events retain the required parent property after Azure Monitor export.

## [v2.4.14] - 2026-06-28

### Fixed

- **Zero Trust ACR builds no longer depend on Docker Hub:** The frontend build stage now uses the Microsoft Container Registry Node 20 devcontainer image instead of Docker Hub's `node:20-slim`, so remote ACR builds no longer pull the frontend base image from `registry-1.docker.io`.
- **Remote ACR build retry is bounded and visible:** Both deployment scripts now retry transient `az acr build` failures with visible attempt counts, retry delays, and a final actionable error message instead of failing once without guidance.

### Validation

- Full GPT-RAG Zero Trust validation passed in Switzerland North using the PR head (`gptrag-zt-che06272059` / `rg-gptrag-zt-che06272059`): provision, postProvision, deploy, ACR, and readiness all passed.
- ACR logs from that validation showed no `registry-1.docker.io` pulls.

## [v2.4.13] - 2026-06-19

### Fixed

- **Operator dashboard Jobs tab: "Run now" strip and "Queue and schedule" panel moved to a new Schedules tab.** The Jobs tab top half stacked two operator-control panels (the 7-button *Run now* strip and the *Queue and schedule* table) above the actual recent-runs list, so the tab tried to be both a control surface and a history view at the same time. The two panels now live on a dedicated **Schedules** tab inserted between *Jobs* and *Files* (new tab order: *Jobs | Schedules | Files | Configuration*). The Jobs tab is now focused on watching what is running right now: search box, type filter, refresh, and the recent-runs table. All existing functionality is preserved (manual triggering, burst polling on click, cron/last-run display) — the panels are simply relocated. Clicking *Run now* on the Schedules tab keeps the operator on Schedules so they can watch the queue table update; the success toast is the confirmation. The *Queue and schedule* panel on its own tab no longer needs a collapse chevron and defaults to expanded.
- **Operator dashboard: success toast on *Run now* stayed visible forever ([#254](https://github.com/Azure/gpt-rag-ingestion/issues/254)).** After clicking a job button the green `"Started <job_type>."` toast had no auto-dismiss path and no manual close button, so it sat on screen until the operator reloaded the page. Toasts now auto-dismiss 4 seconds after they are raised, each toast gets its own timer (a fresh trigger does not extend an older toast), the toast also has a manual `×` button so operators can close it early, and the dismiss timer is cleared on tab unmount so it cannot leak into another view. Same behavior applies to the warning toast (`"<job_type> is already running."`) and error toasts.

### Validation

- Frontend: `npm run lint` clean, `npm run build` clean, `npm test` green (`SchedulesTab` auto-dismiss test asserts the toast is removed from the DOM after the timeout).
- Sandbox validation: image deployed to `ca-4oa7xxpgqecaa-dataingest` in `rg-gptrag-sandbox-2606181758`; `GET /api/version` returns `2.4.13`; new Schedules tab visible between Jobs and Files; *Run now* trigger toast disappears within ~5s without operator action.

## [v2.4.12] - 2026-06-18

### Fixed

- **Misleading placeholders on the Configuration tab.** Operator-reported follow-up to v2.4.11: every text/number input on the Configuration tab used its own example or default as the placeholder, styled close enough to a real entered value that operators could not tell empty fields apart from configured ones (e.g. the Scheduling section showed `0 * * * *`, `0 2 * * *`, `*/15 * * * *` inside the inputs). The same examples were also repeated as helper text below the input, so the placeholder added nothing but confusion. Placeholders now describe what an empty field means at runtime: `"Not configured"` when empty disables the feature (5 cron inputs and all 8 numeric inputs), `"Default: <value>"` for the two inputs (`CRON_RUN_BLOB_INDEX`, `CRON_RUN_BLOB_PURGE`) where the backend `SettingSpec.default` actually applies when empty. The example moves to (or stays in) the helper line below the input, so it is never duplicated. No backend, schema, or styling changes — placeholder rendering keeps using the existing `placeholder:text-muted-foreground` token from `SettingField.tsx`. 15 inputs updated in total.

### Validation

- Frontend: `npm run lint` clean, `npm run build` clean.
- Sandbox validation: image deployed to `ca-4oa7xxpgqecaa-dataingest` in `rg-gptrag-sandbox-2606181758`; `GET /api/version` returns `2.4.12`.

## [v2.4.11] - 2026-06-18

### Fixed

- **Operator dashboard Queue panel rough edges from v2.4.10 ([#247](https://github.com/Azure/gpt-rag-ingestion/issues/247) follow-up):** Five small but visible issues bundled into one release so operators get the polish in one container update.
  - *Run now* toast wording. The success toast said `"Queued <job_type>."` but APScheduler fires the job immediately, so operators already saw it running in the runs table before they finished reading the toast. The toast now says `"Started <job_type>."` on success, `"<job_type> is already running."` (warning variant) on `409 Conflict`, and keeps the existing error toast for other failures.
  - `cron` field on `GET /api/jobs/queue` was `null` for every `job_type`. The endpoint was reading from app config keys with a pattern that did not match what `main.py` writes, so the *Cron* column was empty for the whole panel. The endpoint now reads cron directly from `scheduler.get_job(job_id).trigger` — single source of truth, identical to what APScheduler is actually firing. A small helper inside `api/admin.py` walks `trigger.fields` **by field name** (`minute`, `hour`, `day`, `month`, `day_of_week`) instead of positional slicing, because APScheduler's `fields` also includes `year`, `week`, and `second`, which would have produced a wrong 5-field string. Non-`CronTrigger` triggers (`DateTrigger`, `IntervalTrigger`, or no job registered) keep returning `null`.
  - Queue panel polling cadence after *Run now*. The panel polled every 10 seconds, so a *Run now* click could take up to a full poll cycle before the new in-flight state showed up. After any *Run now* click the panel now bursts to a 1-second poll for 15 seconds, then reverts to 10 seconds. A single `setTimeout` + `setInterval` ref pair is cleared and reset on every click (does not stack) and cleared on unmount.

### Added

- **Collapsible Queue panel.** The 7-row Queue table was pushing the runs table down ~250 px on every page load even when operators were not actively watching the queue. The *Queue and schedule* header is now a chevron toggle (`▸` collapsed, `▾` expanded) and defaults to **collapsed**. The preference is persisted in `localStorage` under the key `gpt-rag-ingestion.queuePanel.expanded` and hydrated on mount. When collapsed, the header still shows a compact summary line: `"Queue and schedule — N jobs scheduled, M in flight"` where `N` counts items with `next_scheduled_at != null` and `M` counts items with `in_flight != null`, so the panel earns its space even when not expanded.
- **"Last run" column in the Queue panel.** The panel previously only showed `Next run`, so it felt static and never reflected manual *Run now* activity. A new `last_run` field on each `GET /api/jobs/queue` item carries `{started_at, finished_at, status, indexed_count}` (or `null`), derived from the same cached runs store the `/api/jobs/runs` endpoint reads — no second frontend request per poll. The column renders as `"<relative time> · <status> · <indexed> indexed"` (for example `"3s ago · finished · 0 indexed"` or `"5m ago · failed"`). Column order in the table is now: `Job | In flight | Last run | Next run | Cron`.

### Validation

- Full pytest suite: 37 passed (3 new in `tests/test_admin_jobs_queue.py`: `test_queue_cron_is_read_from_trigger_not_app_config`, `test_queue_last_run_populated_from_runs_store`, `test_queue_last_run_handles_failed_run_without_indexed_count`).
- Frontend: `npm run lint` clean, `npm run build` clean.
- Sandbox validation: image deployed to `ca-4oa7xxpgqecaa-dataingest` in `rg-gptrag-sandbox-2606181758`; `GET /api/version` returns `2.4.11`; `GET /api/jobs/queue` returns non-null `cron` for `blob_index` and `blob_purge` and populated `last_run` for `blob_index`.

## [v2.4.10] - 2026-06-18

### Added

- **Operator dashboard now shows queued jobs and the next-run ETA per `job_type` ([#247](https://github.com/Azure/gpt-rag-ingestion/issues/247)):** Before this release, the *Run now* button (added in v2.4.7) was fire-and-forget — clicking it queued a job and showed a toast, then disappeared. Operators had no way to see what was in flight or when the next cron would fire without tailing container logs. This release adds a compact *Queue and schedule* panel above the Jobs table that answers both questions.
  - A new read-only endpoint `GET /api/jobs/queue` returns one row per `job_type` with `in_flight` (`{run_id, started_at}` or `null`), `next_scheduled_at` (ISO-8601 UTC from APScheduler's `next_run_time`, or `null` if no cron is registered), and the current `cron` string from app config. Same network-only auth posture as `GET /api/jobs` and `GET /api/config` — no `Admin` role required to view.
  - The existing in-process `_running_jobs` registry (introduced in v2.4.7 for mutual exclusion between manual and cron runs) was extended to also record `started_at` at the same insertion sites, so manual and cron runs continue to share one lock — no parallel registry was added.
  - The frontend Queue panel polls `GET /api/jobs/queue` every 10 seconds while the Jobs tab is mounted (plain `setInterval`, no new data-fetching library). Columns: Job, In flight (run id + elapsed), Next run (relative like "in 12 min" with the absolute ISO timestamp in a tooltip), Cron.
  - When a job is reported as in flight, the matching *Run now* button is rendered disabled with the tooltip "Job already running", so operators learn before they click and get a `409 Conflict`.
  - The cron string is read from the same config source used by the *Configuration* tab, so a *Run-now-then-edit-CRON* cycle reflects the new schedule on the next poll without requiring a refresh.

### Validation

- Full pytest suite: 34 passed (7 new in `tests/test_admin_jobs_queue.py`, covering empty queue with crons, in-flight reporting, naive-datetime normalization, missing cron registration, and cron set but no `next_run_time`).
- Frontend: `npm run lint` clean, `npm run build` clean.
- No persistent queue (Service Bus, Storage Queue, etc.) was added — explicitly out of scope per the issue.

## [v2.4.9] - 2026-06-18

### Fixed

- **Configuration tab in the operator dashboard was still blank after v2.4.8 ([#242](https://github.com/Azure/gpt-rag-ingestion/issues/242) follow-up):** v2.4.8 fixed the missing top-level `settings` array, but `GET /api/config` was also emitting each section as `{id, label, settings}` while the typed frontend `ConfigSection` reads `{id, title, keys}`. `section.keys.map(...)` therefore crashed with `TypeError: undefined is not iterable` and the tab still rendered nothing. The endpoint now also emits `title` (mirror of `label`) and `keys` (the ordered list of setting keys, matching the nested `settings` order) on every section. The legacy `label` and nested `settings` fields stay, so no other callers are affected.

## [v2.4.8] - 2026-06-18

### Fixed

- **Configuration tab in the operator dashboard was blank ([#242](https://github.com/Azure/gpt-rag-ingestion/issues/242)):** `GET /api/config` returned `{sections, canEdit}` but the typed `ConfigResponse` and the `ConfigurationTab` frontend also read a top-level `settings` array and an `authEnabled` flag. With `res.settings` undefined the tab crashed with `TypeError: undefined is not iterable` and rendered nothing. The endpoint now returns both the grouped `sections` and a flat `settings` list (built from the same `_read_setting(cfg, spec)` calls so the two views stay in lock-step) plus `authEnabled` derived from `_auth_enabled()`. The existing `sections` shape is unchanged, so no other callers are affected.

## [v2.4.7] - 2026-06-18

### User and operator impact

This release adds two operator-facing improvements to the ingestion dashboard, both requested in the GPT-RAG issue tracker. Existing deployments keep the same default behavior: the dashboard is opt-in via `ENABLE_DASHBOARD`, the new endpoints are gated by the `Admin` Entra app role when authentication is on, and nothing else changes.

### Added

- **Run-now button for ingestion jobs ([Azure/GPT-RAG#510](https://github.com/Azure/GPT-RAG/issues/510)):** You no longer have to wait for the next cron tick or restart the container to kick off a job.
  - A *Run now* button appears for each `job_type` (blob, SharePoint, NL2SQL, etc.) above the Jobs table in the dashboard.
  - Backed by `POST /api/jobs/{job_type}/run`, which enqueues the matching scheduler function as a one-shot trigger — the exact same code path the cron schedule uses.
  - A shared in-process registry (`_running_jobs`) makes manual and cron-triggered runs share the same mutual exclusion: if the same job is already running, the manual trigger gets `409 Conflict` instead of a duplicate run.
  - The endpoint is gated by the new `require_admin` dependency (no-op when `OAUTH_AZURE_AD_TENANT_ID` is unset; otherwise requires a bearer token with the `Admin` Entra app role).
  - A companion `GET /api/identity` returns `{authEnabled, isAdmin}` so the frontend can disable the button with an accessible tooltip ("Admin role required") when the caller lacks the role. Read-only dashboard endpoints stay network-only as before.

- **Configuration tab in the dashboard ([Azure/GPT-RAG#512](https://github.com/Azure/GPT-RAG/issues/512)):** A new *Configuration* tab lets admins view and edit a curated set of 17 runtime settings without going to the Azure portal.
  - **What you can edit.** Seven sections grouped the way operators think about ingestion: *Scheduling* (the seven `CRON_RUN_*` expressions), *Chunking*, *Indexing*, *Throughput and concurrency*, *Processing limits*, *Multimodal*, and *SharePoint*. Each field uses the right input type for what it represents.
  - **Accessible tooltips.** Every field has an info popover that is keyboard- and screen-reader-reachable (not hover-only) explaining what the setting does and what the trade-offs are.
  - **Cron edits take effect immediately.** When you change a `CRON_RUN_*` value, the scheduler is rescheduled via `AsyncIOScheduler.reschedule_job` as soon as the save succeeds. No restart needed for schedule changes to apply.
  - **Cron validation matches startup.** Expressions are validated through `CronTrigger.from_crontab`, the same parser the runtime uses at startup. What you see at startup is what you get when you save.
  - **Two safety nets.** An explicit allow-list of 17 keys, plus a defense-in-depth segment denylist that rejects any key whose underscore-delimited tokens overlap with secret/connection terms (`SECRET`, `PASSWORD`, `CONNECTION`, `ENDPOINT`, `APIKEY`, `CLIENTID`, `TENANTID`, `URI`, `URL`, `CONNSTR`). Never-exposed settings like the Key Vault URI, SharePoint client secret name, MCP API key, and any connection string or endpoint are absent from the allow-list and additionally trapped by the denylist.
  - **Helpful errors.** `PUT /api/config` returns `200` on full success, `207` for partial success with a per-field error map (valid fields are saved, invalid ones come back with a reason), or `422` if every field is invalid. The UI surfaces each error inline next to its field.
  - **Honest action buttons.** *Reload settings cache* (`POST /api/config/reload`) refreshes the in-process App Configuration cache. *Apply changes* (`POST /api/config/apply`) is a soft restart that refreshes the cache, reschedules every known cron job, and invalidates the runs/files caches — and returns `note: "In-process refresh. Hard container restart is not supported by this endpoint."` so operators are not misled.
  - **Read-only viewing for non-admins.** When auth is enabled and the caller does not hold the `Admin` role, the tab still loads with current values but every input is disabled and a banner explains "Admin role required to edit". Useful for hand-offs and audits.
  - **Where values are written.** Accepted values are written to App Configuration under the `gpt-rag-ingestion` label so it is easy to filter who wrote what in the Azure portal.

### Validation

- Full pytest suite: 25 passed (9 new in `tests/test_admin_config.py`).
- Frontend: `npm run lint` clean, `npm run build` clean (277 KB JS, 88 KB gzipped).
- Endpoint surface manually exercised: `GET /api/config` (open, returns `canEdit`), `PUT /api/config` (allow-list accept, denylist reject, range reject, cron validation reject, `200/207/422` shape), `POST /api/config/reload`, `POST /api/config/apply` (cron reschedule observed), `POST /api/jobs/{job_type}/run` (admin-gated, `409` on duplicate).

## [v2.4.6] - 2026-06-15

### Reverted
- **`react-dom` and `@types/react-dom` major bump to 19.x ([#212](https://github.com/Azure/gpt-rag-ingestion/pull/212))** reverted back to `^18.3.1` / `^18.3.7`. The bump pulled React 19 into `frontend/` while the `@radix-ui/*` chain still pins `@types/react@^18`, which breaks `npm install` resolution in the admin dashboard frontend build. The major version bump will be re-evaluated together with a coordinated upgrade of the Radix UI and `@types/react` chain.

## [v2.4.5] - 2026-06-15

### Changed
- **Dependency refresh:** Absorbed Dependabot bumps merged into `develop`:
  - `@tailwindcss/typography` in `/frontend` ([#210](https://github.com/Azure/gpt-rag-ingestion/pull/210))
  - `requests` 2.33.0 → 2.34.2 ([#211](https://github.com/Azure/gpt-rag-ingestion/pull/211))
  - `react-dom` and `@types/react-dom` in `/frontend` ([#212](https://github.com/Azure/gpt-rag-ingestion/pull/212))
  - `typescript-eslint` in `/frontend` ([#215](https://github.com/Azure/gpt-rag-ingestion/pull/215))
  - `azure-appconfiguration` 1.7.1 → 1.8.1 ([#217](https://github.com/Azure/gpt-rag-ingestion/pull/217))
  - `pymupdf` 1.25.4 → 1.27.2.3 ([#218](https://github.com/Azure/gpt-rag-ingestion/pull/218))
  - `uvicorn` 0.34.2 → 0.49.0 ([#219](https://github.com/Azure/gpt-rag-ingestion/pull/219))

## [v2.4.4] - 2026-06-14

### Added
- **Blob storage metadata indexed as `custom_metadata` (issue [#487](https://github.com/Azure/GPT-RAG/issues/487)):** The blob storage indexer now extracts every user-defined tag from `blob.metadata` (skipping reserved `metadata_security_*` keys) and stamps it onto each AI Search chunk as `custom_metadata`, a `Collection(Edm.ComplexType)` with `{key, value}` pairs. Keys are normalized to trimmed lowercase, empty or whitespace-only values are dropped, and the field is filterable and facetable so retrievers can target documents by metadata. Requires the matching `custom_metadata` field to be present in the RAG index schema (shipped in `gpt-rag` via `config/search/search.j2`).

### Fixed
- **Content Understanding multimodal ingestion regression:** Restored the format-based figure extraction path for `MultimodalChunker` when Content Understanding is used, including PDF page/region rendering and Office embedded image extraction. This brings back the behavior released in v2.3.3 without changing the `/ingest-documents` upload ACL fix.

### Changed
- **Warn when `$env:APP_CONFIG_ENDPOINT` diverges from the azd environment during component deploy (issue [Azure/GPT-RAG#491](https://github.com/Azure/GPT-RAG/issues/491)):** `scripts/deploy.ps1` and `scripts/deploy.sh` now read both the shell `APP_CONFIG_ENDPOINT` and the azd env value and, when both are present and disagree (trimmed, case-insensitive), print a yellow warning that shows both values, states which one is being used (the shell value still wins, preserving existing precedence for jumpbox and CI flows), and tells the operator how to clear the shell override (`Remove-Item env:APP_CONFIG_ENDPOINT` in PowerShell, `unset APP_CONFIG_ENDPOINT` in bash). When only one source is set, the previous behavior is unchanged.

## [v2.4.3] - 2026-06-05

### Fixed
- **Uploaded document ACLs for permission-trimmed indexes (issue #478):** `/ingest-documents` now accepts an optional `securityUserIds` array and stamps it onto each chunk's `metadata_security_user_ids` instead of always writing an empty ACL. When the search index has `permissionFilterOption` enabled, this lets the uploader retrieve their own uploaded chunks (which AI Search would otherwise trim out). Anonymous/placeholder ids are ignored, and the default empty-ACL behavior is preserved when the field is absent.

All notable changes to this project will be documented in this file.
This format follows [Keep a Changelog](https://keepachangelog.com/) and adheres to [Semantic Versioning](https://semver.org/).

## [v2.4.2] - 2026-05-28

### Fixed
- **Managed Identity client ID fallback:** Preserved App Configuration precedence for `AZURE_CLIENT_ID` while falling back to the Container Apps-injected environment variable when the key is not published, fixing `/ingest-documents` and background indexing paths that authenticate with the user-assigned Managed Identity.

## [v2.4.1] - 2026-05-28

### Fixed
- **Managed Identity token acquisition in Container Apps:** Updated `azure-identity` to 1.25.1 so the ingestion service can authenticate sync runtime paths such as `/ingest-documents`, Content Understanding, Azure OpenAI embeddings, and Blob Storage from Azure Container Apps using user-assigned Managed Identity.

## [v2.4.0] - 2026-05-27

### Changed
- **Dependency refresh:** Updated ingestion runtime dependencies to `python-dotenv` 1.2.2, `PyJWT` 2.12.0, `Pillow` 12.2.0, `pypdf` 6.10.2, and `langchain-text-splitters` 1.1.2. The PDF and text chunking upgrades were validated against page counting, PDF splitting, LangChain text chunking, and document-analysis PDF auto-split smoke tests.
- **Frontend dependency refresh:** Updated the ingestion frontend `postcss` dependency to 8.5.15 and refreshed the npm lockfile.

## [v2.3.8] - 2026-05-27

### Changed
- **Dependency refresh:** Updated `requests` to 2.33.0, `aiohttp` to 3.13.4, and `Pillow` to 12.1.1 for ingestion runtime dependencies.

## [v2.3.7] - 2026-05-26

### Fixed
- **Azure CLI warning-safe deploy verification:** Filter Azure CLI warning and progress lines from App Configuration, Container Apps update, and image verification output before consuming TSV values, so Windows deploys do not fail when the Azure CLI or Container Apps extension emits non-data output. Fixes [Azure/GPT-RAG#449](https://github.com/Azure/GPT-RAG/issues/449).

## [v2.3.6] - 2026-05-26

### Fixed
- **Container Apps image update verification:** Replaced the mandatory latest-revision restart with explicit image verification after `az containerapp update --image`, avoiding transient `Not Found` failures immediately after revision creation while still confirming the new image is configured. Fixes [Azure/GPT-RAG#449](https://github.com/Azure/GPT-RAG/issues/449).

## [v2.3.5] - 2026-05-25

### Fixed
- **Docker-free component deployment:** Updated Bash and PowerShell deploy scripts to choose the build mode before touching Docker, use `az acr build` when Docker is unavailable or remote build is requested, configure Container App registry identity, and restart the latest revision after image updates. Fixes [Azure/GPT-RAG#449](https://github.com/Azure/GPT-RAG/issues/449).

## [v2.3.4] – 2026-05-19

### Added
- **Per-conversation document upload endpoint (`POST /ingest-documents`)**: Users can now upload files directly through the chat interface and have them ingested, indexed, and made available for retrieval scoped to a specific conversation. The endpoint persists the original bytes under `conversations/<conversationId>/<recordId>/<filename>` in the `conversation-documents` storage container, chunks and embeds the document with `DocumentChunkerFactory`, and writes each chunk to Azure AI Search tagged with the camelCase `conversationId` field that the orchestrator filter expects. Authentication is enforced via the `DATA_INGEST_APP_APIKEY` (also accepts the legacy `INGESTION_APP_APIKEY`). Implements [Azure/GPT-RAG#401](https://github.com/Azure/GPT-RAG/issues/401). ([#183](https://github.com/Azure/gpt-rag-ingestion/pull/183))

### Fixed
- **`conversationId` field naming**: `/ingest-documents` now writes the camelCase `conversationId` field (matching the index schema, blob/SharePoint indexers, and the orchestrator retrieval filter). The original PR used snake_case `conversation_id`, which would have caused chunks to land with null `conversationId` and never match the orchestrator's per-conversation filter.
- **Missing imports inside the endpoint**: Deferred imports of `_make_chunk_key` (from `jobs.sharepoint_ingestion_config`) and `upload_bytes_to_container` (from `tools.blob`) inside the request handler, preventing `NameError` during chunk indexing and blob persistence.
- **Original document bytes were discarded**: The endpoint now actually calls `upload_bytes_to_container` to persist the uploaded bytes to the `conversation-documents` container, and propagates the resulting blob path/URL into the indexed `url` and `filepath` fields so the orchestrator can cite back to the uploaded file.
- **`documentUrl` missing from chunker input**: The chunker `input_data` now includes `documentUrl` (previously raised `KeyError: 'documentUrl'` inside `BaseChunker.__init__`, which was caught as a generic "Embedding error" with zero chunks indexed).

## [v2.3.2] – 2026-04-08

### Changed
- **Default `INDEXER_MAX_CONCURRENCY` lowered to 2**: Reduced the default concurrency for all indexers (blob storage, SharePoint, NL2SQL) from 8/4 to 2. This reduces memory pressure and rate-limit contention when processing large documents, improving reliability on default Container App configurations. Still overridable via the `INDEXER_MAX_CONCURRENCY` App Config key.

### Fixed
- **Frontend source files excluded by `.gitignore` breaking Docker build**: The root `.gitignore` contained a bare `lib/` pattern (intended for Python packaging artifacts) that inadvertently excluded `frontend/src/lib/`, preventing `api.ts` and `utils.ts` from being committed. This caused TypeScript compilation errors during the Docker frontend build stage, resulting in a non-existent image being referenced by the Container App. Fixed by scoping the pattern to `/lib/` (root-only) and committing the missing files.
- **`deploy.ps1` silently continuing after failed commands**: The deploy script used PowerShell `try/catch` around native executables (`docker build`, `docker push`, `az containerapp update`, etc.), which does not catch non-zero exit codes. The script would print success messages and continue even when commands failed, masking build and push failures. Replaced with explicit `$LASTEXITCODE` checks after each critical command.
- **Dashboard retries column showing inflated count during processing**: The `processingAttempts` counter is pre-incremented before processing starts (for crash detection), so first-attempt files showed "1 retry" instead of "0". Both the Files table and the detail dialog now display `processingAttempts - 1` to reflect actual retries.
- **Cost estimate displayed with excessive decimal places**: The `formatUSD()` function in the dashboard detail dialog used 4 decimal places (e.g., `$22.7500`). Changed to 2 decimal places (`$22.75`) for cleaner display. Backend cost calculations also rounded to 2 decimals.
- **Stale "running" jobs stuck forever after container crash/restart**: When a container was killed (OOM, restart) mid-run, the `finally` block that writes `runFinishedAt` never executed, leaving the run summary blob permanently stuck with `status: "running"`. The admin API now detects runs that started more than 2 hours ago without finishing and marks them as `"interrupted"` with an orange status badge.
- **Literal `\u21b3` text displayed instead of arrow character**: The 429 rate-limit sub-item in the timings bar rendered the raw Unicode escape `\u21b3` as text instead of the ↳ arrow. Fixed by using a JSX expression `{"\u21b3"}` for proper rendering.
- **Unclear 429 rate-limit display text**: Changed from `"90× 429 Rate-limit wait (5m 42s)"` to `"429 Rate-limit — 90 retries, 5m 42s wait"` for better readability when both count and duration are present.

## [v2.3.1] – 2026-04-08

### Added
- **Processing timings breakdown in dashboard**: Each file processing run now records per-phase timing data (download, analysis, chunking + embeddings, index upload) and stores it in the file log. The admin dashboard detail dialog displays a stacked color bar and a legend with durations for each phase, plus a total. Rate-limit retry wait time (429 backoff) is tracked separately and shown as a sub-item under chunking + embeddings. Run history entries also show a Duration column. This makes it easy to identify bottlenecks when processing large documents.
- **429 rate-limit count and improved display**: The number of 429 (Too Many Requests) retries is now tracked per file and displayed alongside the rate-limit wait time in the format "N× 429 Rate-limit wait (duration)". Both the count and the wait time are only shown when retries actually occurred.
- **Per-file cost estimation**: Processing cost is now estimated per file, broken down by service: analysis (Content Understanding or Document Intelligence, per page), Azure OpenAI Embeddings (per token), and Azure OpenAI Completions (per token, when applicable). Unit prices are configurable via App Config keys (`COST_PER_PAGE_ANALYSIS`, `COST_PER_1K_EMBEDDING_TOKENS`, `COST_PER_1K_COMPLETION_INPUT_TOKENS`, `COST_PER_1K_COMPLETION_OUTPUT_TOKENS`) with sensible defaults based on April 2026 list pricing. The dashboard displays the breakdown in a dedicated "Cost Estimate" section with a short disclaimer.
- **Automatic PDF splitting for large documents**: PDFs exceeding the Azure analysis service page limit (configurable via `MAX_PAGES_PER_ANALYSIS`, default 300) are now automatically split into smaller parts before analysis. Each part is analyzed separately and the markdown results are concatenated with correct absolute page numbering. This prevents `InputPageCountExceeded` errors and is transparent to the rest of the pipeline — same `parent_id`, same chunk keys, same search index behavior. Requires the new `pypdf` dependency.
- **Memory guard before blob download**: Before downloading a blob for processing, the indexer now checks the file size against available container memory (via cgroups + `psutil`). If the estimated peak memory usage would exceed available capacity, processing is skipped with a descriptive error instead of risking an OOM crash that restarts the container. Configurable via `MEMORY_SAFETY_MULTIPLIER` (default 4.0) and `MEMORY_SAFETY_THRESHOLD` (default 0.85).
- **Temp file download for large PDFs**: PDFs larger than 10 MB are now downloaded to a temporary file on disk instead of being held entirely in memory. The auto-split logic operates on these temp files, keeping peak memory usage bounded to one part at a time (~200 MB) instead of the full document (~1.5 GB+).

### Fixed
- **`_as_datetime` NameError crashing every indexer run**: The helper function `_as_datetime` was called in four places within `blob_storage_indexer.py` but was never defined, causing a `NameError` on every run after the retry-tracking feature was added. Added the missing function definition at module level.
- **Orphaned `value` variable causing NameError in memory guard**: A leftover code block from an earlier refactor inside `_check_memory_capacity()` referenced an undefined variable `value`, crashing the memory guard check before any file could be processed. Removed the dead code.
- **Dashboard unresponsive during file processing**: The FastAPI event loop was blocked by synchronous chunking and document iteration calls, making the admin dashboard and health endpoints unresponsive for the entire duration of large file processing (20+ minutes). Wrapped the blocking `list(docs_iter)` calls with `asyncio.to_thread()` so they run in a worker thread without blocking the event loop.
- **Stale error field on successful re-processing**: When a file was re-processed successfully after previous failures, the top-level `error` field in the file log retained the last error message despite `status` being `success`. The field is now explicitly cleared to `null` on success.

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
