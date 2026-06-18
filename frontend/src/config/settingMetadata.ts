/**
 * Single source of truth for human-readable labels, accessible tooltips,
 * and input hints used by the Configuration tab.
 *
 * Keep this file in sync with the SETTINGS allowlist in api/admin.py.
 * The backend remains authoritative for type, range, and section assignment;
 * this file only enriches the rendered UI.
 */

export interface SettingMetadata {
  /** Short, human-friendly label shown next to the input. */
  label: string;
  /** Accessible tooltip text shown via the (i) icon. */
  tooltip: string;
  /** Optional placeholder shown inside text/number inputs. */
  placeholder?: string;
  /** Optional secondary hint rendered under the input (e.g. cron sample). */
  hint?: string;
}

export const SETTING_META: Record<string, SettingMetadata> = {
  // ─── Scheduling ────────────────────────────────────────────────────────
  CRON_RUN_SHAREPOINT_INDEX: {
    label: "SharePoint index",
    tooltip:
      "Cron expression that controls when the SharePoint indexer runs. Use 5 fields (minute hour day-of-month month day-of-week). Leave empty to disable the schedule.",
    placeholder: "0 * * * *",
    hint: "Example: 0 * * * * runs at the top of every hour (UTC unless TZ overridden).",
  },
  CRON_RUN_SHAREPOINT_PURGE: {
    label: "SharePoint purge",
    tooltip:
      "Cron expression that controls when SharePoint deletions are reconciled into the search index. Empty disables purges.",
    placeholder: "0 2 * * *",
    hint: "Example: 0 2 * * * runs daily at 02:00.",
  },
  CRON_RUN_IMAGES_PURGE: {
    label: "Multimodality images purge",
    tooltip:
      "Cron expression that controls when orphaned multimodal image chunks are removed from the index. Empty disables the schedule.",
    placeholder: "0 3 * * *",
    hint: "Example: 0 3 * * * runs daily at 03:00.",
  },
  CRON_RUN_BLOB_INDEX: {
    label: "Blob index",
    tooltip:
      "Cron expression that controls when the blob indexer runs. Empty disables the schedule.",
    placeholder: "*/15 * * * *",
    hint: "Example: */15 * * * * runs every 15 minutes.",
  },
  CRON_RUN_BLOB_PURGE: {
    label: "Blob purge",
    tooltip:
      "Cron expression that controls when blob deletions are reconciled into the search index. Empty disables purges.",
    placeholder: "0 1 * * *",
    hint: "Example: 0 1 * * * runs daily at 01:00.",
  },
  CRON_RUN_NL2SQL_INDEX: {
    label: "NL2SQL index",
    tooltip:
      "Cron expression that controls when the NL2SQL indexer refreshes prompt assets. Empty disables the schedule.",
    placeholder: "0 4 * * *",
    hint: "Example: 0 4 * * * runs daily at 04:00.",
  },
  CRON_RUN_NL2SQL_PURGE: {
    label: "NL2SQL purge",
    tooltip:
      "Cron expression that controls when stale NL2SQL artifacts are removed from the index. Empty disables purges.",
    placeholder: "0 5 * * *",
    hint: "Example: 0 5 * * * runs daily at 05:00.",
  },

  // ─── Chunking ──────────────────────────────────────────────────────────
  CHUNKING_NUM_TOKENS: {
    label: "Target tokens per chunk",
    tooltip:
      "Soft target for the number of tokens in each text chunk. Larger chunks reduce retrieval noise but increase embedding cost and may exceed model context for downstream LLMs. Typical range: 256-2048.",
    placeholder: "1024",
  },
  CHUNKING_MIN_CHUNK_SIZE: {
    label: "Minimum chunk size (tokens)",
    tooltip:
      "Minimum token count for a chunk. Chunks below this size are merged with neighbors. Very small minimums create many tiny chunks (higher cost, weaker context); very high minimums can drop short sections.",
    placeholder: "100",
  },
  SPREADSHEET_CHUNKING_BY_ROW: {
    label: "Chunk spreadsheets row-by-row",
    tooltip:
      "When enabled, spreadsheets are chunked one row per chunk (preserves per-row context, useful for tabular Q&A). When disabled, rows are grouped by token budget (cheaper, fewer chunks).",
  },

  // ─── Indexing ──────────────────────────────────────────────────────────
  INDEXER_BATCH_SIZE: {
    label: "Indexer batch size",
    tooltip:
      "Number of documents uploaded to Azure AI Search per batch. Larger batches improve throughput but raise per-call latency and memory pressure. Search service caps individual batches at 1000 documents.",
    placeholder: "100",
  },

  // ─── Throughput and concurrency ────────────────────────────────────────
  INDEXER_MAX_CONCURRENCY: {
    label: "Indexer max concurrency",
    tooltip:
      "Maximum number of files processed in parallel by the indexer. Higher values increase throughput at the cost of CPU, memory, and downstream rate-limit pressure (AOAI, Document Intelligence).",
    placeholder: "4",
  },
  AOAI_MAX_CONCURRENCY: {
    label: "Azure OpenAI max concurrency",
    tooltip:
      "Maximum number of concurrent calls to Azure OpenAI (embeddings, multimodal). Increase to push more throughput if your AOAI quota allows; decrease to reduce 429 throttling.",
    placeholder: "10",
  },

  // ─── Processing limits ─────────────────────────────────────────────────
  MAX_FILE_PROCESSING_ATTEMPTS: {
    label: "Max file processing attempts",
    tooltip:
      "How many times the pipeline retries a failing file before marking it blocked. Higher values are more tolerant of transient errors but delay surfacing real failures.",
    placeholder: "3",
  },
  MAX_PAGES_PER_ANALYSIS: {
    label: "Max pages per analysis call",
    tooltip:
      "Maximum page count submitted to Document Intelligence in a single call. Smaller values reduce per-call latency and memory; larger values reduce per-document cost but risk timeouts on very large PDFs.",
    placeholder: "10",
  },

  // ─── Multimodal ────────────────────────────────────────────────────────
  MULTIMODAL: {
    label: "Enable multimodal extraction",
    tooltip:
      "When enabled, images and figures are captioned through Azure OpenAI Vision and indexed alongside text. Adds AOAI cost per image and increases indexing time.",
  },

  // ─── SharePoint ────────────────────────────────────────────────────────
  SHAREPOINT_MAX_FILE_COUNT: {
    label: "SharePoint max files per run",
    tooltip:
      "Upper bound on files pulled from SharePoint per scheduled run. Caps blast radius during backfills and prevents long-running jobs from blocking other schedules. Use a low value during testing.",
    placeholder: "500",
  },
};

/**
 * Look up label/tooltip for a key. Falls back to humanizing the raw key so
 * unknown additions still render rather than crash.
 */
export function getMeta(key: string): SettingMetadata {
  const entry = SETTING_META[key];
  if (entry) return entry;
  return {
    label: key
      .toLowerCase()
      .split("_")
      .map((s) => (s ? s[0].toUpperCase() + s.slice(1) : s))
      .join(" "),
    tooltip: "No description available for this setting.",
  };
}
