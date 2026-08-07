const BASE = "/api";

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  pageSize: number;
  indexerTypes?: string[];
  availableJobTypes?: string[];
  runningJobTypes?: string[];
}

export interface JobRun {
  indexerType?: string;
  runId?: string;
  status?: string;
  runStartedAt?: string;
  runFinishedAt?: string;
  sourceContainer?: string;
  sourceFiles?: number;
  candidates?: number;
  indexedItems?: number;
  skippedNoChange?: number;
  skippedBlocked?: number;
  success?: number;
  failed?: number;
  totalChunksUploaded?: number;
  // Purger-specific
  blobDocumentsCount?: number;
  indexParentsCountBefore?: number;
  indexChunkDocumentsBefore?: number;
  indexParentsPurged?: number;
  indexChunkDocumentsDeleted?: number;
  indexParentsCountAfter?: number;
  error?: string;
  _blobName?: string;
  [key: string]: unknown;
}

export interface RunHistoryEntry {
  runId: string;
  status: string;
  startedAt?: string;
  finishedAt?: string;
  chunks?: number;
  error?: string;
}

export interface FileLog {
  indexerType?: string;
  fileName?: string;
  blob?: string;
  parent_id?: string;
  status?: string;
  startedAt?: string;
  finishedAt?: string;
  runId?: string;
  chunks?: number;
  processingAttempts?: number;
  blocked?: boolean;
  blockedAt?: string;
  blockedReason?: string;
  unblockedAt?: string;
  error?: string;
  runHistory?: RunHistoryEntry[];
  _blobName?: string;
  [key: string]: unknown;
}

interface ListParams {
  page?: number;
  pageSize?: number;
  search?: string;
  sortField?: string;
  sortOrder?: "asc" | "desc";
  indexerType?: string;
}

function qs(params: Record<string, string | number | boolean | undefined>): string {
  const entries = Object.entries(params).filter(([, v]) => v !== undefined && v !== "");
  return entries.length ? "?" + new URLSearchParams(entries.map(([k, v]) => [k, String(v)])).toString() : "";
}

export async function fetchJobs(params: ListParams = {}, signal?: AbortSignal): Promise<PaginatedResponse<JobRun>> {
  const q = qs(params as Record<string, string>);
  const r = await fetch(`${BASE}/jobs${q}`, { signal });
  if (!r.ok) throw new Error(`Failed to fetch jobs: ${r.status}`);
  return r.json();
}

export async function fetchFiles(
  params: ListParams & { blocked?: boolean } = {},
  signal?: AbortSignal,
): Promise<PaginatedResponse<FileLog>> {
  const q = qs(params as Record<string, string>);
  const r = await fetch(`${BASE}/files${q}`, { signal });
  if (!r.ok) throw new Error(`Failed to fetch files: ${r.status}`);
  return r.json();
}

export async function unblockFile(blobName: string): Promise<void> {
  const r = await fetch(`${BASE}/files/unblock?blobName=${encodeURIComponent(blobName)}`, {
    method: "POST",
  });
  if (!r.ok) throw new Error(`Failed to unblock: ${r.status}`);
}

export async function fetchVersion(): Promise<string> {
  const r = await fetch(`${BASE}/version`);
  if (!r.ok) return "unknown";
  const data = await r.json();
  return data.version ?? "unknown";
}

export interface Identity {
  authEnabled: boolean;
  isAdmin: boolean;
}

export async function fetchIdentity(signal?: AbortSignal): Promise<Identity> {
  try {
    const r = await fetch(`${BASE}/identity`, { signal });
    if (!r.ok) return { authEnabled: true, isAdmin: false };
    return (await r.json()) as Identity;
  } catch {
    return { authEnabled: true, isAdmin: false };
  }
}

export interface RunJobError extends Error {
  status: number;
  /** True when the backend returned 409 (job already running). */
  conflict: boolean;
  /** True when the backend returned 403 (admin required). */
  forbidden: boolean;
}

// ─── Jobs queue (Queue panel above the Jobs table) ──────────────────────

export interface QueueInFlight {
  run_id: string;
  /** ISO-8601 UTC timestamp with `Z` suffix, e.g. "2026-06-18T20:05:30.123Z". */
  started_at: string;
}

export interface QueueLastRun {
  /** ISO-8601 UTC timestamp, or null. */
  started_at: string | null;
  /** ISO-8601 UTC timestamp, or null when the run is still in flight / interrupted. */
  finished_at: string | null;
  /** "finished", "failed", "interrupted", "running", ... */
  status: string | null;
  /** Items indexed/purged; null when the job does not report a count. */
  indexed_count: number | null;
}

export interface QueueRow {
  job_type: string;
  in_flight: QueueInFlight | null;
  /** ISO-8601 UTC timestamp, or null when no cron is registered. */
  next_scheduled_at: string | null;
  /** Crontab expression from the registered APScheduler trigger, or null when no cron is registered. */
  cron: string | null;
  /** Most recent finished/failed run for this job_type, or null when no runs have been recorded. */
  last_run: QueueLastRun | null;
}

export interface QueueResponse {
  items: QueueRow[];
}

export async function getJobsQueue(signal?: AbortSignal): Promise<QueueResponse> {
  const r = await fetch(`${BASE}/jobs/queue`, { signal });
  if (!r.ok) throw new Error(`Failed to fetch jobs queue: ${r.status}`);
  return r.json();
}

export async function runJob(jobType: string): Promise<{ jobType: string; triggerId: string; status: string }> {
  const r = await fetch(`${BASE}/jobs/${encodeURIComponent(jobType)}/run`, { method: "POST" });
  if (r.ok) return r.json();
  let detail = `Failed to run job: ${r.status}`;
  try {
    const body = await r.json();
    if (body && typeof body.detail === "string") detail = body.detail;
  } catch {
    /* ignore */
  }
  const err = new Error(detail) as RunJobError;
  err.status = r.status;
  err.conflict = r.status === 409;
  err.forbidden = r.status === 403;
  throw err;
}

/** Format ISO timestamp to readable UTC string */
export function formatUtc(iso?: string | null): string {
  if (!iso) return "-";
  try {
    const d = new Date(iso);
    return d.toISOString().replace("T", " ").replace(/\.\d+Z$/, "");
  } catch {
    return iso;
  }
}

// ─── Configuration tab ──────────────────────────────────────────────────

export type ConfigType = "int" | "bool" | "cron";

export interface ConfigSetting {
  key: string;
  type: ConfigType;
  value: number | boolean | string | null;
  default?: number | boolean | string | null;
  min?: number;
  max?: number;
}

export interface ConfigSection {
  id: string;
  title: string;
  keys: string[];
}

export interface ConfigResponse {
  canEdit: boolean;
  authEnabled: boolean;
  sections: ConfigSection[];
  settings: ConfigSetting[];
}

export interface ConfigUpdate {
  key: string;
  value: number | boolean | string | null;
}

export interface ConfigSaveFailure {
  key: string;
  error: string;
}

export interface ConfigSaveResponse {
  applied: string[];
  failed: ConfigSaveFailure[];
  rescheduled: string[];
}

export class ConfigError extends Error {
  status: number;
  forbidden: boolean;
  failures: ConfigSaveFailure[];
  constructor(message: string, status: number, failures: ConfigSaveFailure[] = []) {
    super(message);
    this.status = status;
    this.forbidden = status === 403;
    this.failures = failures;
  }
}

export async function fetchConfig(signal?: AbortSignal): Promise<ConfigResponse> {
  const r = await fetch(`${BASE}/config`, { signal });
  if (!r.ok) throw new ConfigError(`Failed to fetch config: ${r.status}`, r.status);
  return r.json();
}

export async function saveConfig(updates: ConfigUpdate[]): Promise<ConfigSaveResponse> {
  const r = await fetch(`${BASE}/config`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ updates }),
  });
  // 200 = full success, 207 = partial (some failed but body still describes both)
  if (r.status === 200 || r.status === 207) return r.json();
  let detail = `Save failed: ${r.status}`;
  let failures: ConfigSaveFailure[] = [];
  try {
    const body = await r.json();
    if (Array.isArray(body?.failed)) failures = body.failed;
    if (typeof body?.detail === "string") detail = body.detail;
    else if (failures.length) detail = `${failures.length} setting(s) rejected by the server`;
  } catch {
    /* ignore */
  }
  throw new ConfigError(detail, r.status, failures);
}

export async function reloadConfig(): Promise<{ status: string }> {
  const r = await fetch(`${BASE}/config/reload`, { method: "POST" });
  if (!r.ok) {
    let detail = `Reload failed: ${r.status}`;
    try {
      const body = await r.json();
      if (typeof body?.detail === "string") detail = body.detail;
    } catch {
      /* ignore */
    }
    throw new ConfigError(detail, r.status);
  }
  return r.json();
}

export async function applyConfig(): Promise<{ status: string; note?: string; rescheduled?: string[] }> {
  const r = await fetch(`${BASE}/config/apply`, { method: "POST" });
  if (!r.ok) {
    let detail = `Apply failed: ${r.status}`;
    try {
      const body = await r.json();
      if (typeof body?.detail === "string") detail = body.detail;
    } catch {
      /* ignore */
    }
    throw new ConfigError(detail, r.status);
  }
  return r.json();
}

// ─── Operator panel: overview metrics + corpus curation (issue #611) ───────
//
// These endpoints are NOT prefixed with /api (they live at /panel/... per
// the shared conversations-panel-v1 contract) and require a validated
// delegated operator bearer token. This dashboard does not yet perform
// browser-side interactive sign-in (no MSAL/token acquisition wired in), so
// these calls only succeed once the surrounding deployment injects a
// forwarded Authorization header (for example via a reverse-auth proxy).
// Until then every card/table below renders the exact 401/403/503 the
// backend returns -- never a fabricated success-shaped placeholder.

export class PanelApiError extends Error {
  status: number;
  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

async function panelFetch(path: string, init?: RequestInit): Promise<Response> {
  const r = await fetch(path, init);
  if (!r.ok) {
    let detail = `Request failed: ${r.status}`;
    try {
      const body = await r.json();
      if (typeof body?.detail === "string") detail = body.detail;
    } catch {
      /* ignore */
    }
    throw new PanelApiError(detail, r.status);
  }
  return r;
}

export interface OverviewCounts {
  conversation_count: number | null;
  feedback_count: number | null;
  corpus_pending_count: number | null;
  corpus_decided_count: number | null;
}

export interface OverviewMetricsResponse {
  schema_version: 1;
  generated_at: string;
  correlation_id: string;
  counts: OverviewCounts;
}

export async function fetchOverviewMetrics(): Promise<OverviewMetricsResponse> {
  const r = await panelFetch("/panel/overview/metrics");
  return r.json();
}

export interface CorpusCurationItem {
  item_id: string;
  document_id: string;
  title: string;
  reason_code: string;
  submitted_at: string;
}

export interface CorpusCurationQueueResponse {
  items: CorpusCurationItem[];
  next_cursor: string | null;
}

export async function fetchCurationQueue(cursor?: string | null): Promise<CorpusCurationQueueResponse> {
  const q = cursor ? `?cursor=${encodeURIComponent(cursor)}` : "";
  const r = await panelFetch(`/panel/corpus-curation/queue${q}`);
  return r.json();
}

export type CurationDecision = "approve" | "reject" | "defer";

export interface CorpusCurationDecisionResponse {
  item_id: string;
  decision: CurationDecision;
  decided_at: string;
}

export async function postCurationDecision(
  itemId: string,
  decision: CurationDecision,
  note?: string,
): Promise<CorpusCurationDecisionResponse> {
  const r = await panelFetch(`/panel/corpus-curation/${encodeURIComponent(itemId)}/decision`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ decision, note: note || null }),
  });
  return r.json();
}
