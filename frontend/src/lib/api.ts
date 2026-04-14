const BASE = "/api";

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  pageSize: number;
  indexerTypes?: string[];
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
