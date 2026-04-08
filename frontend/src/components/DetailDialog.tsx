import { useState } from "react";
import { ChevronDown, ChevronRight, X, Clock, DollarSign } from "lucide-react";
import { formatUtc } from "../lib/api";
import { StatusBadge } from "./StatusBadge";

interface DetailDialogProps {
  title: string;
  data: Record<string, unknown> | null;
  onClose: () => void;
}

interface RunHistoryEntry {
  runId: string;
  status: string;
  startedAt?: string;
  finishedAt?: string;
  chunks?: number;
  error?: string;
  timings?: TimingsData;
}

const KEY_LABELS: Record<string, string> = {
  processingAttempts: "retries",
};

/** Convert camelCase to spaced label, e.g. "skippedNoChange" → "skipped No Change" */
function humanize(key: string): string {
  if (KEY_LABELS[key]) return KEY_LABELS[key];
  return key.replace(/([a-z])([A-Z])/g, "$1 $2").replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2");
}

function isTimestamp(key: string, val: unknown): boolean {
  if (typeof val !== "string") return false;
  return /At$|^last_modified$|^startedAt$|^finishedAt$/.test(key) && /\d{4}-\d{2}-\d{2}T/.test(val);
}

function formatValue(key: string, val: unknown): string {
  if (val === null || val === undefined) return "-";
  if (isTimestamp(key, val)) return formatUtc(val as string);
  if (typeof val === "object") return JSON.stringify(val, null, 2);
  if (typeof val === "boolean") return val ? "Yes" : "No";
  return String(val);
}

function navigateToJob(runId: string, onClose: () => void) {
  onClose();
  window.dispatchEvent(new CustomEvent("navigate-to-job", { detail: { runId } }));
}

/** Labels for timing keys in display order */
const TIMING_LABELS: Record<string, string> = {
  downloadSec: "Download",
  analysisSec: "Analysis",
  chunkEmbedSec: "Chunking + embeddings",
  indexUploadSec: "Index upload",
  overheadSec: "Processing overhead",
  processingSec: "Processing (staged)",
};
const TIMING_COLORS: Record<string, string> = {
  downloadSec: "bg-blue-500",
  analysisSec: "bg-amber-500",
  chunkEmbedSec: "bg-emerald-500",
  indexUploadSec: "bg-violet-500",
  overheadSec: "bg-gray-400",
  processingSec: "bg-emerald-500",
};
/** Keys that are sub-items (not shown in bar or main legend) */
const SUB_ITEM_KEYS = new Set(["retryWaitSec", "retryCount"]);
const SUB_ITEM_PARENT: Record<string, string> = { retryWaitSec: "chunkEmbedSec", retryCount: "chunkEmbedSec" };
function formatDuration(secs: number): string {
  if (secs < 60) return `${secs.toFixed(1)}s`;
  const m = Math.floor(secs / 60);
  const s = secs % 60;
  return `${m}m ${s.toFixed(0)}s`;
}

interface TimingsData {
  [key: string]: number;
}

function TimingsBar({ timings }: { timings: TimingsData }) {
  const totalSec = timings.totalSec ?? 0;
  const entries = Object.entries(timings).filter(
    ([k]) => k !== "totalSec" && !SUB_ITEM_KEYS.has(k) && typeof timings[k] === "number"
  );
  if (entries.length === 0) return null;

  // Build sub-items grouped by parent key
  const subItemsByParent: Record<string, [string, number][]> = {};
  for (const [k, v] of Object.entries(timings)) {
    if (SUB_ITEM_KEYS.has(k) && typeof v === "number" && v > 0) {
      const parent = SUB_ITEM_PARENT[k] ?? "";
      (subItemsByParent[parent] ??= []).push([k, v]);
    }
  }

  // Compose rate-limit sub-item label
  const retryWait = timings.retryWaitSec ?? 0;
  const retryCount = timings.retryCount ?? 0;
  const hasRetryInfo = retryWait > 0 || retryCount > 0;

  return (
    <div className="space-y-2">
      {/* Stacked bar */}
      {totalSec > 0 && (
        <div className="flex h-4 w-full overflow-hidden rounded-full bg-muted">
          {entries.map(([key, val]) => {
            const pct = totalSec > 0 ? (val / totalSec) * 100 : 0;
            if (pct < 0.5) return null;
            return (
              <div
                key={key}
                className={`${TIMING_COLORS[key] ?? "bg-gray-400"} h-full`}
                style={{ width: `${pct}%` }}
                title={`${TIMING_LABELS[key] ?? key}: ${formatDuration(val)} (${pct.toFixed(0)}%)`}
              />
            );
          })}
        </div>
      )}
      {/* Legend */}
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
        {entries.flatMap(([key, val]) => [
          <div key={key} className="flex items-center gap-1.5">
            <span className={`inline-block h-2.5 w-2.5 rounded-sm ${TIMING_COLORS[key] ?? "bg-gray-400"}`} />
            <span className="text-muted-foreground">{TIMING_LABELS[key] ?? key}</span>
            <span className="ml-auto font-mono">{formatDuration(val)}</span>
          </div>,
          ...(key === "chunkEmbedSec" && hasRetryInfo ? [
            <div key="retryInfo" className="flex items-center gap-1.5 col-span-2 pl-5">
              <span className="text-muted-foreground/60">\u21b3</span>
              <span className="text-muted-foreground/80 italic">
                {retryCount > 0 ? `${retryCount}\u00d7` : ""} 429 Rate-limit wait{retryWait > 0 ? ` (${formatDuration(retryWait)})` : ""}
              </span>
            </div>,
          ] : []),
        ])}
        {totalSec > 0 && (
          <div className="col-span-2 flex items-center gap-1.5 border-t pt-1 font-medium">
            <Clock className="h-3 w-3 text-muted-foreground" />
            <span>Total</span>
            <span className="ml-auto font-mono">{formatDuration(totalSec)}</span>
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Cost Estimate helpers ─────────────────────────────────────────────── */

const ANALYSIS_SVC_LABELS: Record<string, string> = {
  content_understanding: "Content Understanding",
  document_intelligence: "Document Intelligence",
};

function formatUSD(val: number): string {
  return `$${val.toFixed(4)}`;
}

function CostEstimateSection({ costEstimate }: { costEstimate: Record<string, unknown> }) {
  const svc = String(costEstimate.analysisService ?? "");
  const pages = Number(costEstimate.pagesAnalyzed ?? 0);
  const analysisCost = Number(costEstimate.analysisCost ?? 0);
  const embedCalls = Number(costEstimate.embeddingCalls ?? 0);
  const embedTokens = Number(costEstimate.embeddingTokens ?? 0);
  const embedCost = Number(costEstimate.embeddingCost ?? 0);
  const complCalls = Number(costEstimate.completionCalls ?? 0);
  const complIn = Number(costEstimate.completionInputTokens ?? 0);
  const complOut = Number(costEstimate.completionOutputTokens ?? 0);
  const complCost = Number(costEstimate.completionCost ?? 0);
  const totalCost = Number(costEstimate.totalCost ?? 0);

  return (
    <div className="mt-4 border-t pt-4">
      <h3 className="mb-2 text-sm font-semibold flex items-center gap-1.5">
        <DollarSign className="h-4 w-4" /> Cost Estimate
      </h3>
      <div className="space-y-2 text-xs">
        {/* Analysis */}
        <div className="flex items-baseline justify-between">
          <span className="text-muted-foreground">
            {(ANALYSIS_SVC_LABELS[svc] ?? svc) || "Analysis"}{" "}
            <span className="font-mono">({pages} {pages === 1 ? "page" : "pages"})</span>
          </span>
          <span className="font-mono">{formatUSD(analysisCost)}</span>
        </div>

        {/* Embeddings */}
        <div className="flex items-baseline justify-between">
          <span className="text-muted-foreground">
            Azure OpenAI Embeddings{" "}
            <span className="font-mono">({embedCalls} {embedCalls === 1 ? "call" : "calls"}, {embedTokens.toLocaleString()} tokens)</span>
          </span>
          <span className="font-mono">{formatUSD(embedCost)}</span>
        </div>

        {/* Completions (optional) */}
        {complCalls > 0 && (
          <div className="flex items-baseline justify-between">
            <span className="text-muted-foreground">
              Azure OpenAI Completions{" "}
              <span className="font-mono">({complCalls} {complCalls === 1 ? "call" : "calls"}, {complIn.toLocaleString()}↑ {complOut.toLocaleString()}↓ tokens)</span>
            </span>
            <span className="font-mono">{formatUSD(complCost)}</span>
          </div>
        )}

        {/* Total */}
        <div className="flex items-baseline justify-between border-t pt-1 font-semibold">
          <span>Estimated Total</span>
          <span className="font-mono">{formatUSD(totalCost)}</span>
        </div>

        {/* Disclaimer */}
        <p className="text-[10px] text-muted-foreground/60 italic">
          Estimates based on list pricing (Apr 2026). Actual costs may vary.
        </p>
      </div>
    </div>
  );
}

export function DetailDialog({ title, data, onClose }: DetailDialogProps) {
  const [expandedRun, setExpandedRun] = useState<number | null>(null);

  if (!data) return null;

  const runHistory = Array.isArray(data.runHistory) ? (data.runHistory as RunHistoryEntry[]) : [];
  const hasRunHistory = runHistory.length > 0;
  const timings = (data.timings && typeof data.timings === "object" && !Array.isArray(data.timings))
    ? (data.timings as TimingsData)
    : null;
  const costEstimate = (data.costEstimate && typeof data.costEstimate === "object" && !Array.isArray(data.costEstimate))
    ? (data.costEstimate as Record<string, unknown>)
    : null;

  // Filter out internal fields, runHistory, timings, costEstimate (shown separately), and error when runHistory exists
  const entries = Object.entries(data).filter(
    ([k]) => !k.startsWith("_") && k !== "runHistory" && k !== "itemsDiscovered" && k !== "timings" && k !== "costEstimate" && (k !== "error" || !hasRunHistory)
  );

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50" onClick={onClose}>
      <div
        className="relative mx-4 max-h-[80vh] w-full max-w-2xl overflow-auto rounded-lg border bg-card p-6 shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <button
          onClick={onClose}
          className="absolute right-3 top-3 rounded-md p-1 hover:bg-accent"
          aria-label="Close"
        >
          <X className="h-5 w-5" />
        </button>

        <h2 className="mb-4 text-lg font-semibold">{title}</h2>

        <table className="w-full text-sm">
          <tbody>
            {entries.map(([key, val]) => (
              <tr key={key} className="border-b last:border-0">
                <td className="whitespace-nowrap px-3 py-2 font-medium text-muted-foreground">
                  {humanize(key)}
                </td>
                <td className="px-3 py-2 break-all font-mono text-xs">
                  {key === "runId" && val ? (
                    <button
                      className="text-primary hover:underline"
                      onClick={() => navigateToJob(String(val), onClose)}
                    >
                      {formatValue(key, val)}
                    </button>
                  ) : typeof val === "object" && val !== null && !Array.isArray(val) ? (
                    <pre className="whitespace-pre-wrap">{formatValue(key, val)}</pre>
                  ) : (
                    formatValue(key, val)
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>

        {timings && (
          <div className="mt-4 border-t pt-4">
            <h3 className="mb-2 text-sm font-semibold flex items-center gap-1.5">
              <Clock className="h-4 w-4" /> Processing Timings
            </h3>
            <TimingsBar timings={timings} />
          </div>
        )}

        {costEstimate && (
          <CostEstimateSection costEstimate={costEstimate} />
        )}

        {hasRunHistory && (
          <div className="mt-4 border-t pt-4">
            <h3 className="mb-2 text-sm font-semibold">Run History</h3>
            <div className="rounded-md border">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b bg-muted/50 text-muted-foreground">
                    <th className="w-6 px-1 py-1.5" />
                    <th className="px-2 py-1.5 text-left">Run ID</th>
                    <th className="px-2 py-1.5 text-left">Status</th>
                    <th className="px-2 py-1.5 text-left">Started (UTC)</th>
                    <th className="px-2 py-1.5 text-left">Finished (UTC)</th>
                    <th className="px-2 py-1.5 text-right">Duration</th>
                    <th className="px-2 py-1.5 text-right">Chunks</th>
                  </tr>
                </thead>
                <tbody>
                  {runHistory.map((entry, i) => (
                    <>
                      <tr
                        key={i}
                        className={`border-b last:border-0 ${entry.error ? "cursor-pointer hover:bg-muted/30" : ""}`}
                        onClick={() => entry.error && setExpandedRun(expandedRun === i ? null : i)}
                      >
                        <td className="px-1 py-1.5 text-center">
                          {entry.error ? (
                            expandedRun === i
                              ? <ChevronDown className="inline h-3 w-3 text-muted-foreground" />
                              : <ChevronRight className="inline h-3 w-3 text-muted-foreground" />
                          ) : null}
                        </td>
                        <td className="px-2 py-1.5">
                          <button
                            className="font-mono text-primary hover:underline"
                            onClick={(e) => { e.stopPropagation(); navigateToJob(entry.runId, onClose); }}
                          >
                            {entry.runId}
                          </button>
                        </td>
                        <td className="px-2 py-1.5"><StatusBadge status={entry.status} /></td>
                        <td className="px-2 py-1.5">{formatUtc(entry.startedAt)}</td>
                        <td className="px-2 py-1.5">{formatUtc(entry.finishedAt)}</td>
                        <td className="px-2 py-1.5 text-right font-mono">
                          {entry.timings?.totalSec != null ? formatDuration(entry.timings.totalSec) : "-"}
                        </td>
                        <td className="px-2 py-1.5 text-right">{entry.chunks ?? "-"}</td>
                      </tr>
                      {expandedRun === i && entry.error && (
                        <tr key={`${i}-error`}>
                          <td colSpan={7} className="bg-destructive/5 px-4 py-2">
                            <pre className="whitespace-pre-wrap text-xs text-destructive/90">
                              {entry.error}
                            </pre>
                          </td>
                        </tr>
                      )}
                    </>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
