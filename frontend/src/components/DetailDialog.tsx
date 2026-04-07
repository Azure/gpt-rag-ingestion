import { useState } from "react";
import { ChevronDown, ChevronRight, X } from "lucide-react";
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

export function DetailDialog({ title, data, onClose }: DetailDialogProps) {
  const [expandedRun, setExpandedRun] = useState<number | null>(null);

  if (!data) return null;

  const runHistory = Array.isArray(data.runHistory) ? (data.runHistory as RunHistoryEntry[]) : [];
  const hasRunHistory = runHistory.length > 0;

  // Filter out internal fields, runHistory (shown separately), and error when runHistory exists
  const entries = Object.entries(data).filter(
    ([k]) => !k.startsWith("_") && k !== "runHistory" && k !== "itemsDiscovered" && (k !== "error" || !hasRunHistory)
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
                        <td className="px-2 py-1.5 text-right">{entry.chunks ?? "-"}</td>
                      </tr>
                      {expandedRun === i && entry.error && (
                        <tr key={`${i}-error`}>
                          <td colSpan={6} className="bg-destructive/5 px-4 py-2">
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
