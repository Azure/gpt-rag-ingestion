import { useCallback, useEffect, useRef, useState } from "react";
import {
  fetchJobs,
  formatUtc,
  getJobsQueue,
  runJob,
  type Identity,
  type JobRun,
  type QueueRow,
  type RunJobError,
} from "../lib/api";
import { StatusBadge } from "./StatusBadge";
import { Pagination } from "./Pagination";
import { SearchInput } from "./SearchInput";
import { SortHeader } from "./SortHeader";
import { DetailDialog } from "./DetailDialog";
import { Play, RefreshCw } from "lucide-react";

interface JobsTableProps {
  navigateRunId?: string | null;
  onNavigated?: () => void;
  identity: Identity;
}

type AlertKind = "success" | "error";
interface AlertState {
  kind: AlertKind;
  message: string;
}

const QUEUE_POLL_MS = 10_000;

/** Format an ISO-8601 UTC timestamp as a short "in 12 min" / "2 min ago" string. */
function formatRelative(iso: string | null | undefined, nowMs: number): string {
  if (!iso) return "-";
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return iso;
  const deltaSec = Math.round((t - nowMs) / 1000);
  const abs = Math.abs(deltaSec);
  const dir = deltaSec >= 0 ? "in " : "";
  const suffix = deltaSec >= 0 ? "" : " ago";
  if (abs < 60) return `${dir}${abs}s${suffix}`;
  if (abs < 3600) return `${dir}${Math.round(abs / 60)} min${suffix}`;
  if (abs < 86_400) return `${dir}${Math.round(abs / 3600)} h${suffix}`;
  return `${dir}${Math.round(abs / 86_400)} d${suffix}`;
}

export function JobsTable({ navigateRunId, onNavigated, identity }: JobsTableProps) {
  const [items, setItems] = useState<JobRun[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState("");
  const [sortField, setSortField] = useState("runStartedAt");
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc");
  const [indexerTypes, setIndexerTypes] = useState<string[]>([]);
  const [indexerType, setIndexerType] = useState("");
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<JobRun | null>(null);
  const [availableJobTypes, setAvailableJobTypes] = useState<string[]>([]);
  const [runningJobTypes, setRunningJobTypes] = useState<string[]>([]);
  const [triggering, setTriggering] = useState<string | null>(null);
  const [alert, setAlert] = useState<AlertState | null>(null);
  const [queue, setQueue] = useState<QueueRow[]>([]);
  const [nowMs, setNowMs] = useState<number>(() => Date.now());
  const abortRef = useRef<AbortController | null>(null);
  const queueAbortRef = useRef<AbortController | null>(null);

  const load = useCallback(async () => {
    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;
    setLoading(true);
    try {
      const res = await fetchJobs({ page, pageSize: 20, search, sortField, sortOrder, indexerType }, ctrl.signal);
      if (ctrl.signal.aborted) return;
      setItems(res.items);
      setTotal(res.total);
      if (res.indexerTypes) setIndexerTypes(res.indexerTypes);
      if (res.availableJobTypes) setAvailableJobTypes(res.availableJobTypes);
      if (res.runningJobTypes) setRunningJobTypes(res.runningJobTypes);
    } catch (err) {
      if ((err as Error).name === "AbortError") return;
      console.error(err);
    } finally {
      if (!ctrl.signal.aborted) setLoading(false);
    }
  }, [page, search, sortField, sortOrder, indexerType]);

  useEffect(() => {
    load();
    return () => { abortRef.current?.abort(); };
  }, [load]);

  // Poll the queue endpoint every 10s while this tab is mounted.
  useEffect(() => {
    let cancelled = false;
    const tick = async () => {
      queueAbortRef.current?.abort();
      const ctrl = new AbortController();
      queueAbortRef.current = ctrl;
      try {
        const res = await getJobsQueue(ctrl.signal);
        if (!cancelled) setQueue(res.items || []);
      } catch (err) {
        if ((err as Error).name === "AbortError") return;
        // Stay silent — a transient queue poll failure should not toast the UI.
      }
    };
    tick();
    const id = window.setInterval(tick, QUEUE_POLL_MS);
    const clockId = window.setInterval(() => setNowMs(Date.now()), 1000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
      window.clearInterval(clockId);
      queueAbortRef.current?.abort();
    };
  }, []);

  useEffect(() => {
    if (navigateRunId) {
      setSearch(navigateRunId);
      setPage(1);
      onNavigated?.();
    }
  }, [navigateRunId, onNavigated]);

  const handleSort = (field: string) => {
    if (field === sortField) {
      setSortOrder((o) => (o === "asc" ? "desc" : "asc"));
    } else {
      setSortField(field);
      setSortOrder("desc");
    }
    setPage(1);
  };

  const handleSearch = useCallback((v: string) => { setSearch(v); setPage(1); }, []);

  const canRun = !identity.authEnabled || identity.isAdmin;
  const disabledTooltip = !canRun ? "Admin role required" : undefined;

  const handleRun = async (jobType: string) => {
    if (!canRun) return;
    setTriggering(jobType);
    setAlert(null);
    try {
      await runJob(jobType);
      setAlert({ kind: "success", message: `Queued ${jobType}.` });
      // Refresh immediately; the backend already reports the job as running.
      load();
    } catch (err) {
      const e = err as RunJobError;
      setAlert({ kind: "error", message: e.message || `Failed to run ${jobType}` });
    } finally {
      setTriggering(null);
    }
  };

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-3">
        <SearchInput value={search} onChange={handleSearch} placeholder="Search jobs..." />
        <select
          value={indexerType}
          onChange={(e) => { setIndexerType(e.target.value); setPage(1); }}
          className="h-9 rounded-md border border-input bg-background px-3 text-sm"
        >
          <option value="">All types</option>
          {indexerTypes.map((t) => (
            <option key={t} value={t}>{t}</option>
          ))}
        </select>
        <div className="flex-1" />
        <button onClick={load} className="rounded-md p-2 hover:bg-accent" title="Refresh">
          <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
        </button>
      </div>

      {availableJobTypes.length > 0 && (
        <div
          className="flex flex-wrap items-center gap-2 rounded-lg border bg-muted/30 px-3 py-2"
          aria-label="Run jobs on demand"
        >
          <span className="text-xs font-medium text-muted-foreground">Run now:</span>
          {availableJobTypes.map((jt) => {
            const queueRow = queue.find((q) => q.job_type === jt);
            const isRunning = !!queueRow?.in_flight || runningJobTypes.includes(jt);
            const isPending = triggering === jt;
            const disabled = !canRun || isRunning || isPending;
            const tooltip = !canRun
              ? disabledTooltip
              : isRunning
                ? "Job already running"
                : `Trigger ${jt}`;
            return (
              <button
                key={jt}
                type="button"
                onClick={() => handleRun(jt)}
                disabled={disabled}
                title={tooltip}
                aria-label={tooltip}
                aria-disabled={disabled}
                className="inline-flex items-center gap-1.5 rounded-md border bg-background px-2.5 py-1 text-xs font-medium hover:bg-accent disabled:cursor-not-allowed disabled:opacity-50"
              >
                {isPending ? (
                  <RefreshCw className="h-3 w-3 animate-spin" />
                ) : (
                  <Play className="h-3 w-3" />
                )}
                {jt}
                {isRunning && <span className="text-[10px] text-muted-foreground">(running)</span>}
              </button>
            );
          })}
        </div>
      )}

      {queue.length > 0 && (
        <div className="rounded-lg border" aria-label="Job queue and schedule">
          <div className="border-b bg-muted/30 px-3 py-1.5 text-xs font-medium text-muted-foreground">
            Queue and schedule
          </div>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-xs text-muted-foreground">
                <th className="px-3 py-1.5 font-medium">Job</th>
                <th className="px-3 py-1.5 font-medium">In flight</th>
                <th className="px-3 py-1.5 font-medium">Next run</th>
                <th className="px-3 py-1.5 font-medium">Cron</th>
              </tr>
            </thead>
            <tbody>
              {queue.map((row) => {
                const inFlight = row.in_flight;
                const elapsedLabel = inFlight
                  ? formatRelative(inFlight.started_at, nowMs)
                  : null;
                const nextLabel = row.next_scheduled_at
                  ? formatRelative(row.next_scheduled_at, nowMs)
                  : "-";
                return (
                  <tr key={row.job_type} className="border-b last:border-b-0">
                    <td className="px-3 py-1.5 font-mono text-xs">{row.job_type}</td>
                    <td className="px-3 py-1.5 text-xs">
                      {inFlight ? (
                        <span title={`Started ${inFlight.started_at}`}>
                          <span className="font-mono">{inFlight.run_id}</span>
                          <span className="text-muted-foreground"> ({elapsedLabel})</span>
                        </span>
                      ) : (
                        <span className="text-muted-foreground">-</span>
                      )}
                    </td>
                    <td className="px-3 py-1.5 text-xs">
                      {row.next_scheduled_at ? (
                        <span title={row.next_scheduled_at}>{nextLabel}</span>
                      ) : (
                        <span className="text-muted-foreground">-</span>
                      )}
                    </td>
                    <td className="px-3 py-1.5 font-mono text-xs">
                      {row.cron ?? <span className="text-muted-foreground">-</span>}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {alert && (
        <div
          role="alert"
          className={`rounded-md border px-3 py-2 text-sm ${
            alert.kind === "error"
              ? "border-destructive/50 bg-destructive/10 text-destructive"
              : "border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
          }`}
        >
          {alert.message}
        </div>
      )}

      <div className="rounded-lg border">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b bg-muted/50 text-muted-foreground">
              <th className="px-4 py-2">
                <SortHeader label="Type" field="indexerType" currentField={sortField} currentOrder={sortOrder} onSort={handleSort} />
              </th>
              <th className="px-4 py-2">
                <SortHeader label="Run ID" field="runId" currentField={sortField} currentOrder={sortOrder} onSort={handleSort} />
              </th>
              <th className="px-4 py-2">
                <SortHeader label="Status" field="status" currentField={sortField} currentOrder={sortOrder} onSort={handleSort} />
              </th>
              <th className="px-4 py-2">
                <SortHeader label="Started (UTC)" field="runStartedAt" currentField={sortField} currentOrder={sortOrder} onSort={handleSort} />
              </th>
              <th className="px-4 py-2">
                <SortHeader label="Finished (UTC)" field="runFinishedAt" currentField={sortField} currentOrder={sortOrder} onSort={handleSort} />
              </th>
              <th className="px-4 py-2 text-right">Candidates</th>
              <th className="px-4 py-2 text-right">Skipped</th>
              <th className="px-4 py-2 text-right">Blocked</th>
              <th className="px-4 py-2 text-right">Indexed</th>
              <th className="px-4 py-2 text-right">Failed</th>
            </tr>
          </thead>
          <tbody key={`${page}-${indexerType}`}>
            {items.length === 0 && (
              <tr>
                <td colSpan={10} className="px-4 py-8 text-center text-muted-foreground">
                  {loading ? "Loading..." : "No job runs found."}
                </td>
              </tr>
            )}
            {items.map((job, i) => (
              <tr
                key={job._blobName ?? `${job.indexerType}-${job.runId}-${i}`}
                className="cursor-pointer border-b last:border-0 hover:bg-muted/30"
                onClick={() => setSelected(job)}
              >
                <td className="px-4 py-2 font-medium">{job.indexerType ?? "-"}</td>
                <td className="px-4 py-2 font-mono text-xs">{job.runId ?? "-"}</td>
                <td className="px-4 py-2"><StatusBadge status={job.status} /></td>
                <td className="px-4 py-2 text-xs">{formatUtc(job.runStartedAt)}</td>
                <td className="px-4 py-2 text-xs">{formatUtc(job.runFinishedAt)}</td>
                <td className="px-4 py-2 text-right">{job.candidates ?? 0}</td>
                <td className="px-4 py-2 text-right">{job.skippedNoChange ?? 0}</td>
                <td className="px-4 py-2 text-right">{job.skippedBlocked ?? 0}</td>
                <td className="px-4 py-2 text-right">{job.indexedItems ?? job.indexParentsPurged ?? 0}</td>
                <td className="px-4 py-2 text-right">{job.failed ?? 0}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <Pagination page={page} pageSize={20} total={total} onChange={setPage} />

      {selected && (
        <DetailDialog
          title={`Job: ${selected.runId ?? "unknown"}`}
          data={selected as Record<string, unknown>}
          onClose={() => setSelected(null)}
        />
      )}
    </div>
  );
}
