import { useCallback, useEffect, useRef, useState } from "react";
import {
  fetchJobs,
  getJobsQueue,
  runJob,
  type Identity,
  type QueueRow,
  type RunJobError,
} from "../lib/api";
import { Play, RefreshCw, X } from "lucide-react";

interface SchedulesTabProps {
  identity: Identity;
}

type AlertKind = "success" | "warning" | "error";
interface AlertState {
  id: number;
  kind: AlertKind;
  message: string;
}

// Queue panel polling cadences. After any Run-now click we accelerate to
// QUEUE_BURST_POLL_MS for QUEUE_BURST_DURATION_MS so the panel reflects
// the new in-flight state (and, once it finishes, the new last_run)
// within seconds rather than waiting up to a full 10s normal-poll cycle.
const QUEUE_POLL_MS = 10_000;
const QUEUE_BURST_POLL_MS = 1_000;
const QUEUE_BURST_DURATION_MS = 15_000;

// Auto-dismiss timeout for Run-now feedback toasts. 4s is long enough to
// read a short message ("Started blob_index.") but short enough that the
// toast does not linger when the operator triggers several jobs in a row.
// Exported so tests can reference the same constant.
export const ALERT_AUTO_DISMISS_MS = 4_000;

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

/** Render the "Last run" cell, e.g. "3s ago · finished · 0 indexed". */
function formatLastRun(lastRun: QueueRow["last_run"], nowMs: number): string {
  if (!lastRun) return "-";
  const when = lastRun.finished_at || lastRun.started_at;
  const rel = when ? formatRelative(when, nowMs) : "-";
  const parts: string[] = [rel];
  if (lastRun.status) parts.push(lastRun.status);
  if (typeof lastRun.indexed_count === "number") {
    parts.push(`${lastRun.indexed_count} indexed`);
  }
  return parts.join(" · ");
}

export function SchedulesTab({ identity }: SchedulesTabProps) {
  const [availableJobTypes, setAvailableJobTypes] = useState<string[]>([]);
  const [runningJobTypes, setRunningJobTypes] = useState<string[]>([]);
  const [triggering, setTriggering] = useState<string | null>(null);
  const [alert, setAlert] = useState<AlertState | null>(null);
  const [queue, setQueue] = useState<QueueRow[]>([]);
  const [nowMs, setNowMs] = useState<number>(() => Date.now());
  const queueAbortRef = useRef<AbortController | null>(null);
  const queueTickRef = useRef<() => Promise<void>>(async () => {});
  const queueIntervalRef = useRef<number | null>(null);
  const queueBurstTimeoutRef = useRef<number | null>(null);
  // Auto-dismiss timer. Each new alert resets the timer, so a fresh
  // trigger does not extend an older toast. Cleared on unmount.
  const alertTimeoutRef = useRef<number | null>(null);
  const alertSeqRef = useRef(0);

  // Pull the list of available job types and any currently running ones
  // from /api/jobs/runs (which is the same source the Jobs tab uses).
  // We only need the metadata, so request the smallest page possible.
  const loadJobTypes = useCallback(async () => {
    try {
      const res = await fetchJobs({
        page: 1,
        pageSize: 1,
        search: "",
        sortField: "runStartedAt",
        sortOrder: "desc",
        indexerType: "",
      });
      if (res.availableJobTypes) setAvailableJobTypes(res.availableJobTypes);
      if (res.runningJobTypes) setRunningJobTypes(res.runningJobTypes);
    } catch (err) {
      // Stay silent — the queue panel still works without this metadata,
      // and the Jobs tab will surface any persistent /api/jobs failures.
      console.error(err);
    }
  }, []);

  useEffect(() => {
    void loadJobTypes();
  }, [loadJobTypes]);

  // Poll the queue endpoint while this tab is mounted.
  useEffect(() => {
    let cancelled = false;
    queueTickRef.current = async () => {
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
    void queueTickRef.current();
    queueIntervalRef.current = window.setInterval(() => {
      void queueTickRef.current();
    }, QUEUE_POLL_MS);
    const clockId = window.setInterval(() => setNowMs(Date.now()), 1000);
    return () => {
      cancelled = true;
      if (queueIntervalRef.current !== null) {
        window.clearInterval(queueIntervalRef.current);
        queueIntervalRef.current = null;
      }
      if (queueBurstTimeoutRef.current !== null) {
        window.clearTimeout(queueBurstTimeoutRef.current);
        queueBurstTimeoutRef.current = null;
      }
      window.clearInterval(clockId);
      queueAbortRef.current?.abort();
    };
  }, []);

  // Clear the auto-dismiss timer on unmount so a pending toast does not
  // try to setState on an unmounted component (e.g. operator switches tabs
  // right after clicking Run now).
  useEffect(() => {
    return () => {
      if (alertTimeoutRef.current !== null) {
        window.clearTimeout(alertTimeoutRef.current);
        alertTimeoutRef.current = null;
      }
    };
  }, []);

  /**
   * Switch the queue poll loop to 1s for 15s, then back to 10s. Resets on
   * every call (does not stack). Cleared on unmount by the polling effect.
   */
  const burstQueuePolling = useCallback(() => {
    if (queueIntervalRef.current !== null) {
      window.clearInterval(queueIntervalRef.current);
    }
    if (queueBurstTimeoutRef.current !== null) {
      window.clearTimeout(queueBurstTimeoutRef.current);
    }
    queueIntervalRef.current = window.setInterval(() => {
      void queueTickRef.current();
    }, QUEUE_BURST_POLL_MS);
    queueBurstTimeoutRef.current = window.setTimeout(() => {
      if (queueIntervalRef.current !== null) {
        window.clearInterval(queueIntervalRef.current);
      }
      queueIntervalRef.current = window.setInterval(() => {
        void queueTickRef.current();
      }, QUEUE_POLL_MS);
      queueBurstTimeoutRef.current = null;
    }, QUEUE_BURST_DURATION_MS);
  }, []);

  /**
   * Show a toast and schedule it to disappear on its own. A fresh call
   * cancels any previous pending timer, so consecutive triggers do not
   * extend an older toast — each toast gets its own ALERT_AUTO_DISMISS_MS
   * window starting from when it was raised.
   */
  const raiseAlert = useCallback((next: Omit<AlertState, "id">) => {
    alertSeqRef.current += 1;
    const id = alertSeqRef.current;
    setAlert({ id, ...next });
    if (alertTimeoutRef.current !== null) {
      window.clearTimeout(alertTimeoutRef.current);
    }
    alertTimeoutRef.current = window.setTimeout(() => {
      // Only clear if this is still the latest toast — a newer raiseAlert
      // would have replaced both the state and the ref.
      setAlert((current) => (current && current.id === id ? null : current));
      alertTimeoutRef.current = null;
    }, ALERT_AUTO_DISMISS_MS);
  }, []);

  const dismissAlert = useCallback(() => {
    if (alertTimeoutRef.current !== null) {
      window.clearTimeout(alertTimeoutRef.current);
      alertTimeoutRef.current = null;
    }
    setAlert(null);
  }, []);

  const canRun = !identity.authEnabled || identity.isAdmin;
  const disabledTooltip = !canRun ? "Admin role required" : undefined;

  const handleRun = async (jobType: string) => {
    if (!canRun) return;
    setTriggering(jobType);
    // Burst-poll the queue regardless of the request outcome — even a 409
    // means the panel state on the operator's screen is stale and should
    // refresh quickly.
    burstQueuePolling();
    try {
      await runJob(jobType);
      // APScheduler fires immediately; "Started" matches what operators
      // actually observe (rows appear in the runs table within ~1s).
      raiseAlert({ kind: "success", message: `Started ${jobType}.` });
      // Refresh job-type metadata so the running indicator updates.
      void loadJobTypes();
    } catch (err) {
      const e = err as RunJobError;
      if (e.conflict) {
        raiseAlert({ kind: "warning", message: `${jobType} is already running.` });
      } else {
        raiseAlert({ kind: "error", message: e.message || `Failed to run ${jobType}` });
      }
    } finally {
      setTriggering(null);
    }
  };

  return (
    <div className="space-y-3">
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

      {alert && (
        <div
          role="alert"
          data-testid="schedules-alert"
          className={`flex items-start gap-2 rounded-md border px-3 py-2 text-sm ${
            alert.kind === "error"
              ? "border-destructive/50 bg-destructive/10 text-destructive"
              : alert.kind === "warning"
                ? "border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-300"
                : "border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
          }`}
        >
          <span className="flex-1">{alert.message}</span>
          <button
            type="button"
            onClick={dismissAlert}
            aria-label="Dismiss notification"
            className="rounded p-0.5 hover:bg-black/5 dark:hover:bg-white/10"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      )}

      <div className="rounded-lg border" aria-label="Job queue and schedule">
        <div className="border-b bg-muted/30 px-3 py-1.5 text-xs font-medium text-muted-foreground">
          Queue and schedule
        </div>
        {queue.length === 0 ? (
          <div className="px-3 py-6 text-center text-sm text-muted-foreground">
            No scheduled jobs.
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-xs text-muted-foreground">
                <th className="px-3 py-1.5 font-medium">Job</th>
                <th className="px-3 py-1.5 font-medium">In flight</th>
                <th className="px-3 py-1.5 font-medium">Last run</th>
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
                const lastRunLabel = formatLastRun(row.last_run, nowMs);
                const lastRunTitle = row.last_run?.finished_at
                  || row.last_run?.started_at
                  || undefined;
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
                      {row.last_run ? (
                        <span title={lastRunTitle}>{lastRunLabel}</span>
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
        )}
      </div>
    </div>
  );
}
