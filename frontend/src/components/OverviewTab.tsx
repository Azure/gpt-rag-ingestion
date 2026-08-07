import { useCallback, useEffect, useState } from "react";
import { AlertTriangle, BarChart3, Lock, RefreshCw, ShieldOff } from "lucide-react";
import { fetchOverviewMetrics, PanelApiError, type OverviewMetricsResponse } from "../lib/api";

function formatCount(value: number | null): string {
  if (value === null) return "suppressed";
  return value.toLocaleString();
}

export function OverviewTab() {
  const [data, setData] = useState<OverviewMetricsResponse | null>(null);
  const [error, setError] = useState<PanelApiError | Error | null>(null);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetchOverviewMetrics();
      setData(res);
    } catch (err) {
      setError(err as Error);
      setData(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  if (error instanceof PanelApiError && error.status === 503) {
    return (
      <div className="flex items-center gap-2 rounded-md border border-dashed p-6 text-sm text-muted-foreground">
        <ShieldOff className="h-4 w-4" />
        Operator overview is disabled ({error.message}).
      </div>
    );
  }

  if (error instanceof PanelApiError && (error.status === 401 || error.status === 403)) {
    return (
      <div className="flex items-center gap-2 rounded-md border border-dashed p-6 text-sm text-muted-foreground">
        <Lock className="h-4 w-4" />
        Sign in with an operator account to view overview metrics ({error.message}).
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center gap-2 rounded-md border border-red-200 bg-red-50 p-6 text-sm text-red-800 dark:border-red-900 dark:bg-red-950 dark:text-red-200">
        <AlertTriangle className="h-4 w-4" />
        {error.message}
      </div>
    );
  }

  const cards: Array<{ label: string; value: number | null }> = data
    ? [
        { label: "Conversations", value: data.counts.conversation_count },
        { label: "Feedback", value: data.counts.feedback_count },
        { label: "Corpus pending", value: data.counts.corpus_pending_count },
        { label: "Corpus decided", value: data.counts.corpus_decided_count },
      ]
    : [];

  return (
    <div>
      <div className="mb-4 flex items-center justify-between">
        <h2 className="flex items-center gap-2 text-lg font-semibold">
          <BarChart3 className="h-4 w-4" /> Overview
        </h2>
        <button
          onClick={load}
          disabled={loading}
          className="flex items-center gap-1 rounded-md border px-3 py-1.5 text-sm hover:bg-accent disabled:opacity-50"
        >
          <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </button>
      </div>

      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        {cards.map((card) => (
          <div key={card.label} className="rounded-lg border p-4">
            <div className="text-xs uppercase tracking-wide text-muted-foreground">{card.label}</div>
            <div className="mt-1 text-2xl font-bold">{formatCount(card.value)}</div>
          </div>
        ))}
        {!data && !loading && !error && (
          <div className="col-span-full text-sm text-muted-foreground">No data.</div>
        )}
      </div>

      {data && (
        <p className="mt-4 text-xs text-muted-foreground">
          Aggregate counts only; buckets below the configured cardinality threshold are shown as
          "suppressed" rather than a small exact count. Generated at {data.generated_at}.
        </p>
      )}
    </div>
  );
}
