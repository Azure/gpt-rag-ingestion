import { useCallback, useEffect, useState } from "react";
import { AlertTriangle, CheckCircle2, ClipboardList, Lock, RefreshCw, ShieldOff, XCircle } from "lucide-react";
import {
  fetchCurationQueue,
  postCurationDecision,
  PanelApiError,
  type CorpusCurationItem,
  type CurationDecision,
} from "../lib/api";

export function CurationTab() {
  const [items, setItems] = useState<CorpusCurationItem[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [error, setError] = useState<PanelApiError | Error | null>(null);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [notes, setNotes] = useState<Record<string, string>>({});
  const [pendingDecision, setPendingDecision] = useState<string | null>(null);
  const [decided, setDecided] = useState<Record<string, CurationDecision>>({});

  const loadFirstPage = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetchCurationQueue(null);
      setItems(res.items);
      setCursor(res.next_cursor);
    } catch (err) {
      setError(err as Error);
      setItems([]);
      setCursor(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadFirstPage();
  }, [loadFirstPage]);

  const loadMore = useCallback(async () => {
    if (!cursor) return;
    setLoadingMore(true);
    try {
      const res = await fetchCurationQueue(cursor);
      setItems((prev) => [...prev, ...res.items]);
      setCursor(res.next_cursor);
    } catch (err) {
      setError(err as Error);
    } finally {
      setLoadingMore(false);
    }
  }, [cursor]);

  const decide = useCallback(
    async (itemId: string, decision: CurationDecision) => {
      setPendingDecision(itemId + decision);
      try {
        await postCurationDecision(itemId, decision, notes[itemId]);
        setDecided((prev) => ({ ...prev, [itemId]: decision }));
      } catch (err) {
        setError(err as Error);
      } finally {
        setPendingDecision(null);
      }
    },
    [notes],
  );

  if (error instanceof PanelApiError && error.status === 503) {
    return (
      <div className="flex items-center gap-2 rounded-md border border-dashed p-6 text-sm text-muted-foreground">
        <ShieldOff className="h-4 w-4" />
        Corpus curation is disabled ({error.message}).
      </div>
    );
  }

  if (error instanceof PanelApiError && (error.status === 401 || error.status === 403)) {
    return (
      <div className="flex items-center gap-2 rounded-md border border-dashed p-6 text-sm text-muted-foreground">
        <Lock className="h-4 w-4" />
        Sign in with an operator account to curate the corpus queue ({error.message}).
      </div>
    );
  }

  return (
    <div>
      <div className="mb-4 flex items-center justify-between">
        <h2 className="flex items-center gap-2 text-lg font-semibold">
          <ClipboardList className="h-4 w-4" /> Corpus curation
        </h2>
        <button
          onClick={loadFirstPage}
          disabled={loading}
          className="flex items-center gap-1 rounded-md border px-3 py-1.5 text-sm hover:bg-accent disabled:opacity-50"
        >
          <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </button>
      </div>

      {error && !(error instanceof PanelApiError && [401, 403, 503].includes(error.status)) && (
        <div className="mb-4 flex items-center gap-2 rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-800 dark:border-red-900 dark:bg-red-950 dark:text-red-200">
          <AlertTriangle className="h-4 w-4" />
          {error.message}
        </div>
      )}

      {!loading && items.length === 0 && !error && (
        <div className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
          No documents are awaiting curation.
        </div>
      )}

      <div className="space-y-3">
        {items.map((item) => {
          const outcome = decided[item.item_id];
          return (
            <div key={item.item_id} className="rounded-lg border p-4">
              <div className="flex flex-wrap items-start justify-between gap-2">
                <div>
                  <div className="font-medium">{item.title}</div>
                  <div className="text-xs text-muted-foreground">{item.document_id}</div>
                  <div className="mt-1 text-xs text-muted-foreground">
                    reason: {item.reason_code} · submitted: {item.submitted_at}
                  </div>
                </div>
                {outcome ? (
                  <span className="inline-flex items-center gap-1 rounded-full bg-green-100 px-2.5 py-0.5 text-xs font-medium text-green-800 dark:bg-green-900 dark:text-green-200">
                    <CheckCircle2 className="h-3 w-3" /> {outcome}
                  </span>
                ) : (
                  <div className="flex items-center gap-2">
                    <input
                      type="text"
                      placeholder="Optional note"
                      value={notes[item.item_id] ?? ""}
                      onChange={(e) =>
                        setNotes((prev) => ({ ...prev, [item.item_id]: e.target.value }))
                      }
                      className="w-40 rounded-md border px-2 py-1 text-xs"
                      maxLength={2000}
                    />
                    <button
                      onClick={() => decide(item.item_id, "approve")}
                      disabled={pendingDecision === item.item_id + "approve"}
                      className="rounded-md bg-green-600 px-2 py-1 text-xs font-medium text-white hover:bg-green-700 disabled:opacity-50"
                    >
                      Approve
                    </button>
                    <button
                      onClick={() => decide(item.item_id, "reject")}
                      disabled={pendingDecision === item.item_id + "reject"}
                      className="rounded-md bg-red-600 px-2 py-1 text-xs font-medium text-white hover:bg-red-700 disabled:opacity-50"
                    >
                      <XCircle className="inline h-3 w-3" /> Reject
                    </button>
                    <button
                      onClick={() => decide(item.item_id, "defer")}
                      disabled={pendingDecision === item.item_id + "defer"}
                      className="rounded-md border px-2 py-1 text-xs font-medium hover:bg-accent disabled:opacity-50"
                    >
                      Defer
                    </button>
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {cursor && (
        <div className="mt-4 flex justify-center">
          <button
            onClick={loadMore}
            disabled={loadingMore}
            className="rounded-md border px-4 py-1.5 text-sm hover:bg-accent disabled:opacity-50"
          >
            {loadingMore ? "Loading…" : "Load more"}
          </button>
        </div>
      )}
    </div>
  );
}
