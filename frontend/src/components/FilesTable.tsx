import { useCallback, useEffect, useRef, useState } from "react";
import { fetchFiles, unblockFile, formatUtc, type FileLog } from "../lib/api";
import { StatusBadge } from "./StatusBadge";
import { Pagination } from "./Pagination";
import { SearchInput } from "./SearchInput";
import { SortHeader } from "./SortHeader";
import { DetailDialog } from "./DetailDialog";
import { RefreshCw, ShieldOff } from "lucide-react";

export function FilesTable() {
  const [items, setItems] = useState<FileLog[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState("");
  const [sortField, setSortField] = useState("startedAt");
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc");
  const [blockedFilter, setBlockedFilter] = useState<boolean | undefined>(undefined);
  const [indexerTypes, setIndexerTypes] = useState<string[]>([]);
  const [indexerType, setIndexerType] = useState("");
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState<FileLog | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const load = useCallback(async () => {
    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;
    setLoading(true);
    try {
      const res = await fetchFiles({
        page, pageSize: 20, search, sortField, sortOrder,
        blocked: blockedFilter, indexerType,
      }, ctrl.signal);
      if (ctrl.signal.aborted) return;
      setItems(res.items);
      setTotal(res.total);
      if (res.indexerTypes) setIndexerTypes(res.indexerTypes);
    } catch (err) {
      if ((err as Error).name === "AbortError") return;
      console.error(err);
    } finally {
      if (!ctrl.signal.aborted) setLoading(false);
    }
  }, [page, search, sortField, sortOrder, blockedFilter, indexerType]);

  useEffect(() => {
    load();
    return () => { abortRef.current?.abort(); };
  }, [load]);

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

  const handleUnblock = async (e: React.MouseEvent, blobName: string) => {
    e.stopPropagation();
    if (!confirm("Unblock this file? It will be reprocessed on the next run.")) return;
    try {
      await unblockFile(blobName);
      await load();
    } catch (err) {
      console.error(err);
      alert("Failed to unblock file.");
    }
  };

  /** Resolve display name from available fields */
  const displayName = (f: FileLog) =>
    f.fileName || f.blob || f.parent_id?.split("/").pop() || "-";

  return (
    <div className="space-y-3">
      <div className="flex items-center gap-3 flex-wrap">
        <SearchInput value={search} onChange={handleSearch} placeholder="Search files..." />
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
        <select
          value={blockedFilter === undefined ? "" : blockedFilter ? "true" : "false"}
          onChange={(e) => {
            const v = e.target.value;
            setBlockedFilter(v === "" ? undefined : v === "true");
            setPage(1);
          }}
          className="h-9 rounded-md border border-input bg-background px-3 text-sm"
        >
          <option value="">All files</option>
          <option value="true">Blocked only</option>
          <option value="false">Not blocked</option>
        </select>
        <div className="flex-1" />
        <button onClick={load} className="rounded-md p-2 hover:bg-accent" title="Refresh">
          <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
        </button>
      </div>

      <div className="rounded-lg border">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b bg-muted/50 text-muted-foreground">
              <th className="px-4 py-2">
                <SortHeader label="File" field="fileName" currentField={sortField} currentOrder={sortOrder} onSort={handleSort} />
              </th>
              <th className="px-4 py-2">
                <SortHeader label="Type" field="indexerType" currentField={sortField} currentOrder={sortOrder} onSort={handleSort} />
              </th>
              <th className="px-4 py-2">
                <SortHeader label="Status" field="status" currentField={sortField} currentOrder={sortOrder} onSort={handleSort} />
              </th>
              <th className="px-4 py-2 text-right">
                <SortHeader label="Retries" field="processingAttempts" currentField={sortField} currentOrder={sortOrder} onSort={handleSort} />
              </th>
              <th className="px-4 py-2">
                <SortHeader label="Started (UTC)" field="startedAt" currentField={sortField} currentOrder={sortOrder} onSort={handleSort} />
              </th>
              <th className="px-4 py-2">
                <SortHeader label="Finished (UTC)" field="finishedAt" currentField={sortField} currentOrder={sortOrder} onSort={handleSort} />
              </th>
              <th className="px-4 py-2 text-center">Actions</th>
            </tr>
          </thead>
          <tbody key={`${page}-${indexerType}`}>
            {items.length === 0 && (
              <tr>
                <td colSpan={7} className="px-4 py-8 text-center text-muted-foreground">
                  {loading ? "Loading..." : "No file logs found."}
                </td>
              </tr>
            )}
            {items.map((file, i) => (
              <tr
                key={file._blobName ?? `${file.indexerType}-${i}`}
                className="cursor-pointer border-b last:border-0 hover:bg-muted/30"
                onClick={() => setSelected(file)}
              >
                <td className="max-w-[260px] truncate px-4 py-2 font-mono text-xs" title={displayName(file)}>
                  {displayName(file)}
                </td>
                <td className="px-4 py-2 text-xs">{file.indexerType ?? "-"}</td>
                <td className="px-4 py-2">
                  <StatusBadge status={file.status} blocked={file.blocked} />
                </td>
                <td className="px-4 py-2 text-right">{Math.max(0, (file.processingAttempts ?? 1) - 1)}</td>
                <td className="px-4 py-2 text-xs">{formatUtc(file.startedAt)}</td>
                <td className="px-4 py-2 text-xs">{formatUtc(file.finishedAt)}</td>
                <td className="px-4 py-2 text-center">
                  {file.blocked && file._blobName && (
                    <button
                      onClick={(e) => handleUnblock(e, file._blobName!)}
                      className="inline-flex items-center gap-1 rounded-md bg-primary px-2.5 py-1 text-xs font-medium text-primary-foreground hover:bg-primary/90"
                      title="Unblock this file"
                    >
                      <ShieldOff className="h-3 w-3" />
                      Unblock
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <Pagination page={page} pageSize={20} total={total} onChange={setPage} />

      {selected && (
        <DetailDialog
          title={`File: ${displayName(selected)}`}
          data={selected as Record<string, unknown>}
          onClose={() => setSelected(null)}
        />
      )}
    </div>
  );
}
