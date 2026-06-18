import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  applyConfig,
  ConfigError,
  fetchConfig,
  reloadConfig,
  saveConfig,
  type ConfigResponse,
  type ConfigSaveFailure,
  type ConfigSetting,
  type ConfigUpdate,
  type Identity,
} from "../lib/api";
import { SettingField } from "./SettingField";
import { AlertTriangle, CheckCircle2, Lock, RefreshCw, RotateCw, Save, Undo2 } from "lucide-react";

interface ConfigurationTabProps {
  identity: Identity;
}

type Banner =
  | { kind: "success"; message: string }
  | { kind: "error"; message: string }
  | { kind: "info"; message: string }
  | null;

type DraftValue = number | boolean | string | null;
type DraftMap = Record<string, DraftValue>;

function settingsToDraft(settings: ConfigSetting[]): DraftMap {
  const draft: DraftMap = {};
  for (const s of settings) draft[s.key] = s.value;
  return draft;
}

function valuesEqual(a: DraftValue, b: DraftValue): boolean {
  if (a === b) return true;
  if (a === null || b === null || a === undefined || b === undefined) return false;
  return String(a) === String(b);
}

export function ConfigurationTab({ identity }: ConfigurationTabProps) {
  const [config, setConfig] = useState<ConfigResponse | null>(null);
  const [draft, setDraft] = useState<DraftMap>({});
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({});
  const [banner, setBanner] = useState<Banner>(null);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [reloading, setReloading] = useState(false);
  const [applying, setApplying] = useState(false);
  const [confirmApply, setConfirmApply] = useState(false);
  const abortRef = useRef<AbortController | null>(null);

  // Resolve read-only state. The backend's canEdit is authoritative, but we
  // also fall back to identity in case the GET returned before /api/identity.
  const canEdit = config?.canEdit ?? (!identity.authEnabled || identity.isAdmin);
  const authEnabled = config?.authEnabled ?? identity.authEnabled;
  const readOnly = authEnabled && !canEdit;

  const load = useCallback(async () => {
    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;
    setLoading(true);
    try {
      const res = await fetchConfig(ctrl.signal);
      if (ctrl.signal.aborted) return;
      setConfig(res);
      setDraft(settingsToDraft(res.settings));
      setFieldErrors({});
    } catch (err) {
      if ((err as Error).name === "AbortError") return;
      setBanner({
        kind: "error",
        message: err instanceof Error ? err.message : "Failed to load configuration.",
      });
    } finally {
      if (!ctrl.signal.aborted) setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
    return () => {
      abortRef.current?.abort();
    };
  }, [load]);

  const dirtyKeys = useMemo(() => {
    if (!config) return [] as string[];
    const map: Record<string, ConfigSetting> = {};
    for (const s of config.settings) map[s.key] = s;
    return Object.keys(draft).filter((k) => map[k] && !valuesEqual(draft[k], map[k].value));
  }, [config, draft]);

  const isDirty = dirtyKeys.length > 0;

  const handleChange = useCallback((key: string, value: DraftValue) => {
    setDraft((d) => ({ ...d, [key]: value }));
    setFieldErrors((e) => {
      if (!e[key]) return e;
      const next = { ...e };
      delete next[key];
      return next;
    });
  }, []);

  const handleDiscard = () => {
    if (!config) return;
    setDraft(settingsToDraft(config.settings));
    setFieldErrors({});
    setBanner(null);
  };

  const handleSave = async () => {
    if (!config || !isDirty) return;
    setSaving(true);
    setBanner(null);
    const updates: ConfigUpdate[] = dirtyKeys.map((k) => ({ key: k, value: draft[k] }));
    try {
      const res = await saveConfig(updates);
      // Merge applied values back into config.settings; leave failed ones in
      // the draft so the user can fix and retry.
      setConfig((prev) => {
        if (!prev) return prev;
        const appliedSet = new Set(res.applied);
        const updatedDraft: DraftMap = { ...draft };
        const settings = prev.settings.map((s) => {
          if (appliedSet.has(s.key)) {
            const newVal = draft[s.key];
            updatedDraft[s.key] = newVal;
            return { ...s, value: newVal };
          }
          return s;
        });
        // Apply must run after the closure resolves; capture into ref-like local
        queueMicrotask(() => setDraft(updatedDraft));
        return { ...prev, settings };
      });
      const errMap: Record<string, string> = {};
      for (const f of res.failed) errMap[f.key] = f.error;
      setFieldErrors(errMap);
      if (res.failed.length === 0) {
        const parts = [`Saved ${res.applied.length} setting${res.applied.length === 1 ? "" : "s"}.`];
        if (res.rescheduled.length) {
          parts.push(`Rescheduled jobs: ${res.rescheduled.join(", ")}.`);
        }
        setBanner({ kind: "success", message: parts.join(" ") });
      } else if (res.applied.length === 0) {
        setBanner({
          kind: "error",
          message: `No settings saved. ${res.failed.length} validation error${res.failed.length === 1 ? "" : "s"} — see fields below.`,
        });
      } else {
        setBanner({
          kind: "error",
          message: `Partial save: ${res.applied.length} applied, ${res.failed.length} rejected — see fields below.`,
        });
      }
    } catch (err) {
      if (err instanceof ConfigError) {
        if (err.forbidden) {
          setBanner({
            kind: "error",
            message: "You don't have permission to change configuration. Admin role required.",
          });
        } else {
          setBanner({ kind: "error", message: err.message });
        }
        if (err.failures.length) {
          const errMap: Record<string, string> = {};
          for (const f of err.failures as ConfigSaveFailure[]) errMap[f.key] = f.error;
          setFieldErrors(errMap);
        }
      } else {
        setBanner({
          kind: "error",
          message: err instanceof Error ? err.message : "Save failed.",
        });
      }
    } finally {
      setSaving(false);
    }
  };

  const handleReload = async () => {
    setReloading(true);
    setBanner(null);
    try {
      await reloadConfig();
      await load();
      setBanner({
        kind: "success",
        message: "Configuration cache reloaded from App Configuration.",
      });
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Reload failed.";
      setBanner({ kind: "error", message: msg });
    } finally {
      setReloading(false);
    }
  };

  const handleApply = async () => {
    setApplying(true);
    setBanner(null);
    try {
      const res = await applyConfig();
      await load();
      const parts = ["Soft restart completed."];
      if (res.note) parts.push(res.note);
      if (res.rescheduled?.length) parts.push(`Rescheduled: ${res.rescheduled.join(", ")}.`);
      setBanner({ kind: "success", message: parts.join(" ") });
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Apply failed.";
      setBanner({ kind: "error", message: msg });
    } finally {
      setApplying(false);
      setConfirmApply(false);
    }
  };

  if (loading && !config) {
    return (
      <div className="flex items-center gap-2 py-12 text-sm text-muted-foreground">
        <RotateCw className="h-4 w-4 animate-spin" /> Loading configuration…
      </div>
    );
  }

  if (!config) {
    return (
      <div className="space-y-3">
        {banner && <BannerView banner={banner} />}
        <button
          type="button"
          onClick={load}
          className="inline-flex items-center gap-1.5 rounded-md border border-input bg-background px-3 py-1.5 text-sm font-medium shadow-sm hover:bg-accent"
        >
          <RotateCw className="h-4 w-4" /> Retry
        </button>
      </div>
    );
  }

  const settingsByKey: Record<string, ConfigSetting> = {};
  for (const s of config.settings) settingsByKey[s.key] = s;

  return (
    <div className="space-y-4 pb-32">
      {readOnly && (
        <div
          role="status"
          className="flex items-start gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-sm text-amber-700 dark:text-amber-300"
        >
          <Lock className="mt-0.5 h-4 w-4 shrink-0" />
          <span>
            Read-only mode. Admin role is required to change configuration. You can still view the current values.
          </span>
        </div>
      )}

      {banner && <BannerView banner={banner} />}

      {config.sections.map((section) => {
        const items = section.keys
          .map((k) => settingsByKey[k])
          .filter((s): s is ConfigSetting => Boolean(s));
        if (items.length === 0) return null;
        return (
          <section
            key={section.id}
            aria-labelledby={`section-${section.id}`}
            className="rounded-lg border border-border bg-card"
          >
            <header className="border-b border-border px-4 py-2.5">
              <h2 id={`section-${section.id}`} className="text-sm font-semibold text-foreground">
                {section.title}
              </h2>
            </header>
            <div className="px-4">
              {items.map((s) => (
                <SettingField
                  key={s.key}
                  setting={s}
                  draftValue={draft[s.key] ?? null}
                  isDirty={!valuesEqual(draft[s.key] ?? null, s.value)}
                  error={fieldErrors[s.key]}
                  disabled={readOnly}
                  onChange={(v) => handleChange(s.key, v)}
                />
              ))}
            </div>
          </section>
        );
      })}

      <div className="sticky bottom-0 -mx-4 mt-6 border-t border-border bg-background/95 px-4 py-3 backdrop-blur supports-[backdrop-filter]:bg-background/75">
        <div className="flex flex-wrap items-center justify-end gap-2">
          <span className="mr-auto text-xs text-muted-foreground">
            {isDirty
              ? `${dirtyKeys.length} unsaved change${dirtyKeys.length === 1 ? "" : "s"}`
              : "No unsaved changes"}
          </span>
          <button
            type="button"
            onClick={handleReload}
            disabled={reloading || saving || applying}
            className="inline-flex items-center gap-1.5 rounded-md border border-input bg-background px-3 py-1.5 text-sm font-medium shadow-sm transition-colors hover:bg-accent disabled:cursor-not-allowed disabled:opacity-60"
            title="Reload settings cache from App Configuration"
          >
            <RotateCw className={`h-4 w-4 ${reloading ? "animate-spin" : ""}`} />
            Reload cache
          </button>
          <button
            type="button"
            onClick={() => setConfirmApply(true)}
            disabled={readOnly || saving || reloading || applying}
            className="inline-flex items-center gap-1.5 rounded-md border border-input bg-background px-3 py-1.5 text-sm font-medium shadow-sm transition-colors hover:bg-accent disabled:cursor-not-allowed disabled:opacity-60"
            title="Refresh cache and reschedule jobs in-process"
          >
            <RefreshCw className="h-4 w-4" />
            Restart service
          </button>
          <button
            type="button"
            onClick={handleDiscard}
            disabled={!isDirty || saving}
            className="inline-flex items-center gap-1.5 rounded-md border border-input bg-background px-3 py-1.5 text-sm font-medium shadow-sm transition-colors hover:bg-accent disabled:cursor-not-allowed disabled:opacity-60"
          >
            <Undo2 className="h-4 w-4" /> Discard
          </button>
          <button
            type="button"
            onClick={handleSave}
            disabled={!isDirty || readOnly || saving}
            className="inline-flex items-center gap-1.5 rounded-md bg-primary px-3 py-1.5 text-sm font-medium text-primary-foreground shadow-sm transition-colors hover:bg-primary/90 disabled:cursor-not-allowed disabled:opacity-60"
          >
            <Save className={`h-4 w-4 ${saving ? "animate-pulse" : ""}`} />
            Save changes
          </button>
        </div>
      </div>

      {confirmApply && (
        <div
          role="dialog"
          aria-modal="true"
          aria-labelledby="confirm-apply-title"
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
          onClick={(e) => {
            if (e.target === e.currentTarget) setConfirmApply(false);
          }}
        >
          <div className="w-full max-w-md rounded-lg border border-border bg-card p-5 shadow-lg">
            <h3 id="confirm-apply-title" className="text-base font-semibold">
              Restart ingestion service?
            </h3>
            <p className="mt-2 text-sm text-muted-foreground">
              This performs an in-process refresh: reloads settings from App Configuration,
              reschedules cron jobs, and invalidates cached job/file listings. It does NOT restart
              the underlying container. Hard container restarts must be done from the Azure portal.
            </p>
            <div className="mt-4 flex justify-end gap-2">
              <button
                type="button"
                onClick={() => setConfirmApply(false)}
                disabled={applying}
                className="inline-flex items-center gap-1.5 rounded-md border border-input bg-background px-3 py-1.5 text-sm font-medium shadow-sm hover:bg-accent disabled:opacity-60"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleApply}
                disabled={applying}
                className="inline-flex items-center gap-1.5 rounded-md bg-primary px-3 py-1.5 text-sm font-medium text-primary-foreground shadow-sm hover:bg-primary/90 disabled:opacity-60"
              >
                <RefreshCw className={`h-4 w-4 ${applying ? "animate-spin" : ""}`} />
                {applying ? "Restarting…" : "Restart"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function BannerView({ banner }: { banner: NonNullable<Banner> }) {
  const styles =
    banner.kind === "success"
      ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
      : banner.kind === "error"
        ? "border-destructive/40 bg-destructive/10 text-destructive"
        : "border-blue-500/40 bg-blue-500/10 text-blue-700 dark:text-blue-300";
  const Icon = banner.kind === "success" ? CheckCircle2 : AlertTriangle;
  return (
    <div
      role={banner.kind === "error" ? "alert" : "status"}
      className={`flex items-start gap-2 rounded-md border px-3 py-2 text-sm ${styles}`}
    >
      <Icon className="mt-0.5 h-4 w-4 shrink-0" />
      <span>{banner.message}</span>
    </div>
  );
}
