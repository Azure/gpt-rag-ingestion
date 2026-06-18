import { type ConfigSetting } from "../lib/api";
import { getMeta } from "../config/settingMetadata";
import { InfoTooltip } from "./InfoTooltip";

interface SettingFieldProps {
  setting: ConfigSetting;
  /** Current draft value (may differ from setting.value while editing). */
  draftValue: number | boolean | string | null;
  /** True when the draft differs from the persisted value. */
  isDirty: boolean;
  /** Per-field error from the most recent save attempt, if any. */
  error?: string | null;
  /** Disable all editing affordances (read-only mode for non-admins). */
  disabled?: boolean;
  /** Notify parent of a new draft value. */
  onChange: (value: number | boolean | string | null) => void;
}

/**
 * Renders a single configuration row: label + tooltip + appropriate input
 * for the setting's type. The component is intentionally stateless; the
 * parent owns the draft map and dirty tracking.
 */
export function SettingField({
  setting,
  draftValue,
  isDirty,
  error,
  disabled = false,
  onChange,
}: SettingFieldProps) {
  const meta = getMeta(setting.key);
  const inputId = `cfg-${setting.key}`;

  const labelRow = (
    <div className="flex items-center gap-1.5">
      <label htmlFor={inputId} className="text-sm font-medium text-foreground">
        {meta.label}
      </label>
      <InfoTooltip content={meta.tooltip} label={`About ${meta.label}`} />
      {isDirty && (
        <span
          className="rounded-full bg-amber-500/15 px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide text-amber-600 dark:text-amber-400"
          aria-label="Unsaved changes"
        >
          modified
        </span>
      )}
    </div>
  );

  let control: JSX.Element;
  if (setting.type === "bool") {
    const checked = draftValue === true;
    control = (
      <label className="inline-flex cursor-pointer items-center gap-2 text-sm">
        <input
          id={inputId}
          type="checkbox"
          checked={checked}
          disabled={disabled}
          onChange={(e) => onChange(e.target.checked)}
          className="h-4 w-4 rounded border-input text-primary focus:ring-2 focus:ring-ring"
          aria-describedby={error ? `${inputId}-error` : undefined}
        />
        <span className="text-muted-foreground">
          {checked ? "Enabled" : "Disabled"}
        </span>
      </label>
    );
  } else if (setting.type === "int") {
    control = (
      <input
        id={inputId}
        type="number"
        inputMode="numeric"
        value={draftValue === null || draftValue === undefined ? "" : String(draftValue)}
        placeholder={meta.placeholder}
        min={setting.min}
        max={setting.max}
        disabled={disabled}
        onChange={(e) => {
          const raw = e.target.value;
          if (raw === "") {
            onChange(null);
            return;
          }
          const parsed = Number(raw);
          onChange(Number.isFinite(parsed) ? parsed : raw);
        }}
        aria-describedby={error ? `${inputId}-error` : undefined}
        className="w-full max-w-[12rem] rounded-md border border-input bg-background px-3 py-1.5 text-sm shadow-sm transition-colors placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring disabled:cursor-not-allowed disabled:opacity-60"
      />
    );
  } else {
    // cron / string
    control = (
      <input
        id={inputId}
        type="text"
        value={draftValue === null || draftValue === undefined ? "" : String(draftValue)}
        placeholder={meta.placeholder}
        disabled={disabled}
        onChange={(e) => onChange(e.target.value)}
        aria-describedby={error ? `${inputId}-error` : undefined}
        className="w-full max-w-md rounded-md border border-input bg-background px-3 py-1.5 font-mono text-sm shadow-sm transition-colors placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring disabled:cursor-not-allowed disabled:opacity-60"
      />
    );
  }

  return (
    <div className="grid gap-2 border-b border-border/50 py-3 last:border-b-0 md:grid-cols-[minmax(0,1fr)_minmax(0,1.2fr)] md:items-start md:gap-6">
      <div>
        {labelRow}
        <p className="mt-1 text-xs text-muted-foreground">
          <code className="rounded bg-muted px-1 py-0.5 font-mono">{setting.key}</code>
        </p>
      </div>
      <div>
        {control}
        {meta.hint && !error && (
          <p className="mt-1 text-xs text-muted-foreground">{meta.hint}</p>
        )}
        {error && (
          <p
            id={`${inputId}-error`}
            role="alert"
            className="mt-1 text-xs text-destructive"
          >
            {error}
          </p>
        )}
      </div>
    </div>
  );
}
