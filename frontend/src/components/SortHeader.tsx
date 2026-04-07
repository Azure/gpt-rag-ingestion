import { ArrowDown, ArrowUp, ArrowUpDown } from "lucide-react";

interface SortHeaderProps {
  label: string;
  field: string;
  currentField: string;
  currentOrder: "asc" | "desc";
  onSort: (field: string) => void;
}

export function SortHeader({ label, field, currentField, currentOrder, onSort }: SortHeaderProps) {
  const active = currentField === field;
  return (
    <button
      onClick={() => onSort(field)}
      className="flex items-center gap-1 text-left font-medium hover:text-foreground"
    >
      {label}
      {active ? (
        currentOrder === "asc" ? (
          <ArrowUp className="h-3 w-3" />
        ) : (
          <ArrowDown className="h-3 w-3" />
        )
      ) : (
        <ArrowUpDown className="h-3 w-3 opacity-40" />
      )}
    </button>
  );
}
