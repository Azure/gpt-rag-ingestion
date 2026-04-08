import { cn } from "../lib/utils";

interface StatusBadgeProps {
  status?: string;
  blocked?: boolean;
}

export function StatusBadge({ status, blocked }: StatusBadgeProps) {
  if (blocked) {
    return (
      <span className="inline-flex items-center rounded-full bg-red-100 px-2.5 py-0.5 text-xs font-medium text-red-800 dark:bg-red-900 dark:text-red-200">
        blocked
      </span>
    );
  }

  const colours: Record<string, string> = {
    success: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
    finished: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
    error: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200",
    failed: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200",
    running: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200",
    interrupted: "bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200",
    "skipped-no-change": "bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200",
    "skipped-blocked": "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200",
  };

  const colour = colours[status ?? ""] ?? "bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-200";

  return (
    <span className={cn("inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium", colour)}>
      {status ?? "unknown"}
    </span>
  );
}
