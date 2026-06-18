import * as Tooltip from "@radix-ui/react-tooltip";
import { Info } from "lucide-react";

interface InfoTooltipProps {
  /** The tooltip content. Plain text only — markup is escaped by React. */
  content: string;
  /** Accessible label for the trigger button. Defaults to "More info". */
  label?: string;
}

/**
 * Accessible info tooltip. The trigger is a real <button> so keyboard users
 * can focus it (Tab) and reveal the tooltip with Enter/Space. Screen readers
 * announce the label and the tooltip content via aria-describedby that
 * Radix wires up automatically.
 */
export function InfoTooltip({ content, label = "More info" }: InfoTooltipProps) {
  return (
    <Tooltip.Provider delayDuration={150} skipDelayDuration={0}>
      <Tooltip.Root>
        <Tooltip.Trigger asChild>
          <button
            type="button"
            aria-label={label}
            className="inline-flex h-4 w-4 items-center justify-center rounded-full text-muted-foreground transition-colors hover:text-foreground focus:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
          >
            <Info className="h-3.5 w-3.5" />
          </button>
        </Tooltip.Trigger>
        <Tooltip.Portal>
          <Tooltip.Content
            side="top"
            align="start"
            sideOffset={6}
            className="z-50 max-w-xs rounded-md border border-border bg-popover px-3 py-2 text-xs leading-relaxed text-popover-foreground shadow-md data-[state=delayed-open]:animate-in data-[state=delayed-open]:fade-in-0"
          >
            {content}
            <Tooltip.Arrow className="fill-popover" />
          </Tooltip.Content>
        </Tooltip.Portal>
      </Tooltip.Root>
    </Tooltip.Provider>
  );
}
