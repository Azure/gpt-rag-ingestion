import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import { ALERT_AUTO_DISMISS_MS, SchedulesTab } from "./SchedulesTab";
import * as api from "../lib/api";

// Smoke test for Bug 2 (v2.4.13): the Run-now success toast must disappear
// on its own after ALERT_AUTO_DISMISS_MS, not sit there forever.

describe("SchedulesTab — auto-dismissing toast", () => {
  beforeEach(() => {
    // Stub the two endpoints the tab calls on mount so the component
    // does not try to hit a real network or warn about unhandled promises.
    vi.spyOn(api, "fetchJobs").mockResolvedValue({
      items: [],
      total: 0,
      page: 1,
      pageSize: 1,
      availableJobTypes: ["blob_index"],
      runningJobTypes: [],
    });
    vi.spyOn(api, "getJobsQueue").mockResolvedValue({ items: [] });
    vi.spyOn(api, "runJob").mockResolvedValue({
      jobType: "blob_index",
      triggerId: "trig-1",
      status: "scheduled",
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    cleanup();
  });

  // Mount-time effects fire fetchJobs + getJobsQueue + the 1s clock setInterval.
  // We let microtasks settle a few times so React picks up the resolved
  // promises and renders the "Run now" button.
  async function flushMicrotasks(times = 5) {
    for (let i = 0; i < times; i++) {
      await act(async () => {
        await Promise.resolve();
      });
    }
  }

  it("removes the success toast from the DOM after the auto-dismiss timeout", async () => {
    render(<SchedulesTab identity={{ authEnabled: false, isAdmin: true }} />);

    await flushMicrotasks();

    const runButton = screen.getByRole("button", { name: /Trigger blob_index/ });

    await act(async () => {
      runButton.click();
    });
    await flushMicrotasks();

    // Toast is up immediately after the runJob promise resolves.
    expect(screen.getByTestId("schedules-alert")).toBeTruthy();

    // Wait for the real auto-dismiss timeout (plus a small margin).
    await act(async () => {
      await new Promise((r) => setTimeout(r, ALERT_AUTO_DISMISS_MS + 200));
    });

    expect(screen.queryByTestId("schedules-alert")).toBeNull();
  }, 10_000);
});
