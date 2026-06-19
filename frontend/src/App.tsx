import { useEffect, useState } from "react";
import { ThemeProvider } from "next-themes";
import { ThemeToggle } from "./components/ThemeToggle";
import { JobsTable } from "./components/JobsTable";
import { SchedulesTab } from "./components/SchedulesTab";
import { FilesTable } from "./components/FilesTable";
import { ConfigurationTab } from "./components/ConfigurationTab";
import { fetchIdentity, fetchVersion, type Identity } from "./lib/api";
import { Calendar, Database, FileText, Settings } from "lucide-react";

type Tab = "jobs" | "schedules" | "files" | "config";

function Dashboard() {
  const [tab, setTab] = useState<Tab>("jobs");
  const [version, setVersion] = useState("");
  const [navRunId, setNavRunId] = useState<string | null>(null);
  const [identity, setIdentity] = useState<Identity>({ authEnabled: false, isAdmin: true });

  useEffect(() => {
    fetchVersion().then(setVersion);
    fetchIdentity().then(setIdentity);
  }, []);

  useEffect(() => {
    const handler = (e: Event) => {
      const runId = (e as CustomEvent).detail?.runId;
      if (runId) {
        setNavRunId(runId);
        setTab("jobs");
      }
    };
    window.addEventListener("navigate-to-job", handler);
    return () => window.removeEventListener("navigate-to-job", handler);
  }, []);

  return (
    <div className="mx-auto min-h-screen max-w-7xl px-4 py-6">
      <header className="mb-6 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <img src="/logo.png" alt="GPT-RAG" className="h-10 w-10" />
          <div>
            <h1 className="text-2xl font-bold tracking-tight">GPT-RAG Ingestion</h1>
            {version && (
              <span className="text-xs text-muted-foreground">v{version}</span>
            )}
          </div>
        </div>
        <ThemeToggle />
      </header>

      <nav className="mb-4 flex gap-1 border-b">
        <button
          onClick={() => setTab("jobs")}
          className={`flex items-center gap-1.5 border-b-2 px-4 py-2 text-sm font-medium transition-colors ${
            tab === "jobs"
              ? "border-primary text-foreground"
              : "border-transparent text-muted-foreground hover:text-foreground"
          }`}
        >
          <Database className="h-4 w-4" />
          Jobs
        </button>
        <button
          onClick={() => setTab("schedules")}
          className={`flex items-center gap-1.5 border-b-2 px-4 py-2 text-sm font-medium transition-colors ${
            tab === "schedules"
              ? "border-primary text-foreground"
              : "border-transparent text-muted-foreground hover:text-foreground"
          }`}
        >
          <Calendar className="h-4 w-4" />
          Schedules
        </button>
        <button
          onClick={() => setTab("files")}
          className={`flex items-center gap-1.5 border-b-2 px-4 py-2 text-sm font-medium transition-colors ${
            tab === "files"
              ? "border-primary text-foreground"
              : "border-transparent text-muted-foreground hover:text-foreground"
          }`}
        >
          <FileText className="h-4 w-4" />
          Files
        </button>
        <button
          onClick={() => setTab("config")}
          className={`flex items-center gap-1.5 border-b-2 px-4 py-2 text-sm font-medium transition-colors ${
            tab === "config"
              ? "border-primary text-foreground"
              : "border-transparent text-muted-foreground hover:text-foreground"
          }`}
        >
          <Settings className="h-4 w-4" />
          Configuration
        </button>
      </nav>

      {tab === "jobs" && (
        <JobsTable navigateRunId={navRunId} onNavigated={() => setNavRunId(null)} identity={identity} />
      )}
      {tab === "schedules" && <SchedulesTab identity={identity} />}
      {tab === "files" && <FilesTable />}
      {tab === "config" && <ConfigurationTab identity={identity} />}
    </div>
  );
}

export default function App() {
  return (
    <ThemeProvider attribute="class" defaultTheme="system" enableSystem>
      <Dashboard />
    </ThemeProvider>
  );
}
