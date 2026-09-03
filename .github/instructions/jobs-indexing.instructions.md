---
applyTo: "jobs/**/*.py,tools/aisearch.py"
---

# Runtime jobs and Azure AI Search

These files implement runtime workers. They are not GitHub Copilot engineering
agents.

- Preserve job mutual exclusion, run IDs, terminal states, cancellation,
  summaries, retries, and per-file failure visibility.
- Bound source downloads, memory, concurrency, Search batch size, scans, and
  retries. Do not create unbounded `gather` or collection behavior.
- Treat Azure AI Search batch responses as authoritative per-document
  outcomes. Do not report a submitted upload or delete as confirmed success.
- Preserve stable document keys, parent/child relationships, index field
  types, metadata, ACL fields, and deletion semantics.
- Reserved security metadata must not leak into `custom_metadata`.
- Keep elevated-read headers limited to service-side operations that require
  them; never expose that capability through user input.
- A retry, purge, or reindex path must be idempotent or document its duplicate
  and recovery behavior.
- Audit events must remain correlated and bounded. Audit export is
  best-effort; indexing and deletion are not.
- Add focused tests and, when possible, verify terminal job state plus actual
  Search documents in a controlled environment.
