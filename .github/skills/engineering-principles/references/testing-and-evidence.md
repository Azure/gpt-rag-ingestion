# Testing and evidence

Choose evidence according to the changed boundary:

- Pure Python behavior: focused `pytest` tests.
- Admin API behavior: FastAPI tests with Azure and scheduler boundaries
  replaced by existing fakes or stubs.
- Chunking: representative format fixtures plus assertions on chunk content,
  IDs, metadata, ordering, and errors.
- Search payload or shared schema: contract, golden-file, and integrity-hash
  tests.
- Frontend: Vitest behavior tests, ESLint, and TypeScript/Vite build.
- Deployment scripts: syntax and behavioral parity for PowerShell and shell.
- Source or Azure integration: a controlled container/deployment run and
  confirmed Azure AI Search results.

For every change, capture:

1. Acceptance criterion and observable result.
2. Commands and results.
3. Relevant configuration and component versions.
4. Compatibility, migration, retry, and rollback impact.
5. Validation that could not run and residual risk.

Do not treat a process exit, queued job, HTTP 202, or successful deployment as
sufficient evidence when the change affects document correctness,
authorization, deletion, audit correlation, or Search results.
