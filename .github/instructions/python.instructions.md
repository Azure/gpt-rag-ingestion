---
applyTo: "*.py,api/**/*.py,chunking/**/*.py,jobs/**/*.py,telemetry/**/*.py,tools/**/*.py,utils/**/*.py,tests/**/*.py"
---

# Python 3.12

- Keep modules focused and follow local naming and import conventions.
- Use explicit type hints and typed boundary records where practical.
- Preserve async behavior through API, job, and Azure SDK paths. Do not call
  blocking network or file I/O on the event loop.
- Bound concurrency, retries, batches, memory, and external-call duration.
- Catch expected exceptions where context and recovery are known; do not add
  broad catches or success-shaped fallbacks.
- Use configured logging and telemetry, never `print`.
- Keep pure transformations testable without Azure credentials or network
  access.
- Run focused `pytest` tests, then the broader suite according to risk.
- Load `engineering-principles` for meaningful design, security, data,
  integration, or operational changes.
