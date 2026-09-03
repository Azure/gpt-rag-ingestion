---
applyTo: "chunking/**/*.py"
---

# Document chunking

- Add a new format with a focused class under `chunking/chunkers/` and register
  it in `chunking/chunker_factory.py`.
- Keep extension dispatch centralized. Do not add format `if/elif` branches to
  jobs, APIs, or callers.
- Reuse base chunker behavior and shared parsing helpers before adding new
  abstractions.
- Preserve chunk IDs, ordering, source/page references, content, embeddings,
  metadata, and error/warning shapes unless a coordinated contract change is
  approved.
- Treat document bytes and extracted content as untrusted and potentially
  sensitive. Enforce format, size, page, and resource bounds.
- Errors must identify the document operation safely without logging document
  content or credentials.
- Test representative valid, malformed, empty, large/bounded, and
  format-routing cases when independently testable.
- For behavior changes, complete an end-to-end representative ingestion and
  verify the resulting Azure AI Search documents when an environment is
  available.
