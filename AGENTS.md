## Engineering Standards

### Clean Code and Modularity

All implementations in this repository should follow clean code best practices.
The ingestion service is a Python 3.12 pipeline that turns documents into
chunks and embeddings indexed in Azure AI Search; keep the pipeline modular
and easy to extend with new formats, and avoid letting any single module
become a catch-all for unrelated behavior.

- Keep each module and file focused on a single, clear responsibility.
- Extract non-trivial logic into the right layer instead of growing
  `main.py` or a single chunker:
  - `chunking/` — format-specific chunkers, selected through
    `chunking/chunker_factory.py`.
  - `api/` — HTTP entrypoints (keep handlers thin).
  - `jobs/` — long-running / scheduled ingestion work.
  - `tools/` — connectors to data sources (SharePoint, blob, etc.).
  - `utils/` and `telemetry/` — cross-cutting helpers.
- Prefer small, cohesive functions and classes over large procedural blocks.
  Respect async correctness — do not block the event loop with synchronous
  I/O in async paths.
- Use clear, intent-revealing names so the code reads without excessive
  comments. Comment only non-obvious decisions.

### New Formats Go Through the Chunker Factory

Add support for a new document format by extending
`chunking/chunker_factory.py` and adding a focused chunker class — do **not**
add format `if/elif` branches at call sites. Reuse the existing chunker base
behavior and shared helpers before introducing new ones. Avoid duplication
and speculative abstractions; extract only when code is genuinely repeated or
a file is mixing concerns.

### Configuration, Secrets, and Contracts

- Read runtime settings from **Azure App Configuration** (label `gpt-rag`) via
  the existing config provider; resolve secrets through **Key Vault**
  references. Never hardcode endpoints, index names, container names, or
  feature flags in code.
- Prefer typed, explicit data contracts (type hints, dataclasses, or Pydantic
  models) for chunk records, document metadata, and any payload crossing a
  service or index boundary.
- Surface errors clearly and consistently through the telemetry/logging
  helpers. Do not swallow exceptions or add silent fallbacks that hide a
  failed parse, embedding, or index operation. Never use `print` for
  diagnostics — use the configured logger.

### Verifying Changes

There is no maintained `tests/` suite here. Verify changes by running the
container locally or via `scripts/deploy.*`, exercising the affected
format/chunker end to end, and confirming documents land correctly in Azure
AI Search. Add a focused test alongside new logic when it is independently
testable.
