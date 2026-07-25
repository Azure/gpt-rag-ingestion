# Ingestion architecture

## Purpose and flow

The service ingests documents from supported sources, selects a
format-specific chunker, enriches chunks with embeddings and metadata, and
writes compatible documents to Azure AI Search. Runtime index and purge work
is implemented under `jobs/` and scheduled from `main.py`.

The operator API and dashboard observe and control that pipeline; they are not
the pipeline's core. GitHub Copilot engineering agents under `.github/agents/`
are repository-development roles and are unrelated to runtime jobs.

## Boundaries

- Source access belongs in `tools/` or a focused source integration under
  `jobs/`.
- Format recognition is centralized in
  `chunking/chunker_factory.py`.
- Format parsing and chunk production belong in dedicated
  `chunking/chunkers/` classes.
- Job orchestration, retries, batching, run summaries, and source/index
  reconciliation belong in `jobs/`.
- Azure AI Search calls and confirmed result handling remain at the Search
  boundary.
- API routes remain thin and delegate scheduling, configuration, and storage
  behavior.
- Audit behavior and shared schema interpretation belong in `telemetry/` and
  `contracts/`.

Do not duplicate extension dispatch, configuration knowledge, ACL mapping, or
Search result interpretation across callers.

## Design questions

1. Which source, format, job, index, or operator capability owns the change?
2. Does it alter chunk shape, index fields, IDs, metadata, or deletion
   semantics?
3. Which document authorization and elevated-read boundaries are crossed?
4. Does it preserve bounded memory, concurrency, retries, and partial-batch
   failure visibility?
5. Which Azure/GPT-RAG component versions and contracts must be validated
   together?
6. Can the change be retried safely, and what is the rollback or reindex path?

Prefer a focused adapter or chunker over conditionals in unrelated paths.
Prefer explicit typed records over implicit dictionaries when data crosses a
source, job, chunk, index, API, or audit boundary.
