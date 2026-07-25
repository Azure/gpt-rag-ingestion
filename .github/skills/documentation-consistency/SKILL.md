---
name: documentation-consistency
description: Keeps ingestion repository and published GPT-RAG documentation aligned with shipped formats, configuration, indexing, deployment, audit, and operator behavior.
---

# Ingestion documentation consistency

1. Identify the user, operator, or cross-component behavior that changed.
2. Search this repository for the format, configuration key, index field,
   endpoint, job type, contract, and previous terminology.
3. Update ingestion-specific service or audit guidance in `README.md`.
4. Search the `docs` branch of `Azure/GPT-RAG` for cross-component user and
   operator guidance and update every affected page in the coordinated change.
5. Register new published pages in that branch's `mkdocs.yml`.
6. Ensure examples match current defaults, App Configuration labels,
   supported deployment modes, and released component behavior.

Keep the service README concise and link to
https://azure.github.io/GPT-RAG/ for broad product guidance. Report the
documentation branch or pull request in the implementation handoff.
