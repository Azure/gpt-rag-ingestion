<!-- 
page_type: sample
languages:
- azdeveloper
- powershell
- bicep
products:
- azure
- azure-ai-foundry
- azure-openai
- azure-ai-search
urlFragment: GPT-RAG
name: Multi-repo ChatGPT and Enterprise data with Azure OpenAI and AI Search
description: GPT-RAG core is a Retrieval-Augmented Generation pattern running in Azure, using Azure AI Search for retrieval and Azure OpenAI large language models to power ChatGPT-style and Q&A experiences.
-->
# GPT-RAG Data Ingestion

Part of the [GPT-RAG](https://github.com/Azure/gpt-rag) solution.

The **GPT-RAG Data Ingestion** service automates the processing of diverse document types—such as PDFs, images, spreadsheets, transcripts, and SharePoint files—preparing them for indexing in Azure AI Search. It uses intelligent chunking strategies tailored to each format, generates text and image embeddings, and enables rich, multimodal retrieval experies for agent-based RAG applications.

For full documentation, visit the **[GPT-RAG documentation site](https://azure.github.io/GPT-RAG/)**.

## Governance and audit events

This service emits a versioned, correlated audit trail for ingestion runs and
document outcomes, sharing the `audit-event-v1` contract owned by
[`Azure/GPT-RAG`](https://github.com/Azure/GPT-RAG) (pinned by SHA-256 in
[`contracts/`](contracts/) and consumed the same way by
`gpt-rag-orchestrator`). Reuses the existing OpenTelemetry / Application
Insights pipeline — no separate audit backend, queue, or export path is
introduced.

### Event taxonomy

This service emits exactly these seven event types (no aliases):

| Event | When |
| --- | --- |
| `ingestion.run.started` | An ingestion job begins (blob index/purge, SharePoint index/purge, NL2SQL index/purge, multimodal image purge) |
| `ingestion.run.completed` | The job finished without an unhandled or logged error |
| `ingestion.run.failed` | The job caught and logged an error, or an exception escaped it |
| `ingestion.run.cancelled` | The job's task was cancelled (e.g. container shutdown) |
| `ingestion.document.indexed` | Azure AI Search confirmed a document upload succeeded |
| `ingestion.document.rejected` | Azure AI Search confirmed a document upload failed |
| `ingestion.document.deleted` | Azure AI Search confirmed a document deletion succeeded |

Every ingestion run emits exactly one `started` event and exactly one
terminal event. Document events carry the run's `correlation_id` as their
own and the run's `started` event as their `parent_event_id`, so a run and
its documents can be reconstructed from Application Insights without
inspecting unrelated telemetry.

Audit emission is **best-effort and never blocking**: a failure to build,
sanitize, or export an audit event is logged as a warning and dropped — it
never turns a successful index, delete, or run into a failure.

### Provenance flags

| Setting | Default | Effect |
| --- | --- | --- |
| `INGESTION_PROVENANCE_ENABLED` | `false` | When `true`, `ingestion.document.indexed`/`.deleted` events additionally carry `provenance_id`, `source_uri_id`, `source_version_id`, `content_checksum_sha256`, `ingested_at`, `ingest_run_id`, `data_classification`, `right_to_use`, and (when present on the document) `retention_class`/`delete_after`. When `false` (default), none of these fields are attached — this is the exact disabled behavior prior to this feature; the audit trail itself is unaffected. |
| `INGESTION_REQUIRE_GOVERNANCE_METADATA` | `false` | Strict governance mode. Requires **both** `data_classification` and `right_to_use` to be explicitly present on the document; if either is missing, both fields are omitted from the event rather than filled in from the configured defaults — a default can never be reported as if it satisfied strict mode. **Invalid configuration:** setting this to `true` while `INGESTION_PROVENANCE_ENABLED=false` fails startup with an actionable error, since strict governance cannot be enforced when provenance capture itself is off. |
| `INGESTION_DEFAULT_CLASSIFICATION` | `unclassified` | Fallback `data_classification` used only when provenance is enabled and strict governance is **not** required. |
| `INGESTION_DEFAULT_RIGHT_TO_USE` | `not_asserted` | Fallback `right_to_use` used only when provenance is enabled and strict governance is **not** required. |

Flag matrix:

| `INGESTION_PROVENANCE_ENABLED` | `INGESTION_REQUIRE_GOVERNANCE_METADATA` | Result |
| --- | --- | --- |
| `false` | `false` | Default. Audit events emitted; no provenance fields. |
| `true` | `false` | Provenance fields attached; classification/right-to-use fall back to the configured defaults when a document doesn't supply its own. |
| `true` | `true` | Provenance fields attached; classification/right-to-use are only reported when the document explicitly supplies both — otherwise omitted. |
| `false` | `true` | **Invalid.** Startup fails with an actionable error. |

### Known limitations and evidence gaps

- **`source_uri_id` and `source_version_id` are opaque, unsalted SHA-256
  digests**, never the raw blob path, SharePoint URL, or filename. They are
  deterministic (the same source always hashes the same way, which is what
  makes correlation across events possible) but are **not** cryptographically
  keyed — this repository has no existing HMAC/key-management convention, so
  a plain digest is used instead. Do not treat these as secrets: a small,
  known set of candidate paths could in principle be dictionary-matched
  against the digest.
- **`delete_after` is policy intent only.** Setting it on a document does not
  schedule or trigger any automatic purge; only a confirmed deletion result
  from Azure AI Search ever emits `ingestion.document.deleted`.
- **Audit metadata and checksums can themselves be sensitive.** A content
  checksum can reveal that two ingested documents are identical or
  reveal document existence in an offline dictionary attack against a small
  candidate set; apply the same access control to Application Insights as to
  the ingested content itself.
- **This audit trail does not, by itself, establish legal compliance** with
  any regulatory framework; it provides technical evidence adopters can use
  in their own governance and risk assessments.

## Contributing

We welcome contributions! See the [contribution guidelines](https://azure.github.io/GPT-RAG/contributing/) for details on how to contribute.

## Trademarks

This project may contain trademarks or logos. Authorized use of Microsoft trademarks or logos must follow [Microsoft’s Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general). Modified versions must not imply sponsorship or cause confusion. Third-party trademarks are subject to their own policies.
