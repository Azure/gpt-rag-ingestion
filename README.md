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

## Hosted retrieval through Foundry Toolbox

`POST /retrieve` is a preview-only OpenAPI tool boundary for hosted mode. It is
disabled by default and has no effect on classic ingestion. The request schema
is intentionally small:

```json
{
  "query": "bounded string, 1-1000 characters",
  "top": "optional integer, 1-10; default 5"
}
```

Identity, groups, and index selection are not request fields. Configure the
Toolbox connection for `UserEntraToken` identity passthrough so it sends a
delegated bearer in `Authorization`. Configure the connection audience and
`HOSTED_RETRIEVAL_TOKEN_AUDIENCE` to the same Azure AI Search query-token
audience proven by INV-002. The service validates the signature, issuer, tenant,
exact audience, expiry, delegated scope, identity type, and user object ID, then
forwards the unchanged token to the configured `SEARCH_RAG_INDEX_NAME` through
Azure AI Search's `x-ms-query-source-authorization` header. Never configure this
tool with a static API key, project managed identity, caller-supplied object ID,
or caller-supplied group list as the authorization boundary.

The feature requires both App Configuration settings below (label `gpt-rag`):

| Setting | Default | Contract |
| --- | --- | --- |
| `HOSTED_RETRIEVAL_ENABLED` | `false` | Enables the hosted retrieval route. |
| `HOSTED_RETRIEVAL_INV_002_VALIDATED` | `false` | Operator attestation that ADR-0001 INV-002 passed in an isolated non-production topology without a group-filter fallback. |
| `HOSTED_RETRIEVAL_TOKEN_AUDIENCE` | unset | Exact audience accepted from the Toolbox `UserEntraToken` connection and forwarded unchanged to Search; record the validated value in INV-002. |

Keep the second setting false until two users in different groups prove that a
restricted user cannot retrieve another user's or group's content. The bounded
INV-002 evidence must also record the Toolbox connection auth type and audience,
the Search index permission-filter configuration, and the exact negative-test
results. There is no manual user/group fallback in this service. Do not infer
that a document is public from an empty user-ID field: Search evaluates user,
group, and RBAC-scope permission fields independently. Only Search's native
permission-filter semantics determine whether a document is public.

Required access handoff:

- Grant each end user the **Foundry User** role on the project for the
  `UserEntraToken` identity-passthrough flow.
- Grant the ingestion service managed identity **Search Index Data Reader** on
  the configured Search service for query access. Existing ingestion deployments
  may already hold the broader **Search Index Data Contributor** role for writes.
- Enable native permission filters on the configured index fields for user IDs,
  group IDs, and RBAC scope before setting the INV-002 gate.

Failure contract: `401` for a missing/invalid bearer, `403` for a non-delegated
or non-user token, `422` for schema/bounds violations, `500` for missing server
configuration, `502` for Search failures, and `503` while hosted retrieval or
the INV-002 evidence gate is disabled. Responses contain at most 10 results;
content is capped at 8,000 characters per result, URLs at 2,048, titles at 512,
and other string metadata at 256. Vectors, ACL fields, tokens, and raw
authorization claims are never returned or logged.

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

### Querying events in Application Insights

Events appear in the `customEvents` table. Filter the `name` column to find
ingestion audit events:

```kusto
customEvents
| where name startswith "gptrag.audit.ingestion"
| project timestamp, name, tostring(customDimensions.event_id),
    tostring(customDimensions.correlation_id),
    tostring(customDimensions.parent_event_id),
    tostring(customDimensions.source_type),
    tostring(customDimensions.status)
| order by timestamp desc
```

A `parent_event_id` value of
`evt_00000000000000000000000000000000` represents a root event with no
logical parent. Treat the sentinel as null when correlating events; do not
attempt to join it as an event ID.

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
