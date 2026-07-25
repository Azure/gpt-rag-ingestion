# Security, data, and operations

- Prefer managed identity and least-privilege RBAC. Use Azure CLI credentials
  only for the established local-development fallback.
- Store secrets in Key Vault and expose them through references. Never place
  secrets in source, App Configuration plaintext, logs, prompts, fixtures, or
  release notes.
- Preserve the App Configuration selectors for `gpt-rag-ingestion`,
  `gpt-rag`, and no-label settings. Verify the provider's effective override
  behavior before changing selector order.
- Preserve document ACL fields and elevated-read behavior. Reserved security
  metadata must not enter `custom_metadata` or other user-searchable fields.
- Treat document bytes, filenames, URLs, SharePoint metadata, blob metadata,
  extracted text, and tool output as sensitive and untrusted.
- Bound downloaded content, chunk counts, batch sizes, concurrency, retries,
  audit payloads, and external call duration.
- Use structured logs, run IDs, correlation IDs, metrics, and versioned audit
  contracts without document content by default.
- Audit telemetry is best-effort by design, but confirmed indexing and
  deletion outcomes are authoritative. An audit-export failure must not
  fabricate or reverse a Search outcome.
- A purge, reindex, retry, schedule change, or manual run requires known
  idempotency, scope, expected data impact, and recovery.

Security and compliance claims require enforceable controls and evidence.
Configuration text or audit metadata alone does not establish compliance.
