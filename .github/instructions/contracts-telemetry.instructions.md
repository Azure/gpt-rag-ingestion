---
applyTo: "contracts/**,telemetry/audit*.py,tests/test_audit*.py,tests/golden/**"
---

# Shared audit contracts and telemetry

- Treat schema files and their SHA-256 pins as versioned cross-repository
  compatibility boundaries shared with Azure/GPT-RAG and the orchestrator.
- Keep logical schema, Application Insights wire schema, constants, golden
  fixtures, and integrity tests aligned with the exact committed bytes.
- Preserve the seven ingestion event types, run correlation, root-parent
  sentinel encoding, bounded payloads, and sanitization unless a coordinated
  contract change is approved.
- Never emit document text, raw source URLs, filenames, credentials, prompts,
  or other sensitive content in audit records.
- Provenance fields remain opt-in; strict governance defaults must not be
  represented as explicit document evidence.
- Audit creation, sanitization, or export failure remains logged and
  non-blocking. Do not apply best-effort behavior to source, indexing,
  deletion, or authorization failures.
- Add logical, wire, hash, sanitizer, correlation, and exporter-envelope tests
  for changed behavior.
- Do not claim legal or regulatory compliance from technical audit evidence.
