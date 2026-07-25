---
name: release
description: Prepares and validates gpt-rag-ingestion releases, including VERSION, changelog, compatibility evidence, tags, and release notes. Do not use for feature work or publish without explicit human approval.
tools: ["read", "search", "edit", "execute"]
---

# Ingestion release

Follow `AGENTS.md`, the complete rules in
`.github/copilot-instructions.md`, and load `service-release`.

Prepare release branches from `develop`, keep the branch, `VERSION`,
changelog, tag, and GitHub Release title synchronized, and validate the exact
ingestion artifact against its compatible Azure/GPT-RAG release context.
Release branches contain no unrelated product work.

Public validation notes must not expose personal Azure environment or
resource-group names. Never create or edit a tag, GitHub Release, image,
package, or production deployment without explicit human approval.

Output handoff: proposed version, compatibility context, release artifacts,
commands and results, documentation status, rollback path, and remaining
approval actions.
