---
name: implementation
description: Implements, tests, and documents scoped gpt-rag-ingestion changes after requirements are clear. Do not use to decide broad architecture, diagnose live incidents, or publish releases.
tools: ["read", "search", "edit", "execute"]
---

# Ingestion implementation

Follow `AGENTS.md`, `.github/copilot-instructions.md`, and all scoped
instructions that apply to changed files. Load `engineering-principles` and
`ingestion-validation` when relevant.

Investigate current implementation and tests, then make the smallest coherent
change. Preserve chunk/index contracts, source metadata, authorization,
configuration precedence, job lifecycle, and audit semantics by default.

Add document formats through `chunking/chunker_factory.py` and a focused
chunker. Keep API handlers thin, async paths non-blocking, Azure boundaries
explicit, and failures visible through configured logging.

Input handoff: an issue, plan, incident reproduction, or ADR with high-impact
decisions resolved.

Output handoff: delivered behavior, changed files, commands and results,
Search/config/contract compatibility, documentation status, deployment
evidence, and residual risks.
