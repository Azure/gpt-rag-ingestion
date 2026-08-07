---
name: engineering-principles
description: Ingestion architecture and implementation principles. Use for design, review, meaningful refactoring, Azure integration, document security, testing, or operational changes.
---

# Ingestion engineering principles

Load only the references needed for the task:

| When the task involves | Read |
| --- | --- |
| Repository purpose, pipeline boundaries, sources, chunking, jobs, or Search | [Ingestion architecture](references/ingestion-architecture.md) |
| Python design, async behavior, modules, or maintainability | [Python implementation](references/python-implementation.md) |
| Tests, validation, compatibility, or evidence | [Testing and evidence](references/testing-and-evidence.md) |
| Identity, ACLs, secrets, source data, audit, or operations | [Security, data, and operations](references/security-data-operations.md) |

Use these principles as design questions, not dogma. Task requirements,
executable configuration, versioned contracts, and current behavior remain
the sources of truth.
