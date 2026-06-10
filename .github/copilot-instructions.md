# Repository Development and Release Instructions

## Overview

This repository follows a structured workflow based on two primary branches:

- `develop` → ongoing development
- `main` → stable, released versions

This repository uses a root-level `VERSION` file and a `CHANGELOG.md` file as release metadata.

All work must follow the branching, release, versioning, and changelog rules defined below.

---

## Branching Strategy

### Default Behavior

Unless explicitly instructed otherwise:

- All development work MUST start from `develop`
- All new implementation work MUST be done in a feature branch
- Feature work MUST target `develop`
- Release preparation MUST target `main`

---

## Feature Development Workflow

### Branch Creation

- Always create feature branches from `develop`
- Naming convention:
  - `feature/<short-description>`

Examples:
- `feature/conversation-metadata`
- `feature/improve-chat-history`
- `feature/add-evaluation-logging`

---

### Feature Pull Requests

- Source branch: `feature/*`
- Target branch: `develop`
- Never target `main` from a feature branch

---

### Expected Flow

1. Start from `develop`
2. Create `feature/<name>`
3. Implement changes
4. Commit changes
5. Open pull request to `develop`

---

## Release Workflow

### Release Branch Creation

- Release branches MUST be created from `develop`
- Naming convention:
  - `release/x.y.z`
- Release branch names MUST NOT include the `v` prefix

Examples:
- `release/2.4.2`
- `release/2.5.0`

---

### Release Responsibilities

When preparing a release branch, the agent MUST:

- update the root `VERSION` file
- update `CHANGELOG.md`
- ensure both are synchronized with the intended release number
- ensure the repository reflects a releasable state
- do NOT introduce unrelated feature work

---

### Release Pull Requests

- Source branch: `release/x.y.z`
- Target branch: `main`

---

### Expected Flow

1. Start from `develop`
2. Create `release/x.y.z`
3. Update `VERSION`
4. Update `CHANGELOG.md`
5. Open pull request to `main`

---

## Versioning Rules

- Follow semantic versioning: `MAJOR.MINOR.PATCH`

### Important Distinction

In this repository:

- release branch names do NOT use the `v` prefix
- the root `VERSION` file does NOT use the `v` prefix
- tags and changelog entries DO use the `v` prefix
- GitHub Release titles MUST be exactly the tag name (for example, `v2.4.2`);
  never prefix release titles with the product or service name (for example,
  do not use `GPT-RAG Ingestion v2.4.2` or `gpt-rag-ingestion v2.4.2`).

Examples:

- Release branch:
  - `release/2.4.2`
- `VERSION` file:
  - `2.4.2`
- Tag:
  - `v2.4.2`
- Changelog entry:
  - `## [v2.4.2] - YYYY-MM-DD`

---

### Version Increment Guidelines

- PATCH → bug fixes and minor compatible improvements
- MINOR → backward-compatible features
- MAJOR → breaking changes

---

## VERSION File Rules

This repository contains a root-level `VERSION` file.

### Purpose

- the `VERSION` file stores the repository release number
- it is the plain semantic version number without the `v` prefix

Example:

```text
2.4.2
````

---

### Development Phase (`develop` branch)

* the `VERSION` file on `develop` represents the latest version currently present in that branch
* do NOT preemptively change the `VERSION` file on `develop` for an upcoming release unless explicitly instructed
* feature work should normally not modify `VERSION`

---

### Release Phase (`release/x.y.z` branch)

When preparing a release branch, the agent MUST:

* update the `VERSION` file to match the release number without the `v` prefix
* ensure the release branch name, `VERSION` file, changelog, and tag naming are all aligned

For release `2.4.2`:

* Branch:

  * `release/2.4.2`
* `VERSION`:

  * `2.4.2`
* Changelog:

  * `## [v2.4.2] - YYYY-MM-DD`
* Tag:

  * `v2.4.2`

---

### VERSION Safety Rules

Do NOT:

* add the `v` prefix to the `VERSION` file
* update the `VERSION` file in `develop` just to anticipate a release
* leave the `VERSION` file out of sync with the release branch number during release preparation

---

## Changelog Lifecycle Rules

The changelog follows a two-phase lifecycle:

* development phase on `develop`
* release phase on `release/x.y.z`

---

### Development Phase (`develop` branch)

* `CHANGELOG.md` MUST use an `Unreleased` section
* all new changes on `develop` MUST be added under `Unreleased`
* do NOT assign a release number in `develop`
* do NOT create future version sections in `develop`

Required format:

```md
## [Unreleased]
```

Example:

```md
## [Unreleased]

### Added
- New orchestration strategy...

### Fixed
- Fixed resource cleanup issue...
```

---

### Release Phase (`release/x.y.z` branch)

When a release branch is created from `develop`, the agent MUST:

1. convert `Unreleased` into the actual release entry
2. use the correct version with `v` prefix in the changelog
3. add the release date
4. recreate a new empty `Unreleased` section at the top
5. synchronize the changelog with the `VERSION` file

Required transformation:

From:

```md
## [Unreleased]
```

To:

```md
## [vX.Y.Z] - YYYY-MM-DD
```

Example:

```md
## [Unreleased]

## [v2.4.2] - 2026-03-22

### Added
- MafLiteStrategy...

### Fixed
- Agent Service lifecycle cleanup...
```

---

### Changelog Sections

Use when applicable:

* `Added`
* `Changed`
* `Fixed`
* `Removed`

---

### Changelog Writing Style

Entries MUST:

* be clear, technical, and descriptive
* explain what changed and why it matters
* include relevant technical context where useful
* avoid vague wording such as:

  * "minor updates"
  * "improvements"
  * "fixes"

Preferred style:

* start each item with a bold title
* follow with a detailed explanation

---

### Changelog Safety Rules

Do NOT:

* assign a release number in `develop`
* remove the `Unreleased` section on `develop`
* create multiple `Unreleased` sections
* leave the release branch without converting `Unreleased`
* mismatch release numbers across branch name, `VERSION`, changelog, and tag naming

### Main Branch Changelog Rule

The `main` branch MUST NEVER contain an `[Unreleased]` section in `CHANGELOG.md`.

* during release preparation, the `[Unreleased]` section is converted into the versioned release entry
* a new empty `[Unreleased]` section is only recreated on `develop` after the release merge — never on the release branch or `main`
* if an `[Unreleased]` section is found on `main`, it is a release process error and must be corrected immediately

---

## Release Synchronization Rules

When preparing a release `X.Y.Z`, the agent MUST ensure consistency across:

* release branch:

  * `release/X.Y.Z`
* `VERSION`:

  * `X.Y.Z`
* changelog:

  * `## [vX.Y.Z] - YYYY-MM-DD`
* tag:

  * `vX.Y.Z`

---

## Commit Guidelines

Use clear, structured commit messages.

Preferred format:

* `feat: add conversation metadata support`
* `fix: correct chat history persistence issue`
* `docs: update changelog for v2.4.2`
* `chore: prepare release 2.4.2`

---

## Pull Request Guidance

### Feature PRs

* From: `feature/*`
* To: `develop`

### Release PRs

* From: `release/x.y.z`
* To: `main`

PR descriptions should clearly state:

* purpose of the change
* whether it is feature work or release preparation
* target branch
* any required validation or follow-up steps

---

## Operational Rules

### Default Decision Logic

If the task involves feature or code changes:

* base on `develop`
* create `feature/*`
* target `develop`

If the task involves release preparation:

* base on `develop`
* create `release/x.y.z`
* update `VERSION`
* update `CHANGELOG.md`
* target `main`

---

## Safety Constraints

Do NOT:

* create feature branches from `main`
* open feature PRs to `main`
* mix new feature work into release branches
* assign release versions on `develop`
* update `VERSION` for future release planning on `develop` unless explicitly instructed

---

## Code Organization

Source files MUST stay focused and small. These limits apply to every source
file in the repository (Python, PowerShell, shell, Bicep, YAML pipelines, etc.).

### File size limits

- **Soft limit: 300 lines.** Above 300 lines, plan to split before adding
  more code.
- **Hard limit: 500 lines.** A PR that leaves any source file above 500
  lines MUST split it as part of the same PR.
- These limits exclude blank lines, comments, license headers, and
  auto-generated content (e.g. `*_pb2.py`, vendored SDKs, generated
  Bicep ARM JSON).

### Function / method / script-block size

- **Functions and methods MUST stay under ~50 lines** of executable code.
- Long functions are a smell — extract private helpers named after what
  they do, not after where they sit in the file.
- PowerShell scripts under `scripts/` (e.g. `preProvision.ps1`,
  `deploy.ps1`) follow the same rule: extract advanced functions or
  dot-source helper `.ps1` / `.psm1` files in the same `scripts/` folder
  rather than letting one script grow past 500 lines.

### One responsibility per file

- One public class per Python module (e.g. one `Strategy`, one `Connector`,
  one `Plugin`, one chunker per file).
- **No generic `utils.py` / `helpers.py` / `common.py` dump files.** Helpers
  live in a module named after the responsibility (for example
  `search_context.py`, `agent_provider_v2.py`, `chunker_factory.py`).
- PowerShell: one top-level concern per `.ps1`. Shared helpers go in a
  `.psm1` module under `scripts/` and are imported with `Import-Module`.

### When (and how) to split

- Split by **axis of change**, not by line count alone. If two sections of
  a file change for different reasons, they belong in different modules.
- When splitting, **mirror the change in `tests/`** — move the corresponding
  tests into a test file that matches the new module name.
- Re-export public names from the original module (or via `__init__.py`)
  so external callers and other repos in the workspace do not break.

### Imports & dependencies

- Avoid lateral imports between sibling modules in `strategies/`,
  `plugins/`, `chunking/`, `connectors/`, etc. Pass collaborators through
  the constructor (dependency injection) — this is already the pattern in
  the codebase.
- If two modules need each other, the shared piece belongs in a third
  module. Do not work around circular imports with deferred imports.

### Configuration & secrets

- Do not hard-code endpoints, deployment names, model names, or feature
  flags. Read them from Azure App Configuration (label `gpt-rag`) and
  Key Vault, as the rest of the codebase does.

### Definition of done

- A PR that touches any file currently above the 500-line hard limit MUST
  extract at least one cohesive responsibility into a new module as part
  of that PR. Reviewers will block PRs that grow an oversized file without
  splitting it.

---

## Final Rule

If instructions are ambiguous:

* default to feature workflow using `develop`
* do not assume a release unless explicitly requested
