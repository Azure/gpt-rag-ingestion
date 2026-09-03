# Repository development and release instructions

Read `AGENTS.md` and every scoped instruction that applies to changed files.
Executable configuration and current implementation take precedence over
examples in prose.

## Branching

This repository uses:

- `develop` for ongoing development;
- `main` for stable released versions.

Unless a maintainer explicitly authorizes an exception:

1. Start implementation work from `develop`.
2. Use `feature/<short-description>` for feature branches.
3. Target feature pull requests to `develop`, never `main`.
4. Use `release/x.y.z` branches created from `develop`.
5. Target release pull requests to `main`.

Do not mix new feature work into release branches. A maintainer-authorized
one-off branch or pull-request target exception applies only to that task and
does not change repository policy.

Use clear conventional commit subjects such as:

- `feat: add document format support`
- `fix: preserve index authorization metadata`
- `docs: document ingestion configuration`
- `chore: prepare release 2.6.0`

## Versioning

Follow semantic versioning:

- PATCH: compatible bug fixes and small compatible improvements.
- MINOR: backward-compatible features.
- MAJOR: breaking changes.

For release `2.6.0`, keep these forms aligned:

| Surface | Value |
| --- | --- |
| Branch | `release/2.6.0` |
| Root `VERSION` | `2.6.0` |
| Changelog heading | `## [v2.6.0] - YYYY-MM-DD` |
| Git tag and GitHub Release title | `v2.6.0` |

Never add `v` to `VERSION` or a release branch name. Feature work must not
preemptively change `VERSION`.

The GitHub Release title must be exactly the tag, with no product or service
prefix: use `v2.6.0`, never `GPT-RAG Ingestion v2.6.0` or
`gpt-rag-ingestion v2.6.0`.

## Changelog lifecycle

`CHANGELOG.md` follows Keep a Changelog and uses `Added`, `Changed`, `Fixed`,
and `Removed` when applicable.

On `develop`:

- maintain exactly one `## [Unreleased]` section;
- add every user-, operator-, deployment-, contract-, or release-relevant
  change under it;
- do not create a future numbered release section.

On `release/x.y.z`:

- replace the staged `## [Unreleased]` heading with
  `## [vX.Y.Z] - YYYY-MM-DD`;
- do not create a new `Unreleased` section on the release branch;
- synchronize the release branch, `VERSION`, changelog, tag, and GitHub
  Release title.

On `main`:

- `CHANGELOG.md` must never contain `[Unreleased]`;
- finding `[Unreleased]` on `main` is a release-process error.

After a release merge, recreate the empty `Unreleased` section separately on
`develop`.

Changelog entries must start with a descriptive bold title and explain what
changed and why it matters. Avoid vague entries such as "minor updates" or
"fixes."

## Release safety

Release preparation contains metadata and compatibility work only. Read the
release number from the task and current repository state; never infer it from
stale examples.

Before publishing:

- validate the exact ingestion commit or image with the compatible GPT-RAG
  umbrella/component versions;
- capture test, build, and deployment evidence appropriate to the change;
- verify rollback or roll-forward steps;
- remove personal Azure environment and resource-group names from public
  notes;
- obtain explicit human approval before creating a tag, GitHub Release,
  package, image, or production deployment.

## Documentation consistency

Documentation must describe current shipped behavior.

- The repository `README.md` owns concise ingestion-specific service and audit
  behavior that must stay synchronized with code and contracts.
- Cross-component user and operator documentation lives on the `docs` branch
  of `Azure/GPT-RAG` and is published at
  https://azure.github.io/GPT-RAG/.
- Update relevant documentation in the same coordinated change when a format,
  configuration key, default, index field, deployment step, operator flow,
  contract, or breaking behavior changes.
- Search the published documentation source for old names and affected keys.
- Keep service READMEs concise and link to the published site rather than
  duplicating broad product guidance.

A change with user or operator impact is incomplete until documentation is
updated or repository and published-doc searches demonstrate that no page is
affected.