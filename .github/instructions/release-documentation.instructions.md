---
applyTo: "VERSION,CHANGELOG.md,README.md,docs/**"
---

# Release metadata and documentation

- Follow `.github/copilot-instructions.md` completely.
- On feature work, do not change `VERSION`.
- On `develop`, stage changes under exactly one `## [Unreleased]`.
- On `release/x.y.z`, set `VERSION` to `x.y.z` and convert `Unreleased` to
  `## [vX.Y.Z] - YYYY-MM-DD`.
- Never add `Unreleased` to a release branch or `main`.
- Changelog entries use a bold descriptive title and explain behavior and
  impact with relevant technical context.
- Keep ingestion-specific service and audit guidance in `README.md` aligned
  with code and contracts.
- Coordinate cross-component user/operator documentation on the `docs` branch
  of Azure/GPT-RAG.
- Never publish private Azure environment or resource-group names.
- Load `service-release` for release work and
  `documentation-consistency` for user- or operator-visible changes.
