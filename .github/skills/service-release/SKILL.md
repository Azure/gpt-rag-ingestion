---
name: service-release
description: Prepares and validates gpt-rag-ingestion releases. Use for release branches, VERSION, changelog entries, compatible GPT-RAG pins, tags, images, and GitHub Release notes.
---

# Ingestion service release

Read `.github/copilot-instructions.md` completely before changing a release
artifact.

1. Determine the intended semantic version and create `release/x.y.z` from
   `develop`.
2. Set root `VERSION` to `x.y.z` without `v`.
3. Replace the staged `## [Unreleased]` heading with
   `## [vX.Y.Z] - YYYY-MM-DD`; do not add a new `Unreleased` section on the
   release branch.
4. Verify branch, `VERSION`, changelog, Git tag, and GitHub Release title are
   synchronized.
5. Identify the compatible Azure/GPT-RAG umbrella and component versions and
   validate the exact ingestion commit or image in that context.
6. Record Python, frontend, container, and controlled Azure evidence required
   by the changed behavior.
7. Confirm documentation status and rollback or roll-forward steps.
8. Target the release pull request to `main`.
9. After merge, create exactly `vX.Y.Z` for the tag and release title only
   with explicit human approval.
10. Re-fetch the release and verify formatting and the absence of private
    Azure environment or resource-group names.

After the release, recreate the empty `Unreleased` section separately on
`develop`. Report incompatible contracts, missing validation, or
documentation drift as blockers rather than filling gaps by assumption.
