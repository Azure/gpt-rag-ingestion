---
name: release
description: Prepare and reconcile repository releases, including version selection, SemVer checks, release branches, changelog updates, release notes, tags, GitHub releases, and publication approval gates. Use when asked to prepare, version, tag, publish, roll back, or repair a release.
---

# Release

Prepare releases for this repository without publishing artifacts or changing
Azure resources until a human explicitly approves the publication step.

## Non-negotiable rules

- Treat `develop` as the release source and `main` as the stable release target.
- Create `release/X.Y.Z` from an up-to-date `origin/develop`; never include a
  `v` prefix in the branch name or root `VERSION` file.
- Open the release pull request from `release/X.Y.Z` to `main`.
- Use Semantic Versioning (`MAJOR.MINOR.PATCH`) and reject malformed,
  prerelease, or build-metadata versions unless the human request explicitly
  defines a supported policy for them.
- Use exactly `vX.Y.Z` for both the Git tag and GitHub release title.
- Keep the release branch, `VERSION`, changelog heading, tag, release title,
  and release target commit consistent.
- Run the repository's existing validation before requesting publication
  approval. Do not invent a passing result or skip a failing check.
- Do not add or depend on organization-only manifests, central release
  catalogs, private automation, or data that is absent from this repository.
- Do not create a release custom agent. This skill owns the reusable release
  guidance.
- Never create or mutate Azure resources as part of release preparation.

## 1. Discover the authoritative released version

Start from a clean worktree and refresh remote state:

```bash
git fetch origin develop main --tags --prune
git status --short
git tag --list "v*" --sort=-v:refname
gh release list --limit 100
```

Read every tracked version source that exists, including:

- root `VERSION`;
- versioned headings in `CHANGELOG.md`;
- package manifests or generated version modules found in the repository;
- published Git tags matching `vX.Y.Z`;
- published GitHub releases and their target commits.

The highest valid published `vX.Y.Z` tag and GitHub release are the
authoritative released baseline. They must describe the same version and
commit. Repository version files on `develop` must agree with the latest
released baseline unless the repository documents a deliberate post-release
version update. If tags, releases, version files, or changelog entries
disagree, stop release preparation and reconcile the discrepancy before
selecting the next version. Do not infer a version from branch names, commit
counts, dates, image tags, or private deployment state.

Choose the next version only after reviewing changes since the authoritative
release:

- increment `PATCH` for backward-compatible fixes;
- increment `MINOR` for backward-compatible functionality;
- increment `MAJOR` for breaking changes.

If the requested version is not explicit, present the discovered baseline,
the proposed version, and the SemVer rationale for human confirmation before
writing release metadata.

## 2. Create the release branch

Verify that `origin/develop` contains the intended changes and then create:

```bash
git switch --create release/X.Y.Z origin/develop
```

Do not add unrelated feature work. Update the root `VERSION` to exactly
`X.Y.Z`.

Convert the single `## [Unreleased]` section in `CHANGELOG.md` into:

```markdown
## [vX.Y.Z] - YYYY-MM-DD
```

The date is the intended release date in UTC. A release branch targeting
`main` must not retain an `Unreleased` section because `main` must contain only
released entries. Restore a new empty `Unreleased` section on `develop` in a
separate post-release change after the release is merged.

Before committing, verify there is exactly one `VERSION` value, exactly one
changelog entry for `vX.Y.Z`, and no other tracked release declaration with a
different version.

## 3. Write sanitized release notes

Derive notes from the release changelog and the public commit or pull-request
history since the authoritative tag. Organize them under applicable `Added`,
`Changed`, `Fixed`, and `Removed` headings. Explain user-visible behavior,
compatibility impact, migration steps, and validation precisely.

Release notes are public. Remove or generalize:

- Azure subscription, tenant, resource-group, resource, registry, cluster,
  environment, deployment, and private DNS names;
- internal hostnames, dashboard or log-query links, incident identifiers, and
  non-public repository or work-item references;
- customer names, user aliases, credentials, tokens, secrets, and unique
  infrastructure identifiers.

Keep public Azure product names when technically useful, but never include a
private Azure resource name. Re-scan both the changelog text and generated
notes before publication. If uncertain whether a name is public, omit or
generalize it.

## 4. Validate and open the release pull request

Discover validation from the repository itself: CI workflows, contributor
instructions, README commands, package scripts, and existing test
configuration. Run the smallest existing checks that cover release metadata,
then run any required release, unit, lint, build, or packaging checks. Record
the exact commands and results in the pull request. A documentation-only
change does not justify running deployment scripts or touching Azure.

Commit the release metadata with the repository's required commit trailers,
push the branch, and open a pull request to `main`. The pull request must state:

- authoritative prior version and proposed version;
- SemVer rationale;
- metadata consistency results;
- sanitized release-note summary;
- exact validation performed;
- that publication remains blocked on explicit human approval.

Resolve all required checks and review feedback before merging.

## 5. Require explicit human publication approval

Branch creation, metadata commits, validation, and pull-request creation are
preparation. After the release pull request is merged, stop and request an
explicit human approval that names `vX.Y.Z` and authorizes publication.

Without that approval, do not:

- create, move, force, push, or delete a release tag;
- create, publish, edit, or delete a GitHub release;
- publish or promote a package or container image;
- run a deployment or mutate an Azure resource.

Approval for release preparation, pull-request merge, or a previous release
does not authorize publication. Immediately before an approved publication,
fetch remote state again and verify that `origin/main`, `VERSION`, the
changelog entry, tag name, release title, notes, and intended commit still
match. Use an annotated `vX.Y.Z` tag and create the GitHub release with the
exact title `vX.Y.Z`. Never publish from an unmerged release branch.

## 6. Roll back or reconcile safely

Prefer forward correction and preserve an auditable history:

- Before merge, correct the release branch and update its pull request.
- After merge but before publication, use a corrective pull request; do not
  publish inconsistent metadata.
- If publication partially succeeds, stop. Inventory the remote tag, GitHub
  release, packages, images, deployments, and their target commits. Report the
  mismatch and propose the least destructive reconciliation.
- If the correct tag exists but the GitHub release is missing, verify the tag
  target and notes, then request explicit human approval before creating the
  missing release.
- If a tag, release, package, image, or deployment is wrong, do not silently
  overwrite, retag, delete, unpublish, promote, or redeploy it. Obtain explicit
  human approval for the named corrective action and document what consumers
  may already have observed.
- Never reuse a published version for different content. When immutability or
  consumer visibility makes correction unsafe, prepare a new patch release.

Finish by reconciling the tag, release title, release target, `VERSION`,
changelog, and published artifact versions, and record any corrective action
in public-safe release notes.
