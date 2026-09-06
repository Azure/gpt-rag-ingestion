# Python quality gates (bootstrap candidate)

This is the ingestion implementation of
[Azure/GPT-RAG#681](https://github.com/Azure/GPT-RAG/issues/681) and the accepted
[ADR-0005](https://github.com/Azure/GPT-RAG/blob/feature/python-module-boundaries/docs/adr/ADR-0005-python-quality-gates-and-ui-package.md).
It is **not yet an activated, green merge policy**. The draft intentionally
reports inherited broad handlers and lint findings instead of blessing them.
The parent coordination PR is
[Azure/GPT-RAG#689](https://github.com/Azure/GPT-RAG/pull/689); published
contributor documentation is coordinated in
[Azure/GPT-RAG#688](https://github.com/Azure/GPT-RAG/pull/688).

## Contributor commands

Use Python 3.12 and the existing runtime requirements. Quality packages are
development-only and are not installed by the container.

```powershell
python -m pip install -r requirements.txt
python -m pip install -r requirements-quality.txt
python -m pip install pytest pytest-asyncio
git fetch origin develop
$Base = git merge-base HEAD origin/develop
python -m pytest -q tests/test_quality_policy.py tests/test_jobs_runtime.py
python -m pytest tests -q --junitxml=.artifacts/pytest.xml -o junit_family=legacy
python .github/scripts/quality-evidence.py --junit .artifacts/pytest.xml --base-ref $Base --report .artifacts/test-evidence.json
python .github/scripts/check-quality.py --check all --base-ref $Base --test-evidence .artifacts/test-evidence.json --report .artifacts/quality.json
```

Use the PR's actual fetched target SHA for release-target work. `--check`
accepts `all`, `lint`, `typing`, `architecture`, `exceptions`, and `policy`.
`--repository` lets the protected-base evaluator inspect a separate candidate
checkout; ordinary contributors can omit it. `--test-evidence` is needed when
an exception or dynamic-import record references executed behavior tests.
Every accepted test must have passed in that same candidate/run/attempt.
The report's parent directory is created; code and policy are never rewritten.
Exit codes are **0 passed, 1 violations, 2 incomplete/invalid execution**.
An absent report or nonzero process result is not success.

Pinned versions exercised locally: Python 3.12.9, Ruff 0.16.5, mypy 2.3.1,
Import Linter 2.14, Grimp 3.16. The plan's Ruff 0.16.6, Import Linter 2.15 and
Grimp 3.17 candidates were unavailable from the configured package index.
The available versions were installed and exercised rather than describing
the proposed versions as validated. No runtime dependency pin was changed.

## Ownership and coverage

`main.py` remains the application entry point and composes/starts/stops the
APScheduler instance. `jobs/runtime.py` owns its published reference, registry,
cron-key map, running-job dictionary, lock and tracking wrapper. Operator
routes read that interface instead of importing the application. Existing
`main` names re-export the same objects, not duplicate state. Jobs' public
worker exports are lazy so importing the state API does not load workers or
contact App Configuration before startup authentication.

The existing manual reservation ID, 409 guard, APScheduler date-trigger options,
cleanup-on-failure/cancellation and lifespan order are preserved.
The two overwritten, unused SharePoint wrappers in `main.py` were removed;
the later audit-wrapped definitions remain the actual registered functions.
There is no ingestion `src` migration or application factory.

Primary failure reporting is corrected in the follow-up: cron/manual worker
exceptions propagate through the existing `audit_run` context manager, which
emits the failed terminal event. Startup retains ordered, independent job
execution after one job fails. Search writes require matching successful SDK
results; batch deletion uses the SDK's delete operation, counts only confirmed
keys, and treats missing/excess duplicate results as unconfirmed. NL2SQL purge
propagates failed scans and partial deletions instead of reporting successful
totals. Selected-provider read failures now reach governance/startup instead
of silently becoming disabled defaults. Real missing keys still use defaults,
Azure read errors retain bounded retries, and unexpected errors propagate.
The selector order remains ingestion, base, unlabelled; the pinned provider
lets later matching selections replace earlier duplicate keys. Existing
environment opt-in and bootstrap source fallbacks are unchanged.

Successful operator response shapes and authentication are unchanged.
`POST /api/config/apply` reports 500 when schedules could not be applied;
`PUT /api/config` retains 200/applied after confirmed durable writes even if
its existing best-effort local cache refresh fails. The same key is not added
to `failed` merely because of this refresh. Explicit reload/apply operations,
remote writes and schedule changes retain their own failure contracts.
Manual scheduling and affected configuration errors omit downstream payloads.
Audit events never infer success from a missing or malformed result; audit
sanitizer/export/projection failures still cannot fail the primary operation,
and warning logs contain stage/type metadata rather than exception payloads.
Purge run-level failures, including late pages, post-delete counts and
cancellation, cannot publish a finished summary. Owned Search, Blob and
credential cleanup is attempted without replacing the primary result; cleanup
failures produce safe warnings. SDK-result tests deserialize the pinned Search
model, whose readonly response fields are not populated by constructor kwargs.
Real audit export failures do not change confirmed adapter outcomes.

The write-adapter evidence does **not** cover `/ingest-documents`, which uploads
directly through the SDK. Existing endpoint tests still run, but full direct
upload failure-contract closure is not claimed by these wrapper tests.

`.quality/policy.json` inventories all runtime modules, including flat
`main`, `dependencies`, and `constants`, package roots, and namespace chunkers.
Syntax-derived exports describe the bootstrap surface, not a promise that
every incidental imported name is a new supported external API. The concrete
surface policy still needs maintainer review. Leading-underscore modules and
members are private across areas. Four exact existing compatibility accesses
are proposed for review, without granting access to any other member:

| Consumer | Existing helper | Why preserve it |
| --- | --- | --- |
| `chunking.chunkers.doc_analysis_chunker` | `utils.file_utils._safe_delete` | Existing temporary-file cleanup |
| `jobs.blob_storage_indexer` | `utils.file_utils._safe_delete` | Same cleanup implementation, no duplicate helper |
| `main` | `api.admin._cleanup_old_runs` | Existing startup/cron log-retention callable |
| `main` | `jobs.sharepoint_ingestion_config._make_chunk_key` | Existing document chunk-key algorithm |

The graph includes static imports at every syntax depth, aliases, relative
and `TYPE_CHECKING` imports, and literal dynamic imports. It resolves actual
sibling modules without inventing edges for implicit parent initialization.
Grimp collects the importable packages independently; its overlapping edges
must agree with the AST collector. Import Linter enforces package-to-API
prohibitions; the complete graph also covers flat roots, all cycles, private
access and transitive forbidden directions. Unknown variable dynamic loading
requires an exact target inventory and executed behavior evidence.
The resolver retains conflicting lexical bindings instead of letting an
unrelated local import hide a module-level alias. It follows first-party
reexports, class-member exception aliases and static attribute assignments;
unresolved catch types cannot consume an approval. A changed resolved catch
type invalidates its record even when the handler body itself is unchanged.
Implicit exception/loader builtins remain possible bindings when the
conservative collector cannot prove that a local shadow applies at the site.
Variable-loader records are bound to the qualified symbol and call syntax,
must be active, and cannot be reused for a second site. Relative literal
imports use Python's package resolution, including keyword package arguments;
escaping or unbounded reflective loaders fail explicitly.

Blocking typing starts with `telemetry/audit_contract.py`,
`telemetry/audit_sanitizer.py`, and the new `jobs/runtime.py`. New runtime files
are discovered independently of candidate policy and automatically checked.
The next stages are configuration, delegated authentication, and
retrieval/operator boundaries. Imported diagnostics outside blocking coverage
remain visible in reports; no blanket `ignore_missing_imports` is used.

The initial baseline is empty. Nonempty baselines must identify each inherited
diagnostic by stable module ID, qualified symbol, normalized syntax fingerprint,
rule, message fingerprint and exact multiplicity, with review and removal
stage. Same-count substitutions, duplication and stale allowances fail.
Moves require one-to-one mappings retaining coverage and identity. Removed
annotations, shifted/new suppressions, nested configs and reduced scope fail
policy review. Mypy's cache is not a debt baseline.
Cross-root moves, and moves that change coverage of a protected import
relationship, also require policy review even if the stable module ID and
declared area are retained. Record parsing rejects missing/unknown fields,
invalid nested types and unsupported lifecycle values before running checks.
Boolean or floating-point schema versions are not version 1. The exact
requirements manifest must agree with the policy toolchain.

## Broad handlers and remaining acceptance

The exception ledger contains eight exact **proposed**, not active, records.
Four cover audit boundaries: unexpected sanitizer failure, exporter failure, primary run failure
observation/propagation, and document-audit projection failure. Each cites its
own source fingerprint, boundary-specific rationale and executed failure tests.
The other four cover the established post-write local refresh contract and
each of the purger's Search, Blob and credential cleanup boundaries. They do
not authorize primary-operation success fallbacks.
**No inherited handler is automatically approved.** The syntax check includes
bare handlers, builtins aliases, tuples,
exception groups, and logged/re-raised catches exempted by Ruff BLE001.
Indirect catch types that cannot be established from syntax require review.
An exception needs an exact source/handler fingerprint, necessity, boundary,
failure outcome, diagnostic path, review reference/stage and passing test IDs.
Changed, expired-stage, stale, ambiguous or unused records fail. Candidate
review strings are not authorization. Ruff BLE001 is waived only for the
exact header of a matching active protected-base record, never an entire
file or rule. This does not waive the separate required exceptions job:
without passing same-run behavior evidence, the aggregate still fails.
Proposal records cannot waive either check.

The repository still contains inherited broad-handler and lint violations.
Remediation must classify each operation and add boundary-specific failure
tests before narrowing or proposing a record. In particular, existing
configuration fallbacks and indexing/deletion success-shaped fallbacks cannot
be approved merely because they log. Audit sanitization/export remains
contractually best-effort; it must not become a primary-operation failure,
nor be used to excuse indexing, deletion, configuration or authentication
failures. The concrete Search/NL2SQL/governance/configuration defects above
have regression coverage, but this draft does not claim the entire legacy
failure inventory is complete: T025 and all of T028 / SC-003 still need review
and remaining boundary-specific work.

## CI trust and separate administrative activation

`.github/workflows/tests.yml` keeps the existing pytest job and adds actual
`lint`, `typing`, `architecture`, `exceptions`, and `policy` jobs. The
always-evaluated **quality-gate** depends on those jobs and `unit-tests` in the
same workflow. Existing frontend assets/workflows are unchanged.
Actions are SHA-pinned, permissions are read-only, credentials are not
persisted, and no privileged `pull_request_target` execution is introduced.

For normal PRs the workflow extracts the protected target commit and executes
its evaluator, configuration and tool pins against the candidate. Candidate
changes to policy/checker/workflow/CODEOWNERS cannot self-authorize; policy
changes remain explicit failures pending the protected review route.
Ruff/mypy use explicit base configuration and bounded uncached subprocesses.
The aggregate requires actual success results and fresh reports bound to
base/head/policy/source, run and attempt, with content-integrity digests.
Missing, skipped, neutral, cancelled, error, mismatched or stale results fail.
The digests detect stale/corrupt artifacts; they are not signatures or a
replacement for job status and protected review.

On this first PR the base has no evaluator. Candidate tooling is exercised
for bootstrap evidence, but `policy-bootstrap` deliberately blocks an ordinary
green claim. A repository administrator must **separately**:

1. Review/resolve every inherited violation and concrete surface/exception
   policy; approve the bootstrap through the auditable policy-change process.
2. Protect `.quality`, tool config/pins, checker/evidence scripts, workflow,
   CODEOWNERS and quality fixtures with real latest-head code-owner review.
   `@placerda` was verified as an administrator through GitHub's permission API;
   CODEOWNERS text alone does not enforce review or authorize self-approval.
3. Configure required `quality-gate` and existing test checks for development
   and release targets, dismiss stale approvals and restrict bypass. Policy
   repair needs an explicit administrative review/override, never a normal
   candidate-written approval field.
4. Prove a clean reference PR can pass and controlled failing/skipped/stale/
   self-approval PRs cannot merge under the active rules.

No settings were changed by this work. Bootstrap review, actual rule
activation, controlled merge-eligibility evidence and latest-head review are
pending, not implied by the YAML or this document.

## Compatibility, recovery and evidence

The source baseline is ingestion `38a395586ee1d440a8e1ca8233413f8c25b3fdc2`
(`v2.7.3`), with shipped orchestrator `v4.1.1`
(`9b64a5b962067161cb55252c6e0917a2738ba984`) and UI `v2.6.2`
(`f59cca919f0bc59631d7bba7f3e223dff3718244`). No unmerged peer is required.
Schema bytes/hashes, audit event formats, App Configuration selectors,
credentials, successful API response shapes, `VERSION` and runtime dependency
pins remain unchanged. The `/config/apply` 200-to-500 correction on genuine
application failure and propagated worker/provider errors are observable
contract-restoring changes, not behaviorally identical failure paths.
Exact candidate SHA and command results belong in the component PR.

Recovery requires no data/configuration migration: revert the component slice
through a reviewed PR or, under separate deployment authorization, restore
the previous compatible ingestion artifact. If future release pins change,
restore the full previous manifest combination. Roll forward a defective gate
through protected policy review rather than silently disabling required checks.
Code or artifact rollback does **not** restore deleted Search documents or undo
persisted App Configuration writes. Any required data recovery must restore
authoritative source data and reingest under separate authorization; previous
configuration values must likewise be restored and applied separately.
No deployment, image publication, live Search validation, cross-component
integration or artifact recovery rehearsal was authorized or performed here.
Local unit evidence is not a substitute for those acceptance items.
