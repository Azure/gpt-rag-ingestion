"""Mutation fixtures for the repository-local gates; no runtime imports."""

from __future__ import annotations

import copy
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / ".github" / "scripts" / "quality_policy.py"
spec = importlib.util.spec_from_file_location("quality_policy", SCRIPT)
assert spec is not None and spec.loader is not None
quality = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = quality
spec.loader.exec_module(quality)


def rules(findings):
    return {finding["rule"] for finding in findings}


@pytest.mark.parametrize(
    "sources",
    [
        {"main.py": "import dependencies", "dependencies.py": "import main"},
        {"a.py": "import b", "b.py": "import c", "c.py": "import a"},
        {"main.py": "from api import admin", "api/__init__.py": "",
         "api/admin.py": "def run():\n import main"},
        {"a.py": "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n import b",
         "b.py": "import a"},
        {"a/__init__.py": "from . import child", "a/child.py": "import a"},
    ],
)
def test_all_static_cycles_fail(sources):
    result = quality.analyze_sources(sources)
    assert "dependency-cycle" in rules(result)
    cycle = next(item["dependency_path"] for item in result if item["rule"] == "dependency-cycle")
    assert cycle[0] == cycle[-1]
    assert len(cycle) >= 3


def test_sibling_import_does_not_invent_facade_cycle():
    sources = {
        "p/__init__.py": "from . import a",
        "p/a.py": "from . import b",
        "p/b.py": "VALUE = 1",
    }
    assert not quality.analyze_sources(sources)


def test_symbol_import_retains_real_facade_edge():
    sources = {
        "p/__init__.py": "from .a import value",
        "p/a.py": "from p import value",
    }
    assert "dependency-cycle" in rules(quality.analyze_sources(sources))


@pytest.mark.parametrize("statement", ["import main", "from api import admin", "import bridge"])
def test_forbidden_direct_and_transitive_imports(statement):
    sources = {
        "jobs/task.py": statement, "jobs/__init__.py": "",
        "api/__init__.py": "", "api/admin.py": "",
        "main.py": "", "bridge.py": "import main",
    }
    contracts = {"forbidden": [{"source": "jobs", "target": "main"},
                               {"source": "jobs", "target": "api"}]}
    assert "forbidden-dependency" in rules(quality.analyze_sources(sources, contracts))


@pytest.mark.parametrize(
    "statement",
    ["from owner._impl import value", "import owner._impl as x",
     "from owner import _secret", "import owner as x\nvalue = x._secret",
     "from owner import public\nvalue = public._secret",
     "from owner.public import Public as Alias\nvalue = Alias._secret"],
)
def test_cross_area_private_access_fails(statement):
    sources = {
        "consumer.py": statement, "owner/__init__.py": "_secret = 1",
        "owner/_impl.py": "value = 1", "owner/public.py": "_secret = 1\nclass Public:\n _secret = 2",
    }
    assert "private-access" in rules(quality.analyze_sources(sources))


def test_explicit_public_facade_may_forward_to_private_owner():
    sources = {
        "consumer.py": "from owner import value",
        "owner/__init__.py": "from ._impl import value",
        "owner/_impl.py": "value = 1",
    }
    assert not quality.analyze_sources(sources)


def test_relative_private_member_alias_fails_outside_area():
    sources = {
        "p/__init__.py": "", "p/a/__init__.py": "",
        "p/a/client.py": "from ..b import _secret as value",
        "p/b/__init__.py": "_secret = 1",
    }
    contracts = {"areas": {"p.a.client": "a", "p.b": "b"}}
    assert "private-access" in rules(quality.analyze_sources(sources, contracts))


def test_unresolved_first_party_target_is_not_dropped():
    result = quality.analyze_sources({"p/__init__.py": "", "main.py": "import p.missing"})
    assert "unresolved-import" in rules(result)


def test_literal_dynamic_import_joins_cycle():
    sources = {"a.py": "import importlib as loader\nloader.import_module('b')",
               "b.py": "import a"}
    assert "dependency-cycle" in rules(quality.analyze_sources(sources))


@pytest.mark.parametrize(
    "source",
    ["import importlib\nimportlib.import_module(name)",
     "from importlib import import_module as load\nload(name)",
     "__import__(name)"],
)
def test_uninventoried_variable_dynamic_import_fails(source):
    assert "dynamic-import" in rules(quality.analyze_sources({"main.py": source}))


@pytest.mark.parametrize(
    "source",
    [
        "try:\n work()\nexcept:\n pass",
        "try:\n work()\nexcept Exception:\n raise",
        "try:\n work()\nexcept BaseException:\n logging.exception('failed')\n raise",
        "from builtins import Exception as Error\ntry:\n work()\nexcept Error:\n raise",
        "import builtins as b\ntry:\n work()\nexcept b.Exception:\n raise",
        "try:\n work()\nexcept (ValueError, Exception):\n raise",
        "try:\n work()\nexcept* Exception:\n raise",
        "Error = Exception\ntry:\n work()\nexcept Error:\n raise",
        "try:\n work()\nexcept errors():\n raise",
    ],
)
def test_broad_handlers_require_records_even_when_ruff_exempts_them(source):
    findings = quality.exception_findings({"a.py": source}, [], {})
    assert "unapproved-handler" in rules(findings)


def test_narrow_handler_is_not_broad():
    assert not quality.exception_findings(
        {"a.py": "try:\n int('x')\nexcept ValueError:\n raise"}, [], {}
    )


def handler_record(source):
    site = quality.handler_sites({"a.py": source})[0]
    return {
        "id": "a-boundary", "module_id": "a", "symbol": site["symbol"],
        "handler_fingerprint": site["handler_fingerprint"],
        "caught_types": site["caught_types"], "boundary": "test boundary",
        "reason": "Translate a dependency error to an explicit caller failure.",
        "failure_outcome": "propagation", "diagnostic_path": "caller exception",
        "evidence_tests": ["tests/test_failure.py::test_error"],
        "review": {"status": "active", "reference": "https://github.com/test/repo/pull/1"},
        "review_by_stage": "initial",
    }


def test_exact_exception_requires_passing_non_skipped_evidence():
    source = "try:\n work()\nexcept Exception:\n raise"
    record = handler_record(source)
    test_id = record["evidence_tests"][0]
    accepted = []
    assert not quality.exception_findings(
        {"a.py": source}, [record], {test_id: "passed"}, accepted_ids=accepted,
    )
    assert accepted == ["a-boundary"]
    for result in (None, "skipped", "failed", "error"):
        assert "exception-evidence" in rules(
            quality.exception_findings({"a.py": source}, [record], {test_id: result})
        )


def test_exception_record_cannot_cover_changed_body_or_catch():
    source = "try:\n work()\nexcept Exception:\n raise"
    record = handler_record(source)
    changed = source.replace("raise", "return_value = None")
    result = quality.exception_findings({ "a.py": changed}, [record], {})
    assert {"unapproved-handler", "stale-exception"} <= rules(result)


def test_exception_past_its_review_stage_cannot_be_used():
    source = "try:\n work()\nexcept Exception:\n raise"
    record = handler_record(source)
    record["review_by_stage"] = "retired-stage"
    accepted = []
    result = quality.exception_findings(
        {"a.py": source}, [record],
        {record["evidence_tests"][0]: "passed"}, accepted_ids=accepted,
    )
    assert "invalid-exception" in rules(result)
    assert not accepted


def test_proposed_exception_is_not_an_approval():
    source = "try:\n work()\nexcept Exception:\n raise"
    record = handler_record(source)
    record["review"]["status"] = "proposed"
    accepted = []
    result = quality.exception_findings({"a.py": source}, [record], {}, accepted_ids=accepted)
    assert {"unapproved-handler", "exception-review-pending"} <= rules(result)
    assert not accepted


def diagnostic(**updates):
    finding = {
        "module_id": "audit", "symbol": "sanitize", "source_fingerprint": "syntax-a",
        "rule": "assignment", "message_fingerprint": "type-a", "occurrences": 1,
    }
    finding.update(updates)
    return finding


def test_unchanged_individual_type_debt_passes():
    assert not quality.compare_diagnostics([diagnostic()], [diagnostic()])


@pytest.mark.parametrize(
    "current",
    [[diagnostic(message_fingerprint="type-b")],
     [diagnostic(source_fingerprint="syntax-b")],
     [diagnostic(occurrences=2)],
     [diagnostic(), diagnostic(symbol="new_function")]],
)
def test_same_count_substitution_duplication_and_new_site_fail(current):
    assert "new-type-debt" in rules(quality.compare_diagnostics(current, [diagnostic()]))


def test_retired_debt_must_be_removed_and_cannot_be_reused():
    assert "stale-type-debt" in rules(quality.compare_diagnostics([], [diagnostic()]))
    assert "new-type-debt" in rules(quality.compare_diagnostics([diagnostic()], []))


def scope():
    return {"schema_version": 1, "module_ids": ["a"], "move_map": []}


def test_scope_is_monotonic_and_new_files_are_covered():
    result = quality.compare_scope(scope(), scope(), {"a": "a.py", "b": "b.py"}, {"a": "a.py"})
    assert "new-module-untyped" in rules(result)
    dropped = scope()
    dropped["module_ids"] = []
    assert "typing-scope-reduction" in rules(
        quality.compare_scope(scope(), dropped, {"a": "a.py"}, {"a": "a.py"})
    )


def test_one_to_one_move_preserves_identity():
    moved = scope()
    moved["move_map"] = [{"module_id": "a", "old_path": "a.py", "new_path": "p/a.py"}]
    assert not quality.compare_scope(scope(), moved, {"a": "p/a.py"}, {"a": "a.py"})


def test_unrecorded_move_or_copied_identity_fails():
    assert "unreviewed-move" in rules(
        quality.compare_scope(scope(), scope(), {"a": "p/a.py"}, {"a": "a.py"})
    )
    moved = scope()
    moved["move_map"] = [
        {"module_id": "a", "old_path": "a.py", "new_path": "p/a.py"},
        {"module_id": "a", "old_path": "a.py", "new_path": "p/b.py"},
    ]
    assert "ambiguous-move" in rules(
        quality.compare_scope(scope(), moved, {"a": "p/a.py"}, {"a": "a.py"})
    )


def test_annotation_deletion_and_inline_suppression_fail():
    before = {"a.py": "def f(value: int) -> int:\n return value"}
    after = {"a.py": "def f(value):\n return value  # type: ignore"}
    result = quality.source_policy(before, after, {"a": "a.py"}, {"a": "a.py"}, ["a"])
    assert {"annotation-removal", "new-suppression"} <= rules(result)


@pytest.mark.parametrize("directive", ["# mypy: ignore-errors", "# ruff: noqa",
                                      "# noqa: F821", "# type: ignore[attr-defined]"])
def test_inline_directives_cannot_weaken_policy(directive):
    result = quality.source_policy({"a.py": "x = 1"}, {"a.py": f"x = 1  {directive}"},
                                   {"a": "a.py"}, {"a": "a.py"}, ["a"])
    assert "new-suppression" in rules(result)


def test_count_neutral_suppression_move_is_rejected():
    before = {"a.py": "x = unknown  # type: ignore[name-defined]\ny = 1"}
    after = {"a.py": "x = 1\ny = another_unknown  # type: ignore[name-defined]"}
    result = quality.source_policy(before, after, {"a": "a.py"}, {"a": "a.py"}, ["a"])
    assert "new-suppression" in rules(result)


def test_malformed_policy_and_unknown_versions_error(tmp_path):
    path = tmp_path / "policy.json"
    path.write_text("{", encoding="utf-8")
    with pytest.raises(quality.QualityError):
        quality.read_record(path)
    path.write_text('{"schema_version": 1, "schema_version": 99}', encoding="utf-8")
    with pytest.raises(quality.QualityError, match="Duplicate JSON"):
        quality.read_record(path)
    path.write_text(json.dumps({"schema_version": 99}), encoding="utf-8")
    with pytest.raises(quality.QualityError):
        quality.read_record(path)


def test_tool_timeout_and_crash_are_errors(monkeypatch):
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired("mypy", 1)
    monkeypatch.setattr(subprocess, "run", timeout)
    with pytest.raises(quality.QualityError):
        quality.run_tool(["mypy"], Path.cwd())
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: subprocess.CompletedProcess(a, 2, "", "crash"))
    with pytest.raises(quality.QualityError):
        quality.run_tool(["mypy"], Path.cwd())


def report(name):
    return {"schema_version": 1, "check_name": name, "status": "passed",
            "base_sha": "b" * 40, "head_sha": "c" * 40, "policy_sha": "d" * 64}


def test_aggregate_requires_fixed_actual_results_and_fresh_reports():
    jobs = {name: "success" for name in quality.REQUIRED_CHECKS}
    reports = {name: report(name) for name in quality.REQUIRED_CHECKS if name != "unit-tests"}
    assert not quality.aggregate(jobs, reports, "b" * 40, "c" * 40)
    for bad in ("skipped", "cancelled", "failure", "neutral", None):
        changed = dict(jobs, **{"unit-tests": bad})
        assert quality.aggregate(changed, reports, "b" * 40, "c" * 40)
    stale = copy.deepcopy(reports)
    stale["lint"]["head_sha"] = "e" * 40
    assert quality.aggregate(jobs, stale, "b" * 40, "c" * 40)
    assert quality.aggregate(jobs, {}, "b" * 40, "c" * 40)
    assert quality.aggregate({}, reports, "b" * 40, "c" * 40)


@pytest.fixture
def policy_repository(tmp_path):
    """A committed minimum policy and a separate trusted evaluator copy."""
    root = tmp_path / "repo"
    root.mkdir()
    trusted = tmp_path / "trusted"
    trusted.mkdir()
    repository = SCRIPT.parents[2]
    for name in ("quality_policy.py", "check-quality.py", "quality-gate.py", "quality-evidence.py"):
        shutil.copyfile(repository / ".github" / "scripts" / name, trusted / name)
    (root / ".quality").mkdir()
    (root / "main.py").write_text("def value() -> int:\n return 1\n", encoding="utf-8")
    config = (repository / "pyproject.toml").read_text(encoding="utf-8")
    config = config.replace(
        '["telemetry.audit_contract", "telemetry.audit_sanitizer", "jobs.runtime"]',
        '["main"]',
    )
    (root / "pyproject.toml").write_text(config, encoding="utf-8")
    policy = json.loads((repository / ".quality" / "policy.json").read_text(encoding="utf-8"))
    module = next(item for item in policy["modules"] if item["id"] == "main")
    policy["modules"] = [module]
    policy["runtime_roots"] = ["main.py"]
    policy["contracts"] = {"forbidden": [], "areas": {}, "private_access": []}
    records = {
        "policy": policy, "typing-scope": {"schema_version": 1, "module_ids": ["main"], "move_map": []},
        "typing-baseline": {"schema_version": 1, "entries": []},
        "exceptions": {"schema_version": 1, "entries": []},
    }
    for name, record in records.items():
        (root / ".quality" / f"{name}.json").write_text(json.dumps(record), encoding="utf-8")
    commands = [
        ["init", "--quiet"], ["config", "user.name", "Fixture"],
        ["config", "user.email", "fixture@example.test"],
        ["config", "core.autocrlf", "false"],
        ["config", "core.hooksPath", str(tmp_path / "no-hooks")],
        ["add", "."],
        ["commit", "--quiet", "-m", "test: establish policy fixture\n\nCo-authored-by: Copilot App <223556219+Copilot@users.noreply.github.com>"],
    ]
    for command in commands:
        subprocess.run(["git", *command], cwd=root, check=True, capture_output=True)
    return root, trusted


def invoke_check(repository, check):
    root, trusted = repository
    report_path = root / ".artifacts" / f"{check}.json"
    result = subprocess.run(
        [sys.executable, str(trusted / "check-quality.py"), "--repository", str(root),
         "--base-ref", "HEAD", "--check", check, "--report", str(report_path)],
        cwd=root, capture_output=True, text=True, timeout=90,
    )
    return result, json.loads(report_path.read_text(encoding="utf-8"))


@pytest.mark.parametrize("check", ["lint", "typing", "policy"])
def test_clean_reference_cli_passes(policy_repository, check):
    result, evidence = invoke_check(policy_repository, check)
    assert result.returncode == 0, result.stderr + json.dumps(evidence["findings"])
    assert evidence["status"] == "passed"
    integrity = evidence.pop("artifact_integrity")
    assert integrity == quality.digest(evidence)


def test_actual_lint_and_type_regressions_fail(policy_repository):
    root, _ = policy_repository
    (root / "main.py").write_text("def value() -> int:\n return 'bad'\n\nmissing_name\n", encoding="utf-8")
    lint_result, lint_report = invoke_check(policy_repository, "lint")
    assert lint_result.returncode == 1
    assert "F821" in rules(lint_report["findings"])
    type_result, type_report = invoke_check(policy_repository, "typing")
    assert type_result.returncode == 1
    assert "new-type-debt" in rules(type_report["findings"])
    assert type_report["coverage"]["typing"]["in_scope_diagnostics"][0]["file"] == "main.py"


def test_candidate_cannot_weaken_protected_settings_or_self_approve(policy_repository):
    root, _ = policy_repository
    config = root / "pyproject.toml"
    config.write_text(config.read_text().replace(
        'select = ["E722", "BLE001", "PGH003", "PGH004", "RUF100", "F821", "F822", "F823"]',
        'select = []'), encoding="utf-8")
    (root / "main.py").write_text("missing_name\n", encoding="utf-8")
    result, report_data = invoke_check(policy_repository, "lint")
    assert result.returncode == 1
    assert "F821" in rules(report_data["findings"])
    (root / ".github" / "scripts").mkdir(parents=True)
    (root / ".github" / "scripts" / "check-quality.py").write_text("raise SystemExit(0)", encoding="utf-8")
    (root / ".github" / "CODEOWNERS").write_text("* @self-approved", encoding="utf-8")
    result, report_data = invoke_check(policy_repository, "policy")
    assert result.returncode == 1
    changed = {item["file"] for item in report_data["findings"] if item["rule"] == "protected-policy-change"}
    assert {"pyproject.toml", ".github/scripts/check-quality.py", ".github/CODEOWNERS"} <= changed


def test_uninventoried_new_module_is_typed_and_policy_fails(policy_repository):
    root, _ = policy_repository
    (root / "new.py").write_text("def wrong() -> int:\n return 'bad'\n", encoding="utf-8")
    result, report_data = invoke_check(policy_repository, "typing")
    assert result.returncode == 1
    assert "new" in report_data["coverage"]["typing"]["blocking_modules"]
    result, report_data = invoke_check(policy_repository, "policy")
    assert result.returncode == 1
    assert "module-inventory" in rules(report_data["findings"])


def test_candidate_baseline_growth_cannot_consume_new_error(policy_repository):
    root, _ = policy_repository
    (root / "main.py").write_text("def value() -> int:\n return 'bad'\n", encoding="utf-8")
    _, evidence = invoke_check(policy_repository, "typing")
    entry = evidence["coverage"]["typing"]["in_scope_diagnostics"][0]
    entry.update(id="self-approved", rationale="candidate claims approval", introduced_at="fake",
                 removal_stage="later", review={"status": "active", "reference": "self"})
    (root / ".quality" / "typing-baseline.json").write_text(
        json.dumps({"schema_version": 1, "entries": [entry]}), encoding="utf-8")
    result, evidence = invoke_check(policy_repository, "typing")
    assert result.returncode == 1
    assert "new-type-debt" in rules(evidence["findings"])
    result, evidence = invoke_check(policy_repository, "policy")
    assert result.returncode == 1
    assert "protected-policy-change" in rules(evidence["findings"])


def test_approved_exact_handler_can_satisfy_ruff_but_still_requires_behavior_evidence(policy_repository):
    root, _ = policy_repository
    source = "def value() -> int:\n try:\n  return int('bad')\n except Exception:\n  return 0\n"
    (root / "main.py").write_text(source, encoding="utf-8")
    record = handler_record(source)
    record["module_id"] = "main"
    (root / ".quality" / "exceptions.json").write_text(
        json.dumps({"schema_version": 1, "entries": [record]}), encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=root, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "--quiet", "-m",
         "test: establish approved boundary fixture\n\nCo-authored-by: Copilot App <223556219+Copilot@users.noreply.github.com>"],
        cwd=root, check=True, capture_output=True,
    )
    result, evidence = invoke_check(policy_repository, "lint")
    assert result.returncode == 0, evidence
    assert evidence["exception_ids_used"] == ["a-boundary"]
    result, evidence = invoke_check(policy_repository, "exceptions")
    assert result.returncode == 1
    assert "exception-evidence" in rules(evidence["findings"])
    (root / "main.py").write_text(source + "\ndef other():\n try:\n  value()\n except Exception:\n  pass\n", encoding="utf-8")
    result, evidence = invoke_check(policy_repository, "lint")
    assert result.returncode == 1
    assert "BLE001" in rules(evidence["findings"])


def test_grimp_does_not_execute_package_initializers(tmp_path):
    (tmp_path / "p").mkdir()
    (tmp_path / "p" / "__init__.py").write_text(
        "raise AssertionError('must not import source')\nfrom . import a\n", encoding="utf-8")
    (tmp_path / "p" / "a.py").write_text("from . import b\n", encoding="utf-8")
    (tmp_path / "p" / "b.py").write_text("VALUE = 1\n", encoding="utf-8")
    result = subprocess.run(
        [sys.executable, "-c", "import grimp; g=grimp.build_graph('p',cache_dir=None); "
         "assert g.find_modules_directly_imported_by('p.a') == {'p.b'}"],
        cwd=tmp_path, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
