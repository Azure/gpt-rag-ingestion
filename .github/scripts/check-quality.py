"""Non-mutating repository quality command. Exit 0/1/2: pass/violations/error."""

from __future__ import annotations

import argparse
import ast
from importlib.metadata import PackageNotFoundError, version
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import tomllib

import quality_policy as q


LOG = logging.getLogger("ingestion.quality")
POLICY_FILES = (".quality/policy.json", ".quality/typing-scope.json",
                ".quality/typing-baseline.json", ".quality/exceptions.json")
PROTECTED_FILES = (".github/scripts/check-quality.py", ".github/scripts/quality_policy.py",
                   ".github/scripts/quality-evidence.py", ".github/scripts/quality-gate.py",
                   "tests/test_quality_policy.py",
                   ".github/workflows/tests.yml", ".github/CODEOWNERS", "requirements-quality.txt")


def git(root: Path, *args: str, missing: bool = False) -> str:
    result = subprocess.run(["git", *args], cwd=root, capture_output=True, text=True,
                            encoding="utf-8", timeout=30)
    if result.returncode:
        if missing and result.returncode == 128:
            return ""
        raise q.QualityError(f"Git input unavailable: {args[0]}")
    return result.stdout


def discover(root: Path) -> dict[str, str]:
    paths = git(root, "ls-files", "--cached", "--others", "--exclude-standard", "-z", "*.py").split("\0")
    sources = {}
    for path in sorted(set(paths)):
        if not path or path.startswith(q.NON_RUNTIME):
            continue
        absolute = root / path
        if absolute.is_symlink() or not absolute.resolve().is_relative_to(root):
            raise q.QualityError(f"Source escapes repository: {path}")
        if absolute.is_file():
            sources[path] = absolute.read_text(encoding="utf-8-sig")
    if not sources:
        raise q.QualityError("No runtime sources discovered")
    return sources


def parse_record(text: str, name: str) -> dict:
    try:
        result = json.loads(text, object_pairs_hook=q.unique_keys)
    except json.JSONDecodeError as exc:
        raise q.QualityError(f"Invalid {name}") from exc
    if not isinstance(result, dict) or result.get("schema_version") != 1:
        raise q.QualityError(f"Unsupported {name} version")
    return result


def validate_records(records: list[dict]) -> dict[str, str]:
    policy, scope, baseline, exceptions = records
    required = {"runtime_roots", "modules", "contracts", "toolchain", "required_checks",
                "dynamic_imports", "review", "source_revision"}
    if not required <= policy.keys() or policy["required_checks"] != list(q.REQUIRED_CHECKS):
        raise q.QualityError("Incomplete policy or unexpected required check set")
    modules = {}
    names = set()
    paths = set()
    for module in policy["modules"]:
        fields = {"id", "path", "import_name", "area", "public_exports", "private_modules",
                  "allowed_importers", "legacy_aliases", "typing_status", "responsibilities", "source_revision"}
        if not fields <= module.keys():
            raise q.QualityError("Incomplete module surface")
        path = module["path"]
        if (module["id"] in modules or path in paths or module["import_name"] in names
                or Path(path).is_absolute() or ".." in Path(path).parts
                or path != path.replace("\\", "/") or not path.endswith(".py")
                or module["import_name"] != q.module_name(path)):
            raise q.QualityError("Duplicate or invalid module identity/path")
        modules[module["id"]] = path
        paths.add(path)
        names.add(module["import_name"])
    if (not isinstance(scope.get("module_ids"), list)
            or len(scope["module_ids"]) != len(set(scope["module_ids"]))
            or not set(scope["module_ids"]) <= modules.keys()
            or not isinstance(scope.get("move_map"), list)):
        raise q.QualityError("Invalid typing scope")
    for record, field in ((baseline, "entries"), (exceptions, "entries")):
        if not isinstance(record.get(field), list):
            raise q.QualityError(f"Missing {field} list")
        ids = set()
        for entry in record[field]:
            if (not entry.get("id") or entry["id"] in ids or entry.get("module_id") not in modules
                    or not isinstance(entry.get("review"), dict)):
                raise q.QualityError("Invalid or duplicate debt/exception identity")
            ids.add(entry["id"])
    for entry in baseline["entries"]:
        if (not all(entry.get(field) for field in (*q.DIAGNOSTIC_KEY, "rationale", "introduced_at", "removal_stage"))
                or entry["module_id"] not in scope["module_ids"]
                or entry["review"].get("status") != "active"
                or not entry["review"].get("reference")):
            raise q.QualityError("Type debt requires an individually reviewed inherited finding")
    q.compare_diagnostics([], baseline["entries"])
    for entry in exceptions["entries"]:
        if not all(entry.get(field) for field in (
            "symbol", "handler_fingerprint", "caught_types", "boundary", "reason",
            "failure_outcome", "diagnostic_path", "evidence_tests", "review_by_stage",
        )):
            raise q.QualityError("Incomplete exception justification")
    if not policy["modules"] or not isinstance(policy["toolchain"], dict) or not policy["toolchain"]:
        raise q.QualityError("Missing modules or toolchain")
    return modules


def check_versions(policy: dict) -> dict[str, str]:
    installed = {}
    for distribution, expected in policy["toolchain"].items():
        try:
            actual = version(distribution)
        except PackageNotFoundError as exc:
            raise q.QualityError(f"Missing quality dependency: {distribution}") from exc
        if actual != expected:
            raise q.QualityError(f"{distribution}: expected {expected}, installed {actual}")
        installed[distribution] = actual
    if sys.version_info[:2] != (3, 12):
        raise q.QualityError("Run quality gates with Python 3.12")
    return installed


def policy_settings(config: str) -> dict:
    try:
        tool = tomllib.loads(config)["tool"]
        return {key: tool[key] for key in ("ruff", "mypy", "importlinter")}
    except (KeyError, tomllib.TOMLDecodeError) as exc:
        raise q.QualityError("Missing or invalid tool configuration") from exc


def policy_findings(root: Path, base: str, current: list[dict], protected: list[dict],
                    sources: dict[str, str], modules: dict, base_modules: dict,
                    bootstrap: bool) -> list[dict]:
    policy, scope, baseline, exceptions = current
    base_policy, base_scope, base_baseline, base_exceptions = protected
    findings = []
    if bootstrap:
        findings.append(q.finding("policy-bootstrap", reason=(
            "Protected base has no quality policy. Bootstrap requires maintainer review; enforcement is not active."
        )))
    for name in PROTECTED_FILES:
        previous = git(root, "show", f"{base}:{name}", missing=True)
        now = (root / name).read_text(encoding="utf-8") if (root / name).is_file() else ""
        if previous != now:
            findings.append(q.finding("protected-policy-change", name,
                                      reason="Requires latest-head maintainer review, not candidate approval text"))
    current_config = (root / "pyproject.toml").read_text(encoding="utf-8")
    old_config = git(root, "show", f"{base}:pyproject.toml", missing=True)
    if old_config and policy_settings(old_config) != policy_settings(current_config):
        findings.append(q.finding("protected-policy-change", "pyproject.toml", reason="Tool settings changed"))
    for field in ("runtime_roots", "contracts", "toolchain", "required_checks", "dynamic_imports", "review"):
        if policy[field] != base_policy[field]:
            findings.append(q.finding("protected-policy-change", ".quality/policy.json", reason=field))
    old_surfaces = {module["id"]: module for module in base_policy["modules"]}
    for surface in policy["modules"]:
        old = old_surfaces.get(surface["id"])
        if old:
            # Paths/import names may move one-to-one; public ownership cannot self-approve.
            protected_fields = ("area", "public_exports", "private_modules", "allowed_importers", "legacy_aliases")
            if any(surface[field] != old[field] for field in protected_fields):
                findings.append(q.finding("protected-policy-change", surface["path"], reason="Module surface changed"))
    for name, entries, old_entries in (
        ("typing-baseline", baseline["entries"], base_baseline["entries"]),
        ("exceptions", exceptions["entries"], base_exceptions["entries"]),
    ):
        if any(entry not in old_entries for entry in entries):
            findings.append(q.finding("protected-policy-change", f".quality/{name}.json",
                                      reason="New or changed allowance needs protected review"))
    findings.extend(q.compare_scope(base_scope, scope, modules, base_modules))
    base_sources = {path: git(root, "show", f"{base}:{path}", missing=True) for path in base_modules.values()}
    findings.extend(q.source_policy(base_sources, sources, {k: v for k, v in modules.items() if v in sources},
                                   base_modules, sorted(set(scope["module_ids"]) | set(base_scope["module_ids"]))))
    if set(modules.values()) != sources.keys():
        for path in sorted(sources.keys() ^ set(modules.values())):
            findings.append(q.finding("module-inventory", path, reason="Discovered sources and inventory differ"))
    expected_roots = {path.split("/")[0] for path in sources}
    if expected_roots != set(policy["runtime_roots"]):
        findings.append(q.finding("runtime-roots", reason="All discovered flat files and packages must be inventoried"))
    files = git(root, "ls-files", "--cached", "--others", "--exclude-standard", "-z").split("\0")
    for path in files:
        if Path(path).name in {"ruff.toml", ".ruff.toml", "mypy.ini", ".mypy.ini", "setup.cfg", "pyproject.toml"} and path != "pyproject.toml":
            findings.append(q.finding("nested-tool-config", path, reason="Nested configuration requires protected review"))
    return findings


def lint(root: Path, config: Path, sources: dict) -> tuple[list[dict], dict]:
    result = q.run_tool([sys.executable, "-m", "ruff", "check", "--config", str(config),
                         "--output-format", "json", "--no-cache", *sources], root)
    try:
        diagnostics = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise q.QualityError("Ruff did not return JSON") from exc
    if not isinstance(diagnostics, list) or (result.returncode == 1 and not diagnostics):
        raise q.QualityError("Incomplete Ruff execution")
    findings = []
    for item in diagnostics:
        if not all(key in item for key in ("code", "filename", "location", "message")):
            raise q.QualityError("Unsupported Ruff diagnostic")
        file = Path(item["filename"]).resolve().relative_to(root).as_posix()
        findings.append(q.finding(item["code"], file, item["location"]["row"], item["message"]))
    return findings, {}


def typing(root: Path, config: Path, sources: dict, modules: dict, covered: set,
           baseline: dict) -> tuple[list[dict], dict]:
    paths = sorted(modules[module_id] for module_id in covered)
    result = q.run_tool([sys.executable, "-m", "mypy", "--config-file", str(config),
                         "--no-incremental", "--disallow-untyped-defs", "--output", "json", *paths], root)
    errors, imported = [], []
    path_ids = {path: module_id for module_id, path in modules.items()}
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise q.QualityError("Mypy returned an unrecognized diagnostic") from exc
        if not all(key in item for key in ("file", "line", "column", "message", "code", "severity")):
            raise q.QualityError("Incomplete mypy diagnostic")
        if item["severity"] not in {"error", "note"}:
            raise q.QualityError("Unsupported mypy severity")
        if item["severity"] != "error":
            continue
        path = Path(item["file"])
        path = (root / path).resolve() if not path.is_absolute() else path.resolve()
        if not path.is_relative_to(root):
            raise q.QualityError("Mypy emitted an out-of-repository source error")
        relative = path.relative_to(root).as_posix()
        diagnostic = q.finding(item["code"], relative, item["line"], item["message"])
        module_id = path_ids.get(relative)
        if module_id not in covered:
            imported.append(diagnostic)
            continue
        symbol, fingerprint = q.source_context(ast.parse(sources[relative]), item["line"])
        diagnostic.update(module_id=module_id, symbol=symbol, source_fingerprint=fingerprint,
                          message_fingerprint=q.digest(item["message"].replace(str(root), "<repo>")),
                          occurrences=1)
        errors.append(diagnostic)
    if result.returncode == 1 and not errors and not imported:
        raise q.QualityError("Mypy failed without structured error diagnostics")
    if result.returncode == 0 and (errors or imported):
        raise q.QualityError("Mypy exit status contradicts its diagnostics")
    findings = q.compare_diagnostics(errors, baseline["entries"])
    return findings, {"in_scope_diagnostics": errors, "imported_diagnostics": imported,
                      "blocking_modules": sorted(covered),
                      "uncovered_modules": sorted(modules.keys() - covered),
                      "baseline_entries": len(baseline["entries"])}


def architecture(root: Path, config: Path, sources: dict, policy: dict) -> tuple[list[dict], dict]:
    findings = q.analyze_sources(sources, policy["contracts"], policy["dynamic_imports"])
    script = (
        "import grimp,json; "
        "g=grimp.build_graph('api','chunking','jobs','telemetry','tools','utils',"
        "cache_dir=None,exclude_type_checking_imports=False); "
        "print(json.dumps({m:sorted(g.find_modules_directly_imported_by(m)) for m in sorted(g.modules)}))"
    )
    result = q.run_tool([sys.executable, "-c", script], root)
    if result.returncode:
        raise q.QualityError("Grimp collection failed")
    try:
        package_graph = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise q.QualityError("Grimp returned an invalid graph") from exc
    graph, _ = q.build_import_graph(sources, policy["contracts"], policy["dynamic_imports"])
    for importer, targets in package_graph.items():
        if importer not in graph:
            raise q.QualityError(f"AST inventory missed Grimp module {importer}")
        for target in targets:
            if target not in graph[importer]:
                raise q.QualityError(f"Import collectors disagree: {importer} -> {target}")
    result = q.run_tool(["lint-imports", "--config", str(config), "--no-cache"], root)
    if result.returncode:
        findings.append(q.finding("import-linter-contract", reason="Package contract failed; run lint-imports locally"))
    return findings, {"modules": len(graph), "edges": sum(map(len, graph.values())),
                      "grimp_modules": len(package_graph)}


def load_evidence(path: Path | None, base: str, head: str) -> dict:
    if path is None:
        return {}
    record = q.read_record(path)
    integrity = record.pop("artifact_integrity", None)
    if (integrity != q.digest(record) or record.get("base_sha") != base
            or record.get("head_sha") != head
            or record.get("run_id") != os.environ.get("GITHUB_RUN_ID", "local")
            or record.get("run_attempt") != os.environ.get("GITHUB_RUN_ATTEMPT", "local")):
        raise q.QualityError("Test evidence is stale, unbound or corrupt")
    return record["tests"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", choices=("all", *q.REQUIRED_CHECKS[:-1]), default="all")
    parser.add_argument("--base-ref", required=True)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--test-evidence", type=Path)
    args = parser.parse_args()
    root = args.repository.resolve()
    started = time.monotonic()
    report = {
        "schema_version": 1, "repository": "Azure/gpt-rag-ingestion", "check_name": args.check,
        "base_sha": "", "head_sha": "", "policy_sha": "", "status": "error",
        "toolchain": {}, "findings": [], "coverage": {}, "exception_ids_used": [],
        "run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "local"),
    }
    exit_code = 2
    try:
        base = git(root, "rev-parse", "--verify", f"{args.base_ref}^{{commit}}").strip()
        head = git(root, "rev-parse", "HEAD").strip()
        report.update(base_sha=base, head_sha=head)
        current = [q.read_record(root / path) for path in POLICY_FILES]
        modules = validate_records(current)
        declared_modules = dict(modules)
        raw_base = [git(root, "show", f"{base}:{path}", missing=True) for path in POLICY_FILES]
        bootstrap = not any(raw_base)
        if not bootstrap and not all(raw_base):
            raise q.QualityError("Incomplete protected-base policy")
        protected = current if bootstrap else [parse_record(text, name) for text, name in zip(raw_base, POLICY_FILES)]
        base_modules = {} if bootstrap else validate_records(protected)
        policy = protected[0]
        config_text = ((root / "pyproject.toml").read_text(encoding="utf-8") if bootstrap
                       else git(root, "show", f"{base}:pyproject.toml"))
        policy_settings(config_text)
        report["policy_sha"] = q.digest([protected, config_text])
        report["toolchain"] = check_versions(policy)
        sources = discover(root)
        report["source_sha"] = q.digest(sources)
        # Discovery does not rely on the candidate inventory to cover new modules.
        known_paths = set(modules.values())
        for path in sources.keys() - known_paths:
            new_id = q.module_name(path)
            if new_id in modules:
                raise q.QualityError("New file has an ambiguous module identity")
            modules[new_id] = path
        covered = set(protected[1]["module_ids"]) | set(current[1]["module_ids"]) | (modules.keys() - base_modules.keys())
        if bootstrap:
            # The declared original snapshot distinguishes new modules in this bootstrap.
            old_paths = set(git(root, "ls-tree", "-r", "--name-only", base).splitlines())
            covered = set(current[1]["module_ids"]) | {key for key, path in modules.items() if path not in old_paths}
            base_modules = {key: path for key, path in modules.items() if path in old_paths}
        if any(modules.get(module_id) not in sources for module_id in covered):
            raise q.QualityError("Covered module is missing; coverage cannot disappear")
        selected = q.REQUIRED_CHECKS[:-1] if args.check == "all" else (args.check,)
        with tempfile.TemporaryDirectory(prefix="ingestion-quality-") as temporary:
            config = Path(temporary) / "pyproject.toml"
            config.write_text(config_text, encoding="utf-8")
            for check in selected:
                if check == "policy":
                    findings = policy_findings(root, base, current, protected, sources, declared_modules, base_modules, bootstrap)
                    details = {"bootstrap": bootstrap}
                elif check == "lint":
                    findings, details = lint(root, config, sources)
                elif check == "typing":
                    baseline = current[2] if bootstrap else {"entries": [
                        entry for entry in current[2]["entries"] if entry in protected[2]["entries"]
                    ]}
                    findings, details = typing(root, config, sources, modules, covered, baseline)
                elif check == "architecture":
                    findings, details = architecture(root, config, sources, policy)
                else:
                    evidence = load_evidence(args.test_evidence, base, head)
                    # Candidate additions cannot grant exemptions to base-policy code.
                    entries = current[3]["entries"] if bootstrap else [
                        entry for entry in current[3]["entries"] if entry in protected[3]["entries"]
                    ]
                    findings = q.exception_findings(sources, entries, evidence,
                                                     {path: key for key, path in modules.items()})
                    for record in policy["dynamic_imports"]:
                        if (record.get("review", {}).get("status") != "active"
                                or not record.get("evidence_tests")
                                or any(evidence.get(test) != "passed" for test in record["evidence_tests"])):
                            findings.append(q.finding("dynamic-import-evidence", reason=record["id"]))
                    details = {"handler_count": len(q.handler_sites(sources)), "exception_entries": len(entries)}
                report["findings"].extend(dict(item, check=check) for item in findings)
                report["coverage"][check] = details
        exit_code = 1 if report["findings"] else 0
        report["status"] = "violations" if exit_code else "passed"
    except (q.QualityError, OSError, UnicodeError, KeyError, TypeError, subprocess.TimeoutExpired) as exc:
        report["findings"].append(q.finding("execution-error", reason=str(exc)))
        LOG.error("Quality execution incomplete: %s", exc)
    report["duration_seconds"] = round(time.monotonic() - started, 3)
    report["artifact_integrity"] = q.digest(report)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    LOG.warning("%s: %s (%s findings)", args.check, report["status"], len(report["findings"]))
    return exit_code


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    raise SystemExit(main())
