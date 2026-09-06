"""Non-mutating repository quality command. Exit 0/1/2: pass/violations/error."""

from __future__ import annotations

import argparse
import ast
from importlib.metadata import PackageNotFoundError, version
import json
import logging
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tempfile
import time
import tomllib

sys.path.append(str(Path(__file__).resolve().parent))
import quality_policy as q


LOG = logging.getLogger("ingestion.quality")
POLICY_FILES = (".quality/policy.json", ".quality/typing-scope.json",
                ".quality/typing-baseline.json", ".quality/exceptions.json")
PROTECTED_FILES = (".github/scripts/check-quality.py", ".github/scripts/quality_policy.py",
                   ".github/scripts/quality-tool.py",
                   ".github/scripts/quality-evidence.py", ".github/scripts/quality-gate.py",
                   "tests/test_quality_policy.py",
                   ".github/workflows/tests.yml", ".github/CODEOWNERS", "requirements-quality.txt")


def git(root: Path, *args: str, missing: bool = False) -> str:
    return q.git_output(root, *args, missing=missing)


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
    if (not isinstance(result, dict) or type(result.get("schema_version")) is not int
            or result["schema_version"] != 1):
        raise q.QualityError(f"Unsupported {name} version")
    return result


def object_fields(value: object, required: set[str], optional: frozenset[str] = frozenset()) -> dict:
    if not isinstance(value, dict) or not required <= value.keys() or value.keys() - required - optional:
        raise q.QualityError("Missing, unknown or invalid record fields")
    return value


def text(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise q.QualityError("Record fields require nonempty strings")
    return value


def strings(value: object, *, nonempty: bool = False) -> list[str]:
    if not isinstance(value, list) or (nonempty and not value):
        raise q.QualityError("Expected a string list")
    for item in value:
        text(item)
    if len(value) != len(set(value)):
        raise q.QualityError("Duplicate string-list entries")
    return value


def record_list(value: object) -> list[dict]:
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise q.QualityError("Expected a list of records")
    return value


def relative_path(value: object, *, python: bool = True) -> str:
    path = text(value)
    parsed = PurePosixPath(path)
    if (parsed.is_absolute() or ".." in parsed.parts or parsed.as_posix() != path
            or any(char in path for char in "\\:*?[]") or path == "."
            or (python and not path.endswith(".py"))):
        raise q.QualityError("Expected a canonical repository-relative path")
    return path


def fingerprint(value: object, length: int = 64) -> None:
    if not re.fullmatch(rf"[0-9a-f]{{{length}}}", text(value)):
        raise q.QualityError("Invalid source revision or fingerprint")


def review_record(value: object) -> None:
    review = object_fields(value, {"status", "reference"}, frozenset({"owner", "rationale"}))
    for item in review.values():
        text(item)
    if review["status"] not in {"proposed", "maintainer-approved", "active", "retired"}:
        raise q.QualityError("Invalid review lifecycle status")


def validate_records(records: list[dict]) -> dict[str, str]:
    policy, scope, baseline, exceptions = records
    object_fields(policy, {"schema_version", "runtime_roots", "modules", "contracts", "toolchain",
                           "required_checks", "dynamic_imports", "review", "source_revision"})
    object_fields(scope, {"schema_version", "module_ids", "coverage_stage", "planned_expansion",
                          "move_map", "review"})
    for record in (baseline, exceptions):
        object_fields(record, {"schema_version", "entries"}, frozenset({"review"}))
    for record in records:
        if "review" in record:
            review_record(record["review"])
    fingerprint(policy["source_revision"], 40)
    for root in strings(policy["runtime_roots"], nonempty=True):
        relative_path(root, python=False)
        if "/" in root:
            raise q.QualityError("Runtime roots must be flat files or top-level packages")
    toolchain = object_fields(policy["toolchain"], {"ruff", "mypy", "import-linter", "grimp"})
    if any(not re.fullmatch(r"\d+(?:\.\d+)+", text(pin)) for pin in toolchain.values()):
        raise q.QualityError("Quality tool versions must be exact release pins")
    if strings(policy["required_checks"]) != list(q.REQUIRED_CHECKS):
        raise q.QualityError("Incomplete policy or unexpected required check set")
    modules = {}
    names = set()
    paths = set()
    for module in record_list(policy["modules"]):
        fields = {"id", "path", "import_name", "area", "public_exports", "private_modules",
                  "allowed_importers", "legacy_aliases", "typing_status", "responsibilities", "source_revision"}
        object_fields(module, fields)
        for field in ("id", "import_name", "area", "typing_status", "responsibilities"):
            text(module[field])
        for field in ("public_exports", "private_modules", "allowed_importers", "legacy_aliases"):
            strings(module[field])
        fingerprint(module["source_revision"], 40)
        if module["typing_status"] not in {"inventoried", "blocking"}:
            raise q.QualityError("Invalid module typing status")
        path = relative_path(module["path"])
        if (module["id"] in modules or path in paths or module["import_name"] in names
                or any(char in module["id"] for char in "*?[]")
                or module["import_name"] != q.module_name(path)):
            raise q.QualityError("Duplicate or invalid module identity/path")
        modules[module["id"]] = path
        paths.add(path)
        names.add(module["import_name"])
    text(scope["coverage_stage"])
    strings(scope["planned_expansion"])
    if not set(strings(scope["module_ids"])) <= modules.keys():
        raise q.QualityError("Invalid typing scope")
    for move in record_list(scope["move_map"]):
        object_fields(move, {"module_id", "old_path", "new_path"})
        if text(move["module_id"]) not in modules:
            raise q.QualityError("Unknown moved module")
        relative_path(move["old_path"])
        relative_path(move["new_path"])
    contracts = object_fields(policy["contracts"], {"forbidden", "areas", "private_access"})
    if not isinstance(contracts["areas"], dict):
        raise q.QualityError("Contract areas must be a mapping")
    for key, value in contracts["areas"].items():
        text(key)
        text(value)
    for relationship in record_list(contracts["forbidden"]):
        object_fields(relationship, {"source", "target"})
        for value in relationship.values():
            text(value)
    for access in record_list(contracts["private_access"]):
        object_fields(access, {"importer", "target", "member"})
        for value in access.values():
            text(value)
    dynamic_ids = set()
    for entry in record_list(policy["dynamic_imports"]):
        object_fields(entry, {"id", "module_id", "symbol", "source_fingerprint", "targets",
                              "evidence_tests", "reason", "review"})
        if text(entry["id"]) in dynamic_ids or text(entry["module_id"]) not in modules:
            raise q.QualityError("Invalid dynamic-import identity")
        dynamic_ids.add(entry["id"])
        fingerprint(entry["source_fingerprint"])
        strings(entry["targets"], nonempty=True)
        strings(entry["evidence_tests"], nonempty=True)
        text(entry["reason"])
        text(entry["symbol"])
        review_record(entry["review"])
    for record, field in ((baseline, "entries"), (exceptions, "entries")):
        ids = set()
        for entry in record_list(record[field]):
            if (text(entry.get("id")) in ids or text(entry.get("module_id")) not in modules
                    or any(char in entry["id"] for char in "*?[]")):
                raise q.QualityError("Invalid or duplicate debt/exception identity")
            review_record(entry.get("review"))
            ids.add(entry["id"])
    for entry in baseline["entries"]:
        object_fields(entry, {"id", *q.DIAGNOSTIC_KEY, "occurrences", "rationale",
                              "introduced_at", "removal_stage", "review"})
        fingerprint(entry["source_fingerprint"])
        fingerprint(entry["message_fingerprint"])
        fingerprint(entry["introduced_at"], 40)
        if type(entry["occurrences"]) is not int or entry["occurrences"] < 1:
            raise q.QualityError("Diagnostic multiplicity must be a positive integer")
        for field in (*q.DIAGNOSTIC_KEY, "rationale", "introduced_at", "removal_stage"):
            text(entry[field])
        if (not all(entry.get(field) for field in (*q.DIAGNOSTIC_KEY, "rationale", "introduced_at", "removal_stage"))
                or entry["module_id"] not in scope["module_ids"]
                or entry["review"].get("status") != "active"
                or not entry["review"].get("reference")):
            raise q.QualityError("Type debt requires an individually reviewed inherited finding")
    q.compare_diagnostics([], baseline["entries"])
    for entry in exceptions["entries"]:
        object_fields(entry, {"id", "module_id", "symbol", "handler_fingerprint", "caught_types",
                              "boundary", "reason", "failure_outcome", "diagnostic_path",
                              "evidence_tests", "review", "review_by_stage"})
        fingerprint(entry["handler_fingerprint"])
        for field in ("symbol", "boundary", "reason", "failure_outcome", "diagnostic_path", "review_by_stage"):
            text(entry[field])
        strings(entry["caught_types"], nonempty=True)
        for test in strings(entry["evidence_tests"], nonempty=True):
            file, separator, _ = test.partition("::")
            if not separator or not file.startswith("tests/"):
                raise q.QualityError("Evidence must name a maintained pytest node")
            relative_path(file)
        if entry["failure_outcome"] not in {
            "propagation", "explicit-failure-translation", "cleanup-followed-by-propagation",
            "contractual-best-effort-side-effect",
        }:
            raise q.QualityError("Unknown exception failure outcome")
        if any(char in entry["symbol"] for char in "*?[]"):
            raise q.QualityError("Wildcard exception symbols are not permitted")
    if not policy["modules"]:
        raise q.QualityError("Missing modules")
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


def validate_pins(manifest: str, policy: dict) -> None:
    pins = {}
    for raw in manifest.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"([a-z0-9-]+)==(\d+(?:\.\d+)+)", line)
        if not match or match[1] in pins:
            raise q.QualityError("Quality requirements must contain unique exact pins only")
        pins[match[1]] = match[2]
    if pins != policy["toolchain"]:
        raise q.QualityError("Quality requirements and policy toolchain differ")


def policy_settings(config: str) -> dict:
    try:
        tool = tomllib.loads(config)["tool"]
        if (tool["mypy"].get("plugins") or tool["mypy"].get("python_executable")
                or tool["mypy"].get("mypy_path") or tool["ruff"].get("extend")
                or tool["importlinter"].get("contract_types")
                or any(contract.get("type") not in {"forbidden", "protected", "layers", "independence", "acyclic_siblings"}
                       for contract in tool["importlinter"].get("contracts", []))):
            raise q.QualityError("Executable or external tool configuration is not supported")
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
    for field in ("coverage_stage", "planned_expansion", "review"):
        if scope.get(field) != base_scope.get(field):
            findings.append(q.finding("protected-policy-change", ".quality/typing-scope.json", reason=field))
    old_surfaces = {module["id"]: module for module in base_policy["modules"]}
    for surface in policy["modules"]:
        old = old_surfaces.get(surface["id"])
        if old:
            # Paths/import names may move one-to-one; public ownership cannot self-approve.
            protected_fields = ("area", "public_exports", "private_modules", "allowed_importers", "legacy_aliases")
            if any(surface[field] != old[field] for field in protected_fields):
                findings.append(q.finding("protected-policy-change", surface["path"], reason="Module surface changed"))
            contract_roots = {relationship[field] for relationship in base_policy["contracts"]["forbidden"]
                              for field in ("source", "target")}
            if (old["import_name"].split(".")[0] != surface["import_name"].split(".")[0]
                    or any(q.under(old["import_name"], root) != q.under(surface["import_name"], root)
                           for root in contract_roots)):
                findings.append(q.finding("protected-policy-change", surface["path"],
                                          reason="Move changes ownership or import-contract coverage"))
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


def lint(root: Path, config: Path, sources: dict, entries: list[dict],
         modules: dict[str, str], review_stage: str) -> tuple[list[dict], dict]:
    result = q.run_tool([sys.executable, "-I", "-m", "ruff", "check", "--config", str(config),
                         "--output-format", "json", "--no-cache",
                         *[str(root / path) for path in sources]], config.parent)
    try:
        diagnostics = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise q.QualityError("Ruff did not return JSON") from exc
    if not isinstance(diagnostics, list) or (result.returncode == 1 and not diagnostics):
        raise q.QualityError("Incomplete Ruff execution")
    approved_sites = []
    path_ids = {path: key for key, path in modules.items()}
    for site in q.handler_sites(sources):
        site["module_id"] = path_ids[site["file"]]
        matching = [record for record in q.matching_exception_records(site, entries)
                    if record["review"].get("status") == "active"
                    and q.valid_exception_metadata(record, review_stage)
                    and "<dynamic>" not in site["caught_types"]]
        if len(matching) == 1:
            approved_sites.append((site, matching[0]["id"]))
    findings = []
    used_ids = set()
    for item in diagnostics:
        if not all(key in item for key in ("code", "filename", "location", "message")):
            raise q.QualityError("Unsupported Ruff diagnostic")
        file = Path(item["filename"]).resolve().relative_to(root).as_posix()
        if item["code"] == "BLE001":
            approved = [record_id for site, record_id in approved_sites
                        if site["file"] == file
                        and site["line"] <= item["location"]["row"] <= site["header_end_line"]]
            if len(approved) == 1:
                # The separate required exceptions job proves same-run behavior evidence.
                used_ids.update(approved)
                continue
        findings.append(q.finding(item["code"], file, item["location"]["row"], item["message"]))
    return findings, {"exception_ids_used": sorted(used_ids)}


def typing(root: Path, config: Path, sources: dict, modules: dict, covered: set,
           baseline: dict) -> tuple[list[dict], dict]:
    paths = sorted(modules[module_id] for module_id in covered)
    result = q.run_tool([sys.executable, "-I", "-m", "mypy", "--config-file", str(config),
                         "--no-incremental", "--cache-dir", str(config.parent / "mypy-cache"),
                         "--disallow-untyped-defs", "--output", "json",
                         *[str(root / path) for path in paths]], config.parent,
                        source_root=root)
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
        path = (config.parent / path).resolve() if not path.is_absolute() else path.resolve()
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
    runner = Path(__file__).with_name("quality-tool.py")
    result = q.run_tool([sys.executable, "-I", str(runner), "grimp",
                         "--root", str(root), "--config", str(config)], config.parent)
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
    result = q.run_tool([sys.executable, "-I", str(runner), "import-linter",
                         "--root", str(root), "--config", str(config)], config.parent)
    receipts = [line.removeprefix("QUALITY_IMPORT_LINTER_RESULT=") for line in result.stdout.splitlines()
                if line.startswith("QUALITY_IMPORT_LINTER_RESULT=")]
    if receipts != [json.dumps({"passed": result.returncode == 0})]:
        raise q.QualityError("Import Linter execution incomplete; missing result receipt")
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
        if not sys.flags.isolated:
            raise q.QualityError("Run the protected checker with python -I to isolate Python startup")
        base = git(root, "rev-parse", "--verify", f"{args.base_ref}^{{commit}}").strip()
        head = git(root, "rev-parse", "HEAD").strip()
        report.update(base_sha=base, head_sha=head)
        current = [q.read_record(root / path) for path in POLICY_FILES]
        modules = validate_records(current)
        validate_pins((root / "requirements-quality.txt").read_text(encoding="utf-8"), current[0])
        declared_modules = dict(modules)
        raw_base = [git(root, "show", f"{base}:{path}", missing=True) for path in POLICY_FILES]
        bootstrap = not any(raw_base)
        if not bootstrap and not all(raw_base):
            raise q.QualityError("Incomplete protected-base policy")
        protected = current if bootstrap else [parse_record(text, name) for text, name in zip(raw_base, POLICY_FILES)]
        base_modules = {} if bootstrap else validate_records(protected)
        if not bootstrap:
            validate_pins(git(root, "show", f"{base}:requirements-quality.txt"), protected[0])
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
        entries = current[3]["entries"] if bootstrap else [
            entry for entry in current[3]["entries"] if entry in protected[3]["entries"]
        ]
        with tempfile.TemporaryDirectory(prefix="ingestion-quality-") as temporary:
            config = Path(temporary) / "pyproject.toml"
            config.write_text(config_text, encoding="utf-8")
            for check in selected:
                if check == "policy":
                    findings = policy_findings(root, base, current, protected, sources, declared_modules, base_modules, bootstrap)
                    details = {"bootstrap": bootstrap}
                elif check == "lint":
                    findings, details = lint(root, config, sources, entries, modules,
                                            protected[1].get("coverage_stage", "initial"))
                    report["exception_ids_used"].extend(details["exception_ids_used"])
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
                    findings = q.exception_findings(sources, entries, evidence,
                                                     {path: key for key, path in modules.items()},
                                                     accepted_ids=report["exception_ids_used"],
                                                     review_stage=protected[1].get("coverage_stage", "initial"))
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
    report["exception_ids_used"] = sorted(set(report["exception_ids_used"]))
    report["duration_seconds"] = round(time.monotonic() - started, 3)
    report["artifact_integrity"] = q.digest(report)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    LOG.warning("%s: %s (%s findings)", args.check, report["status"], len(report["findings"]))
    return exit_code


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    raise SystemExit(main())
