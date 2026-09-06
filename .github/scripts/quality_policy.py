"""Static quality-policy primitives, shared only by this repository's CLI/tests.

Source is parsed, never imported. Records describe evidence; they do not grant
review authority. The protected-base evaluator and repository rules supply that
trust boundary.
"""

from __future__ import annotations

import ast
from collections import Counter, deque
import graphlib
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tokenize
from typing import Any


REQUIRED_CHECKS = ("lint", "typing", "architecture", "exceptions", "policy", "unit-tests")
NON_RUNTIME = (".github/", "tests/", "scripts/", "samples/", "frontend/", ".artifacts/")
DIAGNOSTIC_KEY = ("module_id", "symbol", "source_fingerprint", "rule", "message_fingerprint")
EXCEPTION_KEY = ("module_id", "symbol", "handler_fingerprint", "caught_types")


class QualityError(ValueError):
    """Invalid policy, source or incomplete tool execution; not a clean run."""


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def finding(rule: str, file: str = "", line: int = 1, reason: str = "", **extra: Any) -> dict:
    return {"rule": rule, "file": file, "line": line, "reason": reason, **extra}


def read_record(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_keys)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise QualityError(f"Cannot read policy record {path.name}: {type(exc).__name__}") from exc
    if not isinstance(data, dict) or data.get("schema_version") != 1:
        raise QualityError(f"Unsupported record version in {path.name}")
    return data


def unique_keys(pairs: list[tuple[str, Any]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise QualityError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def run_tool(command: list[str], cwd: Path, timeout: int = 300) -> subprocess.CompletedProcess:
    try:
        result = subprocess.run(command, cwd=cwd, text=True, encoding="utf-8",
                                capture_output=True, timeout=timeout)
    except (OSError, UnicodeError, subprocess.TimeoutExpired) as exc:
        raise QualityError(f"Tool execution failed: {type(exc).__name__}") from exc
    if result.returncode not in (0, 1):
        raise QualityError(f"Tool execution returned {result.returncode}; inspect local tool diagnostics")
    return result


def module_name(path: str) -> str:
    parts = list(PurePosixPath(path.replace("\\", "/")).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def parse_sources(sources: dict[str, str]) -> dict[str, ast.Module]:
    trees = {}
    for path, source in sources.items():
        try:
            trees[path] = ast.parse(source, filename=path)
        except SyntaxError as exc:
            raise QualityError(f"Cannot parse {path}:{exc.lineno}") from exc
    return trees


def qualified(node: ast.AST | None) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = qualified(node.value)
        return f"{prefix}.{node.attr}" if prefix else ""
    return ""


def relative_target(path: str, node: ast.ImportFrom) -> str:
    name = module_name(path)
    package = name if path.endswith("__init__.py") else name.rpartition(".")[0]
    if not node.level:
        return node.module or ""
    parts = package.split(".") if package else []
    if node.level > len(parts):
        raise QualityError(f"Relative import escapes package: {path}:{node.lineno}")
    prefix = ".".join(parts[:len(parts) - node.level + 1])
    return ".".join(part for part in (prefix, node.module) if part)


def aliases_for(path: str, tree: ast.Module) -> dict[str, str]:
    aliases = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for item in node.names:
                aliases[item.asname or item.name.split(".")[0]] = (
                    item.name if item.asname else item.name.split(".")[0]
                )
        elif isinstance(node, ast.ImportFrom):
            base = relative_target(path, node)
            for item in node.names:
                aliases[item.asname or item.name] = f"{base}.{item.name}"
    return aliases


def expand_alias(name: str, aliases: dict[str, str]) -> str:
    first, _, rest = name.partition(".")
    return ".".join(part for part in (aliases.get(first, first), rest) if part)


def public_names(tree: ast.Module) -> set[str]:
    """Names supplied by syntax, including explicit lazy facade exports."""
    names = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            names.update(item.asname or item.name.split(".")[0] for item in node.names)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names.update(item.id for target in targets for item in ast.walk(target) if isinstance(item, ast.Name))
            if any(isinstance(target, ast.Name) and target.id == "__all__" for target in targets):
                if isinstance(node.value, (ast.List, ast.Tuple)):
                    names.update(item.value for item in node.value.elts
                                 if isinstance(item, ast.Constant) and isinstance(item.value, str))
    return names


def under(module: str, root: str) -> bool:
    return module == root or module.startswith(root + ".")


def private(name: str) -> bool:
    return name.startswith("_") and not (name.startswith("__") and name.endswith("__"))


def build_import_graph(sources: dict[str, str], contracts: dict | None = None,
                       dynamic_imports: list | None = None) -> tuple[dict[str, set[str]], list[dict]]:
    contracts = contracts or {}
    dynamic_imports = dynamic_imports or []
    trees = parse_sources(sources)
    modules = {module_name(path): path for path in sources}
    if len(modules) != len(sources):
        raise QualityError("Duplicate module resolution (module/package collision)")
    namespaces = {name.rsplit(".", index)[0] for name in modules
                  for index in range(1, len(name.split(".")))}
    known = set(modules) | namespaces
    roots = {name.split(".")[0] for name in known}
    graph = {name: set() for name in known}
    findings = []
    dynamic_used = set()

    def area(name: str) -> str:
        return contracts.get("areas", {}).get(name, name.split(".")[0])

    def access(importer: str, target: str, member: str, path: str, line: int) -> None:
        is_private = any(private(part) for part in (target + "." + member).split("."))
        if not is_private or area(importer) == area(target):
            return
        permitted = {"importer": importer, "target": target, "member": member}
        if permitted not in contracts.get("private_access", []):
            findings.append(finding("private-access", path, line, f"{importer} -> {target}.{member}".rstrip(".")))

    def edge(importer: str, target: str, path: str, line: int, member: str = "") -> None:
        if not target or target.split(".")[0] not in roots:
            return
        if target not in known:
            findings.append(finding("unresolved-import", path, line, f"No first-party module {target}"))
            return
        access(importer, target, member, path, line)
        graph[importer].add(target)

    for path, tree in trees.items():
        importer = module_name(path)
        aliases = aliases_for(path, tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for item in node.names:
                    edge(importer, item.name, path, node.lineno)
            elif isinstance(node, ast.ImportFrom):
                base = relative_target(path, node)
                for item in node.names:
                    sibling = f"{base}.{item.name}"
                    if sibling in known:
                        edge(importer, sibling, path, node.lineno)
                    else:
                        edge(importer, base, path, node.lineno, item.name)
                        if base in modules and item.name != "*":
                            exports = public_names(trees[modules[base]])
                            if item.name not in exports:
                                findings.append(finding("unresolved-import", path, node.lineno,
                                                        f"{base} does not declare {item.name}"))
                        if item.name == "*" and base.split(".")[0] in roots:
                            findings.append(finding("wildcard-import", path, node.lineno,
                                                    "Use explicit first-party imports"))
            elif isinstance(node, ast.Attribute):
                full = expand_alias(qualified(node), aliases)
                components = full.split(".")
                for split in range(len(components) - 1, 0, -1):
                    target = ".".join(components[:split])
                    if target in known:
                        access(importer, target, ".".join(components[split:]), path, node.lineno)
                        break
            elif isinstance(node, ast.Call):
                call = expand_alias(qualified(node.func), aliases)
                if call not in {"importlib.import_module", "__import__", "builtins.__import__"}:
                    continue
                if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                    target = node.args[0].value
                    if target.startswith("."):
                        if len(node.args) < 2 or not isinstance(node.args[1], ast.Constant):
                            findings.append(finding("dynamic-import", path, node.lineno,
                                                    "Relative dynamic import requires a literal package"))
                            continue
                        package = node.args[1].value
                        if not isinstance(package, str):
                            raise QualityError(f"Invalid literal package at {path}:{node.lineno}")
                        level = len(target) - len(target.lstrip("."))
                        target = ".".join(package.split(".")[:len(package.split(".")) - level + 1]) + target[level - 1:]
                    edge(importer, target, path, node.lineno)
                else:
                    fingerprint = digest(ast.dump(node, include_attributes=False))
                    matches = [record for record in dynamic_imports
                               if record.get("module_id") == importer and record.get("source_fingerprint") == fingerprint]
                    if len(matches) != 1 or not matches[0].get("targets") or not matches[0].get("evidence_tests"):
                        findings.append(finding("dynamic-import", path, node.lineno,
                                                "Uninventoried variable dynamic import"))
                        continue
                    record = matches[0]
                    dynamic_used.add(record["id"])
                    for target in record["targets"]:
                        edge(importer, target, path, node.lineno)
    for record in dynamic_imports:
        if record["id"] not in dynamic_used:
            findings.append(finding("stale-dynamic-import", reason=record["id"]))
    return graph, findings


def shortest_path(graph: dict[str, set[str]], start: str, end: str) -> list[str] | None:
    pending = deque([[start]])
    seen = {start}
    while pending:
        path = pending.popleft()
        for target in sorted(graph[path[-1]]):
            if target == end:
                return path + [target]
            if target not in seen:
                seen.add(target)
                pending.append(path + [target])
    return None


def analyze_sources(sources: dict[str, str], contracts: dict | None = None,
                    dynamic_imports: list | None = None) -> list[dict]:
    graph, findings = build_import_graph(sources, contracts, dynamic_imports)
    try:
        tuple(graphlib.TopologicalSorter(graph).static_order())
    except graphlib.CycleError as exc:
        # TopologicalSorter takes predecessor sets; reverse for importer -> imported.
        cycle = list(reversed(exc.args[1]))
        findings.append(finding("dependency-cycle", reason=" -> ".join(cycle), dependency_path=cycle))
    for contract in (contracts or {}).get("forbidden", []):
        for source in sorted(name for name in graph if under(name, contract["source"])):
            for target in sorted(name for name in graph if under(name, contract["target"])):
                chain = shortest_path(graph, source, target)
                if chain:
                    findings.append(finding("forbidden-dependency", reason=" -> ".join(chain),
                                            dependency_path=chain))
    return findings


def source_context(tree: ast.Module, line: int) -> tuple[str, str]:
    symbol = []
    context: ast.AST = tree
    def visit(node: ast.AST, names: list[str]) -> None:
        nonlocal context, symbol
        if hasattr(node, "lineno") and not (node.lineno <= line <= (node.end_lineno or node.lineno)):
            return
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names = names + [node.name]
            symbol = names
        if isinstance(node, ast.stmt):
            context = node
        for child in ast.iter_child_nodes(node):
            visit(child, names)
    visit(tree, [])
    return ".".join(symbol) or "<module>", digest(ast.dump(context, include_attributes=False))


def handler_sites(sources: dict[str, str]) -> list[dict]:
    sites = []
    narrow_builtins = {
        "ArithmeticError", "AssertionError", "AttributeError", "EOFError", "ImportError",
        "IndexError", "KeyError", "LookupError", "MemoryError", "NameError",
        "NotImplementedError", "OSError", "EnvironmentError", "FileNotFoundError",
        "PermissionError", "RuntimeError", "StopIteration", "StopAsyncIteration",
        "SyntaxError", "SystemError", "TypeError", "UnicodeError", "ValueError",
        "ZeroDivisionError", "TimeoutError", "ConnectionError", "OverflowError",
        "UnicodeDecodeError", "UnicodeEncodeError", "KeyboardInterrupt", "SystemExit",
    }
    for path, tree in parse_sources(sources).items():
        aliases = aliases_for(path, tree)
        assigned = {target.id for node in ast.walk(tree) if isinstance(node, ast.Assign)
                    for target in node.targets if isinstance(target, ast.Name)}
        classes = {node.name: node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)}
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Try, ast.TryStar)):
                continue
            for handler in node.handlers:
                caught = handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type]
                names = [expand_alias(qualified(item), aliases) if item else "<bare>" for item in caught]
                broad = False
                for item, name in zip(caught, names):
                    if name in {"<bare>", "Exception", "BaseException", "builtins.Exception", "builtins.BaseException"}:
                        broad = True
                    elif not name or (isinstance(item, ast.Name) and item.id in assigned):
                        broad = True
                    elif "." not in name and name not in narrow_builtins and name not in classes:
                        broad = True
                if broad:
                    symbol, _ = source_context(tree, handler.lineno)
                    sites.append({
                        "module_id": module_name(path), "file": path, "line": handler.lineno,
                        "header_end_line": handler.type.end_lineno if handler.type else handler.lineno,
                        "symbol": symbol, "caught_types": names,
                        "handler_fingerprint": digest({
                            "try": ast.dump(node, include_attributes=False),
                            "handler": ast.dump(handler, include_attributes=False),
                        }),
                    })
    return sites


def matching_exception_records(site: dict, records: list[dict]) -> list[dict]:
    return [record for record in records if all(record.get(key) == site[key] for key in EXCEPTION_KEY)]


def valid_exception_metadata(record: dict, review_stage: str) -> bool:
    required = ("boundary", "reason", "failure_outcome", "diagnostic_path")
    return (all(record.get(key) for key in required)
            and bool(record["review"].get("reference"))
            and record.get("review_by_stage") == review_stage)


def exception_findings(sources: dict[str, str], records: list[dict], evidence: dict,
                       module_ids: dict[str, str] | None = None,
                       accepted_ids: list[str] | None = None,
                       review_stage: str = "initial") -> list[dict]:
    findings = []
    used = set()
    proposed = set()
    identities = Counter()
    for site in handler_sites(sources):
        site["module_id"] = (module_ids or {}).get(site["file"], site["module_id"])
        identity = tuple(str(site[key]) for key in ("module_id", "symbol", "handler_fingerprint", "caught_types"))
        identities[identity] += 1
        matching = matching_exception_records(site, records)
        proposed.update(record["id"] for record in matching if record.get("review", {}).get("status") == "proposed")
        matches = [record for record in matching if record.get("review", {}).get("status") == "active"]
        if len(matches) != 1 or identities[identity] > 1:
            findings.append(finding("unapproved-handler", site["file"], site["line"],
                                    "Requires one exact reviewed boundary record", **{
                                        key: site[key] for key in ("symbol", "handler_fingerprint", "caught_types")
                                    }))
            continue
        record = matches[0]
        used.add(record["id"])
        errors_before = len(findings)
        if not valid_exception_metadata(record, review_stage):
            findings.append(finding("invalid-exception", site["file"], site["line"], record["id"]))
        if not record.get("evidence_tests") or any(evidence.get(test) != "passed" for test in record["evidence_tests"]):
            findings.append(finding("exception-evidence", site["file"], site["line"], record["id"]))
        if len(findings) == errors_before and accepted_ids is not None:
            accepted_ids.append(record["id"])
    for record in records:
        if record["id"] not in used:
            rule = "exception-review-pending" if record["id"] in proposed else "stale-exception"
            findings.append(finding(rule, reason=record["id"]))
    return findings


def compare_diagnostics(current: list[dict], baseline: list[dict]) -> list[dict]:
    def counts(records: list[dict]) -> Counter:
        result = Counter()
        for record in records:
            if not all(key in record for key in DIAGNOSTIC_KEY):
                raise QualityError("Incomplete individual type diagnostic")
            occurrences = record.get("occurrences", 1)
            if type(occurrences) is not int or occurrences < 1:
                raise QualityError("Diagnostic occurrences must be a positive integer")
            result[tuple(record[key] for key in DIAGNOSTIC_KEY)] += occurrences
        return result
    actual, allowed = counts(current), counts(baseline)
    result = []
    for rule, difference in (("new-type-debt", actual - allowed), ("stale-type-debt", allowed - actual)):
        for identity, count in difference.items():
            result.append(finding(rule, reason="Individual diagnostic mismatch",
                                  diagnostic=dict(zip(DIAGNOSTIC_KEY, identity)), occurrences=count))
    return result


def compare_scope(base: dict, candidate: dict, modules: dict[str, str],
                  base_modules: dict[str, str]) -> list[dict]:
    result = []
    old_ids, new_ids = set(base["module_ids"]), set(candidate["module_ids"])
    for module_id in sorted(old_ids - new_ids):
        result.append(finding("typing-scope-reduction", reason=module_id))
    for module_id in sorted(set(modules) - set(base_modules) - new_ids):
        result.append(finding("new-module-untyped", modules[module_id], reason=module_id))
    moves = candidate.get("move_map", [])
    ids = [move["module_id"] for move in moves]
    destinations = [move["new_path"] for move in moves]
    old_paths = [move["old_path"] for move in moves]
    if len(ids) != len(set(ids)) or len(destinations) != len(set(destinations)) or len(old_paths) != len(set(old_paths)):
        result.append(finding("ambiguous-move", reason="Moves must be one-to-one"))
    for module_id, old_path in base_modules.items():
        if module_id not in modules:
            result.append(finding("module-identity-removed", old_path, reason=module_id))
        elif modules[module_id] != old_path:
            expected = {"module_id": module_id, "old_path": old_path, "new_path": modules[module_id]}
            if expected not in moves:
                result.append(finding("unreviewed-move", old_path, reason=module_id))
    for move in moves:
        if move not in base.get("move_map", []) and (
            base_modules.get(move["module_id"]) != move["old_path"]
            or modules.get(move["module_id"]) != move["new_path"]
        ):
            result.append(finding("ambiguous-move", reason="Move does not resolve to the declared identity"))
    return result


def annotations(tree: ast.Module) -> dict[str, dict[str, str]]:
    result = {}
    def visit(node: ast.AST, names: list[str]) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names = names + [node.name]
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            args = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
            args += [arg for arg in (node.args.vararg, node.args.kwarg) if arg is not None]
            result[".".join(names)] = {arg.arg: ast.dump(arg.annotation) for arg in args if arg.annotation}
            if node.returns:
                result[".".join(names)]["return"] = ast.dump(node.returns)
        for child in ast.iter_child_nodes(node):
            visit(child, names)
    visit(tree, [])
    return result


def suppressions(source: str) -> Counter:
    tokens = tokenize.generate_tokens(io.StringIO(source).readline)
    tree = ast.parse(source)
    return Counter((token.string.strip(), source_context(tree, token.start[0]))
                   for token in tokens if token.type == tokenize.COMMENT
                   and re.search(r"(?:noqa|type:\s*ignore|mypy:|pyright:)", token.string, re.I))


def source_policy(base_sources: dict[str, str], sources: dict[str, str],
                  modules: dict[str, str], base_modules: dict[str, str],
                  covered: list[str]) -> list[dict]:
    result = []
    for module_id, path in modules.items():
        old = base_sources.get(base_modules.get(module_id, ""), "")
        current = sources[path]
        if suppressions(current) - suppressions(old):
            result.append(finding("new-suppression", path, reason="Suppression/directive change requires protected review"))
        if module_id in covered:
            old_annotations = annotations(ast.parse(old))
            new_annotations = annotations(ast.parse(current))
            for symbol, parameters in old_annotations.items():
                if symbol not in new_annotations:
                    result.append(finding("typed-symbol-removed", path, reason=symbol))
                    continue
                for parameter in parameters:
                    if parameter not in new_annotations[symbol]:
                        result.append(finding("annotation-removal", path, reason=f"{symbol}.{parameter}"))
    return result


def aggregate(jobs: dict, reports: dict, base_sha: str, head_sha: str) -> list[dict]:
    result = []
    policy_shas = set()
    for name in REQUIRED_CHECKS:
        if jobs.get(name) != "success":
            result.append(finding("required-execution", reason=f"{name}: {jobs.get(name, 'missing')}"))
        if name == "unit-tests":
            continue
        report = reports.get(name, {})
        if not all((
            report.get("schema_version") == 1, report.get("check_name") == name,
            report.get("status") == "passed", report.get("base_sha") == base_sha,
            report.get("head_sha") == head_sha,
            isinstance(report.get("policy_sha"), str) and len(report["policy_sha"]) == 64,
        )):
            result.append(finding("invalid-report", reason=name))
        policy_shas.add(report.get("policy_sha"))
    if len(policy_shas) != 1:
        result.append(finding("policy-report-mismatch"))
    return result
