"""Bind executed pytest node IDs to the candidate without exporting test output."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET

import quality_policy as q


def collect(path: Path) -> dict[str, str]:
    if path.stat().st_size > 20 * 1024 * 1024:
        raise q.QualityError("Oversized pytest evidence")
    tree = ET.parse(path)
    results = {}
    for case in tree.iter("testcase"):
        file = case.get("file", "").replace("\\", "/")
        name = case.get("name", "")
        if not file.startswith("tests/") or not file.endswith(".py") or not name:
            raise q.QualityError("Test evidence requires pytest legacy JUnit file/node identifiers")
        classname = case.get("classname", "")
        module = file[:-3].replace("/", ".")
        suffix = classname.removeprefix(module).lstrip(".")
        node_id = "::".join(part for part in (file, suffix.replace(".", "::"), name) if part)
        if node_id in results:
            raise q.QualityError("Duplicate test node in evidence")
        state = "passed"
        for tag in ("skipped", "failure", "error"):
            if case.find(tag) is not None:
                state = {"failure": "failed"}.get(tag, tag)
        results[node_id] = state
    if not results:
        raise q.QualityError("No executed tests in evidence")
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--junit", type=Path, required=True)
    parser.add_argument("--base-ref", required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    record = {
        "schema_version": 1,
        "base_sha": subprocess.check_output(["git", "rev-parse", "--verify", args.base_ref], text=True).strip(),
        "head_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT", "local"),
        "junit_sha": hashlib.sha256(args.junit.read_bytes()).hexdigest(),
        "tests": collect(args.junit),
    }
    record["artifact_integrity"] = q.digest(record)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
