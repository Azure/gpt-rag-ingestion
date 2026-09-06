"""Aggregate actual same-workflow job results and bound evidence; fail closed."""

import argparse
import json
import logging
import os
from pathlib import Path

import quality_policy as q


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports", type=Path, required=True)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--head-sha", required=True)
    args = parser.parse_args()
    try:
        needs = json.loads(os.environ["NEEDS_JSON"])
        jobs = {name: value["result"] for name, value in needs.items()}
        reports = {}
        sources = set()
        for name in q.REQUIRED_CHECKS[:-1]:
            path = args.reports / f"{name}.json"
            if not path.is_file():
                continue
            record = q.read_record(path)
            integrity = record.pop("artifact_integrity", None)
            if (integrity != q.digest(record)
                    or record.get("repository") != "Azure/gpt-rag-ingestion"
                    or record.get("run_id") != os.environ.get("GITHUB_RUN_ID", "local")
                    or record.get("run_attempt") != os.environ.get("GITHUB_RUN_ATTEMPT", "local")):
                raise q.QualityError(f"Corrupt or stale {name} report")
            reports[name] = record
            sources.add(record.get("source_sha"))
        failures = q.aggregate(jobs, reports, args.base_sha, args.head_sha)
        if len(sources) != 1 or None in sources:
            failures.append(q.finding("source-report-mismatch"))
        evidence = q.read_record(args.reports / "test-evidence.json")
        integrity = evidence.pop("artifact_integrity", None)
        if (integrity != q.digest(evidence) or evidence.get("head_sha") != args.head_sha
                or evidence.get("base_sha") != args.base_sha
                or evidence.get("run_id") != os.environ.get("GITHUB_RUN_ID", "local")
                or evidence.get("run_attempt") != os.environ.get("GITHUB_RUN_ATTEMPT", "local")
                or not evidence.get("tests")
                or any(status != "passed" for status in evidence["tests"].values())):
            failures.append(q.finding("test-evidence", reason="Missing, skipped, failed or stale behavior evidence"))
        for failure in failures:
            logging.error("%s: %s", failure["rule"], failure["reason"])
        return 1 if failures else 0
    except (q.QualityError, OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
        logging.error("Aggregate execution incomplete: %s", exc)
        return 2


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    raise SystemExit(main())
