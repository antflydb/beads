#!/usr/bin/env python3
"""Evaluate cited Antfly context against a JSONL dataset and optional real outcomes.

Each case contains query, optional contains/ids arrays, and optional max_tokens.
Outcome rows use Antfly's native {id,task_id,success,used_memory_ids,detail} shape.
Recording outcomes requires --record-outcomes; evaluations never promote guidance.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys


def rows(path):
    with path.open() as stream:
        return [json.loads(line) for line in stream if line.strip()]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bd", default="bd")
    parser.add_argument("--dataset", required=True, type=Path)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--outcomes", type=Path)
    parser.add_argument("--record-outcomes", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.record_outcomes and not args.outcomes:
        parser.error("--record-outcomes requires --outcomes")
    results = []
    for case in rows(args.dataset):
        budget = case.get("max_tokens", 4096)
        process = subprocess.run(
            [args.bd, "antfly", "context", case["query"], "--max-tokens", str(budget)],
            text=True, capture_output=True, timeout=90, check=False,
        )
        verdict = {"query": case["query"], "passed": False}
        if process.returncode:
            verdict["error"] = process.stderr
        else:
            context = json.loads(process.stdout)
            sources = context["sources"]
            text = "\n".join(source["text"] for source in sources)
            found = {source["id"] for source in sources}
            verdict["missing_text"] = [value for value in case.get("contains", []) if value not in text]
            verdict["missing_ids"] = [value for value in case.get("ids", []) if value not in found]
            verdict["forbidden_text"] = [value for value in case.get("forbid", []) if value in text]
            verdict["uncited"] = sum(not source.get("citation") for source in sources)
            verdict["bytes"] = len(process.stdout.strip().encode("utf-8"))
            verdict["passed"] = not (
                verdict["missing_text"] or verdict["missing_ids"] or verdict["forbidden_text"]
                or verdict["uncited"] or verdict["bytes"] > budget
            )
        results.append(verdict)
    passed = sum(case["passed"] for case in results)
    report = {"cases": results, "passed": passed, "failed": len(results) - passed,
              "success_rate": passed / len(results) if results else 0.0}
    if args.baseline:
        with args.baseline.open() as stream:
            baseline = json.load(stream)
        report["success_rate_delta"] = report["success_rate"] - baseline["success_rate"]
    if args.outcomes:
        outcomes = rows(args.outcomes)
        if any(not isinstance(row.get("success"), bool) for row in outcomes):
            raise ValueError("each outcome must contain a real boolean success verdict")
        report["outcome_success_rate"] = (
            sum(row["success"] for row in outcomes) / len(outcomes) if outcomes else 0.0
        )
        report["recorded_outcomes"] = 0
        if args.record_outcomes:
            for outcome in outcomes:
                subprocess.run(
                    [args.bd, "antfly", "memory", "outcome", "--idempotency-key", "eval:" + outcome["id"]],
                    input=json.dumps(outcome), text=True, capture_output=True, timeout=90, check=True,
                )
                report["recorded_outcomes"] += 1
    encoded = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.write_text(encoded)
    sys.stdout.write(encoded)
    return int(report["failed"] > 0)


if __name__ == "__main__":
    sys.exit(main())
