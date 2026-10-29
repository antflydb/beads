#!/usr/bin/env python3
"""Exercise the native Antfly backend through the real bd CLI in a fresh project.

Pass --library for embedded mode, or --url for a disposable Antfly server.
Remote mode creates a uniquely named test table; it does not delete server data.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import tempfile
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bd", required=True, type=Path)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--library", type=Path)
    mode.add_argument("--url")
    parser.add_argument("--token-env", default="")
    args = parser.parse_args()
    binary = str(args.bd.resolve())
    with tempfile.TemporaryDirectory(prefix="beads-antfly-smoke-") as temporary:
        project = Path(temporary)
        environment = dict(os.environ)
        environment.update(
            BEADS_DIR=str(project / ".beads"),
            BD_DISABLE_METRICS="1",
            BD_DISABLE_EVENT_FLUSH="1",
        )
        if args.library:
            environment["ANTFLY_LIBRARY_PATH"] = str(args.library.resolve())

        def run(*command, body=None, succeeds=True):
            result = subprocess.run(
                [binary, *command],
                cwd=project,
                env=environment,
                input=body,
                capture_output=True,
                text=True,
                timeout=90,
                check=False,
            )
            if (result.returncode == 0) != succeeds:
                raise RuntimeError(
                    f"bd {' '.join(command)}: exit {result.returncode}\n"
                    f"{result.stdout}\n{result.stderr}"
                )
            return result.stdout

        initialize = ["antfly", "init", "--prefix", "smoke"]
        if args.url:
            table = "beads_smoke_" + uuid.uuid4().hex[:12]
            initialize.extend(["--url", args.url, "--table", table])
            if args.token_env:
                initialize.extend(["--token-env", args.token_env])
            print(f"Remote test table: {table}")
        initialized = json.loads(run(*initialize, "--json"))
        assert initialized["backend"] == "antfly", initialized
        run("create", "First issue", "--id", "smoke-first", "--type", "task")
        run(
            "create", "Second issue", "--id", "smoke-second", "--type", "task",
            "--deps", "smoke-first",
        )
        ready = json.loads(run("ready", "--json"))
        assert [issue["id"] for issue in ready] == ["smoke-first"], ready
        run("dep", "add", "smoke-first", "smoke-second", succeeds=False)
        run("update", "smoke-first", "--claim")
        run("update", "smoke-first", "--title", "Claimed first issue")
        run("label", "add", "smoke-first", "tested")
        run("comments", "add", "smoke-first", "Regression verified")
        run("close", "smoke-first", "--reason", "Verified")
        ready = json.loads(run("ready", "--json"))
        assert [issue["id"] for issue in ready] == ["smoke-second"], ready
        with ThreadPoolExecutor(max_workers=2) as pool:
            winners = list(pool.map(
                lambda actor: json.loads(run("--actor", actor, "ready", "--claim", "--json")),
                ["worker-a", "worker-b"],
            ))
        assert sum(len(result) for result in winners) == 1, winners
        run("reopen", "smoke-first")
        ready = json.loads(run("ready", "--json"))
        assert [issue["id"] for issue in ready] == ["smoke-first"], ready
        shown = json.loads(run("show", "smoke-first", "--include-comments", "--json"))[0]
        assert "tested" in shown["labels"], shown
        assert shown["comments"][0]["text"] == "Regression verified", shown
        run("remember", "--key", "test-policy", "Run regression tests before merging")
        assert "Run regression tests" in run("recall", "test-policy")
        run("forget", "test-policy")
        run("recall", "test-policy", succeeds=False)
        run("antfly", "memory", "create",
            "--idempotency-key", "create-space", body="{}")
        observation = {
            "evidence": [{"id": "e1", "text": "Always run the regression suite"}],
            "assertions": [{
                "id": "a1", "subject": "project", "predicate": "guidance",
                "value": "run tests", "statement": "Always run the regression suite",
                "evidence": [{"id": "e1"}],
            }],
        }
        run("antfly", "memory", "observe",
            "--idempotency-key", "observe-guidance", body=json.dumps(observation))
        recalled = run("antfly", "memory", "recall",
                       body='{"query":"regression suite"}')
        assert "Always run the regression suite" in recalled, recalled
        summary = json.loads(run("status", "--json"))["summary"]
        assert summary["total_issues"] == 2, summary
        print("PASS: native storage, dependency readiness, claims, edits, comments, notes, and memory")


if __name__ == "__main__":
    main()
