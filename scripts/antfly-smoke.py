#!/usr/bin/env python3
"""Exercise the native Antfly backend through the real bd CLI in a fresh project.

Pass --library for embedded mode, or --url for a disposable Antfly server.
Remote mode creates a uniquely named test table; it does not delete server data.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
import sys
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
    parser.add_argument("--timeout", type=int, default=300, help="Per-command timeout for replay-heavy embedded runs")
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

        def run(*command, body=None, succeeds=True, beads_dir=None):
            selected_environment = dict(environment)
            if beads_dir:
                selected_environment["BEADS_DIR"] = str(beads_dir)
            print("bd " + " ".join(command), flush=True)
            result = subprocess.run(
                [binary, *command],
                cwd=project,
                env=selected_environment,
                input=body,
                capture_output=True,
                text=True,
                timeout=args.timeout,
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
            "evidence": [{"id": "e1", "text": "Manual guidance sentinel"}],
            "assertions": [{
                "id": "a1", "subject": "project", "predicate": "guidance",
                "value": "run tests", "statement": "Manual guidance sentinel",
                "evidence": [{"id": "e1"}],
            }],
        }
        run("antfly", "memory", "observe",
            "--idempotency-key", "observe-guidance", body=json.dumps(observation))
        recalled = run("antfly", "memory", "recall",
                       body='{"query":"Manual guidance sentinel"}')
        assert "Manual guidance sentinel" in recalled, recalled
        held = json.loads(run("show", "smoke-second", "--json"))[0]
        branch = json.loads(run("antfly", "branch", "create", "feature"))
        branch_dir = Path(branch["directory"])
        run("update", "smoke-first", "--description", "Change from branch", beads_dir=branch_dir)
        run("update", "smoke-first", "--title", "Change from main")
        run("ready", "--claim", "--json", beads_dir=branch_dir, succeeds=False)
        merged = json.loads(run("antfly", "branch", "merge", "feature", "--idempotency-key", "integration-1"))
        assert merged["applied"], merged
        retried = json.loads(run("antfly", "branch", "merge", "feature", "--idempotency-key", "integration-1"))
        assert retried["already_applied"], retried
        changed = json.loads(run("show", "smoke-first", "--json"))[0]
        assert changed["title"] == "Change from main" and changed["description"] == "Change from branch", changed
        still_held = json.loads(run("show", "smoke-second", "--json"))[0]
        assert still_held["assignee"] == held["assignee"] and still_held["status"] == "in_progress", still_held
        conflict = json.loads(run("antfly", "branch", "create", "conflict"))
        run("update", "smoke-first", "--title", "Branch conflict", beads_dir=conflict["directory"])
        run("update", "smoke-first", "--title", "Main conflict")
        preview = json.loads(run("antfly", "branch", "merge", "conflict", "--idempotency-key", "conflict-1", "--dry-run"))
        assert preview["conflicts"], preview
        run("antfly", "branch", "merge", "conflict", "--idempotency-key", "conflict-1", succeeds=False)
        run("create", "Remove on branch", "--id", "smoke-remove", "--type", "task")
        remove = json.loads(run("antfly", "branch", "create", "remove"))
        run("delete", "smoke-remove", "--force", beads_dir=remove["directory"])
        run("antfly", "branch", "merge", "remove", "--idempotency-key", "remove-1")
        run("show", "smoke-remove", "--json", succeeds=False)

        # Adopt the existing native space using its explicit revision, then
        # exercise automatic event ingestion without a model download.
        memory_state = json.loads(run("antfly", "memory", "inspect"))
        config = {"revision": "smoke-v1", "expected_memory_revision": memory_state["state"]["revision"]}
        # Native background memory work may change its revision between inspect
        # and configure. A rejected fence is explicit: inspect again and replace
        # only this exact pending manifest, never an unrelated preparation.
        for attempt in range(5):
            try:
                run("antfly", "knowledge", "configure", body=json.dumps(config))
                break
            except RuntimeError as error:
                if "MemoryRevisionConflict" not in str(error) or attempt == 4:
                    raise
                pending = json.loads(run("antfly", "knowledge", "status"))["preparing"]
                memory_state = json.loads(run("antfly", "memory", "inspect"))
                config["expected_memory_revision"] = memory_state["state"]["revision"]
                config["replace_pending"] = pending["digest"]
        guidance = "Always test the snapshot merge policy"
        run("remember", "--key", "factory-policy", guidance)
        projected = json.loads(run("antfly", "knowledge", "sync", "--limit", "10000"))
        assert projected["pending"] == 0 and projected["configured"], projected
        context = json.loads(run("antfly", "context", "snapshot merge policy", "--max-tokens", "2048"))
        assert any(guidance in source["text"] for source in context["sources"]), context
        assert all(source["citation"] and source["id"] for source in context["sources"]), context
        dataset = project / "context-cases.jsonl"
        dataset.write_text(json.dumps({"query": "snapshot merge policy", "contains": [guidance], "max_tokens": 2048}) + "\n")
        evaluated = subprocess.run(
            [sys.executable, str(Path(__file__).with_name("antfly-eval.py").resolve()),
             "--bd", binary, "--dataset", str(dataset)],
            cwd=project, env=environment, text=True, capture_output=True, timeout=args.timeout, check=False,
        )
        assert evaluated.returncode == 0, evaluated.stdout + evaluated.stderr
        run("antfly", "knowledge", "configure", body=json.dumps({"revision": "smoke-v2"}))
        run("antfly", "knowledge", "sync", "--limit", "10000")
        # Managed processing without an enabled producer must fail closed,
        # leaving the failed source available for a corrected configuration.
        run("antfly", "knowledge", "configure", body=json.dumps({
            "revision": "managed-without-producer", "managed_observations": True,
        }))
        before_failure = json.loads(run("antfly", "knowledge", "status"))
        run("antfly", "knowledge", "sync", succeeds=False)
        after_failure = json.loads(run("antfly", "knowledge", "status"))
        assert after_failure["state"]["checkpoint"] == before_failure["state"]["checkpoint"], after_failure
        run("antfly", "knowledge", "configure", body=json.dumps({"revision": "smoke-v3"}))
        run("antfly", "knowledge", "sync", "--limit", "10000")
        # Invalid native configuration must not advance the active consumer;
        # an operator can replace the pending request by its exact digest.
        run("antfly", "knowledge", "configure", body=json.dumps({
            "revision": "bad-index", "indexes": {"broken": {"type": "invalid-index"}},
        }), succeeds=False)
        pending = json.loads(run("antfly", "knowledge", "status"))
        assert pending["preparing"], pending
        run("antfly", "knowledge", "sync", succeeds=False)
        run("antfly", "knowledge", "configure", body=json.dumps({
            "revision": "recovered", "replace_pending": pending["preparing"]["digest"],
        }))
        run("antfly", "knowledge", "sync", "--limit", "10000")
        run("forget", "factory-policy")
        run("antfly", "knowledge", "sync", "--limit", "10000")
        forgotten = run("antfly", "memory", "recall", body='{"query":"snapshot merge policy"}')
        assert guidance not in forgotten, forgotten

        factory = project / "factory"
        factory.mkdir()
        provider = Path(__file__).resolve().parent.parent / "contrib/gascity-antfly/bin/gc-beads-antfly"
        factory_environment = {key: value for key, value in environment.items() if not key.startswith("GC_ANTFLY_")}
        factory_environment.update(GC_STORE_ROOT=str(factory), GC_BEADS_PREFIX="factory", ANTFLY_BD_BIN=binary)
        if args.url:
            factory_environment.update(GC_ANTFLY_URL=args.url, GC_ANTFLY_TABLE=table)
            if args.token_env:
                factory_environment["GC_ANTFLY_TOKEN_ENV"] = args.token_env

        def invoke(*command, body=None, succeeds=True):
            result = subprocess.run([str(provider), *command], env=factory_environment, cwd=factory,
                                    input=body, text=True, capture_output=True, timeout=args.timeout, check=False)
            assert (result.returncode == 0) == succeeds, result.stdout + result.stderr
            return result.stdout

        invoke("init")
        invoke("health")
        work = json.loads(invoke("create", body='{"title":"Factory work","type":"task"}'))
        ephemeral = json.loads(invoke("create", body='{"title":"Factory run","type":"task","ephemeral":true}'))
        ordinary = json.loads(invoke("ready"))
        assert any(item["id"] == work["id"] for item in ordinary), ordinary
        assert not any(item["id"] == ephemeral["id"] for item in ordinary), ordinary
        all_tiers = json.loads(invoke("ready", "--include-ephemeral"))
        assert any(item["id"] == ephemeral["id"] and item["ephemeral"] for item in all_tiers), all_tiers
        invoke("update", work["id"], body='{"status":"in_progress","assignee":"worker-a"}')
        invoke("update", work["id"], body='{"status":"in_progress","assignee":"worker-b"}', succeeds=False)
        invoke("set-metadata", work["id"], "review", body="passed\nwith evidence")
        stored = json.loads(invoke("get", work["id"]))
        assert stored["metadata"]["review"] == "passed\nwith evidence", stored
        invoke("close", work["id"])
        invoke("delete", "--force", ephemeral["id"])
        summary = json.loads(run("status", "--json"))["summary"]
        assert summary["total_issues"] == 2, summary
        print("PASS: native storage, claims, branches, three-way merges, knowledge replay, forgetting, cited context, evaluation, and Gas City provider")


if __name__ == "__main__":
    main()
