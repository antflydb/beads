# Real Gas City factory trial

Date: 2026-10-29. Gas City: `c221441` (source built locally on macOS).
Beads: `antfly/integration`, with fixes discovered during this trial.

## Work and result

A live Gas City controller launched three independent Claude Code sessions,
with all work items and dependency edges in a real remote Antfly table.
The planner and implementer completed before the reviewer could claim work;
Gas City performed the dependency-driven handoffs. The operator supplied
requirements, observed the run, resolved environmental prompts, and repaired
integration faults. The generated code, tests, plan, and review were agent work.

| Stage | Work item | Deliverable |
| --- | --- | --- |
| Planning | `bd-6ab9118157415b11` | PLAN.md with acceptance checks |
| Implementation | `bd-3c7b34164ce7e0e0` | task_report.py, README.md, 16 tests |
| Independent review | `bd-f9f60ab6515be6ee` | One error-reporting fix, 7 additional tests, REVIEW.md |

All three work items closed. The operator independently ran the final 23 tests
successfully and ran the tool against the real ledger. Its deterministic
Markdown report showed `closed: 3`, total 3, no open work, and no ready work.
After the coding workflow, the operator ingested its 18 source events and a
factual outcome note through the deterministic knowledge pipeline. The consumer
reached pending=0, and FACTORY_CONTEXT.json contains cited source evidence,
promoted entity/event identities, and native memory recall.

The artifact reads exactly `bd list --all --limit 0 --json` and
`bd ready --limit 0 --json`; it does not mutate the ledger.

The isolated local checkout is `.worktrees/factory-report` under the Antfly
repository. The city, its configs, controller logs and transcripts are under
`.worktrees/gascity-antfly-factory/.tools/`. These are local trial artifacts,
not production data or installed global services.

## Topology and compatibility

Antfly served the work ledger through the executable provider. Gas City's
supported split topology used `sqlite-beads` for graph, sessions, messaging,
orders, and nudges. The three agents used explicit task dependency edges.
Dispatch used `gc sling <role> <bead> --no-convoy --no-formula`; this trial does
not establish stock Gas Town pack, formula-v2, merge-queue, or convoy support.
A queued attempt to change the review provider did not change the created
session: the actual reviewer was a separate Claude session, not Codex.

Use the current site-binding format for external rigs: declare `[[rigs]]` in
city.toml and put the checkout's path in `.gc/site.toml` under `[[rig]]`.
Match the rig's configured prefix to the initialized ledger prefix. Lifecycle
init must receive the SDK's positional `<scope-dir> <prefix>` arguments.

The stock generated work query falls back to unsupported `bd query` calls.
For this small workflow each role used a supported query such as:

```toml
work_query = "bd ready --metadata-field gc.routed_to=report/implementer --unassigned --json --limit 1"
```

The custom fork's command tree also fails Gas City's pinned `gc bd` inventory
check on split cities. Workers used `bd` directly for work-ledger commands and
`"$GC_BIN"` for Gas City control, since `gc` was a shell alias for git commit on
this host. Some bundled housekeeping orders using `gc bd` consequently refused;
their refusal was preserved. No command allowlist was bypassed.

## Faults exposed and corrected

- Lifecycle calls do not always set `GC_STORE_ROOT`. The provider now accepts
  positional init scope/prefix and the lifecycle BEADS_DIR/city projection.
  Data calls still require GC_STORE_ROOT. A supplied mismatched scope or an
  existing incompatible prefix is refused. Start does not initialize a scope.
- A close reason containing `->` exposed JSON serialization differences:
  Go had hashed HTML-escaped JSON, and Antfly returned the equivalent literal
  character. The bytes existed, but the content-hash check refused them.
  New immutable records retain exact JSON in a string envelope. Legacy HTML
  escapes recover only when restoring them reproduces the original hash.
  The existing factory resumed with no data rewrite or task recreation.
- The model service had transient API retries. Planning also needed a bounded
  operator interrupt. A review fixture requested optional scratch-file deletion;
  the operator denied it, and review continued using fresh fixture names.

## What this proves

This is evidence of real agent execution, dependency-aware scheduling, native
claims, coding, independent review, lifecycle calls, and closure on an Antfly
work ledger. It is not an unattended soak test or an all-Antfly infrastructure
binding, and no external-model KG extraction was configured. Follow-up work
includes a vetted fork CLI inventory, a supported default work-query profile,
and broader formula/convoy and infrastructure compatibility.


## Validation and shutdown

The operator reran the artifact's 23 tests and live report. The complete remote
CLI smoke, core backend race tests, real ABI-3 native integration tests, and
native/Windows/macOS non-cgo lint passed. The full upstream test gate reported
146 passed, 9 failed locally, and 44 skipped; the remaining failures were host
tooling/platform requirements and existing process/timing tests, documented in
ANTFLY_INTEGRATION.md. The complete upstream gate is not green on this host.

`gc stop` confirmed "City stopped." The disposable Antfly server was then
stopped explicitly. Data, artifacts, configs and logs were retained, and no
production ledger or global supervisor installation was used.
