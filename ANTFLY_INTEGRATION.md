# Beads + Antfly integration

Status: experimental implementation on `antfly/integration` in
`antflydb/beads`, based on upstream `882467e8d`. This document distinguishes
the implemented foundation from the intended long-term architecture.

## Objective

Run Beads against embedded or remote Antfly, retaining Beads' work vocabulary
and command behavior. Use Antfly's ordinary storage engine for exact work
state, its native `/memory/v1` service for evidence-backed guidance, and its
AutoSchemaKG/resolver pipeline for relationships across tasks, components,
decisions, reviews, tests, and incidents. Gas City remains the orchestrator.

Antfly replaces Dolt in an explicitly selected workspace. This is not a
Dolt-to-Antfly dual-write integration and does not require a running Dolt
database. Existing Dolt workspaces continue to select Dolt. The Go module
path remains `github.com/steveyegge/beads` to minimize fork divergence.

## Ownership

| Owner | Responsibility |
| --- | --- |
| Beads | Issue lifecycle, explicit dependencies, readiness, atomic claims, notes |
| Gas City | Scheduling, agent execution, formulas, reviews, workflow policy |
| Antfly primary storage | Durable work state, transactions, versions, source events |
| Antfly memory | Assertions, evidence, corrections, temporal validity, recall |
| Antfly graph/resolution | Canonical concepts/entities, extracted relationships, taxonomy |

Explicit workflow edges and inferred knowledge have different authority.
An extracted `depends_on` relationship never changes whether `bd ready`
offers a task. A proposed entity merge never merges two Beads issue IDs.

## Implemented foundation

The backend registers as `antfly` through the existing backend registry.
`backend/antfly` is the public library entry point; `cmd/bd/antfly.go` adds
the registration to this distribution. Other binaries, including Gas City,
must link/register that package too before opening an Antfly workspace.
Installing this fork's `bd` alone does not register it in an independently
built `gc` binary.

Both transports implement the same conditional-document contract:

* Remote: Antfly HTTP document lookup and stateless OCC transaction commit.
  Lookup requires a nonzero `X-Antfly-Version`. A missing record is version
  zero, and insertion is guarded against a concurrent initial write.
* Embedded: dynamically loaded `libantfly` ABI 3 through cgo on macOS/Linux,
  a persistent `.aflite` database, named table handles, version predicates,
  and the ordinary native batch API. The library is not vendored. The
  checked-in Apache-licensed ABI header comes from Antfly commit `386c2761ea`.
  The header's license is included in `internal/storage/antfly/LICENSE.antfly`.
  Pure-Go/other-platform builds retain the remote transport and explicitly
  refuse embedded mode.

The first storage format is a bounded workspace aggregate containing issue
records, dependency records, exact keyed memories, configuration, and audit
events. An Antfly version predicate atomically publishes the entire mutation.
It is deliberately limited to **4 MiB per workspace aggregate**, including
events. Each mutation reads and rewrites the aggregate; independent writes
in one workspace contend. This is a correctness-first experimental backend,
not the intended storage layout for a large shared factory. Hitting the
bound fails before publication; history is never silently truncated.

Every commit also publishes ordinary issue documents in the same table and
transaction. These documents carry `beads_workspace`, `beads_revision`, and
explicit `_edges.beads` relationships. Their keys include a hash of the
workspace, so equal Beads IDs in different workspaces remain distinct.
New tables receive a `beads` graph index. Ordinary document enrichments and
queries can consume these projections without knowing the aggregate format.
They are derived views; ordinary external writes to them are not Beads edits.
The reserved `bead:` key prefix must not be used as a workspace key.

The implemented core includes guarded create/update/close/reopen, dependency
insertion/removal with cycle refusal and blocked-state propagation, ready
queries, atomic claim/ready-claim, comments, exact keyed memories, and staged
transactions for create-with-dependencies and batch closes. Unsupported
methods return typed `ErrUnsupported` errors through generated shells.
Unsupported nonzero query filters are refused rather than silently ignored.
This is not a claim of full upstream backend conformance.

Claims persist their holder, version, start time, and a one-hour lease.
Automatic heartbeat/reclaim behavior is not implemented yet; a lease timestamp
does not imply a functioning reaper. Custom claim pools, custom status/type
vocabularies, templates, wisps, gates, and molecule lifecycle remain outside
the initial supported workflow. Dolt history/remotes/federation/SQL commands
remain unsupported. Database branch/merge commands are also not implemented.

Remote writes retry only a positively identified transaction conflict.
Lost/malformed commit responses are indeterminate and are never automatically
replayed. Committed-pending responses wait for read visibility; a visibility
timeout reports an indeterminate outcome without replay. Public transaction callbacks execute at most once per call. Exact
keyed notes retain their Beads semantics; they are not silently substituted
with ranked memory recall or model-generated assertions.

### Configuration and usage

Build this distribution through the repository's Bazel targets. For example:

```sh
bazel build //cmd/bd:bd
```

Run initialization in a new project, not in the Beads contributor checkout:

```sh
# Embedded; requires a matching libantfly release on the machine.
bd antfly init --prefix project --library /path/to/libantfly.dylib

# Remote; provision or attach the same named workspace from several machines.
export ANTFLY_TOKEN='...'
bd antfly init --url https://antfly.example.com --table beads_workspaces \
  --workspace project --prefix project --token-env ANTFLY_TOKEN

bd create 'Fix token refresh' --type task
bd ready --json
bd update project-<id> --claim
bd close project-<id> --reason 'Fixed and tested'
bd remember --key test-policy 'Run the regression suite before merging'
```

`metadata.json` selects `backend: antfly`. `antfly.json` stores the endpoint
or local path, table/workspace identity, and optional credential environment
variable name. No bearer token is persisted. Network endpoints require HTTPS
except loopback, URL-embedded credentials are rejected, and redirects are not
followed. Database/namespace headers select catalog scope, not authorization.
Initialization refuses an existing metadata file and checks an existing
remote workspace's prefix before adopting its project identity. Every subsequent
workspace read checks that pinned identity, including native memory commands.
Embedded mode supports the `default/public` catalog scope; other scopes are
refused rather than silently ignored.

The native memory plane is available independently of exact keyed notes:

```sh
printf '{}' | bd antfly memory create --space project --idempotency-key init-v1
printf '{"query":"token refresh guidance"}' | bd antfly memory recall --space project
```

Omitting `--space` derives a stable memory namespace from the configured
database, namespace, table, workspace, and pinned project identity. Use an explicit space only when
sharing guidance is intentional.

`observe` and `forget` accept native Antfly JSON requests on stdin and require
an explicit stable `--idempotency-key`. Managed extraction/distillation and
semantic recall require appropriately configured models. The adapter does
not invent a model configuration or download weights. Automatic conversion
of all issue events/notes into memory assertions is not yet implemented.

## Intended row-oriented storage

Replace the aggregate with ordinary records for issues, dependencies,
comments, settings, exact notes, idempotency receipts, audit/outbox events,
and small coordination records. Use stable workspace/repository namespaces.
Keep generated schemas and table migrations inside the adapter; avoid
application-specific changes to Beads' canonical issue model.

Each domain mutation uses native transactions with explicit read versions:

1. Read the issue and all state authorizing the mutation.
2. Validate lifecycle/ownership guards and relevant graph constraints.
3. Commit issue changes, readiness maintenance, audit records, and outbox
   entries together. Use coordination records to prevent write skew where
   concurrent edge additions or sibling closures share a graph invariant.
4. Return the committed state; classify proved abort versus unknown outcome.

Readiness must not depend on an asynchronously maintained knowledge graph.
Exact task-state queries use authoritative rows and the same snapshot as
their dependency reads. Claim selection and ownership changes share a
transaction. Heartbeats/reclaim require explicit ownership and expiry rules.
Splitting records must preserve the invariants the aggregate currently
enforces by serializing every workspace mutation.

## Memory and AutoSchemaKG

Project commit-ordered source events through a replayable consumer. Capture
immutable evidence versions with workspace/repository identity, Beads ID,
event sequence, author, source text, and code/review/test references.
Checkpoints advance only after the corresponding destination accepts the
operation; retry with the same native idempotency key and exact body.
Retention gaps require an explicit resnapshot, not a silently skipped suffix.

Use explicit assertions for accepted decisions and guidance. Use managed
observations for optional extraction/distillation. Keep proposal/review
states for uncertain identity merges and inferred lessons. Corrections,
forgetting, and source deletions must propagate through native memory
lifecycle operations, with fences preventing replay from resurrecting them.

Configure extraction enrichments on ordinary issue/review/run documents,
canonical entity/event promotion, and optional recursive conceptualization.
Use the existing resolver for deterministic/exact identity and candidate
matching. A shared canonical entity key needs an explicit mapping into the
memory service's entity IDs; the two public models are not interchangeable.
Store inferred relations in a separate graph from `beads` workflow edges.
Provider/model revision, resolver policy, provenance, and extraction coverage
are part of every reproducible configuration.

An initial retrieval path combines exact work filters with full-text/vector
search and graph expansion, then returns cited evidence under a token budget.
Seeded personalized PageRank requires a supported placement; do not promise
cross-shard/serverless behavior that the engine explicitly rejects.
Integrate retrieval into Gas City packs and agent tools, and evaluate whether
it improves review/test outcomes before automatically promoting procedures.

## Snapshot branches and linear integration

Antfly's stable snapshots and transaction/SQL mutation primitives support
this workflow; the Beads-facing branch controller is future adapter work:

1. Capture a consistent base snapshot and manifest. Record workspace identity,
   schema generation, base revision/digest, and branch identity.
2. Open a writable isolated copy. Preserve the base and record branch changes,
   including deletions/tombstones. Avoid treating a database copy as a cheap
   copy-on-write branch unless the chosen engine actually provides that.
3. Serialize integration into main. Compute the difference between base,
   current main, and branch. Serial merges alone do not remove stale-base
   conflicts.
4. Apply branch-only changes, preserve main-only changes, accept equal edits,
   and resolve or report conflicting edits. Use field-aware policy where
   appropriate, such as distinct comment IDs and explicit dependency edges.
5. Validate task invariants against current main and commit the accepted
   mutation set with revision predicates and an idempotent integration receipt.
   Derived projections follow the accepted main writes.

SQL `MERGE INTO` can apply an already-computed change set. It does not infer
ancestry or supply a three-way conflict policy. Deletions and schema changes
need explicit treatment. Never replace current main with an old branch's
whole aggregate. Live claims/leases remain coordinated in main; a copied
lease cannot authorize a second actor to take shared work. Do not merge
opaque native memory backing tables; memory lineage/correction semantics
remain owned by the memory service.

## Verification and promotion gates

* Test concurrent independent writers, at-most-one claim, stale guards,
  dependency cycles, close/reopen readiness, rollback, and unknown outcomes.
* Verify real embedded restart persistence and real remote HTTP behavior,
  in addition to transport-contract tests.
* Exercise existing CLI commands against the backend in disposable projects.
  `python3 scripts/antfly-smoke.py --bd <built-bd> --library <libantfly>`
  covers the core workflow, two competing CLI claim processes, and native
  memory observation/recall. `--url <disposable-server>` selects remote mode.
* Expand the public Beads raw/role conformance suites as capabilities land;
  adapt relational fixture helpers without weakening behavioral assertions.
* Test native memory requests, graph projections, forgetting/replay, and model
  failure behavior before advertising automatic knowledge ingestion.
* Prove branch conflict handling, delete propagation, interrupted integration,
  and lease isolation before enabling branch/merge commands.

The backend is an experimental alternative, not yet a production replacement
for the entire Dolt command surface or a complete Gas City storage provider.

### Initial branch validation

Validated on macOS arm64 with a real ABI 3 library and a disposable standalone
Antfly server. Both modes passed the CLI smoke workflow, including competing
claim processes, restart persistence, and native memory observation/recall.
A real remote graph query traversed a projected `blocks` edge successfully.
The focused backend tests also cover stale guards, rollback, project identity,
indeterminate outcomes, pending-commit visibility, pinned blockers, label
validation, and deferred-parent readiness.

`make ci-pr-lint` passed its native and Windows/macOS non-cgo passes. Command
help/capability tests, formatting, unsupported-generator tests, and Bazel
metadata synchronization passed. The full `make test` gate remains red on
this host: 146 passed, 9 failed, 44 skipped. Failures included missing
`yq`/`timeout`, Bash/Linux-only guards, macOS process-inspection assertions,
a workspace-gate timing assertion, and a scripts-suite timeout. No claim of
a fully green upstream suite or full backend conformance is made here.
