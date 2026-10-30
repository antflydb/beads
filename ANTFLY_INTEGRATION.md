# Native Antfly integration for Beads

This fork implements a native Antfly workflow backend, replayable knowledge
projection, snapshot branches, and a Gas City executable provider. The branch
is `antfly/integration` in `antflydb/beads`, originally based on upstream
`882467e8d`. The Go module path remains `github.com/steveyegge/beads`.

Antfly is authoritative in workspaces initialized with `bd antfly init`.
Existing Dolt workspaces keep their selected backend. This integration requires
no Dolt server and performs no Dolt/Antfly dual writes.

## Responsibilities

| Component | Responsibility |
| --- | --- |
| Beads | Issue lifecycle, explicit dependencies, readiness, claims, exact notes |
| Gas City | Scheduling, agent execution, formulas, review and workflow policy |
| Antfly storage | Durable rows, versions, atomic publication, snapshots and receipts |
| Antfly memory | Evidence, accepted guidance, corrections, forgetting and recall |
| Antfly graph/resolvers | Canonical entities/events, extraction and reviewed curation |

Inferred relationships have no authority over `bd ready`. Resolving two
concepts to one canonical entity never merges their Beads issue IDs. Knowledge
resolvers are refused if they target the authoritative work table.

## Storage and consistency

Both embedded and remote modes use the ordinary Antfly document engine.
Remote mode uses HTTP document versions and stateless OCC commits. Embedded
mode dynamically loads `libantfly` ABI 3 on macOS/Linux through cgo and uses a
persistent `.aflite` database. ABI version, options size, and threading mode
are checked. The checked-in header is from Antfly `386c2761ea`; its Apache
license is in `internal/storage/antfly/LICENSE.antfly`. Non-cgo builds support
remote mode and explicitly refuse embedded mode.

**Format 2 replaces the initial bounded aggregate.** Issues, comments, leases,
settings, exact notes, audit/outbox events, branch manifests, and integration
receipts are separate immutable records. Content-addressed directory pages
reference those records, and a small workspace head selects a complete
snapshot. New immutable records carry their exact hashed JSON bytes in a JSON
string envelope, so document-service whitespace, escaping, and number
reserialization cannot invalidate their content addresses. Existing format-2
records remain readable; normalized Go HTML escapes are restored only when
the original content hash matches. Each mutation stages only changed records/pages, then conditionally
publishes the head and changed ordinary issue/graph projections together.
Readers follow immutable references, so concurrent writes cannot mix snapshots.
Staging records from an aborted or interrupted publication are not visible as
work state. Existing format-1 aggregates migrate on their next successful write.

There is no 4 MiB limit on the entire workspace. Each individual record and
bounded native response is limited to 4 MiB. Real embedded persistence and
portable storage tests exercise workspaces larger than the old aggregate cap.

A workspace head remains the coordination fence for domain mutations. This
prevents write skew across dependency-cycle checks, sibling closes, readiness,
and claim selection. Domain operations currently reconstruct the working set
in memory; row storage reduces persistent write amplification but does not
make independent writes in one workspace concurrent. Immutable old records
remain retained for snapshots and recovery; online reachability GC is not
implemented. These are capacity/performance limits, not silently relaxed
consistency guarantees.

Only positively identified aborts are retried. Unknown commit outcomes are
reported as indeterminate. A committed-pending remote response waits for read
visibility, including immutable staging records, without replaying the write.
Application transaction callbacks execute at most once per call. Workspace
project identity is pinned in local metadata and checked on every read.

Ordinary `bead:` documents carry `beads_workspace`, `beads_revision`, and
explicit `_edges.beads` relationships. They support normal Antfly graph/query
consumers without decoding storage records. Their keys namespace equal IDs in
different workspaces. They are derived views; editing them is not a Beads edit.
`bead:` and `row:` workspace keys are reserved.

## Workflow capabilities

Implemented operations include guarded create/update/close/reopen, atomic
create-with-dependencies, batch closes, deterministic queries, dependency and
parent edits, cycle refusal, blocked-state propagation, deferred-parent
readiness, comments, exact keyed notes, guarded/cascading deletion with text
reference rewriting, atomic claims and ready-claims, release, heartbeat, and
scoped expired-lease recovery.

Durable claims have a one-hour lease. Heartbeat refreshes lease state without
changing the issue revision, update timestamp, or audit history. Reclaim and
release clear ownership and mint a revision that fences stale holders. Lease
time uses the caller's clock; deployments must maintain synchronized clocks.
The backend exposes explicit recovery operations; it does not invent an
orchestrator liveness policy or start a reaper automatically.

Ephemeral/no-history run beads retain their persistence flags. Ordinary ready
queries exclude the wisp tier; explicit ephemeral queries include it. Wisps
are not granted durable leases and do not emit versioned issue snapshots into
the knowledge outbox. Runtime wisps stay in main when a branch is created.

Unsupported methods and unsupported query fields return typed
`ErrUnsupported` errors. Beads-specific proto/gate/molecule lifecycle, custom
claim pools and status/type vocabularies, federation, Dolt SQL/history/remotes,
and the entire upstream storage interface are not claimed as compatible.
Gas City continues to own its orchestration policy through its provider API.

## Setup and per-city credentials

Build the fork with Bazel:

```sh
bazel build //cmd/bd:bd
```

Initialize a new project directory:

```sh
bd antfly init --prefix project --library /path/to/libantfly.dylib

# Or connect to remote Antfly. The variable name is persisted; its value is not.
bd antfly init --url https://antfly.example.com --table beads_workspaces \
  --workspace project --prefix project --token-env ANTFLY_TOKEN

bd create 'Fix token refresh' --type task
bd ready --json
bd update project-<id> --claim
bd heartbeat project-<id>
bd close project-<id> --reason 'Fixed and tested'
bd remember --key test-policy 'Run regression tests before merging'
```

`metadata.json` selects `backend: antfly`; `antfly.json` contains routing only.
Initialization refuses an existing different workspace. Network endpoints
require HTTPS except standard loopback names. URL credentials and redirects
are refused. Embedded mode supports `default/public`; other scopes are
refused rather than ignored.

`backend/antfly.Register()` plugs the backend into an embedding application's
registry. `backend/antfly.Open` also accepts `backend.OpenOptions`: pass a
separate `TokenCredential`, HTTP client, and user-agent per city. Explicit
credentials override ambient token lookup without changing environment
variables. Credential types the backend does not understand fail closed.
Branch and knowledge operations preserve these per-open injections.

## Knowledge ingestion, memory and entity resolution

Configure a versioned pipeline, then run its replayable consumer:

```sh
printf '{"revision":"factory-v1"}' | bd antfly knowledge configure
bd antfly knowledge sync --limit 1000
bd antfly knowledge watch --interval 2s
bd antfly knowledge status
```

The default needs no model. It creates separately scoped source, entity, and
event tables; publishes immutable source evidence; and configures label-routed
native graph resolvers to promote exact canonical entities/events. Scope
identity includes catalog, work table, workspace, and pinned project identity.
The inferred graph is separate from the workflow graph.

A configuration may add native `enrichments`, `indexes`, an `entity_resolver`,
and a native memory `space_config`. Supply real provider/model configuration
and an explicit recipe `revision` to enable extractor artifacts, recursive
conceptualization, vector indexes, and managed memory processing. Set
`managed_observations` only with a compatible managed memory configuration.
The adapter does not select a model, download weights, or invent credentials.
Inline credential values are refused; use native secret references or provider
environment defaults. The stored recipe, native producer configuration, and
configuration digest travel with source provenance.

Events preserve project identity, Beads ID, sequence, author, time, and source
text. The consumer uses stable native idempotency keys and frozen request
bodies. Checkpoints advance only after the destination accepts each event.
Consumer leases serialize work; a failed destination leaves the checkpoint
unchanged. Exact keyed notes remain exact Beads notes. Their accepted contents
are also projected as explicit memory guidance, while ordinary issue versions
remain evidence rather than automatically accepted lessons.

Forgetting, replacement, and source deletion invoke native source forgetting
and tombstone the corresponding knowledge documents. Native source fences and
conditional source publication prevent an expired worker or replay from
resurrecting forgotten evidence. History gaps fail closed. There is no automatic
audit pruning; restore a consistent engine snapshot after manual history loss
rather than silently skipping an event suffix.

Configuration also has a durable preparation fence. Retry the exact manifest
to resume an interrupted configuration. To supersede a pending request, supply
its digest as `replace_pending` in a new manifest. Changing an existing native
memory configuration requires `expected_memory_revision`, obtained from
`bd antfly memory inspect` (`state.revision`); the caller's guarded body/key remain fixed across
retries. Ordinary issue writes remain independent of knowledge availability.

Source documents freeze a crosswalk from qualified canonical graph keys into
namespaced memory subject IDs. Inspect it with:

```sh
bd antfly knowledge mapping entity/<canonical-key>
```

This crosswalk is not a native memory entity declaration. Memory entities have
independent immutable evidence and curation rules. The adapter does not
rewrite entity records on every source update. Query resolution, proposal
review, merge/suppression, assertion correction, and memory entity curation
are explicit native operations:

```sh
printf '{"text":"token refresh","label":"concept"}' | bd antfly knowledge render entities
printf '{"extraction":{"entities":[]}}' | bd antfly knowledge resolve entities
bd antfly knowledge reviews entities
# review, merge, suppress and memory correct/curate-entity take native JSON.
```

Direct memory access remains available through
`bd antfly memory create|inspect|configure|observe|recall|correct|curate-entity|outcome|forget`.
Mutations require an explicit stable `--idempotency-key`. Omitting `--space`
derives a project-scoped native memory namespace from routing and identity.
Sharing a space requires an explicit choice.

## Cited retrieval and evaluation

```sh
bd antfly context 'guidance for token refresh' --max-tokens 2048
```

Context combines exact work, current-configuration full-text retrieval, native
semantic search when embedding indexes are configured, graph expansion, and
memory recall. Results contain source IDs, citations, and producer provenance.
The complete encoded response is bounded conservatively in UTF-8 bytes against
the token budget. Unsupported graph placements fail through the native engine;
no cross-shard personalized-PageRank guarantee is invented.

`scripts/antfly-eval.py` runs versioned JSONL query/citation fixtures, checks
expected/forbidden evidence and context budgets, and can compare a prior report.
It can record supplied real review/test outcomes through native memory when
`--record-outcomes` is explicitly selected. It does not fabricate outcome labels
or promote procedures automatically. Retrieval tests demonstrate correctness;
production quality improvements require the factory's own measured outcomes.

## Snapshot branches and linear integration

```sh
bd antfly branch create feature
BEADS_DIR="$PWD/.beads/branches/feature" bd update project-<id> --title 'Branch edit'
bd antfly branch merge feature --idempotency-key integration-1 --dry-run
bd antfly branch merge feature --idempotency-key integration-1
bd antfly branch list
```

A branch captures a consistent immutable workspace head and records project,
branch, schema-format, and base-revision identity. It shares unchanged immutable
records/pages, with its own writable workspace head and checkout. This is a
logical workspace snapshot; it does not copy opaque native memory backing
tables or claim every database engine offers physical copy-on-write branches.
Creation is resumable and refuses to retarget an existing checkout.

Integration compares base, current main, and branch. Main-only changes survive;
branch-only changes apply; equal changes coalesce; conflicting edits are
reported with paths before anything is published. Labels merge as sets and
comments by stable unique IDs. Deletions propagate. The accepted graph and
close policies are revalidated against current main, including concurrent
cycles and open children. Branches cannot grant shared live claims or run
runtime wisps. Main ownership and leases are preserved.

Changes, the merge receipt, and the next integration base publish together.
The base advances to the accepted branch snapshot so a later integration
recognizes already accepted edits. A repeated key on the same branch state
returns its receipt; reusing it for another state fails. A failed or conflicting
integration publishes nothing. Previewing does not consume the key. Conflicts
can be resolved by explicit edits against current main or a new branch based
on the updated main state, followed by a fresh preview.

SQL `MERGE INTO` can apply a previously computed change set; it is not an
ancestry or conflict-policy substitute. This controller uses native OCC and
explicit lineage rather than pretending SQL supplies those semantics.

## Gas City

`contrib/gascity-antfly` is a schema-2 pack with context/sync/watch commands and
an executable Beads provider. It implements the documented city/rig data and
lifecycle protocol, including ephemeral readiness and atomic durable claims.
The provider initializes isolated embedded or remote scopes and refuses an
existing different backend. No `gc` rebuild is required for this executable
route. Its README provides configuration and credential examples.

A Go embedder can instead register this backend and use `OpenOptions` per city.
Executable-provider `Tx` remains sequential, as Gas City's protocol specifies;
use the Go workspace transaction API for atomic application callbacks.

## Validation

The shared portable transaction/lease conformance suite runs without SQL
fixture statements. Native fixtures open independent handles and inspect their
own durable representation. Shared role boundary cases also run; the backend's
registration records a ratcheting adoption ceiling for the remaining upstream
SQL-shaped fixtures and unsupported domain roles. This is not a claim of full
upstream backend conformance.

The repeatable `scripts/antfly-smoke.py` exercises real embedded or remote
Antfly through the actual CLI, including competing claims, branches, conflict
previews, deletions, knowledge configuration/replay/forgetting, cited context,
evaluation, and the Gas City executable provider. Focused tests cover unknown
outcomes, immutable staging visibility, credentials, quiet heartbeats, stale
holders, project identity, large workspaces, and graph/merge invariants.

Validation on this revision:

- `make ci-pr-lint` passed for native and Windows/macOS non-cgo builds.
- Core Antfly race tests, real ABI-3 persistence/recovery tests, shared portable
  conformance, and backend registration checks passed.
- Command tree/help goldens, proxy capability registry, shared host-validation
  policy, and command-citation policy checks passed. Generated interface shells,
  Gazelle sources, Go source inventory, and Bazel module metadata were synchronized.
- The latest full `make test` run reported 146 passed, 9 failed, and 44 skipped.
  The failures involve host requirements (Bash `mapfile`, `yq`,
  `timeout`, Linux-only checks), macOS process/listener detection, credential
  script assumptions, and an existing command-test timeout. The full upstream
  gate is therefore not green on this host.

Both complete `scripts/antfly-smoke.py` runs passed against a real embedded
ABI-3 library and a disposable remote standalone server. They exercised the
actual CLI and Gas City provider, including managed-processing refusal without
an enabled producer, checkpoint preservation, guarded memory-revision recovery,
configuration replacement, replay, forgetting, retrieval and evaluation. The
repeated embedded history replays required more than the original 90-second
per-command harness limit; the script exposes `--timeout` (default 300 seconds).
This validation uses deterministic extraction. It does not certify a particular
external model, production throughput, or every upstream Dolt workflow.


## Real factory trial (2026-10-29)

A real Gas City controller and three managed worker sessions completed a
planner → implementer → independent reviewer workflow against an Antfly work
ledger. The deliverable was a Python stdlib task-report CLI. Review fixed one
artifact defect and expanded its suite from 16 to 23 passing tests. All three
tasks closed, and the resulting live report showed three closed tasks and no
ready work. See [the trial record](contrib/gascity-antfly/FACTORY_TRIAL.md).

This was a scoped multi-agent coding workflow using explicit task dependencies,
not certification of the stock Gas Town pack or every formula/convoy path.
Gas City's own SQLite binding held its five orchestration classes. The trial's
18 source events were also ingested through the knowledge pipeline, and cited
context returned source evidence, promoted entity/event identities, and native
memory recall. The trial also exposed and fixed lifecycle scope/prefix handling
and an immutable JSON escape/hash mismatch that the earlier CLI smoke suite had missed.

## Gas City native infrastructure transport

`backend/antfly.OpenSnapshot` exposes a narrow generic JSON snapshot transport
for callers with their own domain schema. It uses the same embedded/remote
conditional publication and immutable row storage as the core backend, without
issue lifecycle validation or issue/KG projections. Caller field names are
encoded into a separate row namespace, including names such as issues, events,
comments and leases. A caller must provision a dedicated workspace, validate its
own schema identity, and reconcile unknown publication outcomes. Only
`ErrSnapshotConflict` means a commit was positively rejected.

Gas City's `antfly-beads` provider uses this transport for sessions, graph,
messaging, orders and nudges. It composes Gas City's domain semantics inside
one snapshot transaction; it does not route infrastructure through the work
issue API or a SQLite mirror. Core work and its memory/entity/knowledge outbox
continue to use the existing Beads Antfly workspace.

Validation: the real ABI-3 AntflyLite generic snapshot test passes with a
workspace larger than 4 MiB, byte-sensitive content and stale-CAS rejection.
Row/snapshot/branch regression tests also pass with race instrumentation.

The work lifecycle additionally accepts the fixed `convoy` and `merge-request`
types used by Gas City for durable work grouping and integration requests.
They use the same guarded lifecycle as ordinary work. This does not enable
arbitrary custom types or session/message infrastructure in the work workspace.
Gate and persistent molecule support is described below. The convoy regression was reproduced during the native
factory run and passes with the row/lifecycle regression suite.

Generic snapshots retain a bounded per-handle read cache: up to 8 MiB / 2048
hash-verified immutable records and one reconstructed snapshot up to 16 MiB.
Every load still reads and compares the live conditional head; cached caller
buffers are copied, so mutations cannot alter later reads. This removes the
repeated native per-record reads that made the real Gas City controller slow.
Internal content-addressed records are append-only; editing or deleting them
behind a live head is outside the snapshot protocol. Core work transport keeps
its original uncached integrity-checking behavior.

## Stock-factory compatibility work

Native boolean queries now use Beads' shared Querier role and pass its full
contract suite. Work gates support guarded waiter/await-id updates, blocking
readiness and legacy CLI close through the same lifecycle. Generic snapshots
also expose hash-bucket point/batch lookup with expected-head checks, and a
checksummed portable logical workspace archive with conditional restore.
An archive covers its workspace; native memory held outside it requires a
separate engine backup. These additions do not certify stock factory operation.

Dedicated generic workspaces additionally support explicit, resumable garbage
collection. Collection fences the workspace head; every physical delete and
fence release checks that ownership version. A crash leaves a marker that a
subsequent collection can adopt. Current-head records are retained, unrelated
namespaces are untouched, and old readers can fail rather than receive partial
state. Embedded and remote round trips pass. The generic collector refuses
core workspaces because forks share immutable records. Core history has its own
`bd antfly gc` / `CollectWorkspaceGarbage` operator door. It validates the
workspace project identity, fences main's catalog, fences every registered fork,
and marks their current heads plus recursively referenced merge bases before
scanning those workspaces' immutable namespaces. Every delete checks the main
owner version; a replaced collector cannot delete or release its successor's
fence. Forks are released before main. Pending fork creation must finish first.
All participating clients must honor maintenance markers (the pinned SDK does).
Older in-flight readers may need to retry after collection. Interrupted fences
are resumable through the same operator door, while ordinary opens refuse them.
Embedded and remote tests verify unchanged live snapshots and successful
three-way merges after collection; fault injection covers writers, owner loss,
foreign identity, and pending-fork refusal. Domain audit/outbox retention remains
separate: physically collecting history does not discard live evidence.

Persistent legacy poured molecules are now admitted to the work lifecycle.
Their open-child close guard remains active. This does not admit templates or
infrastructure records into the work ledger.

The native Sweeper passes the shared purge/prune contract, including cutoff and
pattern selection, pinned/live-dependent/reference protection, retained IDs,
dry-run behavior, and one atomic history publication. Retired IDs cannot be
silently recreated. Bulk comment reads now support the relational export path.
Sweep previews select and count from a single committed snapshot.

Embedded database archives use the engine's full native backup interface and
restore into a new database file. The native qualification test verifies two
independent workspaces and memory evidence after restore; existing destinations
are refused. Remote full-cluster backup and restore were also exercised through
the engine's existing administrative API against a fresh server, with matching
catalogs and workspace heads. This is separate from logical workspace archives.
Generic collection accepts a schema validator before adopting an interrupted
maintenance fence, so foreign workspace state is never mutated during recovery.

Typed work-plane retention is available through the executable provider's
`retention` operation. Reaping and compaction select from one committed
snapshot and publish once through native OCC; previews discard the same staged
operations. Orphans close leaf-first, live/young/foreign descendants hold their
ancestors, references and pins prevent deletion, and removed IDs are retired.
Persistent promotion uses the shared persistence normalizer and preserves
assignment, comments and dependencies. Queue state, sessions, operation journals
and external messaging remain owned by their domain services. In a split city,
Gas City exposes this through its typed `storage retain-work` operator command;
the opaque provider protocol stays refused by generic `gc bd` forwarding.

Core issue point reads now use per-issue immutable lookup rows published with
the workspace head. They fetch only the issue, lease, comments and outgoing
edges, validating project identity and pinning the index/record join. Legacy
heads fall back to the original read path until their next write. A 1,000-issue
oracle verifies exact aggregate contents, detached caller buffers and bounded
document reads. The complete backend gate passed against embedded and remote
Antfly after this addition; broad queries and writes still load domain state.

## Stock factory runtime corrections

The executable provider persists an explicit embedded library path at new
initialization, so reopening a work scope does not depend on the initializing
shell environment. A failed prefix read reports its underlying failure; only
a successful, unequal prefix read is a routing mismatch. Gas City service
environment setup is documented in the provider README.

Embedded opens, table discovery and document reads retry only native BUSY
responses with bounded backoff and cancellation. Mutation and indeterminate
commit outcomes retain their existing handling.

The legacy storage facade supplies SearchIssueIDs, including staged transaction
reads, with the same filters, sort and pagination as issue searches. Substring
ID matching supports the CLI partial-ID resolver used during worker claims.
External dependency targets use shared Beads classification: missing local
targets are refused; foreign IDs and external: references are retained without
a local foreign key. Gas City's split-store owner gate must resolve foreign
blockers before dispatch and claim. A raw work-store read alone cannot certify
cross-store readiness.

The full backend suite passes against embedded and remote Antfly with these
corrections. A live two-rig bundled workflow exposed the claim lookup gap;
completion and crash/redelivery qualification remain in progress.


## Atomic native batch operations

The native `BatchApplier` stages create, update, close and dependency additions
against one private snapshot, then publishes once through the revision fence.
Request planning and lifecycle guards are shared with single-item operations;
forward metadata references resolve after all creates. A later item refusal
rolls back the whole request. Type conflicts, cycles and hierarchy conflicts
are refused, including hierarchy introduced after a blocking edge. The bulk
cycle hint never disables graph validation. Commit conflicts and indeterminate
outcomes return without replay or provisional results.

Embedded and remote contract tests cover metadata references, stale-guard
rollback, final hierarchy and cycle refusals, and guarded metadata-plus-close.
The transport fixture additionally proves one publication per successful batch
and no replay of an indeterminate commit. Advanced blocking dependency types
remain explicitly unsupported. This supplies the atomic work-graph capability
used by legacy formula adapters; full live factory qualification is separate.


Generic snapshot caching charges immutable payloads, keys and entry overhead to
an 8 MiB budget. A fixed 2,048-entry cap caused a 3,000-row live snapshot to
reread 2,086 documents after one row changed. The regression now reads only the
fresh head, changed page and changed row, preserving hash validation and the
fresh-head fence. This addresses one measured factory bottleneck; live factory
latency and sustained acceptance remain separate qualification requirements.
