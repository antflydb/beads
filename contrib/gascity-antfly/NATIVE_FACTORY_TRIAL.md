# Native AntflyLite Gas City factory trial

Date: 2026-10-29. This follows the earlier [remote work-ledger trial](FACTORY_TRIAL.md).

The [Gas City fork](https://github.com/antflydb/gascity/tree/antfly/gascity-integration)
adds the compiled `antfly-beads` infrastructure provider. Its
[design](https://github.com/antflydb/gascity/blob/antfly/gascity-integration/ANTFLY_INTEGRATION.md)
describes configuration, transaction guarantees and remaining limits. Work uses
this Beads backend; the five infrastructure class stores use a separate native
Antfly workspace. Neither plane uses SQLite in this topology.

Real Gas City-managed Claude sessions planned, implemented and independently
reviewed a Python task-report tool. All three tasks and their convoy closed.
The implementer supplied 34 tests; review added 11 tests and corrected README
wording. The operator independently reran all 45 tests successfully. An extra
implementation worker lost the atomic work claim and declined to overwrite the
winning worker. The local artifact is `.worktrees/factory-antfly-lite` under the
Antfly repository, including PLAN.md, REVIEW.md and FACTORY_REPORT.md.

Additional live checks published a four-node formula-v2 graph and session mail.
During coding, deferred delivery still used the upstream file queue. The native
queue was subsequently wired and validated through real provider close/reopen,
CLI enqueue, claim, acknowledgement and terminal-shadow lookup. Its disposable
CLI notice was withdrawn after validation. Legacy pending batches use the
existing migration shim; logs, configuration and skip counters remain files.

The production provider constructors, independent native handles, durable
revision/fence counters, guarded batches, callback rollback, graph publication,
native queue routing and the exact SDK pin have Bazel regression coverage.
The immutable generic-snapshot cache still reloads the live head and verifies
record hashes; warm reads of the 81-record live workspace measured 0, 1 and 0 ms.

This was operator-supervised. The planner needed an interrupt and an extra
implementation session was created during diagnosis. An older post1326 native
runtime stalled in timestamp lookup; a post1338 ABI-3 build passed concurrent
handle checks and completed the restored workflow. Qualify the runtime beyond
its ABI number. Several stock housekeeping orders were disabled because the
exec work provider does not support their CLI calls. Generic `gc bd` forwarding
remains restricted, the legacy session reconciler is used, and automatic
physical migration is unsupported. This is not a fully unattended soak test.

The city, foreground supervisor and temporary service were stopped or removed.
Trial data and logs remain local; the artifact was not committed or published.
