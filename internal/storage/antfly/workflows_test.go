package antfly

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/backend"
	"github.com/steveyegge/beads/backend/conformance"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/issueops"
)

func cloneState(t *testing.T, st *state) *state {
	t.Helper()
	b, e := json.Marshal(st)
	if e != nil {
		t.Fatal(e)
	}
	out := emptyState()
	if e = json.Unmarshal(b, out); e != nil {
		t.Fatal(e)
	}
	out.initialize()
	for _, r := range out.Issues {
		r.Issue.RowVersion = r.Version
	}
	return out
}
func TestThreeWayMergePreservesClaimsAndCombinesComments(t *testing.T) {
	s, _ := testStore(t)
	createTestIssue(t, s, "bd-merge")
	ctx := context.Background()
	base, _, e := s.load(ctx)
	if e != nil {
		t.Fatal(e)
	}
	main, branch := cloneState(t, base), cloneState(t, base)
	main.Issues["bd-merge"].Issue.Description = "main description"
	branch.Issues["bd-merge"].Issue.Title = "branch title"
	_, e = main.claim(&main.Issues["bd-merge"].Issue, "worker")
	if e != nil {
		t.Fatal(e)
	}
	main.Issues["bd-merge"].Issue.Comments = []*types.Comment{{ID: "main-comment", IssueID: "bd-merge", Text: "main"}}
	branch.Issues["bd-merge"].Issue.Comments = []*types.Comment{{ID: "branch-comment", IssueID: "bd-merge", Text: "branch"}}
	main.Issues["bd-merge"].Issue.Labels = []string{"main"}
	branch.Issues["bd-merge"].Issue.Labels = []string{"branch"}
	merged, conflicts, e := mergeStates(base, main, branch, "test")
	if e != nil || len(conflicts) > 0 {
		t.Fatalf("merge: %v %v", conflicts, e)
	}
	issue := merged.Issues["bd-merge"].Issue
	if issue.Title != "branch title" || issue.Description != "main description" || issue.Assignee != "worker" || issue.LeaseExpiresAt == nil || len(issue.Comments) != 2 || len(issue.Labels) != 2 {
		t.Fatalf("merged=%+v", issue)
	}
	branch.Issues["bd-merge"].Issue.Description = "branch description"
	_, conflicts, e = mergeStates(base, main, branch, "test")
	if e != nil || len(conflicts) != 1 || conflicts[0] != "issues/bd-merge/description" {
		t.Fatalf("conflicts=%v err=%v", conflicts, e)
	}
}
func TestMergeRejectsConcurrentGraphCycleAndLiveDeletion(t *testing.T) {
	s, _ := testStore(t)
	createTestIssue(t, s, "bd-a")
	createTestIssue(t, s, "bd-b")
	ctx := context.Background()
	base, _, _ := s.load(ctx)
	main, branch := cloneState(t, base), cloneState(t, base)
	if e := main.addEdge("bd-a", "bd-b", types.DepBlocks, "test"); e != nil {
		t.Fatal(e)
	}
	if e := branch.addEdge("bd-b", "bd-a", types.DepBlocks, "test"); e != nil {
		t.Fatal(e)
	}
	if _, _, e := mergeStates(base, main, branch, "test"); e == nil {
		t.Fatal("accepted combined graph cycle")
	}
	main, branch = cloneState(t, base), cloneState(t, base)
	_, _ = main.claim(&main.Issues["bd-a"].Issue, "worker")
	delete(branch.Issues, "bd-a")
	_, conflicts, e := mergeStates(base, main, branch, "test")
	if e != nil || len(conflicts) == 0 {
		t.Fatalf("live deletion: %v %v", conflicts, e)
	}
}
func TestLeasesQuietHeartbeatAndStaleRecovery(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	createTestIssue(t, s, "bd-lease")
	l := &leases{s}
	if _, e := (&lifecycle{s}).Claim(ctx, issueops.ClaimRequest{Actor: "worker", IssueID: "bd-lease"}); e != nil {
		t.Fatal(e)
	}
	before, _, _ := s.load(ctx)
	version := before.Issues["bd-lease"].Version
	updated := before.Issues["bd-lease"].Issue.UpdatedAt
	events := len(before.Events)
	if _, e := l.Heartbeat(ctx, issueops.HeartbeatRequest{Actor: "worker", IssueID: "bd-lease"}); e != nil {
		t.Fatal(e)
	}
	after, _, _ := s.load(ctx)
	if after.Issues["bd-lease"].Version != version || !after.Issues["bd-lease"].Issue.UpdatedAt.Equal(updated) || len(after.Events) != events {
		t.Fatal("heartbeat changed lifecycle history")
	}
	if _, e := l.Heartbeat(ctx, issueops.HeartbeatRequest{Actor: "other", IssueID: "bd-lease"}); !errors.Is(e, storage.ErrAlreadyClaimed) {
		t.Fatal(e)
	}
	if e := s.mutate(ctx, func(st *state) error {
		past := time.Now().Add(-time.Minute)
		st.Issues["bd-lease"].Issue.LeaseExpiresAt = &past
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	recovered, e := l.Reclaim(ctx, issueops.ReclaimRequest{Actor: "reaper", Filter: types.ReclaimFilter{IDs: []string{"bd-lease"}}})
	if e != nil || len(recovered.Reclaimed) != 1 {
		t.Fatalf("reclaim=%+v %v", recovered, e)
	}
	_, e = (&lifecycle{s}).Update(ctx, issueops.UpdateRequest{Actor: "worker", IssueID: "bd-lease", ExpectedVersion: &version, Patch: issueops.IssuePatch{Title: issueops.Field[string]{Set: true, Value: "stale"}}})
	if !errors.Is(e, storage.ErrVersionMismatch) {
		t.Fatal(e)
	}
}
func TestSourceIdsSeparatePlanesAndVersions(t *testing.T) {
	issue := event{ID: "policy", Sequence: 1, Operation: "create"}
	note := event{ID: "policy", Sequence: 1, Operation: "remember"}
	if eventSource("p", "v1", issue) == eventSource("p", "v1", note) {
		t.Fatal("issue and note sources collide")
	}
	if eventSource("p", "v1", issue) == eventSource("p", "v2", issue) {
		t.Fatal("configuration sources collide")
	}
	if e := rejectInlineCredentials(map[string]any{"producer": map[string]any{"api_key": "secret-token"}}); e == nil {
		t.Fatal("inline credential accepted")
	}
	if e := rejectInlineCredentials(map[string]any{"producer": map[string]any{"api_key": "${secret:models}"}}); e != nil {
		t.Fatal(e)
	}
}
func TestContextBudget(t *testing.T) {
	s, _ := testStore(t)
	createTestIssue(t, s, "bd-budget")
	if e := s.mutate(context.Background(), func(st *state) error {
		st.Issues["bd-budget"].Issue.Description = strings.Repeat("é", 1000)
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	out, e := s.Context(context.Background(), ContextRequest{Query: "budget", MaxTokens: 256})
	if e != nil {
		t.Fatal(e)
	}
	b, _ := json.Marshal(out)
	if len(b) > 256 || !out.Truncated || len(out.Sources) == 0 {
		t.Fatalf("budget result %s", b)
	}
}

func TestFactoryProviderEphemeralAndAtomicClaim(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	create := func(body string) string {
		t.Helper()
		raw, e := s.ExecuteFactory(ctx, "create", nil, json.RawMessage(body), "factory")
		if e != nil {
			t.Fatal(e)
		}
		var value struct {
			ID string `json:"id"`
		}
		if e = json.Unmarshal(raw, &value); e != nil {
			t.Fatal(e)
		}
		return value.ID
	}
	ephemeral := create(`{"title":"run bead","type":"task","ephemeral":true,"labels":["run"]}`)
	durable := create(`{"title":"work bead","type":"task"}`)
	ordinary, e := s.ExecuteFactory(ctx, "ready", nil, nil, "factory")
	if e != nil || strings.Contains(string(ordinary), ephemeral) || !strings.Contains(string(ordinary), durable) {
		t.Fatalf("ordinary ready=%s %v", ordinary, e)
	}
	all, e := s.ExecuteFactory(ctx, "ready", []string{"--include-ephemeral"}, nil, "factory")
	if e != nil || !strings.Contains(string(all), ephemeral) {
		t.Fatalf("ephemeral ready=%s %v", all, e)
	}
	if _, e = s.ExecuteFactory(ctx, "update", []string{durable}, json.RawMessage(`{"status":"in_progress","assignee":"worker-a"}`), "factory"); e != nil {
		t.Fatal(e)
	}
	if _, e = s.ExecuteFactory(ctx, "update", []string{durable}, json.RawMessage(`{"status":"in_progress","assignee":"worker-b"}`), "factory"); !errors.Is(e, storage.ErrAlreadyClaimed) {
		t.Fatalf("foreign claim=%v", e)
	}
	if _, e = s.ExecuteFactory(ctx, "set-metadata", []string{durable, "multiline"}, json.RawMessage("line one\nline two"), "factory"); e != nil {
		t.Fatal(e)
	}
	stored, e := s.GetIssue(ctx, durable)
	if e != nil || !strings.Contains(string(stored.Metadata), `line one\nline two`) {
		t.Fatalf("metadata=%s err=%v", stored.Metadata, e)
	}
}

func TestPortableTransactionalWorkflowContract(t *testing.T) {
	conformance.RunTransactionalWorkflowContract(t, func(t *testing.T) conformance.TransactionalWorkflowFixture {
		tr, _ := testRows()
		s := New(tr, false)
		ctx := context.Background()
		if _, e := (&identity{s}).Bootstrap(ctx, issueops.BootstrapRequest{Prefix: "bd", ProjectID: "portable"}); e != nil {
			t.Fatal(e)
		}
		return conformance.TransactionalWorkflowFixture{Store: s, IndependentStore: func() backend.DoltStorage { return New(tr, false) }, SetLeaseExpiry: func(ctx context.Context, id string, expiry time.Time) error {
			return s.mutate(ctx, func(st *state) error { st.Issues[id].Issue.LeaseExpiresAt = &expiry; return nil })
		}, AuditCount: func(ctx context.Context) (int, error) {
			st, _, e := s.load(ctx)
			if e != nil {
				return 0, e
			}
			return len(st.Events), nil
		}}
	})
}

func TestExistingRoleBoundaryContracts(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	heart := conformance.HeartbeaterFixture{IssuePrefix: "bd", Heartbeater: &leases{s}}
	t.Run("heartbeat_absent", func(t *testing.T) { conformance.RunHeartbeaterAnswersNotFoundForAnAbsentID(t, ctx, heart) })
	t.Run("heartbeat_validation", func(t *testing.T) { conformance.RunHeartbeaterRefusesABlankOrOverLongActor(t, ctx, heart) })
	t.Run("release_absent", func(t *testing.T) {
		conformance.RunReleaserRefusesAnAbsentID(t, ctx, conformance.ReleaserFixture{IssuePrefix: "bd", Releaser: &leases{s}})
	})
	t.Run("reclaim_absent", func(t *testing.T) {
		conformance.RunLeaseReclaimerAnswersAnAbsentIDWithAnEmptyResult(t, ctx, conformance.LeaseReclaimerFixture{IssuePrefix: "bd", LeaseReclaimer: &leases{s}})
	})
	t.Run("delete_validation", func(t *testing.T) {
		conformance.RunDeleterRefusesAMalformedRequest(t, ctx, conformance.DeleterFixture{Deleter: &deleter{s}})
	})
}

func TestAntflyWorkGateLifecycleAndWaiterFencing(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	gate := &types.Issue{ID: "bd-gate", Title: "timer", IssueType: types.TypeGate, AwaitType: "timer", AwaitID: "1h"}
	if e := s.CreateIssue(ctx, gate, "test"); e != nil {
		t.Fatal(e)
	}
	createTestIssue(t, s, "bd-aftergate")
	if _, e := (&graph{s}).AddDependencies(ctx, issueops.AddDependenciesRequest{Actor: "test", Edges: []issueops.DependencyEdge{{IssueID: "bd-aftergate", DependsOnID: gate.ID, Type: types.DepBlocks}}}); e != nil {
		t.Fatal(e)
	}
	before, e := s.GetIssue(ctx, gate.ID)
	if e != nil {
		t.Fatal(e)
	}
	old := before.RowVersion
	if _, e = (&lifecycle{s}).Update(ctx, issueops.UpdateRequest{Actor: "test", IssueID: gate.ID, ExpectedVersion: &old, Patch: issueops.IssuePatch{Waiters: issueops.Field[[]string]{Set: true, Value: []string{"worker"}}}}); e != nil {
		t.Fatal(e)
	}
	if _, e = (&lifecycle{s}).Update(ctx, issueops.UpdateRequest{Actor: "test", IssueID: gate.ID, ExpectedVersion: &old, Patch: issueops.IssuePatch{Waiters: issueops.Field[[]string]{Set: true, Value: []string{"loser"}}}}); !errors.Is(e, storage.ErrVersionMismatch) {
		t.Fatalf("stale waiter write: %v", e)
	}
	rows, e := s.GetReadyWork(ctx, types.WorkFilter{})
	if e != nil {
		t.Fatal(e)
	}
	for _, r := range rows {
		if r.ID == "bd-aftergate" {
			t.Fatal("unresolved gate did not block work")
		}
	}
	if e = s.CloseIssue(ctx, gate.ID, "timer elapsed", "test", ""); e != nil {
		t.Fatal(e)
	}
	rows, e = s.GetReadyWork(ctx, types.WorkFilter{})
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, r := range rows {
		if r.ID == "bd-aftergate" {
			found = true
		}
	}
	if !found {
		t.Fatal("resolved gate kept work blocked")
	}
}

func TestAntflyLegacyPouredMoleculeProvider(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	raw, e := s.ExecuteFactory(ctx, "create", nil, json.RawMessage(`{"type":"molecule","title":"legacy work","metadata":{"gc.formula_name":"test"}}`), "test")
	if e != nil {
		t.Fatal(e)
	}
	var root struct {
		ID string `json:"id"`
	}
	if e = json.Unmarshal(raw, &root); e != nil {
		t.Fatal(e)
	}
	child, _ := json.Marshal(map[string]any{"title": "first", "parent_id": root.ID})
	if _, e = s.ExecuteFactory(ctx, "create", nil, child, "test"); e != nil {
		t.Fatal(e)
	}
	if e = s.CloseIssue(ctx, root.ID, "done", "test", ""); !errors.Is(e, storage.ErrCloseOpenChildren) {
		t.Fatalf("legacy root must keep open-child guard: %v", e)
	}
}

func TestAntflyStockSweeperProtectsReferencesAndRetiresIDs(t *testing.T) {
	s, tr := testStore(t)
	ctx := context.Background()
	createTestIssue(t, s, "bd-old")
	createTestIssue(t, s, "bd-live")
	if e := s.CloseIssue(ctx, "bd-old", "done", "test", ""); e != nil {
		t.Fatal(e)
	}
	if _, e := (&lifecycle{s}).Update(ctx, issueops.UpdateRequest{Actor: "test", IssueID: "bd-live", Patch: issueops.IssuePatch{Notes: issueops.Field[string]{Set: true, Value: "See (bd-old)."}}}); e != nil {
		t.Fatal(e)
	}
	sweeper, e := s.Sweeper()
	if e != nil {
		t.Fatal(e)
	}
	result, e := sweeper.Sweep(ctx, issueops.SweepRequest{Actor: "test", Tier: issueops.SweepDurable, IDPattern: "bd-old", ProtectReferenced: true})
	if e != nil || result.Swept != 0 || result.Skipped.Referenced != 1 {
		t.Fatalf("protected sweep: %+v %v", result, e)
	}
	before := tr.version
	result, e = sweeper.Sweep(ctx, issueops.SweepRequest{Actor: "test", Tier: issueops.SweepDurable, IDPattern: "bd-old", RetainIDs: true, DryRun: true})
	if e != nil || result.Swept != 1 || tr.version != before {
		t.Fatalf("sweep preview: %+v %v", result, e)
	}
	result, e = sweeper.Sweep(ctx, issueops.SweepRequest{Actor: "test", Tier: issueops.SweepDurable, IDPattern: "bd-old", RetainIDs: true})
	if e != nil || result.Swept != 1 {
		t.Fatalf("retaining sweep: %+v %v", result, e)
	}
	issue := &types.Issue{ID: "bd-old", Title: "must not replay", IssueType: types.TypeTask}
	if e = s.CreateIssue(ctx, issue, "test"); e == nil {
		t.Fatal("retired id was reused")
	}
}

type countedSnapshotReads struct {
	Transport
	loads int
}

func (t *countedSnapshotReads) Load(ctx context.Context) ([]byte, uint64, error) {
	t.loads++
	return t.Transport.Load(ctx)
}
func TestAntflySweepPreviewUsesOneCommittedSnapshot(t *testing.T) {
	s, tr := testStore(t)
	ctx := context.Background()
	createTestIssue(t, s, "bd-old-preview")
	if e := s.CloseIssue(ctx, "bd-old-preview", "done", "test", ""); e != nil {
		t.Fatal(e)
	}
	counted := &countedSnapshotReads{Transport: tr}
	reader := New(counted, true)
	sweeper, e := reader.Sweeper()
	if e != nil {
		t.Fatal(e)
	}
	out, e := sweeper.Sweep(ctx, issueops.SweepRequest{Tier: issueops.SweepDurable, IDPattern: "bd-old-preview", DryRun: true})
	if e != nil || out.Swept != 1 || counted.loads != 1 {
		t.Fatalf("preview=%+v error=%v root snapshot reads=%d", out, e, counted.loads)
	}
}

func TestNativeExportRelationsCountOnlyBlockingEdgesAndPreservePlanes(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	for _, id := range []string{"bd-export-root", "bd-export-child", "bd-export-blocker"} {
		createTestIssue(t, s, id)
	}
	if err := s.mutate(ctx, func(st *state) error {
		st.Config["project_id"] = "export-project"
		st.Issues["bd-export-child"].Issue.NoHistory = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	g := &graph{s}
	_, err := g.AddDependencies(ctx, issueops.AddDependenciesRequest{Edges: []issueops.DependencyEdge{
		{IssueID: "bd-export-child", DependsOnID: "bd-export-root", Type: types.DepParentChild},
		{IssueID: "bd-export-child", DependsOnID: "bd-export-blocker", Type: types.DepBlocks},
	}, Actor: "test"})
	if err != nil {
		t.Fatal(err)
	}
	counts, err := s.GetDependencyCounts(ctx, []string{"bd-export-child", "bd-export-root", "bd-export-blocker", "bd-missing"})
	if err != nil || counts["bd-export-child"].DependencyCount != 1 || counts["bd-export-root"].DependentCount != 0 || counts["bd-export-blocker"].DependentCount != 1 || *counts["bd-missing"] != (types.DependencyCounts{}) {
		t.Fatalf("export counts=%+v err=%v", counts, err)
	}
	wisps, durable, err := s.PartitionWispIDs(ctx, []string{"bd-export-child", "bd-export-root", "bd-missing"})
	if err != nil || len(wisps) != 1 || wisps[0] != "bd-export-child" || len(durable) != 1 || durable[0] != "bd-export-root" {
		t.Fatalf("planes=%v %v err=%v", wisps, durable, err)
	}
	first, err := s.ArchiveScope(ctx)
	if err != nil || !strings.HasPrefix(first, "antfly_") {
		t.Fatalf("scope=%s err=%v", first, err)
	}
	if err = s.mutate(ctx, func(st *state) error { st.Config["antfly.branch.id"] = "fork"; return nil }); err != nil {
		t.Fatal(err)
	}
	second, err := s.ArchiveScope(ctx)
	if err != nil || first == second {
		t.Fatalf("branch scope collision: %s %s %v", first, second, err)
	}
}

func TestIntegrationPreservesRetiredIDsAndRefusesResurrection(t *testing.T) {
	s, _ := testStore(t)
	createTestIssue(t, s, "bd-retired-merge")
	ctx := context.Background()
	base, _, err := s.load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	main, fork := cloneState(t, base), cloneState(t, base)
	delete(fork.Issues, "bd-retired-merge")
	at := time.Now().UTC()
	fork.Retired["bd-retired-merge"] = at
	merged, conflicts, err := mergeStates(base, main, fork, "test")
	if err != nil || len(conflicts) != 0 || !merged.Retired["bd-retired-merge"].Equal(at) || merged.Issues["bd-retired-merge"] != nil {
		t.Fatalf("retirement lost: state=%+v conflicts=%v err=%v", merged, conflicts, err)
	}
	main = cloneState(t, merged)
	fork = cloneState(t, merged)
	fork.Issues["bd-retired-merge"] = base.Issues["bd-retired-merge"]
	_, conflicts, err = mergeStates(merged, main, fork, "test")
	if err != nil || len(conflicts) == 0 {
		t.Fatalf("retired ID resurrected: conflicts=%v err=%v", conflicts, err)
	}
}
