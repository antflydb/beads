package antfly

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/backends"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/issueops"
)

func exerciseDependencyAnnotations(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	createTestIssue(t, s, "bd-annotations-source")
	createTestIssue(t, s, "bd-annotations-target")
	before, err := s.GetIssue(ctx, "bd-annotations-source")
	if err != nil {
		t.Fatal(err)
	}
	d := &types.Dependency{IssueID: before.ID, DependsOnID: "bd-annotations-target", Type: types.DepRelated, Metadata: `{"tag":"value","count":2}`, ThreadID: "thread-one"}
	if err = s.AddDependency(ctx, d, "test"); err != nil {
		t.Fatal(err)
	}
	d.Metadata = `{"caller":"mutation"}`
	d.ThreadID = "caller-mutation"
	got, err := s.GetIssue(ctx, before.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Dependencies) != 1 || got.Dependencies[0].Metadata != `{"tag":"value","count":2}` || got.Dependencies[0].ThreadID != "thread-one" {
		t.Fatalf("lost dependency annotations: %+v", got.Dependencies)
	}
	if got.RowVersion <= before.RowVersion {
		t.Fatal("dependency publication did not fence stale issue writers")
	}
	stale := before.RowVersion
	if _, err = (&lifecycle{s}).Update(ctx, issueops.UpdateRequest{Actor: "test", IssueID: before.ID, ExpectedVersion: &stale, Patch: issueops.IssuePatch{Title: issueops.Field[string]{Set: true, Value: "stale"}}}); !errors.Is(err, storage.ErrVersionMismatch) {
		t.Fatalf("stale writer: %v", err)
	}
	raw, version, err := s.transport.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	d.Metadata = `{ "count":2.0, "tag":"value" }`
	d.ThreadID = ""
	if err = s.AddDependency(ctx, d, "test"); err != nil {
		t.Fatal(err)
	}
	after, next, err := s.transport.Load(ctx)
	if err != nil || version != next || string(raw) != string(after) {
		t.Fatalf("canonical annotation re-add published a change: %v", err)
	}
	d.ThreadID = "thread-two"
	if err = s.AddDependency(ctx, d, "test"); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetIssue(ctx, d.IssueID)
	if err != nil || got.Dependencies[0].ThreadID != "thread-two" {
		t.Fatalf("thread update: %+v %v", got, err)
	}
	raw, version, err = s.transport.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	d.Metadata = "invalid-json"
	if err = s.AddDependency(ctx, d, "test"); !errors.Is(err, storage.ErrValidation) {
		t.Fatalf("invalid metadata: %v", err)
	}
	after, next, err = s.transport.Load(ctx)
	if err != nil || version != next || string(raw) != string(after) {
		t.Fatalf("invalid annotation changed snapshot: %v", err)
	}
	if err = s.AddDependency(ctx, nil, "test"); !errors.Is(err, storage.ErrValidation) {
		t.Fatalf("nil dependency: %v", err)
	}
	// Create and batch updates must share the same annotation rules.
	l := &lifecycle{s}
	created, err := l.Create(ctx, issueops.CreateRequest{Actor: "test", Issue: &types.Issue{ID: "bd-annotations-created", Title: "created", IssueType: types.TypeTask}, Dependencies: []issueops.CreateDependency{{TargetID: d.DependsOnID, Type: types.DepRelated, Metadata: `{"source":"create"}`, ThreadID: "create-thread"}}})
	if err != nil || len(created.Issue.Dependencies) != 1 || created.Issue.Dependencies[0].ThreadID != "create-thread" {
		t.Fatalf("create annotations: %+v %v", created, err)
	}
	applier, _ := s.BatchApplier()
	batch := issueops.ApplyBatchRequest{Actor: "test", Items: []issueops.ApplyItem{{Kind: issueops.ItemDepAdd, DepAdd: &issueops.DepAddItem{Source: issueops.Ref{ID: created.Issue.ID}, Target: issueops.Ref{ID: d.DependsOnID}, Type: types.DepRelated, Metadata: `{"source":"batch"}`}}}}
	result, err := applier.ApplyBatch(ctx, batch)
	if err != nil || len(result.Items) != 1 || !result.Items[0].Changed {
		t.Fatalf("batch annotation update: %+v %v", result, err)
	}
	got, err = s.GetIssue(ctx, created.Issue.ID)
	if err != nil || got.Dependencies[0].ThreadID != "create-thread" || got.Dependencies[0].Metadata != `{"source":"batch"}` {
		t.Fatalf("batch lost annotation/thread: %+v %v", got, err)
	}
	result, err = applier.ApplyBatch(ctx, batch)
	if err != nil || result.Items[0].Changed {
		t.Fatalf("batch re-add not idempotent: %+v %v", result, err)
	}
}

func TestDependencyAnnotationsRoundTripAndFence(t *testing.T) {
	s, _ := testStore(t)
	exerciseDependencyAnnotations(t, s)
}
func TestDependencyAnnotationsNativeAndRemote(t *testing.T) {
	for _, mode := range []string{"embedded", "remote"} {
		t.Run(mode, func(t *testing.T) {
			target := Target{Table: "dep_annotations_" + digest([]byte(t.Name() + time.Now().String()))[:16], Workspace: "main"}
			if mode == "embedded" {
				target.Library = os.Getenv("ANTFLY_TEST_LIBRARY")
				if target.Library == "" {
					t.Skip("ANTFLY_TEST_LIBRARY required")
				}
				target.Path = filepath.Join(t.TempDir(), "work.aflite")
			} else {
				target.URL = os.Getenv("ANTFLY_TEST_URL")
				if target.URL == "" {
					t.Skip("ANTFLY_TEST_URL required")
				}
			}
			dir := filepath.Join(t.TempDir(), ".beads")
			ctx := context.Background()
			if err := Initialize(ctx, dir, target, "bd"); err != nil {
				t.Fatal(err)
			}
			s, err := OpenStore(ctx, dir, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			exerciseDependencyAnnotations(t, s)
			exerciseConditionalDependencies(t, s)
			exerciseWaitsForFanout(t, s)
		})
	}
}

func TestConditionalDependencyReadinessAndCycleFence(t *testing.T) {
	s, _ := testStore(t)
	exerciseConditionalDependencies(t, s)
}

func exerciseConditionalDependencies(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	for _, id := range []string{"bd-conditional-source", "bd-conditional-blocker", "bd-conditional-child"} {
		createTestIssue(t, s, id)
	}
	if err := s.AddDependency(ctx, &types.Dependency{IssueID: "bd-conditional-source", DependsOnID: "bd-conditional-blocker", Type: types.DepConditionalBlocks}, "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddDependency(ctx, &types.Dependency{IssueID: "bd-conditional-child", DependsOnID: "bd-conditional-source", Type: types.DepParentChild}, "test"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"bd-conditional-source", "bd-conditional-child"} {
		blocked, _, err := s.IsBlocked(ctx, id)
		if err != nil || !blocked {
			t.Fatalf("%s blocked=%v err=%v", id, blocked, err)
		}
	}
	l := &lifecycle{s}
	if _, err := l.Close(ctx, issueops.CloseRequest{IssueID: "bd-conditional-source", Actor: "test"}); !errors.Is(err, storage.ErrCloseBlocked) {
		t.Fatalf("conditional close fence: %v", err)
	}
	if err := s.AddDependency(ctx, &types.Dependency{IssueID: "bd-conditional-blocker", DependsOnID: "bd-conditional-child", Type: types.DepBlocks}, "test"); !errors.Is(err, issueops.ErrDependencyCycle) {
		t.Fatalf("combined scheduling cycle: %v", err)
	}
	if _, err := l.Close(ctx, issueops.CloseRequest{IssueID: "bd-conditional-blocker", Actor: "test"}); err != nil {
		t.Fatal(err)
	}
	blocked, _, err := s.IsBlocked(ctx, "bd-conditional-source")
	if err != nil || blocked {
		t.Fatalf("closed conditional blocker still blocks: %v %v", blocked, err)
	}
}

func TestWaitsForFanoutReadiness(t *testing.T) {
	s, _ := testStore(t)
	exerciseWaitsForFanout(t, s)
}
func exerciseWaitsForFanout(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	for _, id := range []string{"bd-fanout-spawner", "bd-fanout-first", "bd-fanout-second", "bd-fanout-any", "bd-fanout-also"} {
		createTestIssue(t, s, id)
	}
	l := &lifecycle{s}
	created, err := l.Create(ctx, issueops.CreateRequest{Actor: "test", Issue: &types.Issue{ID: "bd-fanout-all", Title: "wait for children", IssueType: types.TypeTask}, WaitsFor: &issueops.WaitsFor{SpawnerID: "bd-fanout-spawner"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(created.Issue.Dependencies) != 1 || created.Issue.Dependencies[0].Type != types.DepWaitsFor {
		t.Fatalf("create lost fanout dependency: %+v", created.Issue)
	}
	for _, d := range []*types.Dependency{
		{IssueID: "bd-fanout-any", DependsOnID: "bd-fanout-spawner", Type: types.DepWaitsFor, Metadata: `{"gate":"any-children","spawner_id":"ignored-other-id"}`},
		{IssueID: "bd-fanout-also", DependsOnID: "bd-fanout-spawner", Type: types.DepWaitsFor, Metadata: `{"gate":"any-children","also_blocks":true}`},
	} {
		if err = s.AddDependency(ctx, d, "test"); err != nil {
			t.Fatal(err)
		}
	}
	check := func(id string, want bool) {
		t.Helper()
		blocked, _, err := s.IsBlocked(ctx, id)
		if err != nil || blocked != want {
			t.Fatalf("%s blocked=%v want=%v err=%v", id, blocked, want, err)
		}
		issue, err := s.GetIssue(ctx, id)
		if err != nil || issue.IsBlocked != want {
			t.Fatalf("%s point-read blocked=%v want=%v err=%v", id, issue, want, err)
		}
		ready, err := s.GetReadyWorkWithCounts(ctx, types.WorkFilter{})
		if err != nil {
			t.Fatal(err)
		}
		present := false
		for _, i := range ready {
			if i.ID == id {
				present = true
			}
		}
		if present == want {
			t.Fatalf("%s ready=%v blocked=%v", id, present, want)
		}
	}
	check("bd-fanout-all", false)
	check("bd-fanout-any", false)
	check("bd-fanout-also", true)
	for _, id := range []string{"bd-fanout-first", "bd-fanout-second"} {
		if err = s.AddDependency(ctx, &types.Dependency{IssueID: id, DependsOnID: "bd-fanout-spawner", Type: types.DepParentChild}, "test"); err != nil {
			t.Fatal(err)
		}
	}
	check("bd-fanout-all", true)
	check("bd-fanout-any", true)
	if _, err = l.Close(ctx, issueops.CloseRequest{IssueID: "bd-fanout-all", Actor: "test"}); !errors.Is(err, storage.ErrCloseBlocked) {
		t.Fatalf("fanout close fence: %v", err)
	}
	if _, err = l.Close(ctx, issueops.CloseRequest{IssueID: "bd-fanout-first", Actor: "test"}); err != nil {
		t.Fatal(err)
	}
	check("bd-fanout-all", true)
	check("bd-fanout-any", false)
	check("bd-fanout-also", true)
	if _, err = l.Close(ctx, issueops.CloseRequest{IssueID: "bd-fanout-second", Actor: "test"}); err != nil {
		t.Fatal(err)
	}
	check("bd-fanout-all", false)
	check("bd-fanout-also", true)
	if _, err = l.Close(ctx, issueops.CloseRequest{IssueID: "bd-fanout-spawner", Actor: "test"}); err != nil {
		t.Fatal(err)
	}
	check("bd-fanout-also", false)
	// A late child must close an already-open fanout gate again. This checks
	// that readiness never uses a one-time expansion or stale source flag.
	createTestIssue(t, s, "bd-fanout-late")
	if err = s.AddDependency(ctx, &types.Dependency{IssueID: "bd-fanout-late", DependsOnID: "bd-fanout-spawner", Type: types.DepParentChild}, "test"); err != nil {
		t.Fatal(err)
	}
	check("bd-fanout-all", true)
	check("bd-fanout-any", false)
	// A malformed legacy gate name defaults to all-children. Empty metadata
	// uses the same compatibility rule; metadata.spawner_id is never authority.
	if err = s.AddDependency(ctx, &types.Dependency{IssueID: "bd-fanout-any", DependsOnID: "bd-fanout-spawner", Type: types.DepWaitsFor, Metadata: `{"gate":"legacy-unknown"}`}, "test"); err != nil {
		t.Fatal(err)
	}
	check("bd-fanout-any", true)
	if _, err = l.Close(ctx, issueops.CloseRequest{IssueID: "bd-fanout-late", Actor: "test"}); err != nil {
		t.Fatal(err)
	}
	check("bd-fanout-any", false)
	raw, version, err := s.transport.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var unsupported *storage.ErrUnsupported
	if err = s.AddDependency(ctx, &types.Dependency{IssueID: "bd-fanout-any", DependsOnID: "external:other:spawner", Type: types.DepWaitsFor}, "test"); !errors.As(err, &unsupported) {
		t.Fatalf("foreign dynamic gate must fail explicitly: %v", err)
	}
	after, next, err := s.transport.Load(ctx)
	if err != nil || version != next || string(raw) != string(after) {
		t.Fatalf("rejected foreign gate published: %v", err)
	}
}
