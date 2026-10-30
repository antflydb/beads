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
		})
	}
}
