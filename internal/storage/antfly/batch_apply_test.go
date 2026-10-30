package antfly

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage/backends"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/issueops"
)

func batchCreate(key string) issueops.ApplyItem {
	return issueops.ApplyItem{Kind: issueops.ItemCreate, Create: &issueops.CreateItem{Key: key, Issue: &types.Issue{Title: key, IssueType: types.TypeTask}}}
}
func batchEdge(from, to string, kind types.DependencyType) issueops.ApplyItem {
	return issueops.ApplyItem{Kind: issueops.ItemDepAdd, DepAdd: &issueops.DepAddItem{Source: issueops.Ref{Key: from}, Target: issueops.Ref{Key: to}, Type: kind}}
}
func exerciseBatchContract(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	applier, e := s.BatchApplier()
	if e != nil {
		t.Fatal(e)
	}
	first := batchCreate("first")
	first.Create.MetadataRefs = map[string]issueops.Ref{"next": {Key: "second"}}
	req := issueops.ApplyBatchRequest{Actor: "test", SkipPerEdgeCycleCheck: true, Items: []issueops.ApplyItem{first, batchCreate("second"), batchEdge("second", "first", types.DepBlocks)}}
	before, _ := json.Marshal(req)
	r, e := applier.ApplyBatch(ctx, req)
	if e != nil {
		t.Fatal(e)
	}
	after, _ := json.Marshal(req)
	if string(before) != string(after) {
		t.Fatal("mutated caller request")
	}
	if len(r.Items) != 3 || r.Items[2].DependsOnID != r.Keys["first"] {
		t.Fatalf("result: %+v", r)
	}
	var metadata map[string]string
	if e = json.Unmarshal(r.Items[0].Issue.Metadata, &metadata); e != nil || metadata["next"] != r.Keys["second"] {
		t.Fatalf("forward metadata: %s %v", r.Items[0].Issue.Metadata, e)
	}
	bytes, version, e := s.transport.Load(ctx)
	if e != nil {
		t.Fatal(e)
	}
	// A later guard refusal must roll back an earlier successful create.
	wrong := types.StatusClosed
	_, e = applier.ApplyBatch(ctx, issueops.ApplyBatchRequest{Actor: "test", Items: []issueops.ApplyItem{batchCreate("rolled-back"), {Kind: issueops.ItemUpdate, Update: &issueops.UpdateItem{Target: issueops.Ref{ID: r.Keys["first"]}, ExpectedStatus: &wrong, Patch: issueops.IssuePatch{Title: issueops.Field[string]{Set: true, Value: "bad"}}}}}})
	var itemErr *issueops.ItemError
	if !errors.Is(e, issueops.ErrStatusMismatch) || !errors.As(e, &itemErr) || itemErr.Index != 1 {
		t.Fatalf("guard: %v", e)
	}
	got, v, e := s.transport.Load(ctx)
	if e != nil || v != version || string(got) != string(bytes) {
		t.Fatalf("refused batch changed snapshot: %v", e)
	}
	// This blocks edge is legal first; the later hierarchy edge invalidates it.
	_, e = applier.ApplyBatch(ctx, issueops.ApplyBatchRequest{Actor: "test", SkipPerEdgeCycleCheck: true, Items: []issueops.ApplyItem{batchCreate("a"), batchCreate("b"), batchEdge("a", "b", types.DepBlocks), batchEdge("a", "b", types.DepParentChild)}})
	// Same-pair type conflict is separately typed and must not publish either.
	var conflict *issueops.DependencyTypeConflictError
	if !errors.As(e, &conflict) {
		t.Fatalf("type conflict: %v", e)
	}
	_, e = applier.ApplyBatch(ctx, issueops.ApplyBatchRequest{Actor: "test", SkipPerEdgeCycleCheck: true, Items: []issueops.ApplyItem{batchCreate("a"), batchCreate("b"), batchCreate("c"), batchEdge("a", "c", types.DepBlocks), batchEdge("a", "b", types.DepParentChild), batchEdge("b", "c", types.DepParentChild)}})
	var hierarchy *issueops.DependencyHierarchyConflictError
	if !errors.As(e, &hierarchy) || !hierarchy.BlockerIsAncestor {
		t.Fatalf("final hierarchy gate: %v", e)
	}
	got, v, e = s.transport.Load(ctx)
	if e != nil || v != version || string(got) != string(bytes) {
		t.Fatalf("end gate changed snapshot: %v", e)
	}
	// The bulk hint never permits a cyclic final graph.
	_, e = applier.ApplyBatch(ctx, issueops.ApplyBatchRequest{Actor: "test", SkipPerEdgeCycleCheck: true, Items: []issueops.ApplyItem{batchCreate("a"), batchCreate("b"), batchEdge("a", "b", types.DepBlocks), batchEdge("b", "a", types.DepBlocks)}})
	if !errors.Is(e, issueops.ErrDependencyCycle) {
		t.Fatalf("cycle gate: %v", e)
	}
	got, v, e = s.transport.Load(ctx)
	if e != nil || v != version || string(got) != string(bytes) {
		t.Fatalf("cycle published: %v", e)
	}
	// Guarded metadata + closure is the factory recovery primitive.
	token := r.Items[0].RowVersion
	closed, e := applier.ApplyBatch(ctx, issueops.ApplyBatchRequest{Actor: "test", Items: []issueops.ApplyItem{{Kind: issueops.ItemUpdate, Update: &issueops.UpdateItem{Target: issueops.Ref{ID: r.Keys["first"]}, ExpectedVersion: &token, Patch: issueops.IssuePatch{Metadata: issueops.MetadataPatch{Set: map[string]json.RawMessage{"receipt": json.RawMessage(`true`)}}}}}, {Kind: issueops.ItemClose, Close: &issueops.CloseItem{Target: issueops.Ref{ID: r.Keys["first"]}}}}})
	if e != nil || closed.Items[1].Issue.Status != types.StatusClosed {
		t.Fatalf("guarded closure: %+v %v", closed, e)
	}
}
func TestBatchApplyAtomicPublicationAndRefusals(t *testing.T) {
	s, transport := testStore(t)
	before := transport.commits
	exerciseBatchContract(t, s)
	if transport.commits != before+2 {
		t.Fatalf("successful batches must publish once each: got %d", transport.commits-before)
	}
	transport.failure = errIndeterminate
	before = transport.commits
	r, e := (&batchApplier{s}).ApplyBatch(context.Background(), issueops.ApplyBatchRequest{Actor: "test", Items: []issueops.ApplyItem{batchCreate("uncertain")}})
	if !errors.Is(e, errIndeterminate) || transport.commits != before+1 || len(r.Items) != 0 {
		t.Fatalf("indeterminate outcome replayed or returned provisional results: %+v %v", r, e)
	}
}
func TestBatchApplyNativeAndRemote(t *testing.T) {
	for _, mode := range []string{"embedded", "remote"} {
		t.Run(mode, func(t *testing.T) {
			target := Target{Table: "batch_" + digest([]byte(t.Name() + time.Now().String()))[:16], Workspace: "main"}
			if mode == "embedded" {
				target.Library = os.Getenv("ANTFLY_TEST_LIBRARY")
				if target.Library == "" {
					t.Skip("ANTFLY_TEST_LIBRARY required")
				}
				target.Path = filepath.Join(t.TempDir(), "batch.aflite")
			} else {
				target.URL = os.Getenv("ANTFLY_TEST_URL")
				if target.URL == "" {
					t.Skip("ANTFLY_TEST_URL required")
				}
			}
			dir := filepath.Join(t.TempDir(), ".beads")
			if e := Initialize(context.Background(), dir, target, "bd"); e != nil {
				t.Fatal(e)
			}
			s, e := OpenStore(context.Background(), dir, backends.OpenOptions{})
			if e != nil {
				t.Fatal(e)
			}
			defer func() { _ = s.Close() }()
			exerciseBatchContract(t, s)
		})
	}
}
