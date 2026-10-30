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
	"github.com/steveyegge/beads/issueops"
)

func TestCoreGarbageCollectionNativeAndRemotePreservesForkBases(t *testing.T) {
	for _, mode := range []string{"embedded", "remote"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, ".beads")
			target := Target{Table: "beads_gc_" + digest([]byte(t.Name() + time.Now().String()))[:16], Workspace: "main"}
			if mode == "embedded" {
				target.Library = os.Getenv("ANTFLY_TEST_LIBRARY")
				if target.Library == "" {
					t.Skip("ANTFLY_TEST_LIBRARY required")
				}
				target.Path = filepath.Join(root, "work.aflite")
			} else {
				target.URL = os.Getenv("ANTFLY_TEST_URL")
				if target.URL == "" {
					t.Skip("ANTFLY_TEST_URL required")
				}
			}
			ctx := context.Background()
			if err := Initialize(ctx, dir, target, "bd"); err != nil {
				t.Fatal(err)
			}
			main, err := OpenStore(ctx, dir, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = main.Close() }()
			createTestIssue(t, main, "bd-shared")
			for n := 0; n < 5; n++ {
				if err = main.SetConfig(ctx, "garbage-probe", time.Now().String()); err != nil {
					t.Fatal(err)
				}
			}
			a, err := main.Fork(ctx, "alpha", "test")
			if err != nil {
				t.Fatal(err)
			}
			b, err := main.Fork(ctx, "beta", "test")
			if err != nil {
				t.Fatal(err)
			}
			alpha, err := OpenStore(ctx, a.Directory, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = alpha.Close() }()
			beta, err := OpenStore(ctx, b.Directory, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = beta.Close() }()
			if _, err = (&lifecycle{alpha}).Update(ctx, issueops.UpdateRequest{Actor: "test", IssueID: "bd-shared", Patch: issueops.IssuePatch{Title: issueops.Field[string]{Set: true, Value: "alpha title"}}}); err != nil {
				t.Fatal(err)
			}
			if _, err = (&lifecycle{beta}).Update(ctx, issueops.UpdateRequest{Actor: "test", IssueID: "bd-shared", Patch: issueops.IssuePatch{Design: issueops.Field[string]{Set: true, Value: "beta design"}}}); err != nil {
				t.Fatal(err)
			}
			if _, err = (&lifecycle{main}).Update(ctx, issueops.UpdateRequest{Actor: "test", IssueID: "bd-shared", Patch: issueops.IssuePatch{Description: issueops.Field[string]{Set: true, Value: "main description"}}}); err != nil {
				t.Fatal(err)
			}
			snapshots := map[string][]byte{}
			for _, workspace := range []*Store{main, alpha, beta} {
				data, _, e := workspace.transport.Load(ctx)
				if e != nil {
					t.Fatal(e)
				}
				snapshots[workspace.localDir] = data
			}
			result, err := CollectWorkspaceGarbage(ctx, dir, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if result.Deleted == 0 {
				t.Fatal("core history was not collected")
			}
			for path, before := range snapshots {
				fresh, e := OpenStore(ctx, path, backends.OpenOptions{})
				if e != nil {
					t.Fatal(e)
				}
				after, _, e := fresh.transport.Load(ctx)
				_ = fresh.Close()
				if e != nil || string(after) != string(before) {
					t.Fatalf("collected live snapshot %s: %v", path, e)
				}
			}
			for _, name := range []string{"alpha", "beta"} {
				if _, err = main.MergeFork(ctx, name, "gc-merge-"+name, "test", false); err != nil {
					t.Fatalf("merge base lost: %v", err)
				}
			}
			issue, err := main.GetIssue(ctx, "bd-shared")
			if err != nil || issue.Title != "alpha title" || issue.Design != "beta design" || issue.Description != "main description" {
				t.Fatalf("merged state: %+v %v", issue, err)
			}
			// Re-enter after an interrupted main fence. The public maintenance door
			// bypasses ordinary opens only after validating project identity.
			rows := main.transport.(*rowTransport)
			raw, v, err := rows.documentTransport.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var head rowHead
			if err = json.Unmarshal(raw, &head); err != nil {
				t.Fatal(err)
			}
			head.Maintenance = "interrupted"
			raw, _ = json.Marshal(head)
			if err = writeSource(ctx, rows.documentTransport, rows.workspace, v, raw); err != nil {
				t.Fatal(err)
			}
			if _, err = OpenStore(ctx, dir, backends.OpenOptions{}); !errors.Is(err, ErrSnapshotMaintenance) {
				t.Fatalf("normal open ignored maintenance: %v", err)
			}
			if _, err = CollectWorkspaceGarbage(ctx, dir, backends.OpenOptions{}); err != nil {
				t.Fatalf("resume: %v", err)
			}
			if _, err = main.GetIssue(ctx, "bd-shared"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Fault injection happens at the document protocol boundary, with the same
// conditional write and scan semantics as the native and remote transports.
type coreGCProbe struct {
	*documentService
	scanHook func()
}

func (s *coreGCProbe) writeSource(_ context.Context, key string, version uint64, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.versions[key] != version {
		return errConflict
	}
	s.clock++
	s.docs[key] = append([]byte(nil), body...)
	s.versions[key] = s.clock
	return nil
}
func (s *coreGCProbe) ScanDocuments(ctx context.Context, from, to string, limit int) ([]string, error) {
	if s.scanHook != nil {
		hook := s.scanHook
		s.scanHook = nil
		hook()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.documentService.ScanDocuments(ctx, from, to, limit)
}
func newCoreGCProbe(t *testing.T) (*Store, *rowTransport, *coreGCProbe) {
	t.Helper()
	ctx := context.Background()
	probe := &coreGCProbe{documentService: &documentService{docs: map[string][]byte{}, versions: map[string]uint64{}, anchor: "main"}}
	rows := newRowTransport(probe, "main").(*rowTransport)
	store := New(rows, false)
	if _, err := (&identity{store}).Bootstrap(ctx, issueops.BootstrapRequest{Prefix: "bd", ProjectID: "gc-project"}); err != nil {
		t.Fatal(err)
	}
	createTestIssue(t, store, "bd-kept")
	for _, value := range []string{"one", "two", "three"} {
		if err := store.SetConfig(ctx, "obsolete", value); err != nil {
			t.Fatal(err)
		}
	}
	return store, rows, probe
}
func TestCoreGarbageCollectionFencesWritersAndSurvivesOwnershipLoss(t *testing.T) {
	store, rows, probe := newCoreGCProbe(t)
	ctx := context.Background()
	before, _, err := rows.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	probe.scanHook = func() {
		if err := store.SetConfig(ctx, "forbidden", "write"); !errors.Is(err, ErrSnapshotMaintenance) {
			t.Fatalf("writer ignored catalog fence: %v", err)
		}
		raw, v, err := probe.Load(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var head rowHead
		if err = json.Unmarshal(raw, &head); err != nil {
			t.Fatal(err)
		}
		head.Maintenance = "new-owner"
		raw, _ = json.Marshal(head)
		if err = probe.writeSource(ctx, "main", v, raw); err != nil {
			t.Fatal(err)
		}
	}
	result, err := rows.collectCoreGarbage(ctx, "gc-project")
	if !errors.Is(err, errConflict) || result.Deleted != 0 {
		t.Fatalf("lost collector deleted rows: %+v %v", result, err)
	}
	raw, _, err := probe.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var head rowHead
	_ = json.Unmarshal(raw, &head)
	if head.Maintenance != "new-owner" {
		t.Fatal("old collector released new owner fence")
	}
	if _, err = rows.collectCoreGarbage(ctx, "gc-project"); err != nil {
		t.Fatal(err)
	}
	after, _, err := rows.Load(ctx)
	if err != nil || string(after) != string(before) {
		t.Fatalf("live state lost after takeover: %v", err)
	}
}
func TestCoreGarbageCollectionRefusesForeignIdentityAndPendingFork(t *testing.T) {
	store, rows, probe := newCoreGCProbe(t)
	ctx := context.Background()
	_, v, _ := probe.Load(ctx)
	if _, err := rows.collectCoreGarbage(ctx, "wrong-project"); err == nil {
		t.Fatal("foreign project accepted")
	}
	_, after, _ := probe.Load(ctx)
	if after != v {
		t.Fatal("foreign schema refusal wrote a fence")
	}
	if err := store.mutate(ctx, func(st *state) error { st.Branches["pending"] = Branch{Name: "pending", Pending: true}; return nil }); err != nil {
		t.Fatal(err)
	}
	_, v, _ = probe.Load(ctx)
	if _, err := rows.collectCoreGarbage(ctx, "gc-project"); err == nil {
		t.Fatal("pending fork admitted")
	}
	_, after, _ = probe.Load(ctx)
	if after != v {
		t.Fatal("pending fork refusal wrote a fence")
	}
}

func TestCoreGarbageCollectionRejectsGenericDoor(t *testing.T) {
	_, _, probe := newCoreGCProbe(t)
	ctx := context.Background()
	generic := &rowTransport{documentTransport: probe, workspace: "main"}
	_, v, _ := probe.Load(ctx)
	if _, err := generic.CollectGarbage(ctx); err == nil {
		t.Fatal("generic collector accepted branching core workspace")
	}
	_, after, _ := probe.Load(ctx)
	if after != v {
		t.Fatal("generic refusal changed core fence")
	}
}
