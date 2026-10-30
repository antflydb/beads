package antfly

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/backends"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/issueops"
)

func metadataRaw(value string) *json.RawMessage { raw := json.RawMessage(value); return &raw }
func exerciseMetadataCoordination(t *testing.T, s, other *Store) {
	t.Helper()
	ctx := context.Background()
	createTestIssue(t, s, "bd-coordination")
	cas, err := s.MetadataCAS()
	if err != nil {
		t.Fatal(err)
	}
	first, err := cas.CompareAndSetKey(ctx, issueops.CompareAndSetKeyRequest{Actor: "owner", IssueID: "bd-coordination", Key: "lease", Value: metadataRaw(`null`)})
	if err != nil || !first.Swapped || first.Current == nil || string(*first.Current) != "null" {
		t.Fatalf("null create: %+v %v", first, err)
	}
	before, version, err := s.transport.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	refused, err := cas.CompareAndSetKey(ctx, issueops.CompareAndSetKeyRequest{Actor: "loser", IssueID: "bd-coordination", Key: "lease", Value: metadataRaw(`"wrong"`)})
	if err != nil || refused.Swapped || refused.Current == nil || string(*refused.Current) != "null" {
		t.Fatalf("absent != null: %+v %v", refused, err)
	}
	same, err := cas.CompareAndSetKey(ctx, issueops.CompareAndSetKeyRequest{Actor: "owner", IssueID: "bd-coordination", Key: "lease", Expected: metadataRaw(`null`), Value: metadataRaw(`null`)})
	if err != nil || !same.Swapped {
		t.Fatalf("no-op match: %+v %v", same, err)
	}
	after, next, err := s.transport.Load(ctx)
	if err != nil || version != next || string(before) != string(after) {
		t.Fatalf("refused/no-op CAS wrote: %v", err)
	}
	removed, err := cas.CompareAndSetKey(ctx, issueops.CompareAndSetKeyRequest{Actor: "owner", IssueID: "bd-coordination", Key: "lease", Expected: metadataRaw(`null`)})
	if err != nil || !removed.Swapped || removed.Current != nil {
		t.Fatalf("remove: %+v %v", removed, err)
	}
	if err = s.MergeMetadata(ctx, "bd-coordination", "object", json.RawMessage(`{"z":2,"a":1}`), "owner"); err != nil {
		t.Fatal(err)
	}
	canonical, err := cas.CompareAndSetKey(ctx, issueops.CompareAndSetKeyRequest{Actor: "owner", IssueID: "bd-coordination", Key: "object", Expected: metadataRaw(`{ "a":1, "z":2 }`), Value: metadataRaw(`true`)})
	if err != nil || !canonical.Swapped {
		t.Fatalf("canonical match: %+v %v", canonical, err)
	}
	for _, store := range []*Store{s, other} {
		if _, err := store.MetadataCAS(); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	out := make(chan issueops.CompareAndSetKeyResult, 2)
	fail := make(chan error, 2)
	for n, store := range []*Store{s, other} {
		wg.Add(1)
		go func(n int, store *Store) {
			defer wg.Done()
			role, _ := store.MetadataCAS()
			value := fmt.Sprintf(`"worker-%d"`, n)
			r, e := role.CompareAndSetKey(ctx, issueops.CompareAndSetKeyRequest{Actor: "worker", IssueID: "bd-coordination", Key: "winner", Value: metadataRaw(value)})
			if e != nil {
				fail <- e
			} else {
				out <- r
			}
		}(n, store)
	}
	wg.Wait()
	close(out)
	close(fail)
	for e := range fail {
		t.Fatal(e)
	}
	wins := 0
	for r := range out {
		if r.Swapped {
			wins++
		}
		if r.Current == nil {
			t.Fatal("winner not reported")
		}
	}
	if wins != 1 {
		t.Fatalf("CAS winners=%d", wins)
	}
	wg = sync.WaitGroup{}
	fail = make(chan error, 2)
	for n, store := range []*Store{s, other} {
		wg.Add(1)
		go func(n int, store *Store) {
			defer wg.Done()
			fail <- store.SlotSet(ctx, "bd-coordination", fmt.Sprintf("sibling%d", n), "kept", "worker")
		}(n, store)
	}
	wg.Wait()
	close(fail)
	for e := range fail {
		if e != nil {
			t.Fatal(e)
		}
	}
	for n := 0; n < 2; n++ {
		v, e := s.SlotGet(ctx, "bd-coordination", fmt.Sprintf("sibling%d", n))
		if e != nil || v != "kept" {
			t.Fatalf("sibling lost: %s %v", v, e)
		}
	}
	if err = s.SlotClear(ctx, "bd-coordination", "sibling0", "owner"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SlotGet(ctx, "bd-coordination", "sibling0"); err == nil {
		t.Fatal("slot not removed")
	}
	if v, err := s.SlotGet(ctx, "bd-coordination", "sibling1"); err != nil || v != "kept" {
		t.Fatalf("clear lost sibling: %s %v", v, err)
	}
}
func exerciseMergeSlot(t *testing.T, s, other *Store) {
	t.Helper()
	ctx := context.Background()
	slot, err := s.MergeSlotCreate(ctx, "operator")
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := other.MergeSlotCreate(ctx, "operator")
	if err != nil || duplicate.ID != slot.ID {
		t.Fatalf("idempotent creation: %+v %v", duplicate, err)
	}
	var wg sync.WaitGroup
	out := make(chan *struct {
		holder            string
		acquired, waiting bool
	}, 2)
	fail := make(chan error, 2)
	for n, store := range []*Store{s, other} {
		wg.Add(1)
		go func(n int, store *Store) {
			defer wg.Done()
			r, e := store.MergeSlotAcquire(ctx, fmt.Sprintf("worker-%d", n), "operator", true)
			if e != nil {
				fail <- e
				return
			}
			out <- &struct {
				holder            string
				acquired, waiting bool
			}{r.Holder, r.Acquired, r.Waiting}
		}(n, store)
	}
	wg.Wait()
	close(out)
	close(fail)
	for e := range fail {
		t.Fatal(e)
	}
	wins, waits := 0, 0
	winner := ""
	for r := range out {
		if r.acquired {
			wins++
			winner = r.holder
		}
		if r.waiting {
			waits++
		}
	}
	if wins != 1 || waits != 1 {
		t.Fatalf("slot race: wins=%d waiters=%d", wins, waits)
	}
	status, err := other.MergeSlotCheck(ctx)
	if err != nil || status.Available || status.Holder != winner || len(status.Waiters) != 1 {
		t.Fatalf("held slot: %+v %v", status, err)
	}
	before, v, err := s.transport.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.MergeSlotRelease(ctx, "wrong-holder", "operator"); err == nil {
		t.Fatal("foreign release accepted")
	}
	queued, err := s.MergeSlotAcquire(ctx, status.Waiters[0], "operator", true)
	if err != nil || !queued.Waiting || queued.Position != 1 {
		t.Fatalf("duplicate waiter: %+v %v", queued, err)
	}
	after, next, err := s.transport.Load(ctx)
	if err != nil || next != v || string(before) != string(after) {
		t.Fatalf("refused release/duplicate waiter wrote: %v", err)
	}
	if err = other.MergeSlotRelease(ctx, winner, "operator"); err != nil {
		t.Fatal(err)
	}
	status, err = s.MergeSlotCheck(ctx)
	if err != nil || !status.Available || status.Holder != "" {
		t.Fatalf("release: %+v %v", status, err)
	}
	if _, err = s.MergeSlotAcquire(ctx, "recovered-worker", "operator", false); err != nil {
		t.Fatal(err)
	}
	status, err = other.MergeSlotCheck(ctx)
	if err != nil || status.Holder != "recovered-worker" || status.Available {
		t.Fatalf("recovered acquire: %+v %v", status, err)
	}
}
func TestNativeMetadataAndMergeSlotCoordination(t *testing.T) {
	for _, mode := range []string{"embedded", "remote"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, ".beads")
			target := Target{Table: fmt.Sprintf("coordination_%d", time.Now().UnixNano()), Workspace: "main"}
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
			first, err := OpenStore(ctx, dir, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = first.Close() }()
			second, err := OpenStore(ctx, dir, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = second.Close() }()
			exerciseMetadataCoordination(t, first, second)
			exerciseMergeSlot(t, first, second)
			fork, err := first.Fork(ctx, "isolated", "operator")
			if err != nil {
				t.Fatal(err)
			}
			branch, err := OpenStore(ctx, fork.Directory, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = branch.Close() }()
			var unsupported *storage.ErrUnsupported
			if _, err = branch.MergeSlotAcquire(ctx, "branch-worker", "operator", false); !errors.As(err, &unsupported) {
				t.Fatalf("branch claimed runtime slot: %v", err)
			}
			if _, err = branch.GetIssue(ctx, "bd-merge-slot"); !errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("runtime slot copied into fork: %v", err)
			}
			if _, err = (&lifecycle{branch}).Create(ctx, issueops.CreateRequest{Actor: "operator", Issue: &types.Issue{ID: "bd-branch-slot", Title: "slot", IssueType: types.TypeTask, Labels: []string{"gt:slot"}}}); !errors.As(err, &unsupported) {
				t.Fatalf("branch created runtime slot: %v", err)
			}
			for _, patch := range []issueops.IssuePatch{
				{Labels: issueops.LabelPatch{Add: []string{"gt:slot"}}},
				{Persistence: issueops.Field[issueops.PersistenceMode]{Set: true, Value: issueops.PersistenceModeEphemeral}},
			} {
				if _, err = (&lifecycle{branch}).Update(ctx, issueops.UpdateRequest{Actor: "operator", IssueID: "bd-coordination", Patch: patch}); !errors.As(err, &unsupported) {
					t.Fatalf("branch converted work to runtime coordination: %v", err)
				}
			}
			if err = first.MergeSlotRelease(ctx, "recovered-worker", "operator"); err != nil {
				t.Fatal(err)
			}
			if _, err = first.MergeSlotAcquire(ctx, "live-after-fork", "operator", false); err != nil {
				t.Fatal(err)
			}
			if err = branch.SlotSet(ctx, "bd-coordination", "branch_evidence", "kept", "operator"); err != nil {
				t.Fatal(err)
			}
			merged, err := first.MergeFork(ctx, "isolated", "merge-slot-preservation", "operator", false)
			if err != nil || len(merged.Conflicts) != 0 {
				t.Fatalf("merge: %+v %v", merged, err)
			}
			held, err := first.MergeSlotCheck(ctx)
			if err != nil || held.Holder != "live-after-fork" || held.Available {
				t.Fatalf("merge rewrote runtime owner: %+v %v", held, err)
			}
			if _, err = first.retain(ctx, retentionOptions{policy: "reap", staleAge: time.Hour, closedAge: 7 * 24 * time.Hour, staleIssueAge: time.Hour, limit: 100, now: time.Now().UTC().Add(400 * 24 * time.Hour)}, "operator"); err != nil {
				t.Fatal(err)
			}
			held, err = first.MergeSlotCheck(ctx)
			if err != nil || held.Holder != "live-after-fork" || held.Available {
				t.Fatalf("housekeeping retired runtime slot: %+v %v", held, err)
			}

		})
	}
}
