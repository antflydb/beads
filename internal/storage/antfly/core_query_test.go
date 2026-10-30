package antfly

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/steveyegge/beads/internal/storage/backends"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/types"
)

func TestCoreQueryIndexPreservesCountsAndBoundsHistoryReads(t *testing.T) {
	ctx := context.Background()
	tr, svc := testRows()
	st := emptyState()
	st.Config["project_id"] = "query-index"
	for n := 0; n < 1000; n++ {
		id := fmt.Sprintf("bd-query-%04d", n)
		st.Issues[id] = &record{Issue: types.Issue{ID: id, Title: id, Status: types.StatusClosed, IssueType: types.TypeTask, Priority: 2, CreatedAt: time.Unix(int64(n), 0).UTC()}, Version: 7}
	}
	id := "bd-query-0123"
	i := &st.Issues[id].Issue
	i.Status = types.StatusOpen
	i.Assignee = "worker"
	i.Labels = []string{"factory"}
	i.Metadata = json.RawMessage(`{"gc.kind":"work"}`)
	i.Comments = []*types.Comment{{ID: "one", IssueID: id, Text: "evidence"}}
	expiry := time.Now().UTC().Add(time.Hour)
	i.LeaseExpiresAt = &expiry
	if err := st.addEdge(id, "bd-query-0000", types.DepParentChild, "test"); err != nil {
		t.Fatal(err)
	}
	if err := st.addEdge("bd-query-0001", id, types.DepBlocks, "test"); err != nil {
		t.Fatal(err)
	}
	st.reblock()
	for _, r := range st.Issues {
		r.Issue.RowVersion = r.Version
	}
	data, _ := json.Marshal(st)
	if err := tr.CompareAndSwap(ctx, 0, data); err != nil {
		t.Fatal(err)
	}
	s := New(tr, false)
	s.expectedProject = "query-index"
	open := types.StatusOpen
	filter := types.IssueFilter{Status: &open}
	want, err := st.search("", filter)
	if err != nil {
		t.Fatal(err)
	}
	svc.reads = 0
	got, err := s.SearchIssuesWithCounts(ctx, "", filter)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("indexed result differs: got=%+v want=%+v", got, want)
	}
	if len(got) != 1 || got[0].DependentCount != 1 || got[0].DependencyCount != 1 || got[0].CommentCount != 1 || got[0].Parent == nil {
		t.Fatalf("lost aggregate: %+v", got)
	}
	if svc.reads > 24 {
		t.Fatalf("selective query loaded unrelated history: %d reads", svc.reads)
	}
	for _, f := range []types.IssueFilter{
		{IDs: []string{id, "bd-missing"}}, {Assignee: &i.Assignee}, {Labels: []string{"factory"}},
		{MetadataFields: map[string]string{"gc.kind": "work"}},
		{MetadataFields: map[string]string{"missing": "<nil>"}}, {Status: &open, NoAssignee: true},
		{Statuses: []types.Status{types.StatusOpen, types.StatusInProgress}, Limit: 1},
		{TitleContains: "0123", SortBy: "id"}, {Limit: 3, Offset: 2, SortBy: "id", SortDesc: true},
	} {
		want, e := st.search("", f)
		if e != nil {
			t.Fatal(e)
		}
		got, e := s.SearchIssuesWithCounts(ctx, "", f)
		if e != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("filter %+v mismatch: %v", f, e)
		}
	}
	readyFilter := types.WorkFilter{Assignee: &i.Assignee}
	want, err = st.ready(readyFilter)
	if err != nil {
		t.Fatal(err)
	}
	svc.reads = 0
	got, err = s.GetReadyWorkWithCounts(ctx, readyFilter)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("ready mismatch: %v", err)
	}
	if svc.reads > 24 {
		t.Fatalf("ready loaded unrelated history: %d reads", svc.reads)
	}
	s.expectedProject = "foreign"
	if _, err = s.SearchIssuesWithCounts(ctx, "", filter); err == nil {
		t.Fatal("query bypassed project identity")
	}
}

func TestCoreQueryIndexNativeAndRemoteReopenAndLifecycle(t *testing.T) {
	for _, mode := range []string{"embedded", "remote"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			target := Target{Table: "core_query_" + digest([]byte(t.Name() + time.Now().String()))[:16], Workspace: "main"}
			if mode == "embedded" {
				target.Library = os.Getenv("ANTFLY_TEST_LIBRARY")
				if target.Library == "" {
					t.Skip("ANTFLY_TEST_LIBRARY required")
				}
				target.Path = filepath.Join(t.TempDir(), "core.aflite")
			} else {
				target.URL = os.Getenv("ANTFLY_TEST_URL")
				if target.URL == "" {
					t.Skip("ANTFLY_TEST_URL required")
				}
			}
			dir := filepath.Join(t.TempDir(), ".beads")
			if err := Initialize(ctx, dir, target, "bd"); err != nil {
				t.Fatal(err)
			}
			s, err := OpenStore(ctx, dir, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"bd-parent", "bd-child", "bd-blocker"} {
				createTestIssue(t, s, id)
			}
			if err = s.AddDependency(ctx, &types.Dependency{IssueID: "bd-child", DependsOnID: "bd-parent", Type: types.DepParentChild}, "test"); err != nil {
				t.Fatal(err)
			}
			if err = s.AddDependency(ctx, &types.Dependency{IssueID: "bd-child", DependsOnID: "bd-blocker", Type: types.DepBlocks}, "test"); err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = OpenStore(ctx, dir, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			verify := func() {
				t.Helper()
				st, _, e := s.load(ctx)
				if e != nil {
					t.Fatal(e)
				}
				open := types.StatusOpen
				for _, f := range []types.IssueFilter{{Status: &open}, {IDs: []string{"bd-child"}}, {MetadataFields: map[string]string{"missing": "<nil>"}}} {
					got, e := s.SearchIssuesWithCounts(ctx, "", f)
					want, we := st.search("", f)
					if e != nil || we != nil || !reflect.DeepEqual(got, want) {
						t.Fatalf("query differs: %v %v", e, we)
					}
				}
				f := types.WorkFilter{}
				got, e := s.GetReadyWorkWithCounts(ctx, f)
				want, we := st.ready(f)
				if e != nil || we != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("ready differs: %v %v", e, we)
				}
			}
			verify()
			if err = s.CloseIssue(ctx, "bd-blocker", "done", "test", ""); err != nil {
				t.Fatal(err)
			}
			verify()
		})
	}
}

func TestCoreQueryIndexLegacyFallbackDoesNotWrite(t *testing.T) {
	ctx := context.Background()
	tr, svc := testRows()
	st := emptyState()
	st.Config["project_id"] = "legacy-query"
	st.Issues["bd-one"] = &record{Issue: types.Issue{ID: "bd-one", Status: types.StatusOpen, IssueType: types.TypeTask}, Version: 3}
	data, _ := json.Marshal(st)
	if err := tr.CompareAndSwap(ctx, 0, data); err != nil {
		t.Fatal(err)
	}
	var head rowHead
	if err := json.Unmarshal(svc.docs["team"], &head); err != nil {
		t.Fatal(err)
	}
	delete(head.Fields, coreQueryField)
	raw, _ := json.Marshal(head)
	if err := svc.CompareAndSwap(ctx, svc.versions["team"], raw); err != nil {
		t.Fatal(err)
	}
	before := svc.clock
	s := New(tr, false)
	got, err := s.SearchIssuesWithCounts(ctx, "", types.IssueFilter{})
	if err != nil || len(got) != 1 {
		t.Fatalf("legacy fallback: %v %v", got, err)
	}
	if svc.clock != before {
		t.Fatal("read migrated legacy snapshot")
	}
	if err = s.SetConfig(ctx, "probe", "migration"); err != nil {
		t.Fatal(err)
	}
	indexed, ok, err := s.indexedQueryState(ctx, types.IssueFilter{})
	if err != nil || !ok || len(indexed.Issues) != 1 {
		t.Fatalf("ordinary publication failed to migrate: %v %v", ok, err)
	}
}

type queryPublicationTransport struct {
	documentTransport
	afterLoad func()
}

func (t *queryPublicationTransport) Load(ctx context.Context) ([]byte, uint64, error) {
	raw, v, err := t.documentTransport.Load(ctx)
	if hook := t.afterLoad; hook != nil {
		t.afterLoad = nil
		hook()
	}
	return raw, v, err
}

func TestCoreQueryIndexRetriesPublicationBeforeReturning(t *testing.T) {
	ctx := context.Background()
	tr, svc := testRows()
	s := New(tr, false)
	st := emptyState()
	st.Config["project_id"] = "query-publication"
	st.Issues["bd-one"] = &record{Issue: types.Issue{ID: "bd-one", Title: "before", Status: types.StatusOpen, IssueType: types.TypeTask}, Version: 3}
	data, _ := json.Marshal(st)
	if err := tr.CompareAndSwap(ctx, 0, data); err != nil {
		t.Fatal(err)
	}
	other := New(newRowTransport(svc, "team"), false)
	interceptor := &queryPublicationTransport{documentTransport: svc, afterLoad: func() {
		if err := other.mutate(ctx, func(st *state) error { st.Issues["bd-one"].Issue.Title = "after"; return nil }); err != nil {
			t.Fatal(err)
		}
	}}
	tr.documentTransport = interceptor
	rows, err := s.SearchIssuesWithCounts(ctx, "", types.IssueFilter{})
	if err != nil || len(rows) != 1 || rows[0].Title != "after" {
		t.Fatalf("returned invalidated join: %+v %v", rows, err)
	}
}

func TestCoreQueryIndexReadyPreservesDeferredAncestors(t *testing.T) {
	ctx := context.Background()
	tr, _ := testRows()
	st := emptyState()
	st.Config["project_id"] = "deferred-query"
	future := time.Now().Add(time.Hour)
	for _, id := range []string{"bd-root", "bd-parent", "bd-child"} {
		st.Issues[id] = &record{Issue: types.Issue{ID: id, Status: types.StatusClosed, IssueType: types.TypeTask}, Version: 3}
	}
	st.Issues["bd-child"].Issue.Status = types.StatusOpen
	st.Issues["bd-root"].Issue.DeferUntil = &future
	for _, pair := range [][2]string{{"bd-child", "bd-parent"}, {"bd-parent", "bd-root"}} {
		if err := st.addEdge(pair[0], pair[1], types.DepParentChild, "test"); err != nil {
			t.Fatal(err)
		}
	}
	data, _ := json.Marshal(st)
	if err := tr.CompareAndSwap(ctx, 0, data); err != nil {
		t.Fatal(err)
	}
	s := New(tr, false)
	for _, f := range []types.WorkFilter{{}, {IncludeDeferred: true}} {
		got, err := s.GetReadyWorkWithCounts(ctx, f)
		want, we := st.ready(f)
		if err != nil || we != nil || len(got) != len(want) {
			t.Fatalf("deferred ancestors lost: got=%d want=%d error=%v", len(got), len(want), err)
		}
	}
	rows, err := s.SearchIssuesWithCounts(ctx, "", types.IssueFilter{IDs: []string{"bd-child"}})
	if err != nil || len(rows) != 1 || rows[0].ID != "bd-child" {
		t.Fatalf("ancestor leaked into candidate result: %+v %v", rows, err)
	}
}
