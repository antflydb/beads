package antfly

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/issueops"
	"github.com/steveyegge/beads/memoryops"
)

// A versioned document service, not a fake Beads store. Independent Store
// handles race through exactly the same conditional-write seam as HTTP/native.
type testTransport struct {
	mu      sync.Mutex
	data    []byte
	version uint64
	commits int
	failure error
}

func (t *testTransport) Load(context.Context) ([]byte, uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]byte(nil), t.data...), t.version, nil
}
func (t *testTransport) CompareAndSwap(_ context.Context, v uint64, b []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.commits++
	if t.failure != nil {
		return t.failure
	}
	if v != t.version {
		return errConflict
	}
	t.data = append([]byte(nil), b...)
	t.version++
	return nil
}
func (t *testTransport) Memory(context.Context, string, string, string, json.RawMessage) (json.RawMessage, error) {
	return nil, errUnsupported("test memory")
}
func (t *testTransport) Close() error { return nil }

func testStore(t *testing.T) (*Store, *testTransport) {
	t.Helper()
	transport := &testTransport{}
	s := New(transport, false)
	_, err := (&identity{s}).Bootstrap(context.Background(), issueops.BootstrapRequest{Prefix: "bd", ProjectID: "test-project"})
	if err != nil {
		t.Fatal(err)
	}
	return s, transport
}
func createTestIssue(t *testing.T, s *Store, id string) {
	t.Helper()
	_, err := (&lifecycle{s}).Create(context.Background(), issueops.CreateRequest{Actor: "test", Issue: &types.Issue{ID: id, Title: id, IssueType: types.TypeTask}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAntflyDependencyLifecycleAndRollback(t *testing.T) {
	s, transport := testStore(t)
	ctx := context.Background()
	createTestIssue(t, s, "bd-first")
	createTestIssue(t, s, "bd-second")
	g := &graph{s}
	_, err := g.AddDependencies(ctx, issueops.AddDependenciesRequest{Actor: "test", Edges: []issueops.DependencyEdge{{IssueID: "bd-second", DependsOnID: "bd-first", Type: types.DepBlocks}}})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := s.GetReadyWork(ctx, types.WorkFilter{})
	if err != nil || len(rows) != 1 || rows[0].ID != "bd-first" {
		t.Fatalf("ready = %v, %v", rows, err)
	}
	before, _, _ := transport.Load(ctx)
	_, err = g.AddDependencies(ctx, issueops.AddDependenciesRequest{Actor: "test", Edges: []issueops.DependencyEdge{{IssueID: "bd-first", DependsOnID: "bd-second", Type: types.DepBlocks}}})
	if !errors.Is(err, storage.ErrValidation) {
		t.Fatalf("cycle = %v", err)
	}
	after, _, _ := transport.Load(ctx)
	if string(before) != string(after) {
		t.Fatal("cycle refusal changed durable state")
	}
	_, err = (&lifecycle{s}).Close(ctx, issueops.CloseRequest{Actor: "test", IssueID: "bd-second"})
	if !errors.Is(err, storage.ErrCloseBlocked) {
		t.Fatalf("blocked close = %v", err)
	}
	if _, err = (&lifecycle{s}).Close(ctx, issueops.CloseRequest{Actor: "test", IssueID: "bd-first"}); err != nil {
		t.Fatal(err)
	}
	rows, err = s.GetReadyWork(ctx, types.WorkFilter{})
	if err != nil || len(rows) != 1 || rows[0].ID != "bd-second" {
		t.Fatalf("unblocked ready = %v, %v", rows, err)
	}
	if _, err = (&lifecycle{s}).Reopen(ctx, issueops.ReopenRequest{Actor: "test", IssueID: "bd-first"}); err != nil {
		t.Fatal(err)
	}
	i, err := s.GetIssue(ctx, "bd-second")
	if err != nil || !i.IsBlocked {
		t.Fatalf("reopen did not reblock: %+v %v", i, err)
	}
}

func TestAntflyConcurrentClaimHasOneWinner(t *testing.T) {
	s, transport := testStore(t)
	createTestIssue(t, s, "bd-claim")
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan error, 12)
	for n := 0; n < 12; n++ {
		wg.Add(1)
		go func(actor string) {
			defer wg.Done()
			_, e := (&lifecycle{New(transport, false)}).Claim(ctx, issueops.ClaimRequest{Actor: actor, IssueID: "bd-claim"})
			results <- e
		}(fmt.Sprintf("actor-%d", n))
	}
	wg.Wait()
	close(results)
	winners := 0
	for e := range results {
		if e == nil {
			winners++
		} else if !errors.Is(e, storage.ErrAlreadyClaimed) {
			t.Fatalf("unexpected claim failure: %v", e)
		}
	}
	if winners != 1 {
		t.Fatalf("claim winners = %d", winners)
	}
	i, e := s.GetIssue(ctx, "bd-claim")
	if e != nil || i.RowVersion == 0 || i.LeaseExpiresAt == nil {
		t.Fatalf("claim did not persist version and lease: %+v %v", i, e)
	}
	before := transport.commits
	again, e := (&lifecycle{s}).Claim(ctx, issueops.ClaimRequest{Actor: i.Assignee, IssueID: i.ID})
	if e != nil || again.Changed || transport.commits != before {
		t.Fatalf("idempotent claim wrote: %+v %v", again, e)
	}
}

func TestAntflyReadyClaimAndStaleUpdate(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	createTestIssue(t, s, "bd-work")
	old, _ := s.GetIssue(ctx, "bd-work")
	claimed, e := (&readyRole{s}).ClaimNext(ctx, issueops.ClaimNextRequest{Actor: "worker"})
	if e != nil || claimed.Claimed == nil || claimed.Claimed.ID != "bd-work" {
		t.Fatalf("claim next = %+v %v", claimed, e)
	}
	_, e = (&lifecycle{s}).Update(ctx, issueops.UpdateRequest{Actor: "test", IssueID: old.ID, ExpectedVersion: &old.RowVersion, Patch: issueops.IssuePatch{Title: issueops.Field[string]{Set: true, Value: "clobbered"}}})
	if !errors.Is(e, storage.ErrVersionMismatch) {
		t.Fatalf("stale update = %v", e)
	}
	i, _ := s.GetIssue(ctx, old.ID)
	if i.Title == "clobbered" {
		t.Fatal("stale update landed")
	}
}

func TestAntflyExactMemoriesAndReopen(t *testing.T) {
	s, transport := testStore(t)
	ctx := context.Background()
	notes, _ := s.Memories()
	key := " arbitrary / Unicode κ key "
	content := "\n  preserve this\nverbatim  "
	r, e := notes.Remember(ctx, memoryops.RememberRequest{Key: key, Content: content})
	if e != nil || r.Replaced {
		t.Fatalf("remember = %+v %v", r, e)
	}
	fresh := New(transport, false)
	notes, _ = fresh.Memories()
	recall, e := notes.Recall(ctx, memoryops.RecallRequest{Key: key})
	if e != nil || !recall.Found || recall.Value != content {
		t.Fatalf("recall = %+v %v", recall, e)
	}
	r, e = notes.Remember(ctx, memoryops.RememberRequest{Key: key, Content: "replacement"})
	if e != nil || !r.Replaced {
		t.Fatalf("replacement = %+v %v", r, e)
	}
	list, e := notes.List(ctx, memoryops.ListRequest{Search: "REPLACEMENT"})
	if e != nil || list.Memories[key] != "replacement" {
		t.Fatalf("list = %+v %v", list, e)
	}
	removed, e := notes.Forget(ctx, memoryops.ForgetRequest{Key: key})
	if e != nil || !removed.Found || removed.Value != "replacement" {
		t.Fatalf("forget = %+v %v", removed, e)
	}
}

func TestAntflyUnknownCommitIsNeverRetried(t *testing.T) {
	s, transport := testStore(t)
	transport.failure = errIndeterminate
	before := transport.commits
	err := s.SetConfig(context.Background(), "key", "value")
	if !errors.Is(err, storage.ErrCommitIndeterminate) || transport.commits != before+1 {
		t.Fatalf("unknown commit = %v, attempts %d", err, transport.commits-before)
	}
}

func TestAntflyTransactionRollbackAndSingleCallback(t *testing.T) {
	s, transport := testStore(t)
	ctx := context.Background()
	calls := 0
	err := s.RunInTransaction(ctx, "rollback", func(tx storage.Transaction) error {
		calls++
		i := &types.Issue{ID: "bd-rollback", Title: "rollback", IssueType: types.TypeTask}
		if e := tx.CreateIssue(ctx, i, "test"); e != nil {
			return e
		}
		return errors.New("abort")
	})
	if err == nil || calls != 1 {
		t.Fatal("callback was not rolled back")
	}
	if _, e := s.GetIssue(ctx, "bd-rollback"); !errors.Is(e, storage.ErrNotFound) {
		t.Fatalf("rolled back issue exists: %v", e)
	}
	transport.failure = errConflict
	calls = 0
	err = s.RunInTransaction(ctx, "conflict", func(tx storage.Transaction) error { calls++; return tx.SetConfig(ctx, "test", "value") })
	if !errors.Is(err, errConflict) || calls != 1 {
		t.Fatalf("callback replayed: %d, %v", calls, err)
	}
}

func TestAntflyRemoteContract(t *testing.T) {
	ctx := context.Background()
	var state json.RawMessage
	version := uint64(0)
	commits := 0
	memory := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" || r.Header.Get("X-Antfly-Database") != "testdb" {
			t.Error("missing scoped authentication")
		}
		switch r.URL.Path {
		case "/db/v1/tables/work/documents/team":
			if state == nil {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("X-Antfly-Version", strconv.FormatUint(version, 10))
			_, _ = w.Write(state)
		case "/db/v1/transactions/commit":
			var req struct {
				ReadSet []struct {
					Version string `json:"version"`
				} `json:"read_set"`
				Tables map[string]struct {
					Inserts map[string]json.RawMessage `json:"inserts"`
					Sync    string                     `json:"sync_level"`
				} `json:"tables"`
			}
			if e := json.NewDecoder(r.Body).Decode(&req); e != nil {
				t.Error(e)
				w.WriteHeader(400)
				return
			}
			if len(req.ReadSet) != 1 || req.ReadSet[0].Version != strconv.FormatUint(version, 10) {
				w.WriteHeader(409)
				_, _ = w.Write([]byte(`{"status":"aborted"}`))
				return
			}
			if req.Tables["work"].Sync != "write" {
				t.Error("wrong synchronization contract")
			}
			state = req.Tables["work"].Inserts["team"]
			version++
			commits++
			_, _ = w.Write([]byte(`{"status":"committed"}`))
		case "/memory/v1/spaces":
			if r.Header.Get("Idempotency-Key") != "create-space" {
				t.Error("missing idempotency")
			}
			var req map[string]any
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req["space"] != "team" || req["config"] == nil {
				t.Errorf("memory create shape: %+v", req)
			}
			memory = true
			_, _ = w.Write([]byte(`{"phase":"complete"}`))
		default:
			t.Errorf("unexpected Antfly path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	t.Setenv("TEST_ANTFLY_TOKEN", "test-token")
	transport, e := OpenTransport(Target{URL: server.URL, Table: "work", Workspace: "team", TokenEnv: "TEST_ANTFLY_TOKEN", Database: "testdb"})
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = transport.Close() }()
	s := New(underlyingTransport(transport), false)
	_, e = (&identity{s}).Bootstrap(ctx, issueops.BootstrapRequest{Prefix: "bd", ProjectID: "project"})
	if e != nil {
		t.Fatal(e)
	}
	createTestIssue(t, s, "bd-http")
	if _, e = s.GetIssue(ctx, "bd-http"); e != nil {
		t.Fatal(e)
	}
	if _, e = transport.Memory(ctx, "team", "create", "create-space", json.RawMessage(`{}`)); e != nil {
		t.Fatal(e)
	}
	if commits != 2 || !memory {
		t.Fatalf("commits=%d memory=%v", commits, memory)
	}
}

func TestAntflyRejectsUnsupportedFiltersAndUnsafeTargets(t *testing.T) {
	s, _ := testStore(t)
	_, e := s.SearchIssues(context.Background(), "", types.IssueFilter{LabelRegex: ".*"})
	var unsupported *storage.ErrUnsupported
	if !errors.As(e, &unsupported) {
		t.Fatalf("unsupported filter = %v", e)
	}
	for _, endpoint := range []string{"http://example.com", "https://user:password@example.com", "https://example.com?token=secret"} {
		e := (Target{URL: endpoint, Table: "work", Workspace: "team"}).Validate()
		if e == nil {
			t.Errorf("accepted unsafe target %s", endpoint)
		}
	}
	_, e = (&reader{s}).Get(context.Background(), issueops.GetRequest{ID: "missing"})
	if !errors.Is(e, storage.ErrNotFound) {
		t.Fatal(e)
	}
	if !strings.Contains(unsupported.Op, "LabelRegex") {
		t.Fatal("unsupported field not identified")
	}
}

func TestAntflyProjectIdentityFencesReadsAndTransactions(t *testing.T) {
	s, _ := testStore(t)
	s.expectedProject = "different-project"
	ctx := context.Background()
	if _, err := s.GetIssue(ctx, "anything"); err == nil {
		t.Fatal("read accepted a different project")
	}
	called := false
	err := s.RunInTransaction(ctx, "test", func(storage.Transaction) error { called = true; return nil })
	if err == nil || called {
		t.Fatalf("identity fence: error=%v callback=%v", err, called)
	}
	if _, err = s.Memory(ctx, "shared", "recall", "", json.RawMessage(`{}`)); err == nil {
		t.Fatal("memory bypassed project identity")
	}
}

func TestAntflyPinnedBlockerAndLabelValidation(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	createTestIssue(t, s, "bd-blocker")
	createTestIssue(t, s, "bd-task")
	if err := s.AddDependency(ctx, &types.Dependency{IssueID: "bd-task", DependsOnID: "bd-blocker", Type: types.DepBlocks}, "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.mutate(ctx, func(st *state) error {
		st.Issues["bd-blocker"].Issue.Status = types.StatusPinned
		st.reblock()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ready, err := s.GetReadyWork(ctx, types.WorkFilter{})
	if err != nil || len(ready) != 1 || ready[0].ID != "bd-task" {
		t.Fatalf("pinned dependency ready: %v, %v", ready, err)
	}
	l := &lifecycle{s}
	_, err = l.Update(ctx, issueops.UpdateRequest{Actor: "test", IssueID: "bd-task", Patch: issueops.IssuePatch{Labels: issueops.LabelPatch{Add: []string{strings.Repeat("a", types.MaxFieldLen+1)}}}})
	if err == nil {
		t.Fatal("accepted oversized label")
	}
}

func TestAntflyPendingCommitWaitsWithoutReplay(t *testing.T) {
	lookups, commits := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			commits++
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"status":"committed_pending"}`))
			return
		}
		lookups++
		version := "4"
		if lookups > 1 {
			version = "5"
		}
		w.Header().Set("X-Antfly-Version", version)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	tr, err := OpenTransport(Target{URL: server.URL, Table: "work", Workspace: "team"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	state, err := json.Marshal(emptyState())
	if err != nil {
		t.Fatal(err)
	}
	if err = underlyingTransport(tr).CompareAndSwap(context.Background(), 4, state); err != nil {
		t.Fatal(err)
	}
	if commits != 1 || lookups != 2 {
		t.Fatalf("commits=%d lookups=%d", commits, lookups)
	}
}

func TestAntflyReadyRespectsParentDeferralAndPaging(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	createTestIssue(t, s, "bd-parent")
	createTestIssue(t, s, "bd-child")
	if err := s.AddDependency(ctx, &types.Dependency{IssueID: "bd-child", DependsOnID: "bd-parent", Type: types.DepParentChild}, "test"); err != nil {
		t.Fatal(err)
	}
	rows, err := s.GetReadyWork(ctx, types.WorkFilter{Offset: 1})
	if err != nil || len(rows) != 1 {
		t.Fatalf("ready paging: %v, %v", rows, err)
	}
	if err := s.mutate(ctx, func(st *state) error {
		future := time.Now().Add(time.Hour)
		st.Issues["bd-parent"].Issue.DeferUntil = &future
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	rows, err = s.GetReadyWork(ctx, types.WorkFilter{})
	if err != nil || len(rows) != 0 {
		t.Fatalf("deferred descendants ready: %v, %v", rows, err)
	}
	rows, err = s.GetReadyWork(ctx, types.WorkFilter{IncludeDeferred: true})
	if err != nil || len(rows) != 2 {
		t.Fatalf("include deferred: %v, %v", rows, err)
	}
}

func TestAntflyFactoryWorkTypes(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	for _, kind := range []types.IssueType{"convoy", "merge-request"} {
		created, err := (&lifecycle{s}).Create(ctx, issueops.CreateRequest{Issue: &types.Issue{Title: "Factory grouping", IssueType: kind, Priority: 2}, Actor: "factory"})
		if err != nil {
			t.Fatal(err)
		}
		_, err = (&lifecycle{s}).Update(ctx, issueops.UpdateRequest{IssueID: created.Issue.ID, Actor: "factory", Patch: issueops.IssuePatch{Title: issueops.Field[string]{Set: true, Value: "Updated grouping"}}})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// External edges retain the shared Beads classification. Gas City's split
// store gate resolves the owner and enforces foreign readiness; this store
// must preserve the edge without inventing a local target or lease.
func TestAntflyExternalDependenciesPreserveRoutingContract(t *testing.T) {
	ctx := context.Background()
	for _, target := range []string{"other-blocker", "external:other-blocker", "external:rig:capability"} {
		t.Run(target, func(t *testing.T) {
			s, _ := testStore(t)
			createTestIssue(t, s, "bd-source")
			if err := s.AddDependency(ctx, &types.Dependency{IssueID: "bd-source", DependsOnID: target, Type: types.DepBlocks}, "test"); err != nil {
				t.Fatal(err)
			}
			issue, err := s.GetIssue(ctx, "bd-source")
			if err != nil {
				t.Fatal(err)
			}
			if issue.IsBlocked || len(issue.Dependencies) != 1 || issue.Dependencies[0].DependsOnID != target {
				t.Fatalf("external edge changed: %+v", issue)
			}
			if _, err = (&lifecycle{s}).Close(ctx, issueops.CloseRequest{Actor: "test", IssueID: "bd-source"}); err != nil {
				t.Fatalf("local lifecycle must leave foreign enforcement to owner router: %v", err)
			}
		})
	}
	s, _ := testStore(t)
	createTestIssue(t, s, "bd-source")
	if err := s.AddDependency(ctx, &types.Dependency{IssueID: "bd-source", DependsOnID: "bd-missing", Type: types.DepBlocks}, "test"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing local target accepted: %v", err)
	}
}

func TestAntflySearchIssueIDsForCLIResolutionAndTransaction(t *testing.T) {
	ctx := context.Background()
	s, _ := testStore(t)
	for _, id := range []string{"bd-hash-one", "bd-hash-two", "bd-other"} {
		createTestIssue(t, s, id)
	}
	for _, query := range []string{"bd-hash-one", "hash", "HASH", "missing"} {
		rows, err := s.SearchIssues(ctx, query, types.IssueFilter{})
		if err != nil {
			t.Fatal(err)
		}
		ids, err := s.SearchIssueIDs(ctx, query, types.IssueFilter{})
		if err != nil {
			t.Fatal(err)
		}
		if len(ids) != len(rows) {
			t.Fatalf("projection differs: %v %v", ids, rows)
		}
		for n, row := range rows {
			if ids[n] != row.ID {
				t.Fatalf("order differs: %v", ids)
			}
		}
		if strings.EqualFold(query, "hash") && len(ids) != 2 {
			t.Fatalf("partial id lookup: %v", ids)
		}
	}
	err := s.RunInTransaction(ctx, "test", func(tx storage.Transaction) error {
		ids, err := tx.SearchIssueIDs(ctx, "hash", types.IssueFilter{Limit: 1})
		if err == nil && len(ids) != 1 {
			t.Fatalf("transaction limit: %v", ids)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRemoteReadAdmissionRetryIsExplicitAndReadOnly(t *testing.T) {
	for _, tc := range []struct {
		name, body       string
		status, attempts int
	}{
		{"read admission", `{"code":"storage_read_temporarily_unavailable","retryable":true}`, 503, 2},
		{"generic unavailable", `{"message":"unavailable"}`, 503, 1},
		{"not retryable", `{"code":"storage_read_temporarily_unavailable","retryable":false}`, 503, 1},
		{"wrong status", `{"code":"storage_read_temporarily_unavailable","retryable":true}`, 500, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempt := attempts.Add(1)
				if attempt == 1 {
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
					return
				}
				w.Header().Set("X-Antfly-Version", "17")
				_, _ = w.Write([]byte(`{"committed":true}`))
			}))
			defer server.Close()
			remote := &remote{target: Target{URL: server.URL, Table: "beads", Workspace: "work"}, client: server.Client()}
			data, v, err := remote.ReadDocument(context.Background(), "work")
			if int(attempts.Load()) != tc.attempts {
				t.Fatalf("read attempts=%d want %d", attempts.Load(), tc.attempts)
			}
			if tc.attempts == 2 && (err != nil || v != 17 || string(data) != `{"committed":true}`) {
				t.Fatalf("recovered read: %s %d %v", data, v, err)
			}
			if tc.attempts == 1 && err == nil {
				t.Fatal("unexpected read success")
			}
			attempts.Store(0)
			err = remote.WriteDocuments(context.Background(), nil, map[string]json.RawMessage{"row": json.RawMessage(`{}`)})
			if attempts.Load() != 1 || err == nil {
				t.Fatalf("write replayed: attempts=%d err=%v", attempts.Load(), err)
			}
		})
	}
}

func TestRemoteReadAdmissionRetryHonorsCancellation(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(503)
		_, _ = w.Write([]byte(`{"code":"storage_read_temporarily_unavailable","retryable":true}`))
	}))
	defer server.Close()
	r := &remote{target: Target{URL: server.URL, Table: "beads", Workspace: "work"}, client: server.Client()}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, _, err := r.ReadDocument(ctx, "work")
	if !errors.Is(err, context.DeadlineExceeded) || attempts.Load() < 1 {
		t.Fatalf("cancellation: attempts=%d err=%v", attempts.Load(), err)
	}
}

func TestTransactionalBurnDeletesEdgesAndRollsBackWholeBatch(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	createTestIssue(t, s, "bd-burn-root")
	createTestIssue(t, s, "bd-burn-child")
	if err := s.AddDependency(ctx, &types.Dependency{IssueID: "bd-burn-child", DependsOnID: "bd-burn-root", Type: types.DepBlocks}, "test"); err != nil {
		t.Fatal(err)
	}
	err := s.RunInTransaction(ctx, "burn", func(tx storage.Transaction) error {
		if err := tx.DeleteIssue(ctx, "bd-burn-root"); err != nil {
			return err
		}
		child, err := tx.GetIssue(ctx, "bd-burn-child")
		if err != nil {
			return err
		}
		if child.IsBlocked || len(child.Dependencies) != 0 {
			t.Fatal("staged delete did not remove blocker")
		}
		return tx.DeleteIssue(ctx, "bd-missing")
	})
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("failed batch: %v", err)
	}
	root, err := s.GetIssue(ctx, "bd-burn-root")
	if err != nil || root == nil {
		t.Fatalf("rollback lost root: %v", err)
	}
	child, err := s.GetIssue(ctx, "bd-burn-child")
	if err != nil || !child.IsBlocked {
		t.Fatalf("rollback lost blocker: %+v %v", child, err)
	}
	if err = s.RunInTransaction(ctx, "burn", func(tx storage.Transaction) error {
		if err := tx.DeleteIssue(ctx, "bd-burn-root"); err != nil {
			return err
		}
		return tx.DeleteIssue(ctx, "bd-burn-child")
	}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"bd-burn-root", "bd-burn-child"} {
		if _, err = s.GetIssue(ctx, id); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("burn retained %s: %v", id, err)
		}
	}
}
