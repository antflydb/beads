package conformance

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/steveyegge/beads/backend"
	"github.com/steveyegge/beads/issueops"
)

// TransactionalWorkflowFixture exposes behavioral capabilities without SQL
// fixture statements. Native engines can age leases and inspect audit counts
// through their own representation while running the same assertions.
type TransactionalWorkflowFixture struct {
	Store            backend.DoltStorage
	IndependentStore func() backend.DoltStorage
	SetLeaseExpiry   func(context.Context, string, time.Time) error
	AuditCount       func(context.Context) (int, error)
}

// RunTransactionalWorkflowContract covers transaction rollback, graph/readiness,
// independent-writer claims, quiet heartbeats, and stale-holder fencing. Each
// case receives an isolated initialized workspace; no model is required.
func RunTransactionalWorkflowContract(t *testing.T, makeFixture func(*testing.T) TransactionalWorkflowFixture) {
	t.Helper()
	seed := func(t *testing.T, s backend.DoltStorage, id string) {
		t.Helper()
		if err := s.CreateIssue(context.Background(), &backend.Issue{ID: id, Title: id, IssueType: backend.TypeTask}, "contract"); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("rollback_is_atomic", func(t *testing.T) {
		f := makeFixture(t)
		ctx := context.Background()
		calls := 0
		abort := errors.New("abort callback")
		err := f.Store.RunInTransaction(ctx, "contract", func(tx backend.Transaction) error {
			calls++
			if err := tx.CreateIssue(ctx, &backend.Issue{ID: "bd-rollback", Title: "rollback", IssueType: backend.TypeTask}, "contract"); err != nil {
				return err
			}
			return abort
		})
		if !errors.Is(err, abort) || calls != 1 {
			t.Fatalf("callback calls=%d error=%v", calls, err)
		}
		if _, err = f.Store.GetIssue(ctx, "bd-rollback"); !errors.Is(err, backend.ErrNotFound) {
			t.Fatalf("rolled-back issue: %v", err)
		}
	})
	t.Run("graph_readiness_and_reopen", func(t *testing.T) {
		f := makeFixture(t)
		ctx := context.Background()
		seed(t, f.Store, "bd-first")
		seed(t, f.Store, "bd-second")
		if err := f.Store.AddDependency(ctx, &backend.Dependency{IssueID: "bd-second", DependsOnID: "bd-first", Type: backend.DepBlocks}, "contract"); err != nil {
			t.Fatal(err)
		}
		ready, err := f.Store.GetReadyWork(ctx, backend.WorkFilter{})
		if err != nil || len(ready) != 1 || ready[0].ID != "bd-first" {
			t.Fatalf("ready=%v %v", ready, err)
		}
		lifecycle, err := f.Store.IssueLifecycle()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = lifecycle.Close(ctx, issueops.CloseRequest{Actor: "contract", IssueID: "bd-first"}); err != nil {
			t.Fatal(err)
		}
		ready, err = f.Store.GetReadyWork(ctx, backend.WorkFilter{})
		if err != nil || len(ready) != 1 || ready[0].ID != "bd-second" {
			t.Fatalf("after close=%v %v", ready, err)
		}
		if _, err = lifecycle.Reopen(ctx, issueops.ReopenRequest{Actor: "contract", IssueID: "bd-first"}); err != nil {
			t.Fatal(err)
		}
		blocked, _, err := f.Store.IsBlocked(ctx, "bd-second")
		if err != nil || !blocked {
			t.Fatalf("reopened blocker=%v %v", blocked, err)
		}
	})
	t.Run("independent_claims_have_one_winner", func(t *testing.T) {
		f := makeFixture(t)
		if f.IndependentStore == nil {
			t.Fatal("fixture must open independent stores")
		}
		seed(t, f.Store, "bd-race")
		ctx := context.Background()
		stores := []backend.DoltStorage{f.Store, f.IndependentStore()}
		actors := []string{"worker-a", "worker-b"}
		results := make(chan error, 2)
		var wait sync.WaitGroup
		for i, s := range stores {
			wait.Add(1)
			go func(s backend.DoltStorage, actor string) {
				defer wait.Done()
				claimer, err := s.IssueClaimer()
				if err == nil {
					_, err = claimer.Claim(ctx, issueops.ClaimRequest{Actor: actor, IssueID: "bd-race"})
				}
				results <- err
			}(s, actors[i])
		}
		wait.Wait()
		close(results)
		winners := 0
		for err := range results {
			if err == nil {
				winners++
			} else if !errors.Is(err, backend.ErrAlreadyClaimed) {
				t.Fatal(err)
			}
		}
		if winners != 1 {
			t.Fatalf("winners=%d", winners)
		}
	})
	t.Run("heartbeat_is_quiet_and_reclaim_fences_holder", func(t *testing.T) {
		f := makeFixture(t)
		if f.SetLeaseExpiry == nil || f.AuditCount == nil {
			t.Fatal("fixture must expose native lease aging and audit inspection")
		}
		ctx := context.Background()
		seed(t, f.Store, "bd-lease")
		claimer, err := f.Store.IssueClaimer()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = claimer.Claim(ctx, issueops.ClaimRequest{Actor: "holder", IssueID: "bd-lease"}); err != nil {
			t.Fatal(err)
		}
		before, err := f.Store.GetIssue(ctx, "bd-lease")
		if err != nil {
			t.Fatal(err)
		}
		count, err := f.AuditCount(ctx)
		if err != nil {
			t.Fatal(err)
		}
		heartbeater, err := f.Store.Heartbeater()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = heartbeater.Heartbeat(ctx, issueops.HeartbeatRequest{Actor: "holder", IssueID: "bd-lease"}); err != nil {
			t.Fatal(err)
		}
		after, err := f.Store.GetIssue(ctx, "bd-lease")
		if err != nil {
			t.Fatal(err)
		}
		newCount, err := f.AuditCount(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if before.RowVersion != after.RowVersion || !before.UpdatedAt.Equal(after.UpdatedAt) || count != newCount {
			t.Fatal("heartbeat changed issue history or revision")
		}
		if err = f.SetLeaseExpiry(ctx, "bd-lease", time.Now().Add(-time.Minute)); err != nil {
			t.Fatal(err)
		}
		reclaimer, err := f.Store.LeaseReclaimer()
		if err != nil {
			t.Fatal(err)
		}
		result, err := reclaimer.Reclaim(ctx, issueops.ReclaimRequest{Actor: "reaper", Filter: issueops.ReclaimFilter{IDs: []string{"bd-lease"}}})
		if err != nil || len(result.Reclaimed) != 1 {
			t.Fatalf("reclaim=%+v %v", result, err)
		}
		lifecycle, err := f.Store.IssueLifecycle()
		if err != nil {
			t.Fatal(err)
		}
		_, err = lifecycle.Update(ctx, issueops.UpdateRequest{Actor: "holder", IssueID: "bd-lease", ExpectedVersion: &before.RowVersion, Patch: issueops.IssuePatch{Title: issueops.Field[string]{Set: true, Value: "stale"}}})
		if !errors.Is(err, backend.ErrVersionMismatch) {
			t.Fatalf("stale holder write=%v", err)
		}
	})
}
