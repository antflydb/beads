package antfly

import (
	"context"
	"github.com/steveyegge/beads/internal/types"
	"testing"

	"github.com/steveyegge/beads/backend/conformance"
)

// TestAntflyQuerierContract runs the Querier contract against the server-backed
// store, which reaches the shared body (internal/workapi/storequerier) and the
// seam that renders LIMIT without OFFSET.
//
// The cases are subtests of one parent so the whole role suite shares one store
// and one copy-on-write branch: each Run scopes its own EXPRESSION to a label
// under the fixture prefix, and WritesNothing takes a before/after history
// delta, which is only meaningful while the subtests run sequentially.
// setupTestStore already marks the PARENT parallel; no subtest calls t.Parallel.
func TestAntflyQuerierContract(t *testing.T) {
	fixture, ctx, cleanup := newDoltQuerierFixture(t, "qry")
	defer cleanup()

	t.Run("DisjunctionAnswersEveryMatch", func(t *testing.T) {
		conformance.RunQuerierDisjunctionAnswersEveryMatch(t, ctx, fixture)
	})
	t.Run("PageIsAPrefixAndHasMoreIsExact", func(t *testing.T) {
		conformance.RunQuerierPageIsAPrefixAndHasMoreIsExact(t, ctx, fixture)
	})
	t.Run("SortBoundsThePageInOrder", func(t *testing.T) {
		conformance.RunQuerierSortBoundsThePageInOrder(t, ctx, fixture)
	})
	t.Run("SortByTitleFoldsCaseBeforeItCutsThePage", func(t *testing.T) {
		conformance.RunQuerierSortByTitleFoldsCaseBeforeItCutsThePage(t, ctx, fixture)
	})
	t.Run("SortByClosedPutsTheUnclosedRowsAtTheFarEnd", func(t *testing.T) {
		conformance.RunQuerierSortByClosedPutsTheUnclosedRowsAtTheFarEnd(t, ctx, fixture)
	})
	t.Run("SortTieBreaksByIDInBothDirections", func(t *testing.T) {
		conformance.RunQuerierSortTieBreaksByIDInBothDirections(t, ctx, fixture)
	})
	t.Run("SortSeesTheWholeMatchingSet", func(t *testing.T) {
		conformance.RunQuerierSortSeesTheWholeMatchingSet(t, ctx, fixture)
	})
	t.Run("HidesClosedUnlessTheExpressionOrTheFlagSaysOtherwise", func(t *testing.T) {
		conformance.RunQuerierHidesClosedUnlessTheExpressionOrTheFlagSaysOtherwise(t, ctx, fixture)
	})
	t.Run("RefusesAMalformedRequest", func(t *testing.T) {
		conformance.RunQuerierRefusesAMalformedRequest(t, ctx, fixture)
	})
	t.Run("OffsetSkipsMatches", func(t *testing.T) {
		conformance.RunQuerierOffsetSkipsMatches(t, ctx, fixture)
	})
	t.Run("EmptyMatchIsAWellFormedPage", func(t *testing.T) {
		conformance.RunQuerierEmptyMatchIsAWellFormedPage(t, ctx, fixture)
	})
	t.Run("WritesNothing", func(t *testing.T) {
		conformance.RunQuerierWritesNothing(t, ctx, fixture)
	})
	t.Run("DoesNotMutateTheCallerRequest", func(t *testing.T) {
		conformance.RunQuerierDoesNotMutateTheCallerRequest(t, ctx, fixture)
	})
}

func newDoltQuerierFixture(t *testing.T, prefix string) (conformance.QuerierFixture, context.Context, func()) {
	t.Helper()
	s, _ := testStore(t)
	ctx := context.Background()
	q, err := s.Querier()
	if err != nil {
		t.Fatal(err)
	}
	return conformance.QuerierFixture{IssuePrefix: "bd-" + prefix, Querier: q, CreateIssue: func(ctx context.Context, i *types.Issue, actor string) error {
		original := *i
		if e := s.CreateIssue(ctx, i, actor); e != nil {
			return e
		}
		// Seed historical timestamps like the other backends' conformance fixtures.
		return s.mutate(ctx, func(st *state) error {
			r := st.Issues[i.ID]
			if !original.CreatedAt.IsZero() {
				r.Issue.CreatedAt = original.CreatedAt
			}
			if !original.UpdatedAt.IsZero() {
				r.Issue.UpdatedAt = original.UpdatedAt
			}
			r.Issue.ClosedAt = original.ClosedAt
			return nil
		})
	}, CountHistory: func(ctx context.Context) (int, error) {
		st, _, e := s.load(ctx)
		if e != nil {
			return 0, e
		}
		return len(st.Events), nil
	}}, ctx, func() {}
}
