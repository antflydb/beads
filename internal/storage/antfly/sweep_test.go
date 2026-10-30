package antfly

import (
	"context"
	"fmt"
	"github.com/steveyegge/beads/backend/conformance"
	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/issueops"
	"strings"
	"testing"
)

func TestAntflySweeperContract(t *testing.T) {
	fixture, ctx, cleanup := newAntflySweeperFixture(t, "swp")
	defer cleanup()

	t.Run("RefusesAnUnfilteredDurableSweep", func(t *testing.T) {
		conformance.RunSweeperRefusesAnUnfilteredDurableSweep(t, ctx, fixture)
	})
	t.Run("RefusesAMalformedRequest", func(t *testing.T) {
		conformance.RunSweeperRefusesAMalformedRequest(t, ctx, fixture)
	})
	t.Run("ClearsOneTierAndLeavesTheOther", func(t *testing.T) {
		conformance.RunSweeperClearsOneTierAndLeavesTheOther(t, ctx, fixture)
	})
	t.Run("TreatsALegacyTypedWispAsEphemeralTier", func(t *testing.T) {
		conformance.RunSweeperTreatsALegacyTypedWispAsEphemeralTier(t, ctx, fixture)
	})
	t.Run("WispsPlaneClearsTheWholeWispsTable", func(t *testing.T) {
		conformance.RunSweeperWispsPlaneClearsTheWholeWispsTable(t, ctx, fixture)
	})
	t.Run("WispsPlaneRequiresAFilter", func(t *testing.T) {
		conformance.RunSweeperWispsPlaneRequiresAFilter(t, ctx, fixture)
	})
	t.Run("ProtectsLiveDependents", func(t *testing.T) {
		conformance.RunSweeperProtectsLiveDependents(t, ctx, fixture)
	})
	t.Run("ProtectsTransitiveLiveDependents", func(t *testing.T) {
		conformance.RunSweeperProtectsTransitiveLiveDependents(t, ctx, fixture)
	})
	t.Run("ProtectsLiveDependentsAcrossPlanes", func(t *testing.T) {
		conformance.RunSweeperProtectsLiveDependentsAcrossPlanes(t, ctx, fixture)
	})
	t.Run("LimitTakesTheOldestClosedFirst", func(t *testing.T) {
		conformance.RunSweeperLimitTakesTheOldestClosedFirst(t, ctx, fixture)
	})
	t.Run("LeavesNoHistoryBeadsToTheDurableTier", func(t *testing.T) {
		conformance.RunSweeperLeavesNoHistoryBeadsToTheDurableTier(t, ctx, fixture)
	})
	t.Run("ProtectsPinnedRows", func(t *testing.T) {
		conformance.RunSweeperProtectsPinnedRows(t, ctx, fixture)
	})
	t.Run("HonorsTheCutoffAndThePattern", func(t *testing.T) {
		conformance.RunSweeperHonorsTheCutoffAndThePattern(t, ctx, fixture)
	})
	t.Run("DryRunChangesNothing", func(t *testing.T) {
		conformance.RunSweeperDryRunChangesNothing(t, ctx, fixture)
	})
	t.Run("ProtectsRowsCitedFromAWispComment", func(t *testing.T) {
		conformance.RunSweeperProtectsRowsCitedFromAWispComment(t, ctx, fixture)
	})
	t.Run("ProtectsCitedRows", func(t *testing.T) {
		conformance.RunSweeperProtectsCitedRows(t, ctx, fixture)
	})
	t.Run("EmptyMatchIsZeroAndNil", func(t *testing.T) {
		conformance.RunSweeperEmptyMatchIsZeroAndNil(t, ctx, fixture)
	})
	t.Run("RecordsExactlyOneHistoryEntry", func(t *testing.T) {
		conformance.RunSweeperRecordsExactlyOneHistoryEntry(t, ctx, fixture)
	})
	t.Run("DoesNotMutateTheCallerRequest", func(t *testing.T) {
		conformance.RunSweeperDoesNotMutateTheCallerRequest(t, ctx, fixture)
	})
}

// The frozen suite's SQL-shaped observer is adapted to native logical planes;
// only its exact count and legacy-seeding fixtures are accepted. No production
// operation parses or executes SQL.
func newAntflySweeperFixture(t *testing.T, prefix string) (conformance.SweeperFixture, context.Context, func()) {
	t.Helper()
	s, tr := testStore(t)
	ctx := context.Background()
	sweeper, e := s.Sweeper()
	if e != nil {
		t.Fatal(e)
	}
	create := func(ctx context.Context, i *types.Issue, actor string) error {
		original := *i
		comments := i.Comments
		copy := *i
		copy.Comments = nil
		if e := s.CreateIssue(ctx, &copy, actor); e != nil {
			return e
		}
		i.ID = copy.ID
		return s.mutate(ctx, func(st *state) error {
			r := st.Issues[i.ID]
			r.Issue.CreatedAt = original.CreatedAt
			r.Issue.UpdatedAt = original.UpdatedAt
			r.Issue.ClosedAt = original.ClosedAt
			r.Issue.Comments = comments
			return nil
		})
	}
	return conformance.SweeperFixture{
		IssuePrefix: "bd-" + prefix, Sweeper: sweeper, CreateIssue: create, CreateWisp: create, AddComment: s.AddComment,
		AddDependencies: func(ctx context.Context, req issueops.AddDependenciesRequest) error {
			_, e := (&graph{s}).AddDependencies(ctx, req)
			return e
		},
		CountHistory: func(context.Context) (int, error) { return int(tr.version), nil }, CommitPending: func(context.Context) error { return nil },
		QueryScalar: func(ctx context.Context, query string, args []any, dest ...any) error {
			plane := ""
			for _, name := range []string{"issues", "wisps"} {
				if strings.HasPrefix(query, "SELECT COUNT(*) FROM "+name+" WHERE id IN (") {
					plane = name
				}
			}
			if plane == "" || len(dest) != 1 {
				return fmt.Errorf("unsupported fixture observer %q", query)
			}
			output, ok := dest[0].(*int)
			if !ok {
				return fmt.Errorf("invalid fixture count destination")
			}
			st, _, e := s.load(ctx)
			if e != nil {
				return e
			}
			*output = 0
			for _, arg := range args {
				id, ok := arg.(string)
				if !ok {
					return fmt.Errorf("invalid fixture id")
				}
				if r := st.Issues[id]; r != nil && storageops.IsWisp(&r.Issue) == (plane == "wisps") {
					*output++
				}
			}
			return nil
		},
		Exec: func(ctx context.Context, statements []conformance.SQLStatement) error {
			for _, statement := range statements {
				if statement.Query != "UPDATE issues SET wisp_type = 'patrol' WHERE id = ?" || len(statement.Args) != 1 {
					return fmt.Errorf("unsupported legacy fixture %q", statement.Query)
				}
				id, ok := statement.Args[0].(string)
				if !ok {
					return fmt.Errorf("invalid legacy fixture id")
				}
				if e := s.mutate(ctx, func(st *state) error {
					r := st.Issues[id]
					if r == nil {
						return fmt.Errorf("missing legacy fixture")
					}
					r.Issue.WispType = "patrol"
					return nil
				}); e != nil {
					return e
				}
			}
			return nil
		},
	}, ctx, func() {}
}
