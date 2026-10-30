package antfly

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strings"
	"time"

	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/internal/workapi"
	"github.com/steveyegge/beads/issueops"
)

type sweeper struct{ s *Store }

// Sweeper exposes native bulk clearance with one conditional publication.
func (s *Store) Sweeper() (issueops.Sweeper, error) { return &sweeper{s}, nil }

func (w *sweeper) Sweep(ctx context.Context, req issueops.SweepRequest) (out issueops.SweepResult, err error) {
	if err = workapi.ValidateSweepRequest(req); err != nil {
		return out, err
	}
	run := func(s *Store) error {
		out = issueops.SweepResult{DryRun: req.DryRun, RetainIDs: req.RetainIDs, ReferencedIDs: []string{}}
		st, _, e := s.load(ctx)
		if e != nil {
			return e
		}
		custom, e := types.ParseCustomStatusConfig(st.Config["status.custom"])
		if e != nil {
			return e
		}
		activeStatuses := workapi.NotDoneStatusesForSweep(custom)
		candidates := []*types.Issue{}
		retained := []string{}
		if value := st.Config["types.retained"]; value != "" {
			if json.Unmarshal([]byte(value), &retained) != nil {
				retained = strings.Split(value, ",")
			}
			for i := range retained {
				retained[i] = strings.TrimSpace(retained[i])
			}
		}
		for _, record := range st.Issues {
			i := &record.Issue
			if i.Status != types.StatusClosed {
				continue
			}
			ephemeral := i.Ephemeral || i.WispType != ""
			switch req.Tier {
			case issueops.SweepEphemeral:
				if !ephemeral {
					continue
				}
			case issueops.SweepDurable:
				if ephemeral {
					continue
				}
			case issueops.SweepWispsPlane:
				if !storageops.IsWisp(i) {
					continue
				}
			}
			if !req.RetainIDs && slices.Contains(retained, string(i.IssueType)) {
				continue
			}
			if req.ClosedBefore != nil && (i.ClosedAt == nil || !i.ClosedAt.Before(*req.ClosedBefore)) {
				continue
			}
			candidates = append(candidates, st.hydrate(i.ID))
		}
		sort.Slice(candidates, func(a, b int) bool {
			if candidates[a].ClosedAt != nil && candidates[b].ClosedAt != nil && !candidates[a].ClosedAt.Equal(*candidates[b].ClosedAt) {
				return candidates[a].ClosedAt.Before(*candidates[b].ClosedAt)
			}
			return candidates[a].ID < candidates[b].ID
		})
		kept, skips := workapi.FilterSweepCandidates(candidates, req.IDPattern, req.ClosedBefore)
		out.Skipped = skips
		if req.ProtectLiveDependents {
			incoming := map[string][]*types.Dependency{}
			live := map[string]bool{}
			for id, record := range st.Issues {
				live[id] = slices.Contains(activeStatuses, record.Issue.Status)
			}
			for _, edge := range st.Edges {
				incoming[edge.DependsOnID] = append(incoming[edge.DependsOnID], edge)
			}
			kept, out.Skipped.LiveDependent = workapi.PartitionSweepLiveDependents(kept, workapi.SweepLiveDependentTargets(incoming, live))
		}
		if req.ProtectReferenced {
			ids := map[string]bool{}
			for _, i := range kept {
				ids[i.ID] = true
			}
			matcher := workapi.NewCandidateIDMatcher(ids)
			cited := map[string]bool{}
			for _, record := range st.Issues {
				if !slices.Contains(activeStatuses, record.Issue.Status) {
					continue
				}
				i := record.Issue
				matcher.FindAll(i.Description, cited)
				matcher.FindAll(i.Notes, cited)
				for _, comment := range i.Comments {
					if comment != nil {
						matcher.FindAll(comment.Text, cited)
					}
				}
			}
			kept, out.Skipped.Referenced, out.ReferencedIDs = workapi.PartitionSweepReferenced(kept, cited)
		}
		kept, out.Remaining = workapi.LimitSweepCandidates(kept, req.Limit)
		if len(kept) == 0 {
			return nil
		}
		ids := make([]string, len(kept))
		retire := map[string]time.Time{}
		for n, i := range kept {
			ids[n] = i.ID
			retire[i.ID] = *i.ClosedAt
		}
		result, e := (&deleter{s}).Delete(ctx, issueops.DeleteRequest{IDs: ids, Actor: req.Actor, Force: true, DryRun: req.DryRun})
		if e != nil {
			return e
		}
		out.Swept = result.Deleted
		out.Dependencies = result.Dependencies
		out.Labels = result.Labels
		out.Events = result.Events
		if req.RetainIDs && !req.DryRun {
			return s.mutate(ctx, func(st *state) error {
				for id, at := range retire {
					st.Retired[id] = at
				}
				return nil
			})
		}
		return nil
	}
	if req.DryRun {
		data, _, loadErr := w.s.transport.Load(ctx)
		if loadErr != nil {
			return issueops.SweepResult{}, loadErr
		}
		staged := New(&stagedTransport{data: data, version: 1}, false)
		staged.expectedProject = w.s.expectedProject
		err = run(staged)
	} else {
		err = w.s.stage(ctx, run)
	}
	if err != nil {
		return issueops.SweepResult{}, err
	}
	return out, nil
}

var _ issueops.Sweeper = (*sweeper)(nil)
