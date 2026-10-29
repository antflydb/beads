package antfly

import (
	"context"
	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/internal/workapi"
	"github.com/steveyegge/beads/issueops"
)

type deleter struct{ s *Store }

func (s *Store) Deleter() (issueops.Deleter, error) { return &deleter{s}, nil }
func (d *deleter) Delete(ctx context.Context, req issueops.DeleteRequest) (out issueops.DeleteResult, err error) {
	if err = workapi.ValidateDeleteRequest(req); err != nil {
		return out, err
	}
	apply := func(st *state) error {
		out = issueops.DeleteResult{DryRun: req.DryRun}
		ids := workapi.NormalizeDeleteIDs(req.IDs)
		set := map[string]bool{}
		missing := []string{}
		for _, id := range ids {
			if _, e := st.issue(id); e != nil {
				missing = append(missing, id)
			} else {
				set[id] = true
			}
		}
		if len(missing) > 0 {
			return &issueops.NotFoundError{IDs: missing}
		}
		if req.ExpectedVersion != nil {
			if e := checkVersion(&st.Issues[ids[0]].Issue, req.ExpectedVersion); e != nil {
				return e
			}
		}
		if req.Cascade {
			for changed := true; changed; {
				changed = false
				for _, edge := range st.Edges {
					if set[edge.DependsOnID] && !set[edge.IssueID] {
						set[edge.IssueID] = true
						changed = true
					}
				}
			}
		}
		orphaned := map[string]bool{}
		neighbors := map[string]bool{}
		for _, edge := range st.Edges {
			if set[edge.DependsOnID] && !set[edge.IssueID] {
				orphaned[edge.IssueID] = true
			}
			if set[edge.IssueID] && !set[edge.DependsOnID] {
				neighbors[edge.DependsOnID] = true
			}
			if set[edge.DependsOnID] && !set[edge.IssueID] {
				neighbors[edge.IssueID] = true
			}
		}
		if !req.Force && !req.Cascade && len(orphaned) > 0 {
			for _, id := range ids {
				deps := map[string]bool{}
				for _, edge := range st.Edges {
					if edge.DependsOnID == id && orphaned[edge.IssueID] {
						deps[edge.IssueID] = true
					}
				}
				if len(deps) > 0 {
					return &issueops.DependentsOutsideRequestError{IssueID: id, Dependents: workapi.SortedDeleteIDs(deps)}
				}
			}
		}
		out.Deleted = len(set)
		out.Orphaned = workapi.SortedDeleteIDs(orphaned)
		for key, edge := range st.Edges {
			if set[edge.IssueID] || set[edge.DependsOnID] {
				out.Dependencies++
				delete(st.Edges, key)
			}
		}
		for id := range set {
			out.Labels += len(st.Issues[id].Issue.Labels)
			for _, event := range st.Events {
				if event.ID == id {
					out.Events++
				}
			}
			st.audit("delete", id, req.Actor)
			delete(st.Issues, id)
		}
		rewritten := map[string]bool{}
		for id := range set {
			pattern := storageops.DeletedReferencePattern(id)
			replacement := `$1[deleted:` + id + `]$3`
			for neighbor := range neighbors {
				issue := &st.Issues[neighbor].Issue
				for _, field := range []*string{&issue.Description, &issue.Notes, &issue.Design, &issue.AcceptanceCriteria} {
					if pattern.MatchString(*field) {
						*field = pattern.ReplaceAllString(*field, replacement)
						rewritten[neighbor] = true
					}
				}
			}
		}
		for id := range rewritten {
			st.touch(&st.Issues[id].Issue)
			st.audit("update", id, req.Actor)
		}
		out.ReferencesUpdated = len(rewritten)
		st.reblock()
		return nil
	}
	if req.DryRun {
		st, _, e := d.s.load(ctx)
		if e != nil {
			return out, e
		}
		err = apply(st)
	} else {
		err = d.s.mutate(ctx, apply)
	}
	return out, err
}
func (s *Store) DeleteIssue(ctx context.Context, id string) error {
	_, e := (&deleter{s}).Delete(ctx, issueops.DeleteRequest{IDs: []string{id}})
	return e
}
func (s *Store) DeleteIssues(ctx context.Context, ids []string, cascade, force, dry bool) (*types.DeleteIssuesResult, error) {
	out, e := (&deleter{s}).Delete(ctx, issueops.DeleteRequest{IDs: ids, Cascade: cascade, Force: force, DryRun: dry})
	return &types.DeleteIssuesResult{DeletedCount: out.Deleted, DependenciesCount: out.Dependencies, LabelsCount: out.Labels, EventsCount: out.Events, OrphanedIssues: out.Orphaned}, e
}
