package antfly

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/steveyegge/beads/internal/storage"
	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/issueops"
)

type batchApplier struct{ s *Store }

func (s *Store) BatchApplier() (issueops.BatchApplier, error) { return &batchApplier{s}, nil }

// ApplyBatch stages the guarded lifecycle verbs against one private snapshot.
// Only the final conditional publication is visible to other handles. Neither
// conflicts nor indeterminate outcomes replay a caller's non-idempotent plan.
func (b *batchApplier) ApplyBatch(ctx context.Context, request issueops.ApplyBatchRequest) (issueops.ApplyBatchResult, error) {
	plan, err := storage.PlanApplyBatch(request)
	if err != nil {
		return issueops.ApplyBatchResult{}, err
	}
	// Snapshot all caller-owned payloads before the first storage read.
	for index, item := range plan.Items {
		switch item.Kind {
		case issueops.ItemCreate:
			c := *item.Create
			c.Issue = storageops.CloneCreateRequest(issueops.CreateRequest{Issue: c.Issue}).Issue
			if c.MetadataRefs != nil {
				refs := map[string]issueops.Ref{}
				for key, ref := range c.MetadataRefs {
					refs[key] = ref
				}
				c.MetadataRefs = refs
			}
			plan.Items[index].Create = &c
		case issueops.ItemUpdate:
			u := *item.Update
			r := storageops.CloneUpdateRequest(issueops.UpdateRequest{Patch: u.Patch, ExpectedVersion: u.ExpectedVersion, ExpectedStatus: u.ExpectedStatus, ExpectedAssignee: u.ExpectedAssignee})
			u.Patch, u.ExpectedVersion, u.ExpectedStatus, u.ExpectedAssignee = r.Patch, r.ExpectedVersion, r.ExpectedStatus, r.ExpectedAssignee
			plan.Items[index].Update = &u
		case issueops.ItemClose:
			c := *item.Close
			c.ExpectedVersion = storageops.CloneCloseRequest(issueops.CloseRequest{ExpectedVersion: c.ExpectedVersion}).ExpectedVersion
			plan.Items[index].Close = &c
		}
	}
	out := issueops.ApplyBatchResult{Keys: map[string]string{}, Items: make([]issueops.ItemResult, len(plan.Items))}
	resolve := func(ref issueops.Ref) string {
		if ref.Key != "" {
			return out.Keys[ref.Key]
		}
		return ref.ID
	}
	planes := map[string]bool{}
	err = b.s.stage(ctx, func(s *Store) error {
		l := &lifecycle{s}
		for index, item := range plan.Items {
			result := issueops.ItemResult{Kind: item.Kind}
			var issue *types.Issue
			var itemErr error
			var key string
			switch item.Kind {
			case issueops.ItemCreate:
				c := item.Create
				key = c.Key
				r, e := l.Create(ctx, issueops.CreateRequest{Actor: plan.Actor, Issue: c.Issue, ForceIDPrefix: plan.ForceIDPrefix, DefaultPriority: c.DefaultPriority})
				issue, itemErr = r.Issue, e
				result.Changed = e == nil
				if e == nil {
					if key != "" {
						out.Keys[key] = issue.ID
					}
					planes[issue.ID] = storageops.IsWisp(issue)
				}
			case issueops.ItemUpdate:
				u := item.Update
				key, result.IssueID = u.Target.Key, resolve(u.Target)
				r, e := l.Update(ctx, issueops.UpdateRequest{Actor: plan.Actor, IssueID: result.IssueID, Patch: u.Patch, ExpectedVersion: u.ExpectedVersion, ExpectedStatus: u.ExpectedStatus, ExpectedAssignee: u.ExpectedAssignee, ForceClosePolicy: u.ForceClosePolicy, ForceAssigneeTransfer: u.ForceAssigneeTransfer, ForceNotesOverwrite: u.ForceNotesOverwrite})
				issue, itemErr, result.Changed = r.Issue, e, r.Changed
			case issueops.ItemClose:
				c := item.Close
				key, result.IssueID = c.Target.Key, resolve(c.Target)
				r, e := l.Close(ctx, issueops.CloseRequest{Actor: plan.Actor, IssueID: result.IssueID, Reason: c.Reason, Session: c.Session, Force: c.Force, ExpectedVersion: c.ExpectedVersion})
				issue, itemErr, result.Changed = r.Issue, e, r.Changed
			case issueops.ItemDepAdd:
				d := item.DepAdd
				key, result.IssueID, result.DependsOnID = d.Source.Key, resolve(d.Source), resolve(d.Target)
				itemErr = s.mutate(ctx, func(st *state) error {
					from, to := result.IssueID, result.DependsOnID
					if from == to {
						return issueops.ErrSelfDependency
					}
					if e := issueops.CheckDottedChildDependency(from, to, d.Type); e != nil {
						return e
					}
					a, am := planes[from]
					z, zm := planes[to]
					if am && zm && a != z {
						return storageops.CrossPlaneBatchEdgeError(from, to)
					}
					if st.Issues[from] == nil {
						return fmt.Errorf("%w: %s", issueops.ErrDependencySourceNotFound, from)
					}
					if st.Issues[to] == nil && !storageops.IsExternalDepTarget(from, to) {
						return fmt.Errorf("%w: %s", issueops.ErrDependencyTargetNotFound, to)
					}
					previous := st.Edges[edgeKey(from, to)]
					if previous != nil && previous.Type != d.Type {
						return &issueops.DependencyTypeConflictError{IssueID: from, DependsOnID: to, ExistingType: string(previous.Type), RequestedType: string(d.Type)}
					}
					metadata := d.Metadata
					if d.HasSpawner {
						var e error
						metadata, e = storageops.StampWaitsForSpawnerID(d.Type, metadata, to)
						if e != nil {
							return fmt.Errorf("%w: %v", issueops.ErrValidation, e)
						}
					}
					// All graph changes are additions: checking every edge also proves the
					// final graph acyclic even when the caller requests the bulk fast path.
					if e := st.addEdge(from, to, d.Type, plan.Actor); e != nil {
						return e
					}
					edge := st.Edges[edgeKey(from, to)]
					edge.Metadata, edge.ThreadID = metadata, d.ThreadID
					result.Changed = previous == nil
					st.reblock()
					return nil
				})
			}
			if itemErr != nil {
				return &issueops.ItemError{Index: index, Kind: item.Kind, Key: key, IssueID: result.IssueID, Err: itemErr}
			}
			if issue != nil {
				result.Issue, result.IssueID, result.RowVersion = issue, issue.ID, issue.RowVersion
			}
			out.Items[index] = result
		}
		for index, item := range plan.Items {
			if item.Create == nil || len(item.Create.MetadataRefs) == 0 {
				continue
			}
			set := map[string]json.RawMessage{}
			for key, ref := range item.Create.MetadataRefs {
				set[key], _ = json.Marshal(resolve(ref))
			}
			id := out.Items[index].IssueID
			r, e := l.Update(ctx, issueops.UpdateRequest{Actor: plan.Actor, IssueID: id, Patch: issueops.IssuePatch{Metadata: issueops.MetadataPatch{Set: set}}})
			if e != nil {
				return &issueops.ItemError{Index: index, Kind: item.Kind, Key: item.Create.Key, IssueID: id, Err: e}
			}
			out.Items[index].Issue, out.Items[index].RowVersion = r.Issue, r.Issue.RowVersion
		}
		st, _, e := s.load(ctx)
		if e != nil {
			return e
		}
		for index, item := range plan.Items {
			if item.DepAdd == nil || item.DepAdd.Type != types.DepBlocks {
				continue
			}
			from, to := resolve(item.DepAdd.Source), resolve(item.DepAdd.Target)
			if storageops.IsExternalDepTarget(from, to) {
				continue
			}
			if ancestor := st.parentAncestor(from, to); ancestor || st.parentAncestor(to, from) {
				return &issueops.ItemError{Index: index, Kind: item.Kind, IssueID: from, Err: &issueops.DependencyHierarchyConflictError{IssueID: from, BlockerID: to, BlockerIsAncestor: ancestor}}
			}
		}
		return nil
	})
	if err != nil {
		return issueops.ApplyBatchResult{}, err
	}
	return out, nil
}

func (st *state) parentAncestor(node, candidate string) bool {
	seen := map[string]bool{}
	for node != "" && !seen[node] {
		seen[node] = true
		parent := ""
		for _, d := range st.Edges {
			if d.IssueID == node && d.Type == types.DepParentChild {
				parent = d.DependsOnID
				break
			}
		}
		if parent == candidate {
			return true
		}
		node = parent
	}
	return false
}
