package antfly

import (
	"context"
	"slices"
	"sort"

	"github.com/steveyegge/beads/internal/storage"
	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/issueops"
)

func (s *Store) EdgeReader() (issueops.EdgeReader, error) { return &graph{s}, nil }
func (g *graph) ReadEdges(ctx context.Context, req issueops.EdgeReadRequest) (issueops.EdgeReadResult, error) {
	if e := storageops.ValidateEdgeReadRequest(req); e != nil {
		return issueops.EdgeReadResult{}, e
	}
	st, _, e := g.s.load(ctx)
	if e != nil {
		return issueops.EdgeReadResult{}, e
	}
	out := issueops.EdgeReadResult{Anchors: []issueops.AnchorEdges{}}
	seen := map[string]bool{}
	for _, id := range req.IDs {
		if seen[id] {
			continue
		}
		seen[id] = true
		anchor := issueops.AnchorEdges{ID: id, Edges: []*types.Dependency{}, Missing: st.Issues[id] == nil}
		if !anchor.Missing {
			for _, d := range st.hydrate(id).Dependencies {
				if len(req.Types) == 0 || slices.Contains(req.Types, d.Type) {
					anchor.Edges = append(anchor.Edges, d)
				}
			}
		}
		out.Anchors = append(out.Anchors, anchor)
	}
	return out, nil
}
func (s *Store) CycleDetector() (issueops.CycleDetector, error) { return &graph{s}, nil }
func (g *graph) DetectCycles(ctx context.Context, req issueops.DetectCyclesRequest) (issueops.CycleReport, error) {
	if req.IncludeTracks {
		return issueops.CycleReport{}, errUnsupported("CycleDetector: IncludeTracks")
	}
	st, _, e := g.s.load(ctx)
	if e != nil {
		return issueops.CycleReport{}, e
	}
	adjacency := map[string][]string{}
	for _, d := range st.Edges {
		if types.IsSchedulingEdge(d.Type) {
			adjacency[d.IssueID] = append(adjacency[d.IssueID], d.DependsOnID)
		}
	}
	paths := storageops.CanonicalCyclePaths(adjacency)
	return issueops.CycleReport{Cycles: storageops.BuildCycles(paths, func(id string) *types.Issue {
		if st.Issues[id] == nil {
			return nil
		}
		return st.hydrate(id)
	})}, nil
}
func (s *Store) GetIssuesByIDs(ctx context.Context, ids []string) ([]*types.Issue, error) {
	r, e := (&batchGetter{s}).GetMany(ctx, issueops.GetManyRequest{IDs: ids})
	return r.Issues, e
}
func (s *Store) GetLabelsForIssues(ctx context.Context, ids []string) (map[string][]string, error) {
	st, _, e := s.load(ctx)
	if e != nil {
		return nil, e
	}
	out := map[string][]string{}
	for _, id := range ids {
		if r := st.Issues[id]; r != nil {
			out[id] = r.Issue.Labels
		}
	}
	return out, nil
}
func (s *Store) IsBlocked(ctx context.Context, id string) (bool, []string, error) {
	st, _, e := s.load(ctx)
	if e != nil {
		return false, nil, e
	}
	i, e := st.issue(id)
	if e != nil {
		return false, nil, e
	}
	blockers := []string{}
	for _, d := range st.Edges {
		if d.IssueID == id && (d.Type == types.DepBlocks || d.Type == types.DepConditionalBlocks) {
			if target := st.Issues[d.DependsOnID]; target == nil || !resolved(target.Issue.Status) {
				blockers = append(blockers, d.DependsOnID)
			}
		}
	}
	sort.Strings(blockers)
	return i.IsBlocked, blockers, nil
}
func (s *Store) IsBlockedBatch(ctx context.Context, ids []string) (map[string]bool, error) {
	st, _, e := s.load(ctx)
	if e != nil {
		return nil, e
	}
	out := map[string]bool{}
	for _, id := range ids {
		if r := st.Issues[id]; r != nil {
			out[id] = r.Issue.IsBlocked
		}
	}
	return out, nil
}
func (s *Store) BlockingAnnotator() (issueops.BlockingAnnotator, error) { return &graph{s}, nil }
func (g *graph) AnnotateBlocking(ctx context.Context, req issueops.BlockingRequest) (issueops.BlockingResult, error) {
	if e := storageops.ValidateBlockingRequest(req); e != nil {
		return issueops.BlockingResult{}, e
	}
	st, _, e := g.s.load(ctx)
	if e != nil {
		return issueops.BlockingResult{}, e
	}
	view := New(&stateTransport{st: st}, true)
	blockers, blocked, parents, e := view.GetBlockingInfoForIssues(ctx, req.IDs)
	if e != nil {
		return issueops.BlockingResult{}, e
	}
	out := issueops.BlockingResult{Items: []issueops.IssueBlocking{}}
	seen := map[string]bool{}
	for _, id := range req.IDs {
		if seen[id] {
			continue
		}
		seen[id] = true
		row := issueops.IssueBlocking{ID: id, BlockedBy: append([]string{}, blockers[id]...), Blocks: append([]string{}, blocked[id]...), Parent: parents[id]}
		if parent := st.Issues[row.Parent]; parent != nil && parent.Issue.Status == types.StatusClosed {
			row.Parent = ""
		}
		if req.IncludeIsBlocked && st.Issues[id] != nil {
			value := st.Issues[id].Issue.IsBlocked
			row.IsBlocked = &value
		}
		sort.Strings(row.BlockedBy)
		sort.Strings(row.Blocks)
		out.Items = append(out.Items, row)
	}
	return out, nil
}

var _ storage.MoleculeAutoCloseProber = (*Store)(nil)
