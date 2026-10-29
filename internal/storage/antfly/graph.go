package antfly

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/issueops"
)

func edgeKey(from, to string) string { return from + "\x00" + to }
func (st *state) addEdge(from, to string, kind types.DependencyType, actor string) error {
	if from == to {
		return fmt.Errorf("%w: self dependency", storage.ErrValidation)
	}
	if _, e := st.issue(from); e != nil {
		return e
	}
	if _, e := st.issue(to); e != nil {
		return e
	}
	if kind == "" {
		return storage.ErrValidation
	}
	if kind.IsBlockingEdge() && kind != types.DepBlocks {
		return errUnsupported("DependencyEditor: advanced blocking edge")
	}
	key := edgeKey(from, to)
	if old := st.Edges[key]; old != nil {
		if old.Type != kind {
			return fmt.Errorf("%w: dependency type conflict", storage.ErrValidation)
		}
		return nil
	}
	if kind == types.DepParentChild {
		for _, d := range st.Edges {
			if d.IssueID == from && d.Type == kind && d.DependsOnID != to {
				return fmt.Errorf("%w: issue already has a parent", storage.ErrValidation)
			}
		}
	}
	st.Edges[key] = &types.Dependency{IssueID: from, DependsOnID: to, Type: kind, CreatedAt: time.Now().UTC(), CreatedBy: actor}
	// Blocking and hierarchy edges share the cycle check. An unchecked
	// per-edge request still cannot publish a cyclic final graph.
	if kind == types.DepBlocks || kind == types.DepParentChild {
		seen := map[string]bool{}
		var reaches func(string) bool
		reaches = func(id string) bool {
			if id == from {
				return true
			}
			if seen[id] {
				return false
			}
			seen[id] = true
			for _, d := range st.Edges {
				if d.IssueID == id && (d.Type == types.DepBlocks || d.Type == types.DepParentChild) && reaches(d.DependsOnID) {
					return true
				}
			}
			return false
		}
		if reaches(to) {
			delete(st.Edges, key)
			return fmt.Errorf("%w: dependency cycle", storage.ErrValidation)
		}
	}
	st.audit("dependency_added", from, actor)
	return nil
}

func (st *state) reblock() {
	for _, r := range st.Issues {
		r.Issue.IsBlocked = false
	}
	// Iterate to a fixed point so a blocked parent withholds its descendants.
	for pass := 0; pass <= len(st.Issues); pass++ {
		changed := false
		for _, d := range st.Edges {
			source, target := st.Issues[d.IssueID], st.Issues[d.DependsOnID]
			if source == nil || source.Issue.Status == types.StatusClosed || source.Issue.Status == types.StatusPinned {
				continue
			}
			blocked := (d.Type == types.DepBlocks && (target == nil || !resolved(target.Issue.Status))) || (d.Type == types.DepParentChild && target != nil && target.Issue.IsBlocked)
			if blocked && !source.Issue.IsBlocked {
				source.Issue.IsBlocked = true
				changed = true
			}
		}
		if !changed {
			break
		}
	}
}
func (st *state) hydrate(id string) *types.Issue {
	stored := &st.Issues[id].Issue
	b, _ := json.Marshal(stored)
	var i types.Issue
	_ = json.Unmarshal(b, &i)
	i.RowVersion = stored.RowVersion
	i.Dependencies = nil
	for _, d := range st.Edges {
		if d.IssueID == id {
			copy := *d
			i.Dependencies = append(i.Dependencies, &copy)
		}
	}
	sort.Slice(i.Dependencies, func(a, b int) bool { return i.Dependencies[a].DependsOnID < i.Dependencies[b].DependsOnID })
	return &i
}
func (st *state) counts(id string) *types.IssueWithCounts {
	i := st.hydrate(id)
	out := &types.IssueWithCounts{Issue: i, DependencyCount: len(i.Dependencies), CommentCount: len(i.Comments)}
	for _, d := range st.Edges {
		if d.DependsOnID == id {
			out.DependentCount++
		}
		if d.IssueID == id && d.Type == types.DepParentChild {
			p := d.DependsOnID
			out.Parent = &p
		}
	}
	return out
}

type graph struct{ s *Store }

func (g *graph) AddDependencies(ctx context.Context, req issueops.AddDependenciesRequest) (issueops.AddDependenciesResult, error) {
	if e := storageops.ValidateAddDependenciesRequest(req); e != nil {
		return issueops.AddDependenciesResult{}, e
	}
	edges := append([]issueops.DependencyEdge(nil), req.Edges...)
	err := g.s.mutate(ctx, func(st *state) error {
		for _, d := range edges {
			if e := st.addEdge(d.IssueID, d.DependsOnID, d.Type, req.Actor); e != nil {
				return e
			}
		}
		st.reblock()
		return nil
	})
	return issueops.AddDependenciesResult{Added: edges}, err
}
func (g *graph) RemoveDependency(ctx context.Context, req issueops.RemoveDependencyRequest) (out issueops.RemoveDependencyResult, err error) {
	if err = storageops.ValidateRemoveDependencyRequest(req); err != nil {
		return out, err
	}
	err = g.s.mutate(ctx, func(st *state) error {
		key := edgeKey(req.IssueID, req.DependsOnID)
		_, out.Removed = st.Edges[key]
		if out.Removed {
			delete(st.Edges, key)
			st.reblock()
			st.audit("dependency_removed", req.IssueID, req.Actor)
		}
		return nil
	})
	return out, err
}
func (g *graph) Related(ctx context.Context, req issueops.RelatedRequest) ([]*issueops.RelatedIssue, error) {
	if req.Direction != issueops.RelationOut && req.Direction != issueops.RelationIn {
		return nil, storage.ErrValidation
	}
	st, _, e := g.s.load(ctx)
	if e != nil {
		return nil, e
	}
	if _, e = st.issue(req.ID); e != nil {
		return nil, e
	}
	out := []*issueops.RelatedIssue{}
	for _, d := range st.Edges {
		if len(req.Types) > 0 {
			match := false
			for _, t := range req.Types {
				if t == d.Type {
					match = true
				}
			}
			if !match {
				continue
			}
		}
		id := ""
		if req.Direction == issueops.RelationOut && d.IssueID == req.ID {
			id = d.DependsOnID
		}
		if req.Direction == issueops.RelationIn && d.DependsOnID == req.ID {
			id = d.IssueID
		}
		if st.Issues[id] != nil {
			out = append(out, &types.IssueWithDependencyMetadata{Issue: *st.hydrate(id), DependencyType: d.Type})
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out, nil
}
func (s *Store) AddDependency(ctx context.Context, d *types.Dependency, actor string) error {
	_, e := (&graph{s}).AddDependencies(ctx, issueops.AddDependenciesRequest{Actor: actor, Edges: []issueops.DependencyEdge{{IssueID: d.IssueID, DependsOnID: d.DependsOnID, Type: d.Type}}})
	return e
}
func (s *Store) RemoveDependency(ctx context.Context, id, target, actor string) error {
	_, e := (&graph{s}).RemoveDependency(ctx, issueops.RemoveDependencyRequest{Actor: actor, IssueID: id, DependsOnID: target})
	return e
}
func (s *Store) GetDependencies(ctx context.Context, id string) ([]*types.Issue, error) {
	rows, e := (&graph{s}).Related(ctx, issueops.RelatedRequest{ID: id, Direction: issueops.RelationOut})
	out := []*types.Issue{}
	for _, r := range rows {
		i := r.Issue
		out = append(out, &i)
	}
	return out, e
}
func (s *Store) GetDependents(ctx context.Context, id string) ([]*types.Issue, error) {
	rows, e := (&graph{s}).Related(ctx, issueops.RelatedRequest{ID: id, Direction: issueops.RelationIn})
	out := []*types.Issue{}
	for _, r := range rows {
		i := r.Issue
		out = append(out, &i)
	}
	return out, e
}

type comments struct{ s *Store }

func (c *comments) AddComment(ctx context.Context, req issueops.AddCommentRequest) (out issueops.AddCommentResult, err error) {
	if err = storageops.ValidateAddCommentRequest(req); err != nil {
		return out, err
	}
	err = c.s.mutate(ctx, func(st *state) error {
		i, e := st.issue(req.IssueID)
		if e != nil {
			return e
		}
		var identity [16]byte
		if _, e = rand.Read(identity[:]); e != nil {
			return e
		}
		comment := &types.Comment{ID: i.ID + ":" + hex.EncodeToString(identity[:]), IssueID: i.ID, Author: req.Author, Text: req.Text, CreatedAt: time.Now().UTC()}
		i.Comments = append(i.Comments, comment)
		st.audit("comment", i.ID, req.Author)
		out.Comment = comment
		return nil
	})
	return out, err
}
func (s *Store) GetComments(ctx context.Context, id string) ([]*types.Comment, error) {
	i, e := s.GetIssue(ctx, id)
	if e != nil {
		return nil, e
	}
	return i.Comments, nil
}
func (s *Store) GetLabels(ctx context.Context, id string) ([]string, error) {
	i, e := s.GetIssue(ctx, id)
	if e != nil {
		return nil, e
	}
	return i.Labels, nil
}
