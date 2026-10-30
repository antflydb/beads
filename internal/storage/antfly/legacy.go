package antfly

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/steveyegge/beads/internal/storage"
	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/internal/workapi/storecounter"
	"github.com/steveyegge/beads/internal/workapi/storestats"
	"github.com/steveyegge/beads/issueops"
)

// These raw reads also serve the existing policy decorators and CLI paths
// that have not yet moved onto the narrower issueops roles.
func (s *Store) GetAllDependencyRecords(ctx context.Context) (map[string][]*types.Dependency, error) {
	st, _, e := s.load(ctx)
	if e != nil {
		return nil, e
	}
	out := map[string][]*types.Dependency{}
	for _, d := range st.Edges {
		out[d.IssueID] = append(out[d.IssueID], d)
	}
	for _, rows := range out {
		sort.Slice(rows, func(a, b int) bool { return rows[a].DependsOnID < rows[b].DependsOnID })
	}
	return out, nil
}
func (s *Store) GetDependencyRecords(ctx context.Context, id string) ([]*types.Dependency, error) {
	i, e := s.GetIssue(ctx, id)
	if e != nil {
		return nil, e
	}
	return i.Dependencies, nil
}
func (s *Store) GetDependencyRecordsForIssues(ctx context.Context, ids []string) (map[string][]*types.Dependency, error) {
	all, e := s.GetAllDependencyRecords(ctx)
	if e != nil {
		return nil, e
	}
	out := map[string][]*types.Dependency{}
	for _, id := range ids {
		out[id] = all[id]
	}
	return out, nil
}
func (s *Store) GetDependenciesWithMetadata(ctx context.Context, id string) ([]*types.IssueWithDependencyMetadata, error) {
	return (&graph{s}).Related(ctx, issueops.RelatedRequest{ID: id, Direction: issueops.RelationOut})
}
func (s *Store) GetDependentsWithMetadata(ctx context.Context, id string) ([]*types.IssueWithDependencyMetadata, error) {
	return (&graph{s}).Related(ctx, issueops.RelatedRequest{ID: id, Direction: issueops.RelationIn})
}
func (s *Store) CountDependencies(ctx context.Context, id string) (int64, error) {
	st, _, e := s.load(ctx)
	if e != nil {
		return 0, e
	}
	return int64(st.countsIfPresent(id).DependencyCount), nil
}
func (s *Store) CountDependents(ctx context.Context, id string) (int64, error) {
	st, _, e := s.load(ctx)
	if e != nil {
		return 0, e
	}
	return int64(st.countsIfPresent(id).DependentCount), nil
}
func (st *state) countsIfPresent(id string) *types.IssueWithCounts {
	if st.Issues[id] == nil {
		return &types.IssueWithCounts{}
	}
	return st.counts(id)
}
func (s *Store) CountIssueComments(ctx context.Context, id string) (int64, error) {
	st, _, e := s.load(ctx)
	if e != nil {
		return 0, e
	}
	return int64(st.countsIfPresent(id).CommentCount), nil
}
func (s *Store) GetIssueComments(ctx context.Context, id string) ([]*types.Comment, error) {
	return s.GetComments(ctx, id)
}
func (s *Store) GetIssueCommentsPage(ctx context.Context, id string, after storage.CommentPageCursor, limit int) ([]*types.Comment, error) {
	rows, e := s.GetComments(ctx, id)
	if e != nil {
		return nil, e
	}
	out := []*types.Comment{}
	for _, r := range rows {
		if r.CreatedAt.After(after.CreatedAt) || r.CreatedAt.Equal(after.CreatedAt) && r.ID > after.ID {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(a, b int) bool {
		if out[a].CreatedAt.Equal(out[b].CreatedAt) {
			return out[a].ID < out[b].ID
		}
		return out[a].CreatedAt.Before(out[b].CreatedAt)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (s *Store) GetBlockingInfoForIssues(ctx context.Context, ids []string) (map[string][]string, map[string][]string, map[string]string, error) {
	st, _, e := s.load(ctx)
	if e != nil {
		return nil, nil, nil, e
	}
	blockers := map[string][]string{}
	blocked := map[string][]string{}
	parents := map[string]string{}
	for _, d := range st.Edges {
		if d.Type == types.DepParentChild && slices.Contains(ids, d.IssueID) {
			parents[d.IssueID] = d.DependsOnID
		}
		if d.Type != types.DepBlocks {
			continue
		}
		target := st.Issues[d.DependsOnID]
		if target == nil || !resolved(target.Issue.Status) {
			if slices.Contains(ids, d.IssueID) {
				blockers[d.IssueID] = append(blockers[d.IssueID], d.DependsOnID)
			}
			if slices.Contains(ids, d.DependsOnID) {
				blocked[d.DependsOnID] = append(blocked[d.DependsOnID], d.IssueID)
			}
		}
	}
	return blockers, blocked, parents, nil
}
func (s *Store) CountIssues(ctx context.Context, q string, f types.IssueFilter) (int64, error) {
	f.Limit = 0
	f.Offset = 0
	rows, e := s.SearchIssues(ctx, q, f)
	return int64(len(rows)), e
}
func (s *Store) CountIssuesByGroup(ctx context.Context, f types.IssueFilter, group string) (map[string]int, error) {
	f.Limit = 0
	f.Offset = 0
	rows, e := s.SearchIssues(ctx, "", f)
	if e != nil {
		return nil, e
	}
	out := map[string]int{}
	for _, i := range rows {
		key := ""
		switch group {
		case "status":
			key = string(i.Status)
		case "priority":
			key = fmt.Sprint(i.Priority)
		case "type":
			key = string(i.IssueType)
		case "assignee":
			key = i.Assignee
		case "label":
			for _, label := range i.Labels {
				out[label]++
			}
			continue
		default:
			return nil, storage.ErrValidation
		}
		out[key]++
	}
	return out, nil
}
func (s *Store) Counter() (issueops.Counter, error)             { return storecounter.New(s) }
func (s *Store) StatsReporter() (issueops.StatsReporter, error) { return storestats.New(s) }
func (s *Store) GetStatistics(ctx context.Context) (*types.Statistics, error) {
	st, _, e := s.load(ctx)
	if e != nil {
		return nil, e
	}
	out := &types.Statistics{TotalIssues: len(st.Issues)}
	blocked := 0
	for _, r := range st.Issues {
		switch r.Issue.Status {
		case types.StatusOpen:
			out.OpenIssues++
		case types.StatusClosed:
			out.ClosedIssues++
		case types.StatusInProgress:
			out.InProgressIssues++
		case types.StatusDeferred:
			out.DeferredIssues++
		}
		if r.Issue.IsBlocked {
			blocked++
		}
		if r.Issue.Pinned {
			out.PinnedIssues++
		}
	}
	ready, e := st.ready(types.WorkFilter{})
	if e != nil {
		return nil, e
	}
	count := len(ready)
	out.ReadyIssues = &count
	out.BlockedIssues = &blocked
	return out, nil
}
func (s *Store) GetStatisticsNoBlocked(ctx context.Context) (*types.Statistics, error) {
	out, e := s.GetStatistics(ctx)
	if e == nil {
		out.ReadyIssues = nil
		out.BlockedIssues = nil
	}
	return out, e
}
func (s *Store) AddDependencyWithOptions(ctx context.Context, d *types.Dependency, actor string, _ storage.DependencyAddOptions) error {
	return s.AddDependency(ctx, d, actor)
}
func (t *transaction) AddDependencyWithOptions(ctx context.Context, d *types.Dependency, actor string, _ storage.DependencyAddOptions) error {
	return t.s.AddDependency(ctx, d, actor)
}
func (t *transaction) CycleThroughEdges(context.Context, [][2]string) (string, error) { return "", nil } // Every inserted scheduling edge already passed the whole-graph check.

// CloseIssue routes legacy CLI callers through the same guarded lifecycle.
func (s *Store) CloseIssue(ctx context.Context, id, reason, actor, session string) error {
	_, err := (&lifecycle{s}).Close(ctx, issueops.CloseRequest{Actor: actor, IssueID: id, Reason: reason, Session: session})
	return err
}

// GetCommentsForIssues answers the bulk relation read used by stock JSONL
// export from one committed snapshot, including empty comment lists.
func (s *Store) GetCommentsForIssues(ctx context.Context, ids []string) (map[string][]*types.Comment, error) {
	st, _, err := s.load(ctx)
	if err != nil {
		return nil, err
	}
	result := make(map[string][]*types.Comment, len(ids))
	for _, id := range ids {
		if st.Issues[id] != nil {
			result[id] = append([]*types.Comment{}, st.hydrate(id).Comments...)
		}
	}
	return result, nil
}

// GetCommentCounts uses the same bulk comment snapshot as export.
func (s *Store) GetCommentCounts(ctx context.Context, ids []string) (map[string]int, error) {
	rows, err := s.GetCommentsForIssues(ctx, ids)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int, len(rows))
	for id, comments := range rows {
		counts[id] = len(comments)
	}
	return counts, nil
}

// AddComment routes legacy consumers through the guarded comment role.
func (s *Store) AddComment(ctx context.Context, id, actor, text string) error {
	_, err := (&comments{s}).AddComment(ctx, issueops.AddCommentRequest{IssueID: id, Author: actor, Text: text})
	return err
}

// AddIssueComment returns the detached comment created by the shared role.
func (s *Store) AddIssueComment(ctx context.Context, id, actor, text string) (*types.Comment, error) {
	result, err := (&comments{s}).AddComment(ctx, issueops.AddCommentRequest{IssueID: id, Author: actor, Text: text})
	return result.Comment, err
}

// GetDependencyCounts counts blocking edges in both logical storage tiers.
// Parent/grouping edges are returned separately by dependency record reads.
func (s *Store) GetDependencyCounts(ctx context.Context, ids []string) (map[string]*types.DependencyCounts, error) {
	st, _, err := s.load(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]*types.DependencyCounts, len(ids))
	for _, id := range ids {
		out[id] = &types.DependencyCounts{}
	}
	for _, edge := range st.Edges {
		if edge.Type != types.DepBlocks {
			continue
		}
		if counts := out[edge.IssueID]; counts != nil {
			counts.DependencyCount++
		}
		if counts := out[edge.DependsOnID]; counts != nil {
			counts.DependentCount++
		}
	}
	return out, nil
}

// PartitionWispIDs reports the native logical plane used by export/import.
func (s *Store) PartitionWispIDs(ctx context.Context, ids []string) (wisps, durable []string, err error) {
	st, _, err := s.load(ctx)
	if err != nil {
		return nil, nil, err
	}
	wisps, durable = []string{}, []string{}
	for _, id := range ids {
		if record := st.Issues[id]; record != nil {
			if storageops.IsWisp(&record.Issue) {
				wisps = append(wisps, id)
			} else {
				durable = append(durable, id)
			}
		}
	}
	return wisps, durable, nil
}

// ArchiveScope labels exported project/branch snapshots without SQL or
// exposing endpoint addresses, database paths or credentials.
func (s *Store) ArchiveScope(ctx context.Context) (string, error) {
	st, _, err := s.load(ctx)
	if err != nil {
		return "", err
	}
	project := st.Config["project_id"]
	if project == "" {
		return "", fmt.Errorf("antfly: project identity required for archive scope")
	}
	return "antfly_" + digest([]byte(project + "\x00" + st.Config["antfly.branch.id"]))[:24], nil
}
