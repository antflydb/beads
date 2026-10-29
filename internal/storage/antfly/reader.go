package antfly

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/domain"
	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/internal/workapi"
	"github.com/steveyegge/beads/issueops"
)

type reader struct{ s *Store }

// Reject unimplemented filters instead of returning a plausible wrong set.
func rejectFields(value any, allowed string) error {
	v := reflect.ValueOf(value)
	t := v.Type()
	set := strings.Fields(allowed)
	for n := 0; n < v.NumField(); n++ {
		if !v.Field(n).IsZero() && !slices.Contains(set, t.Field(n).Name) {
			return errUnsupported("filter." + t.Field(n).Name)
		}
	}
	return nil
}

func (st *state) search(query string, f types.IssueFilter) ([]*types.IssueWithCounts, error) {
	if e := rejectFields(f, "Status Statuses Priority IssueType Assignee Labels LabelsAny ExcludeLabels TitleSearch IDs IDPrefix Limit TitleContains DescriptionContains NotesContains NoAssignee NoLabels ExcludeStatus ExcludeTypes Ephemeral Pinned IsTemplate IsBlocked ParentID NoParent IncludeDependencies SkipWisps NoIDShrink SortBy SortDesc Offset MaxRows MaxRowsSource MetadataFields HasMetadataKey"); e != nil {
		return nil, e
	}
	out := []*types.IssueWithCounts{}
	for id, r := range st.Issues {
		i := &r.Issue
		if f.Status != nil && i.Status != *f.Status || len(f.Statuses) > 0 && !slices.Contains(f.Statuses, i.Status) || slices.Contains(f.ExcludeStatus, i.Status) {
			continue
		}
		if f.Priority != nil && i.Priority != *f.Priority || f.IssueType != nil && string(i.IssueType) != string(*f.IssueType) || slices.Contains(f.ExcludeTypes, i.IssueType) {
			continue
		}
		if f.Assignee != nil && i.Assignee != *f.Assignee || f.NoAssignee && i.Assignee != "" || f.NoLabels && len(i.Labels) > 0 {
			continue
		}
		if len(f.IDs) > 0 && !slices.Contains(f.IDs, id) || !strings.HasPrefix(id, f.IDPrefix) {
			continue
		}
		if (f.Ephemeral != nil && *f.Ephemeral != storageops.IsWisp(i)) || (f.SkipWisps && storageops.IsWisp(i)) || f.IsTemplate != nil && *f.IsTemplate || f.Pinned != nil && *f.Pinned != i.Pinned || f.IsBlocked != nil && *f.IsBlocked != i.IsBlocked {
			continue
		}
		matches := true
		for _, label := range f.Labels {
			if !slices.Contains(i.Labels, label) {
				matches = false
			}
		}
		for _, label := range f.ExcludeLabels {
			if slices.Contains(i.Labels, label) {
				matches = false
			}
		}
		if len(f.LabelsAny) > 0 {
			found := false
			for _, label := range f.LabelsAny {
				if slices.Contains(i.Labels, label) {
					found = true
				}
			}
			matches = matches && found
		}
		if !matches {
			continue
		}
		contains := func(text, q string) bool { return strings.Contains(strings.ToLower(text), strings.ToLower(q)) }
		if !contains(i.Title, f.TitleSearch) || !contains(i.Title, f.TitleContains) || !contains(i.Description, f.DescriptionContains) || !contains(i.Notes, f.NotesContains) {
			continue
		}
		if query != "" && id != query && !contains(i.Title+" "+i.Description+" "+i.Notes, query) {
			continue
		}
		var metadata map[string]any
		_ = json.Unmarshal(i.Metadata, &metadata)
		for key, value := range f.MetadataFields {
			if fmt.Sprint(metadata[key]) != value {
				matches = false
			}
		}
		if f.HasMetadataKey != "" {
			if _, exists := metadata[f.HasMetadataKey]; !exists {
				matches = false
			}
		}
		if !matches {
			continue
		}
		row := st.counts(id)
		if f.ParentID != nil && (row.Parent == nil || *row.Parent != *f.ParentID) || f.NoParent && row.Parent != nil {
			continue
		}
		out = append(out, row)
	}
	if !slices.Contains([]string{"", "priority", "created", "updated", "closed", "status", "id", "title", "type", "assignee"}, f.SortBy) {
		return nil, errUnsupported("sort." + f.SortBy)
	}
	sort.Slice(out, func(a, b int) bool {
		i, j := out[a].Issue, out[b].Issue
		order := workapi.CompareIssuesBy(i, j, f.SortBy)
		if f.SortBy == "" {
			order = cmp.Compare(i.Priority, j.Priority)
			if order == 0 {
				order = -i.CreatedAt.Compare(j.CreatedAt)
			}
		}
		if f.SortDesc {
			order = -order
		}
		if order == 0 {
			order = strings.Compare(i.ID, j.ID)
		}
		return order < 0
	})
	if f.MaxRows > 0 && len(out) > f.MaxRows && (f.Limit == 0 || f.Limit > f.MaxRows) {
		return nil, fmt.Errorf("%w: result exceeds max rows %d", storage.ErrValidation, f.MaxRows)
	}
	if f.Offset > 0 {
		if f.Offset >= len(out) {
			out = out[:0]
		} else {
			out = out[f.Offset:]
		}
	}
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}

func (st *state) ready(f types.WorkFilter) ([]*types.IssueWithCounts, error) {
	if e := rejectFields(f, "Status Statuses Type Priority Assignee Unassigned Labels LabelsAny ExcludeLabels Limit SortPolicy IncludeDeferred IncludeEphemeral ExcludeTypes ExcludeIDs MetadataFields HasMetadataKey Offset MaxRows MaxRowsSource"); e != nil {
		return nil, e
	}
	status := f.Status
	if status == "" {
		status = types.StatusOpen
	}
	it := types.IssueType(f.Type)
	filter := types.IssueFilter{Status: &status, Statuses: f.Statuses, Priority: f.Priority, Assignee: f.Assignee, NoAssignee: f.Unassigned, Labels: f.Labels, LabelsAny: f.LabelsAny, ExcludeLabels: f.ExcludeLabels, ExcludeTypes: f.ExcludeTypes, MetadataFields: f.MetadataFields, HasMetadataKey: f.HasMetadataKey}
	if f.Type != "" {
		filter.IssueType = &it
		filter.ExcludeTypes = nil
	}
	rows, e := st.search("", filter)
	if e != nil {
		return nil, e
	}
	out := rows[:0]
	for _, r := range rows {
		if (!f.IncludeEphemeral && storageops.IsWisp(r.Issue)) || r.Pinned || r.IsBlocked || slices.Contains(f.ExcludeIDs, r.ID) {
			continue
		}
		if !f.IncludeDeferred && st.deferred(r.ID, time.Now()) {
			continue
		}
		out = append(out, r)
	}
	if string(f.SortPolicy) == "oldest" {
		sort.Slice(out, func(a, b int) bool {
			if out[a].CreatedAt.Equal(out[b].CreatedAt) {
				return out[a].ID < out[b].ID
			}
			return out[a].CreatedAt.Before(out[b].CreatedAt)
		})
	} else if f.SortPolicy != "" && string(f.SortPolicy) != "priority" && string(f.SortPolicy) != "hybrid" {
		return nil, storage.ErrValidation
	}
	// Hybrid follows the shared ready policy: recent work first, then priority.
	if string(f.SortPolicy) == "hybrid" || f.SortPolicy == "" {
		cutoff := time.Now().Add(-48 * time.Hour)
		sort.SliceStable(out, func(a, b int) bool { return out[a].CreatedAt.After(cutoff) && !out[b].CreatedAt.After(cutoff) })
	}
	if f.MaxRows > 0 && len(out) > f.MaxRows && (f.Limit == 0 || f.Limit > f.MaxRows) {
		return nil, fmt.Errorf("%w: result exceeds max rows", storage.ErrValidation)
	}
	if f.Offset > 0 {
		if f.Offset >= len(out) {
			out = out[:0]
		} else {
			out = out[f.Offset:]
		}
	}
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}

func (r *reader) List(ctx context.Context, req issueops.ListRequest) (issueops.IssuePage, error) {
	f, e := workapi.BuildListFilter(req, workapi.ListConfig{})
	if e != nil {
		return issueops.IssuePage{}, e
	}
	f.Limit = 0
	f.Offset = 0
	st, _, e := r.s.load(ctx)
	if e != nil {
		return issueops.IssuePage{}, e
	}
	var rows []*types.IssueWithCounts
	if req.ReadyFlag {
		rows, e = st.ready(workapi.ReadyFilterFromIssueFilter(f))
	} else {
		rows, e = st.search(req.Query, f)
	}
	if e != nil {
		return issueops.IssuePage{}, e
	}
	rows, more := workapi.FinishPageAt(rows, req.SortBy, req.Reverse, req.Offset, workapi.PageLimit(req), false)
	for _, row := range rows {
		if !req.IncludeComments {
			if row.CommentCount > 0 {
				yes := true
				row.CommentsOmitted = &yes
			}
			row.Comments = nil
		}
	}
	return issueops.IssuePage{Items: rows, HasMore: more}, nil
}
func (r *reader) Ready(ctx context.Context, req issueops.ReadyRequest) (issueops.IssuePage, error) {
	f, e := workapi.BuildReadyFilter(req)
	if e != nil {
		return issueops.IssuePage{}, e
	}
	limit := f.Limit
	f.Limit = 0
	st, _, e := r.s.load(ctx)
	if e != nil {
		return issueops.IssuePage{}, e
	}
	rows, e := st.ready(f)
	if e != nil {
		return issueops.IssuePage{}, e
	}
	rows, more := workapi.FinishPageAt(rows, "", false, req.Offset, limit, false)
	return issueops.IssuePage{Items: rows, HasMore: more}, nil
}
func (r *reader) Get(ctx context.Context, req issueops.GetRequest) (*issueops.IssueDetails, error) {
	st, _, e := r.s.load(ctx)
	if e != nil {
		return nil, e
	}
	if _, e = st.issue(req.ID); e != nil {
		return nil, e
	}
	row := st.counts(req.ID)
	deps, e := (&graph{New(&stateTransport{st: st}, true)}).Related(ctx, issueops.RelatedRequest{ID: req.ID, Direction: issueops.RelationOut})
	if e != nil {
		return nil, e
	}
	dc, rc, cc := int64(row.DependencyCount), int64(row.DependentCount), int64(row.CommentCount)
	out := &types.IssueDetails{Issue: *row.Issue, Labels: row.Labels, Dependencies: deps, DependencyCount: &dc, DependentCount: &rc, CommentCount: &cc, Parent: row.Parent}
	if req.IncludeComments {
		out.Comments = row.Comments
	} else if cc > 0 {
		yes := true
		out.CommentsOmitted = &yes
	}
	out.Issue.Comments = nil
	if req.IncludeDependents {
		out.Dependents, e = (&graph{New(&stateTransport{st: st}, true)}).Related(ctx, issueops.RelatedRequest{ID: req.ID, Direction: issueops.RelationIn})
	}
	return out, e
}

// stateTransport lets composed read views use one captured workspace snapshot.
type stateTransport struct{ st *state }

func (t *stateTransport) Load(context.Context) ([]byte, uint64, error) {
	b, e := json.Marshal(t.st)
	return b, 1, e
}
func (t *stateTransport) CompareAndSwap(context.Context, uint64, []byte) error {
	return errUnsupported("snapshot write")
}
func (t *stateTransport) Memory(context.Context, string, string, string, json.RawMessage) (json.RawMessage, error) {
	return nil, errUnsupported("snapshot memory")
}
func (t *stateTransport) Close() error { return nil }

func (s *Store) SearchIssues(ctx context.Context, q string, f types.IssueFilter) ([]*types.Issue, error) {
	rows, e := s.SearchIssuesWithCounts(ctx, q, f)
	out := []*types.Issue{}
	for _, r := range rows {
		out = append(out, r.Issue)
	}
	return out, e
}
func (s *Store) SearchIssuesWithCounts(ctx context.Context, q string, f types.IssueFilter) ([]*types.IssueWithCounts, error) {
	st, _, e := s.load(ctx)
	if e != nil {
		return nil, e
	}
	return st.search(q, f)
}
func (s *Store) GetReadyWork(ctx context.Context, f types.WorkFilter) ([]*types.Issue, error) {
	rows, e := s.GetReadyWorkWithCounts(ctx, f)
	out := []*types.Issue{}
	for _, r := range rows {
		out = append(out, r.Issue)
	}
	return out, e
}
func (s *Store) GetReadyWorkWithCounts(ctx context.Context, f types.WorkFilter) ([]*types.IssueWithCounts, error) {
	st, _, e := s.load(ctx)
	if e != nil {
		return nil, e
	}
	return st.ready(f)
}
func (s *Store) GetReadyWorkWithCountsAndTotal(ctx context.Context, f types.WorkFilter) ([]*types.IssueWithCounts, int, error) {
	st, _, e := s.load(ctx)
	if e != nil {
		return nil, 0, e
	}
	limit := f.Limit
	f.Limit = 0
	rows, e := st.ready(f)
	total := len(rows)
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, total, e
}
func (s *Store) GetCustomStatuses(context.Context) ([]string, error) { return nil, nil }
func (s *Store) GetCustomStatusesDetailed(context.Context) ([]types.CustomStatus, error) {
	return nil, nil
}
func (s *Store) GetCustomTypes(context.Context) ([]string, error) { return nil, nil }
func (s *Store) GetInfraTypes(context.Context) map[string]bool    { return map[string]bool{} }
func (s *Store) IsInfraTypeCtx(_ context.Context, t types.IssueType) bool {
	return domain.IsInfraType(t)
}

type readyRole struct{ s *Store }

func (s *Store) ReadyClaimer() (issueops.ReadyClaimer, error) { return &readyRole{s}, nil }
func (s *Store) ReadyCounter() (issueops.ReadyCounter, error) { return &readyRole{s}, nil }
func (r *readyRole) CountReady(ctx context.Context, req issueops.ReadyRequest) (issueops.ReadyCountResult, error) {
	f, e := workapi.BuildReadyCountFilter(req)
	if e != nil {
		return issueops.ReadyCountResult{}, e
	}
	rows, e := r.s.GetReadyWorkWithCounts(ctx, f)
	return issueops.ReadyCountResult{Total: int64(len(rows))}, e
}
func (r *readyRole) ClaimNext(ctx context.Context, req issueops.ClaimNextRequest) (out issueops.ClaimNextResult, err error) {
	if err = storageops.ValidateClaimNextRequest(req); err != nil {
		return out, err
	}
	f, err := workapi.BuildReadyFilter(req.Filter)
	if err != nil {
		return out, err
	}
	f.Limit = 0
	out.Claimed, err = r.s.claimReady(ctx, f, req.Actor)
	return out, err
}

func (s *Store) claimReady(ctx context.Context, f types.WorkFilter, actor string) (out *types.IssueWithCounts, err error) {
	if actor == "" {
		return nil, storage.ErrValidation
	}
	f.Limit = 0
	err = s.mutate(ctx, func(st *state) error {
		out = nil
		rows, e := st.ready(f)
		if e != nil {
			return e
		}
		for _, row := range rows {
			if row.Assignee != "" && row.Assignee != actor {
				continue
			}
			i, _ := st.issue(row.ID)
			if _, e = st.claim(i, actor); e != nil {
				return e
			}
			st.audit("claim", i.ID, actor)
			out = st.counts(i.ID)
			break
		}
		return nil
	})
	return out, err
}

func (s *Store) ClaimReadyIssue(ctx context.Context, f types.WorkFilter, actor string) (*types.Issue, error) {
	r, e := s.claimReady(ctx, f, actor)
	if r == nil {
		return nil, e
	}
	return r.Issue, e
}
func (s *Store) ClaimIssue(ctx context.Context, id, actor string) error {
	_, e := (&lifecycle{s}).Claim(ctx, issueops.ClaimRequest{IssueID: id, Actor: actor})
	return e
}

// Deferral of a parent withholds its descendants just like parent blockers.
func (st *state) deferred(id string, now time.Time) bool {
	seen := map[string]bool{}
	for id != "" && !seen[id] {
		seen[id] = true
		if r := st.Issues[id]; r != nil && r.Issue.DeferUntil != nil && r.Issue.DeferUntil.After(now) {
			return true
		}
		parent := ""
		for _, d := range st.Edges {
			if d.IssueID == id && d.Type == types.DepParentChild {
				parent = d.DependsOnID
				break
			}
		}
		id = parent
	}
	return false
}
