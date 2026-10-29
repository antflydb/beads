package antfly

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/internal/workapi"
	"github.com/steveyegge/beads/issueops"
)

type leases struct{ s *Store }

func (s *Store) Heartbeater() (issueops.Heartbeater, error)       { return &leases{s}, nil }
func (s *Store) LeaseReclaimer() (issueops.LeaseReclaimer, error) { return &leases{s}, nil }
func (s *Store) Releaser() (issueops.Releaser, error)             { return &leases{s}, nil }
func mainClaims(st *state) error {
	if st.Config["antfly.branch.parent"] != "" {
		return fmt.Errorf("%w: live claims are coordinated in the parent workspace", storage.ErrValidation)
	}
	return nil
}
func (l *leases) Heartbeat(ctx context.Context, req issueops.HeartbeatRequest) (out issueops.HeartbeatResult, err error) {
	if err = storageops.ValidateHeartbeatRequest(req); err != nil {
		return out, err
	}
	err = l.s.mutate(ctx, func(st *state) error {
		if e := mainClaims(st); e != nil {
			return e
		}
		i, e := st.issue(req.IssueID)
		if e != nil {
			return e
		}
		if storageops.IsWisp(i) {
			return &issueops.LeaseConflictError{IssueID: i.ID, Status: i.Status, Ephemeral: true, Err: issueops.ErrLeaseNotHeld}
		}
		if i.Assignee != "" && !storageops.ActorMatches(i.Assignee, req.Actor) {
			return &issueops.LeaseConflictError{IssueID: i.ID, Holder: i.Assignee, Status: i.Status, Err: storage.ErrAlreadyClaimed}
		}
		if i.Status != types.StatusInProgress || i.Assignee == "" {
			return &issueops.LeaseConflictError{IssueID: i.ID, Status: i.Status, Err: issueops.ErrLeaseNotHeld}
		}
		rearmed := i.LeaseExpiresAt == nil
		now := time.Now().UTC().Truncate(time.Second)
		expiry := now.Add(time.Hour)
		i.HeartbeatAt = &now
		i.LeaseExpiresAt = &expiry
		if rearmed {
			st.touch(i)
			st.audit("lease_rearmed", i.ID, req.Actor)
		}
		out = issueops.HeartbeatResult{IssueID: i.ID, Holder: req.Actor, HeartbeatAt: now, LeaseExpiresAt: expiry, Rearmed: rearmed}
		return nil
	})
	return out, err
}
func (l *leases) Release(ctx context.Context, req issueops.ReleaseRequest) (out issueops.ReleaseResult, err error) {
	if err = workapi.ValidateReleaseRequest(req); err != nil {
		return out, err
	}
	err = l.s.mutate(ctx, func(st *state) error {
		if e := mainClaims(st); e != nil {
			return e
		}
		i, e := st.issue(req.IssueID)
		if e != nil {
			return e
		}
		if i.Status != types.StatusOpen && i.Status != types.StatusInProgress {
			return issueops.ErrNotReleasable
		}
		if req.ExpectedAssignee != nil && !storageops.ActorMatches(i.Assignee, *req.ExpectedAssignee) {
			return storage.ErrAssigneeMismatch
		}
		if i.Assignee == "" {
			return issueops.ErrNotClaimed
		}
		if req.ExpectedAssignee == nil && !req.Force && !storageops.ActorMatches(i.Assignee, req.Actor) {
			return issueops.ErrNotOwner
		}
		i.Assignee = ""
		i.Status = types.StatusOpen
		i.StartedAt = nil
		i.HeartbeatAt = nil
		i.LeaseExpiresAt = nil
		st.touch(i)
		st.audit("unclaimed", i.ID, req.Actor)
		st.reblock()
		issue := st.hydrate(i.ID)
		issue.Comments = nil
		issue.Dependencies = nil
		out = issueops.ReleaseResult{Issue: issue, Changed: true}
		return nil
	})
	return out, err
}
func (l *leases) Reclaim(ctx context.Context, req issueops.ReclaimRequest) (out issueops.ReclaimResult, err error) {
	if err = storageops.ValidateReclaimRequest(req); err != nil {
		return out, err
	}
	cutoff := time.Now().UTC().Add(-req.OlderThan)
	err = l.s.mutate(ctx, func(st *state) error {
		if e := mainClaims(st); e != nil {
			return e
		}
		out.Reclaimed = []issueops.ReclaimedLease{}
		ids := make([]string, 0, len(st.Issues))
		for id := range st.Issues {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			i := &st.Issues[id].Issue
			f := req.Filter
			if storageops.IsWisp(i) || i.Status != types.StatusInProgress || i.Assignee == "" || i.LeaseExpiresAt == nil || !i.LeaseExpiresAt.Before(cutoff) {
				continue
			}
			if len(f.IDs) > 0 && !slices.Contains(f.IDs, id) || len(f.Assignees) > 0 && !slices.Contains(f.Assignees, i.Assignee) {
				continue
			}
			matches := true
			for _, v := range f.Labels {
				matches = matches && slices.Contains(i.Labels, v)
			}
			for _, v := range f.ExcludeLabels {
				matches = matches && !slices.Contains(i.Labels, v)
			}
			if len(f.LabelsAny) > 0 {
				any := false
				for _, v := range f.LabelsAny {
					any = any || slices.Contains(i.Labels, v)
				}
				matches = matches && any
			}
			if !matches {
				continue
			}
			owner := i.Assignee
			i.Assignee = ""
			i.Status = types.StatusOpen
			i.StartedAt = nil
			i.HeartbeatAt = nil
			i.LeaseExpiresAt = nil
			st.touch(i)
			st.audit("lease_reclaimed", id, strings.TrimSpace(req.Actor))
			out.Reclaimed = append(out.Reclaimed, types.ReclaimedLease{ID: id, PreviousOwner: owner, Revision: types.RevisionToken(i.RowVersion)})
		}
		st.reblock()
		return nil
	})
	return out, err
}
func (s *Store) HeartbeatIssue(ctx context.Context, id, actor string) error {
	_, e := (&leases{s}).Heartbeat(ctx, issueops.HeartbeatRequest{IssueID: id, Actor: actor})
	return e
}
func (s *Store) UnclaimIssue(ctx context.Context, id, actor string, force bool) error {
	_, e := (&leases{s}).Release(ctx, issueops.ReleaseRequest{IssueID: id, Actor: actor, Force: force})
	return e
}
func (s *Store) UnclaimIssueIfAssignee(ctx context.Context, id, actor, expected string) error {
	_, e := (&leases{s}).Release(ctx, issueops.ReleaseRequest{IssueID: id, Actor: actor, ExpectedAssignee: &expected})
	return e
}
func (s *Store) ReclaimExpiredLeases(ctx context.Context, grace time.Duration, f types.ReclaimFilter, actor string) ([]types.ReclaimedLease, error) {
	out, e := (&leases{s}).Reclaim(ctx, issueops.ReclaimRequest{Actor: actor, OlderThan: grace, Filter: f})
	return out.Reclaimed, e
}
