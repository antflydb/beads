package antfly

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/domain"
	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/issueops"
)

type lifecycle struct{ s *Store }

func (l *lifecycle) Create(ctx context.Context, req issueops.CreateRequest) (out issueops.CreateResult, err error) {
	req = storageops.CloneCreateRequest(req)
	err = l.s.mutate(ctx, func(st *state) error {
		attempt, e := storageops.PreparePublicCreateRequest(req, storageops.PublicCreateContext{IssuePrefix: st.Config["issue_prefix"]})
		if e != nil {
			return e
		}
		i := attempt.Issue
		if domain.IsInfraType(i.IssueType) || i.IssueType == types.TypeGate || i.IssueType == types.TypeMolecule || i.Ephemeral || i.NoHistory || i.IsTemplate || i.WispType != "" || i.AwaitType != "" || i.MolType != "" || attempt.WaitsFor != nil {
			return errUnsupported("Lifecycle.Create: ephemeral, template, gate, molecule, or waits-for")
		}
		if i.ID == "" {
			var id [8]byte
			if _, e = rand.Read(id[:]); e != nil {
				return e
			}
			i.ID = strings.TrimSuffix(st.Config["issue_prefix"], "-") + "-" + hex.EncodeToString(id[:])
		}
		if st.Issues[i.ID] != nil {
			return storage.ErrAlreadyExists
		}
		st.Issues[i.ID] = &record{Issue: *i}
		if attempt.ParentID != "" {
			if e = st.addEdge(i.ID, attempt.ParentID, types.DepParentChild, req.Actor); e != nil {
				return e
			}
			if attempt.InheritLabelsFromParent {
				parent, _ := st.issue(attempt.ParentID)
				i.Labels = append(i.Labels, parent.Labels...)
				st.Issues[i.ID].Issue.Labels = i.Labels
			}
		}
		for _, d := range attempt.Dependencies {
			from, to := i.ID, d.TargetID
			if d.Reverse {
				from, to = to, from
			}
			if d.Metadata != "" || d.ThreadID != "" {
				return errUnsupported("Lifecycle.Create: dependency metadata")
			}
			if e = st.addEdge(from, to, d.Type, req.Actor); e != nil {
				return e
			}
		}
		stored := &st.Issues[i.ID].Issue
		st.touch(stored)
		st.reblock()
		st.audit("create", i.ID, req.Actor)
		out = issueops.CreateResult{Issue: st.hydrate(i.ID)}
		return nil
	})
	return out, err
}

func (l *lifecycle) Update(ctx context.Context, req issueops.UpdateRequest) (out issueops.UpdateResult, err error) {
	req = storageops.CloneUpdateRequest(req)
	if err = storageops.ValidateUpdateRequest(req); err != nil {
		return out, err
	}
	if err = storageops.ValidateMetadataPatch(req.Patch.Metadata); err != nil {
		return out, err
	}
	if req.Actor == "" || req.IssueID == "" {
		return out, storage.ErrValidation
	}
	if req.Patch.Persistence.Set || req.Patch.AwaitID.Set || req.Patch.Waiters.Set {
		return out, errUnsupported("Lifecycle.Update: persistence or gate")
	}
	err = l.s.mutate(ctx, func(st *state) error {
		i, e := st.issue(req.IssueID)
		if e != nil {
			return e
		}
		if e = checkVersion(i, req.ExpectedVersion); e != nil {
			return e
		}
		if req.ExpectedAssignee != nil && *req.ExpectedAssignee != i.Assignee {
			return storage.ErrAssigneeMismatch
		}
		if req.ExpectedStatus != nil && *req.ExpectedStatus != i.Status {
			return storage.ErrStatusMismatch
		}
		if e = storageops.AuthorizeAssigneeTransferWithPools(i, req, nil); e != nil {
			return e
		}
		if req.Patch.Notes.Set && !req.ForceNotesOverwrite && issueops.NotesReplacement(i.Notes, req.Patch.Notes.Value) {
			return issueops.ErrNotesOverwrite
		}
		before := st.hydrate(i.ID)
		if req.Claim {
			if _, e = st.claim(i, req.Actor); e != nil {
				return e
			}
		}
		fields := storageops.UpdateFields(req.Patch)
		delete(fields, storageops.OpAppendNotes)
		b, e := json.Marshal(fields)
		if e != nil {
			return e
		}
		if e = json.Unmarshal(b, i); e != nil {
			return e
		}
		if req.Patch.AppendNotes.Set {
			if i.Notes != "" {
				i.Notes += "\n"
			}
			i.Notes += req.Patch.AppendNotes.Value
		}
		if req.Patch.Status.Set && i.Status == types.StatusClosed && before.Status != types.StatusClosed {
			if e = st.canClose(before, req.Actor, req.ForceClosePolicy); e != nil {
				return e
			}
			now := time.Now().UTC()
			i.ClosedAt = &now
			i.LeaseExpiresAt = nil
			i.HeartbeatAt = nil
		}
		if req.Patch.Status.Set && i.Status != types.StatusClosed {
			i.ClosedAt = nil
			i.CloseReason = ""
			i.ClosedBySession = ""
		}
		if domain.IsInfraType(i.IssueType) || i.IssueType == types.TypeGate || i.IssueType == types.TypeMolecule {
			return errUnsupported("Lifecycle.Update: infrastructure, gate, or molecule type")
		}
		if e = i.Validate(); e != nil {
			return fmt.Errorf("%w: %v", storage.ErrValidation, e)
		}
		labels := i.Labels
		if req.Patch.Labels.Replace.Set {
			labels = append([]string{}, req.Patch.Labels.Replace.Value...)
		}
		for _, label := range req.Patch.Labels.Add {
			if label != "" && !slices.Contains(labels, label) {
				labels = append(labels, label)
			}
		}
		labels = slices.DeleteFunc(labels, func(label string) bool { return label == "" || slices.Contains(req.Patch.Labels.Remove, label) })
		for _, label := range labels {
			if e = types.CheckFieldLen("label", label); e != nil {
				return e
			}
		}
		slices.Sort(labels)
		i.Labels = slices.Compact(labels)
		var metadata json.RawMessage
		if metadata, _, e = storageops.ApplyMetadataPatch(i.Metadata, req.Patch.Metadata); e != nil {
			return e
		}
		i.Metadata = metadata
		if req.Patch.ParentID.Set {
			for k, d := range st.Edges {
				if d.IssueID == i.ID && d.Type == types.DepParentChild {
					delete(st.Edges, k)
				}
			}
			if req.Patch.ParentID.Value != "" {
				if e = st.addEdge(i.ID, req.Patch.ParentID.Value, types.DepParentChild, req.Actor); e != nil {
					return e
				}
			}
		}
		after := st.hydrate(i.ID)
		out.Changed = !reflect.DeepEqual(before, after)
		if out.Changed {
			st.touch(i)
			st.reblock()
			st.audit("update", i.ID, req.Actor)
		}
		out.Issue = st.hydrate(i.ID)
		return nil
	})
	return out, err
}

func checkVersion(i *types.Issue, v *int64) error {
	if v != nil && *v != i.RowVersion {
		return storage.ErrVersionMismatch
	}
	return nil
}

func (st *state) canClose(i *types.Issue, actor string, force bool) error {
	if force {
		return nil
	}
	if i.Status == types.StatusPinned || i.Pinned {
		return fmt.Errorf("%w: pinned issue", storage.ErrValidation)
	}
	if i.Assignee != "" && i.Assignee != actor {
		return storage.ErrAssigneeMismatch
	}
	children := 0
	for _, d := range st.Edges {
		if d.IssueID == i.ID && d.Type == types.DepBlocks {
			target := st.Issues[d.DependsOnID]
			if target == nil || !resolved(target.Issue.Status) {
				return storage.ErrCloseBlocked
			}
		}
		if d.DependsOnID == i.ID && d.Type == types.DepParentChild {
			if c := st.Issues[d.IssueID]; c != nil && !resolved(c.Issue.Status) {
				children++
			}
		}
	}
	if children > 0 {
		return fmt.Errorf("%w: %d open children", storage.ErrCloseOpenChildren, children)
	}
	return nil
}

func (l *lifecycle) Close(ctx context.Context, req issueops.CloseRequest) (out issueops.CloseResult, err error) {
	req = storageops.CloneCloseRequest(req)
	if req.Actor == "" || req.IssueID == "" {
		return out, storage.ErrValidation
	}
	if req.AutoCloseMolecule {
		return out, errUnsupported("Lifecycle.Close: molecule autoclose")
	}
	err = l.s.mutate(ctx, func(st *state) error {
		i, e := st.issue(req.IssueID)
		if e != nil {
			return e
		}
		if e = checkVersion(i, req.ExpectedVersion); e != nil {
			return e
		}
		out = issueops.CloseResult{}
		for _, d := range st.Edges {
			if d.DependsOnID == i.ID && d.Type == types.DepParentChild {
				if c := st.Issues[d.IssueID]; c != nil && !resolved(c.Issue.Status) {
					out.OpenChildren++
				}
			}
		}
		if i.Status != types.StatusClosed {
			if e = st.canClose(i, req.Actor, req.Force); e != nil {
				return e
			}
			now := time.Now().UTC()
			i.Status = types.StatusClosed
			i.ClosedAt = &now
			i.CloseReason = req.Reason
			i.ClosedBySession = req.Session
			i.LeaseExpiresAt = nil
			i.HeartbeatAt = nil
			st.touch(i)
			st.reblock()
			st.audit("close", i.ID, req.Actor)
			out.Changed = true
		}
		out.Issue = st.hydrate(i.ID)
		return nil
	})
	return out, err
}
func (l *lifecycle) Reopen(ctx context.Context, req issueops.ReopenRequest) (out issueops.ReopenResult, err error) {
	req = storageops.CloneReopenRequest(req)
	if req.Actor == "" || req.IssueID == "" {
		return out, storage.ErrValidation
	}
	err = l.s.mutate(ctx, func(st *state) error {
		i, e := st.issue(req.IssueID)
		if e != nil {
			return e
		}
		if e = checkVersion(i, req.ExpectedVersion); e != nil {
			return e
		}
		out = issueops.ReopenResult{}
		if i.Status == types.StatusClosed {
			i.Status = types.StatusOpen
			i.ClosedAt = nil
			i.CloseReason = ""
			i.ClosedBySession = ""
			st.touch(i)
			st.reblock()
			st.audit("reopen", i.ID, req.Actor)
			out.Changed = true
		}
		out.Issue = st.hydrate(i.ID)
		return nil
	})
	return out, err
}

func (st *state) claim(i *types.Issue, actor string) (bool, error) {
	if i.Assignee != "" && i.Assignee != actor {
		return false, &issueops.ClaimConflictError{IssueID: i.ID, Assignee: i.Assignee, Status: i.Status, Err: fmt.Errorf("%w by %s", storage.ErrAlreadyClaimed, i.Assignee)}
	}
	if i.Status == types.StatusInProgress && i.Assignee == actor {
		return false, nil
	}
	if i.Status != types.StatusOpen {
		return false, &issueops.ClaimConflictError{IssueID: i.ID, Assignee: i.Assignee, Status: i.Status, Err: storage.ErrNotClaimable}
	}
	now := time.Now().UTC()
	expiry := now.Add(time.Hour)
	i.Status = types.StatusInProgress
	i.Assignee = actor
	i.StartedAt = &now
	i.HeartbeatAt = &now
	i.LeaseExpiresAt = &expiry
	st.touch(i)
	return true, nil
}
func (l *lifecycle) Claim(ctx context.Context, req issueops.ClaimRequest) (out issueops.ClaimResult, err error) {
	if req.Actor == "" || req.IssueID == "" {
		return out, storage.ErrValidation
	}
	err = l.s.mutate(ctx, func(st *state) error {
		i, e := st.issue(req.IssueID)
		if e != nil {
			return e
		}
		changed, e := st.claim(i, req.Actor)
		if e != nil {
			return e
		}
		if changed {
			st.audit("claim", i.ID, req.Actor)
		}
		out = issueops.ClaimResult{Issue: st.hydrate(i.ID), Changed: changed}
		return nil
	})
	return out, err
}

func (s *Store) CreateIssue(ctx context.Context, i *types.Issue, actor string) error {
	r, e := (&lifecycle{s}).Create(ctx, issueops.CreateRequest{Actor: actor, Issue: i})
	if e == nil {
		*i = *r.Issue
	}
	return e
}
func (s *Store) GetIssue(ctx context.Context, id string) (*types.Issue, error) {
	st, _, e := s.load(ctx)
	if e != nil {
		return nil, e
	}
	if _, e = st.issue(id); e != nil {
		return nil, e
	}
	return st.hydrate(id), nil
}
func (s *Store) GetWisp(context.Context, string) (*types.Issue, error) {
	return nil, storage.ErrNotFound
}
