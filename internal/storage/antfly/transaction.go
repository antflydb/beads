package antfly

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/steveyegge/beads/internal/storage"
	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/issueops"
)

type stagedTransport struct {
	data    []byte
	version uint64
}

func (t *stagedTransport) Load(context.Context) ([]byte, uint64, error) {
	return append([]byte(nil), t.data...), t.version, nil
}
func (t *stagedTransport) CompareAndSwap(_ context.Context, v uint64, b []byte) error {
	if v != t.version {
		return errConflict
	}
	t.data = append([]byte(nil), b...)
	t.version++
	return nil
}
func (t *stagedTransport) Memory(context.Context, string, string, string, json.RawMessage) (json.RawMessage, error) {
	return nil, errUnsupported("transaction memory")
}
func (t *stagedTransport) Close() error { return nil }

func (s *Store) stage(ctx context.Context, fn func(*Store) error) error {
	if s.readOnly {
		return errors.New("Antfly store is read-only")
	}
	b, v, e := s.transport.Load(ctx)
	if e != nil {
		return e
	}
	t := &stagedTransport{data: b, version: 1}
	staged := New(t, false)
	staged.expectedProject = s.expectedProject
	if _, _, e = staged.load(ctx); e != nil {
		return e
	}
	if e = fn(staged); e != nil {
		return e
	}
	if string(t.data) == string(b) {
		return nil
	}
	// Public transaction callbacks execute once. A conflict is a proved abort
	// for the caller to handle, not permission to replay arbitrary callbacks.
	return s.transport.CompareAndSwap(ctx, v, t.data)
}

type transaction struct {
	unsupportedTransaction
	s *Store
}

var _ storage.Transaction = (*transaction)(nil)

func (s *Store) RunInTransaction(ctx context.Context, _ string, fn func(storage.Transaction) error) error {
	return s.stage(ctx, func(staged *Store) error { return fn(&transaction{s: staged}) })
}
func (t *transaction) CreateIssue(ctx context.Context, i *types.Issue, a string) error {
	return t.s.CreateIssue(ctx, i, a)
}
func (t *transaction) GetIssue(ctx context.Context, id string) (*types.Issue, error) {
	return t.s.GetIssue(ctx, id)
}
func (t *transaction) SearchIssues(ctx context.Context, q string, f types.IssueFilter) ([]*types.Issue, error) {
	return t.s.SearchIssues(ctx, q, f)
}
func (t *transaction) SearchIssueIDs(ctx context.Context, q string, f types.IssueFilter) ([]string, error) {
	return t.s.SearchIssueIDs(ctx, q, f)
}
func (t *transaction) AddDependency(ctx context.Context, d *types.Dependency, a string) error {
	return t.s.AddDependency(ctx, d, a)
}
func (t *transaction) RemoveDependency(ctx context.Context, id, target, a string) error {
	return t.s.RemoveDependency(ctx, id, target, a)
}
func (t *transaction) GetDependencyRecords(ctx context.Context, id string) ([]*types.Dependency, error) {
	i, e := t.s.GetIssue(ctx, id)
	if e != nil {
		return nil, e
	}
	return i.Dependencies, nil
}
func (t *transaction) SetConfig(ctx context.Context, k, v string) error {
	return t.s.SetConfig(ctx, k, v)
}
func (t *transaction) GetConfig(ctx context.Context, k string) (string, error) {
	return t.s.GetConfig(ctx, k)
}
func (t *transaction) GetLabels(ctx context.Context, id string) ([]string, error) {
	return t.s.GetLabels(ctx, id)
}
func (t *transaction) AddLabel(ctx context.Context, id, label, actor string) error {
	_, e := (&lifecycle{t.s}).Update(ctx, issueops.UpdateRequest{Actor: actor, IssueID: id, Patch: issueops.IssuePatch{Labels: issueops.LabelPatch{Add: []string{label}}}})
	return e
}
func (t *transaction) RemoveLabel(ctx context.Context, id, label, actor string) error {
	_, e := (&lifecycle{t.s}).Update(ctx, issueops.UpdateRequest{Actor: actor, IssueID: id, Patch: issueops.IssuePatch{Labels: issueops.LabelPatch{Remove: []string{label}}}})
	return e
}
func (t *transaction) CloseIssue(ctx context.Context, id, reason, actor, session string) error {
	_, e := (&lifecycle{t.s}).Close(ctx, issueops.CloseRequest{IssueID: id, Actor: actor, Reason: reason, Session: session})
	return e
}
func (t *transaction) AddComment(ctx context.Context, id, actor, text string) error {
	_, e := (&comments{t.s}).AddComment(ctx, issueops.AddCommentRequest{IssueID: id, Author: actor, Text: text})
	return e
}

type batchCloser struct{ s *Store }

func (s *Store) BatchCloser() (issueops.BatchCloser, error)            { return &batchCloser{s}, nil }
func (s *Store) ServesMoleculeAutoClose(context.Context) (bool, error) { return false, nil }
func (b *batchCloser) CloseBatch(ctx context.Context, req issueops.CloseBatchRequest) (out issueops.CloseBatchResult, err error) {
	if err = storageops.ValidateCloseBatchRequest(req); err != nil {
		return out, err
	}
	if req.AutoCloseMolecule {
		return out, errUnsupported("BatchCloser: molecule autoclose")
	}
	err = b.s.stage(ctx, func(staged *Store) error {
		out = issueops.CloseBatchResult{Outcomes: []issueops.CloseOutcome{}}
		changed := false
		for _, item := range req.Items {
			r, e := (&lifecycle{staged}).Close(ctx, issueops.CloseRequest{Actor: req.Actor, IssueID: item.IssueID, Reason: item.Reason, Session: req.Session, Force: req.Force})
			out.Outcomes = append(out.Outcomes, issueops.CloseOutcome{IssueID: item.IssueID, Issue: r.Issue, Changed: r.Changed, OpenChildren: r.OpenChildren, Err: e})
			changed = changed || r.Changed
		}
		if changed && req.ClaimNext != nil {
			r, e := (&readyRole{staged}).ClaimNext(ctx, issueops.ClaimNextRequest{Actor: req.Actor, Filter: *req.ClaimNext})
			if e != nil {
				return e
			}
			out.ClaimedNext = r.Claimed
		}
		return nil
	})
	return out, err
}

type batchGetter struct{ s *Store }

func (s *Store) BatchGetter() (issueops.BatchGetter, error) { return &batchGetter{s}, nil }
func (b *batchGetter) GetMany(ctx context.Context, req issueops.GetManyRequest) (issueops.GetManyResult, error) {
	out := issueops.GetManyResult{Issues: []*types.Issue{}, Missing: []string{}}
	if len(req.IDs) > issueops.MaxGetManyIDs {
		return out, storage.ErrValidation
	}
	st, _, e := b.s.load(ctx)
	if e != nil {
		return out, e
	}
	seen := map[string]bool{}
	for _, id := range req.IDs {
		if id == "" {
			return out, storage.ErrValidation
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		if r := st.Issues[id]; r != nil {
			i := r.Issue
			i.Comments = nil
			i.Dependencies = nil
			out.Issues = append(out.Issues, &i)
		} else {
			out.Missing = append(out.Missing, id)
		}
	}
	return out, nil
}
