package antfly

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/steveyegge/beads/internal/storage"
	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/issueops"
)

type mergeSlotMetadata struct {
	Holder  string   `json:"holder,omitempty"`
	Waiters []string `json:"waiters,omitempty"`
}

// These internal callbacks only stage lifecycle writes. Retrying a proved
// rejected publication re-reads the winner, never replays a public callback or
// an unknown commit outcome. Runtime coordination always belongs in main.
func (s *Store) changeMergeSlot(ctx context.Context, fn func(*Store, string) error) error {
	for attempt := 0; attempt < 16; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := s.stage(ctx, func(stage *Store) error {
			parent, err := stage.GetConfig(ctx, "antfly.branch.parent")
			if err != nil {
				return err
			}
			if parent != "" {
				return errUnsupported("merge slots belong in main")
			}
			prefix, err := stage.GetConfig(ctx, "issue_prefix")
			if err != nil {
				return err
			}
			return fn(stage, strings.TrimSuffix(prefix, "-")+"-merge-slot")
		})
		if !errors.Is(err, errConflict) {
			return err
		}
	}
	return errConflict
}
func decodeMergeSlot(issue *types.Issue) (mergeSlotMetadata, error) {
	var meta mergeSlotMetadata
	if len(issue.Metadata) > 0 {
		if err := json.Unmarshal(issue.Metadata, &meta); err != nil {
			return meta, fmt.Errorf("merge slot metadata: %w", err)
		}
	}
	return meta, nil
}
func (s *Store) MergeSlotCreate(ctx context.Context, actor string) (*types.Issue, error) {
	if actor == "" {
		return nil, storage.ErrValidation
	}
	var result *types.Issue
	err := s.changeMergeSlot(ctx, func(stage *Store, id string) error {
		existing, err := stage.GetIssue(ctx, id)
		if err == nil {
			result = existing
			return nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		created, err := (&lifecycle{stage}).Create(ctx, issueops.CreateRequest{Actor: actor, Issue: &types.Issue{ID: id, Title: "Merge Slot", Description: "Exclusive access slot for serialized conflict resolution in the merge queue.", IssueType: types.TypeTask, Status: types.StatusOpen, Priority: 0, Labels: []string{"gt:slot"}}})
		result = created.Issue
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
func (s *Store) MergeSlotCheck(ctx context.Context) (*storage.MergeSlotStatus, error) {
	prefix, err := s.GetConfig(ctx, "issue_prefix")
	if err != nil {
		return nil, err
	}
	id := strings.TrimSuffix(prefix, "-") + "-merge-slot"
	issue, err := s.GetIssue(ctx, id)
	if err != nil {
		return nil, err
	}
	meta, err := decodeMergeSlot(issue)
	if err != nil {
		return nil, err
	}
	return &storage.MergeSlotStatus{SlotID: id, Available: issue.Status == types.StatusOpen, Holder: meta.Holder, Waiters: append([]string{}, meta.Waiters...)}, nil
}
func (s *Store) MergeSlotAcquire(ctx context.Context, holder, actor string, wait bool) (*storage.MergeSlotResult, error) {
	if holder == "" || actor == "" {
		return nil, storage.ErrValidation
	}
	var result storage.MergeSlotResult
	err := s.changeMergeSlot(ctx, func(stage *Store, id string) error {
		issue, err := stage.GetIssue(ctx, id)
		if err != nil {
			return err
		}
		meta, err := decodeMergeSlot(issue)
		if err != nil {
			return err
		}
		result = storage.MergeSlotResult{SlotID: id, Holder: meta.Holder}
		patch := issueops.IssuePatch{}
		if issue.Status != types.StatusOpen {
			if !wait {
				return nil
			}
			result.Waiting = true
			if index := slices.Index(meta.Waiters, holder); index >= 0 {
				result.Position = index + 1
				return nil
			}
			meta.Waiters = append(meta.Waiters, holder)
			result.Position = len(meta.Waiters)
			raw, _ := json.Marshal(meta.Waiters)
			patch.Metadata.Set = map[string]json.RawMessage{"waiters": raw}
		} else {
			raw, _ := json.Marshal(holder)
			patch.Status = issueops.Field[types.Status]{Set: true, Value: types.StatusInProgress}
			patch.Metadata.Set = map[string]json.RawMessage{"holder": raw}
			result.Acquired = true
			result.Holder = holder
		}
		_, err = (&lifecycle{stage}).Update(ctx, issueops.UpdateRequest{Actor: actor, IssueID: id, Patch: patch})
		return err
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}
func (s *Store) MergeSlotRelease(ctx context.Context, holder, actor string) error {
	if actor == "" {
		return storage.ErrValidation
	}
	return s.changeMergeSlot(ctx, func(stage *Store, id string) error {
		issue, err := stage.GetIssue(ctx, id)
		if err != nil {
			return err
		}
		meta, err := decodeMergeSlot(issue)
		if err != nil {
			return err
		}
		if holder != "" && meta.Holder != holder {
			return fmt.Errorf("slot held by %s, not %s", meta.Holder, holder)
		}
		if issue.Status == types.StatusOpen {
			return nil
		}
		_, err = (&lifecycle{stage}).Update(ctx, issueops.UpdateRequest{Actor: actor, IssueID: id, Patch: issueops.IssuePatch{Status: issueops.Field[types.Status]{Set: true, Value: types.StatusOpen}, Metadata: issueops.MetadataPatch{Unset: []string{"holder"}}}})
		return err
	})
}

func runtimeOwnedIssue(issue *types.Issue) bool {
	return issue != nil && (storageops.IsWisp(issue) || slices.Contains(issue.Labels, "gt:slot"))
}
