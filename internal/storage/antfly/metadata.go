package antfly

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/steveyegge/beads/internal/storage"
	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/issueops"
)

type metadataCAS struct{ s *Store }

func (s *Store) MetadataCAS() (issueops.MetadataCAS, error) { return &metadataCAS{s}, nil }

func (m *metadataCAS) CompareAndSetKey(ctx context.Context, req issueops.CompareAndSetKeyRequest) (issueops.CompareAndSetKeyResult, error) {
	plan, err := storage.PlanCompareAndSetKey(req)
	if err != nil {
		return issueops.CompareAndSetKeyResult{}, err
	}
	var out issueops.CompareAndSetKeyResult
	err = m.s.mutate(ctx, func(st *state) error {
		issue, err := st.issue(plan.IssueID)
		if err != nil {
			return err
		}
		values := map[string]json.RawMessage{}
		if len(issue.Metadata) > 0 {
			if err = json.Unmarshal(issue.Metadata, &values); err != nil || values == nil {
				return fmt.Errorf("metadata on %s is not a JSON object", plan.IssueID)
			}
		}
		var current *json.RawMessage
		if value, ok := values[plan.Key]; ok {
			raw, err := storage.CanonicalMetadataValue(value)
			if err != nil {
				return err
			}
			current = &raw
		}
		equal := func(a, b *json.RawMessage) bool {
			if a == nil || b == nil {
				return a == nil && b == nil
			}
			return bytes.Equal(*a, *b)
		}
		out = issueops.CompareAndSetKeyResult{Current: current}
		if !equal(current, plan.Expected) {
			return nil
		}
		out.Swapped = true
		if equal(current, plan.Value) {
			return nil
		}
		patch := issueops.MetadataPatch{}
		if plan.Value == nil {
			patch.Unset = []string{plan.Key}
		} else {
			patch.Set = map[string]json.RawMessage{plan.Key: *plan.Value}
		}
		metadata, _, err := storageops.ApplyMetadataPatch(issue.Metadata, patch)
		if err != nil {
			return err
		}
		issue.Metadata = metadata
		st.touch(issue)
		st.audit("update", issue.ID, plan.Actor)
		// Report the committed representation rather than the input spelling.
		values = map[string]json.RawMessage{}
		if err = json.Unmarshal(metadata, &values); err != nil {
			return err
		}
		out.Current = nil
		if value, ok := values[plan.Key]; ok {
			raw, err := storage.CanonicalMetadataValue(value)
			if err != nil {
				return err
			}
			out.Current = &raw
		}
		return nil
	})
	if err != nil {
		return issueops.CompareAndSetKeyResult{}, err
	}
	return out, nil
}

func (s *Store) MergeMetadata(ctx context.Context, id, key string, value json.RawMessage, actor string) error {
	_, err := (&lifecycle{s}).Update(ctx, issueops.UpdateRequest{Actor: actor, IssueID: id, Patch: issueops.IssuePatch{Metadata: issueops.MetadataPatch{Set: map[string]json.RawMessage{key: bytes.Clone(value)}}}})
	return err
}
func (s *Store) SlotSet(ctx context.Context, id, key, value, actor string) error {
	raw, _ := json.Marshal(value)
	return s.MergeMetadata(ctx, id, key, raw, actor)
}
func (s *Store) SlotClear(ctx context.Context, id, key, actor string) error {
	if err := storage.ValidateMetadataKey(key); err != nil {
		return fmt.Errorf("%w: %v", issueops.ErrValidation, err)
	}
	_, err := (&lifecycle{s}).Update(ctx, issueops.UpdateRequest{Actor: actor, IssueID: id, Patch: issueops.IssuePatch{Metadata: issueops.MetadataPatch{Unset: []string{key}}}})
	return err
}
func (s *Store) SlotGet(ctx context.Context, id, key string) (string, error) {
	issue, err := s.GetIssue(ctx, id)
	if err != nil {
		return "", err
	}
	values := map[string]json.RawMessage{}
	if len(issue.Metadata) > 0 {
		if err = json.Unmarshal(issue.Metadata, &values); err != nil {
			return "", err
		}
	}
	raw, ok := values[key]
	if !ok {
		return "", fmt.Errorf("no slot %q on %s", key, id)
	}
	var value string
	if json.Unmarshal(raw, &value) == nil {
		return value, nil
	}
	return string(raw), nil
}
