package antfly

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type tableProfile uint8

const (
	coreTableProfile tableProfile = iota
	snapshotTableProfile
)
const snapshotTableType = "beads_snapshot"

func tableSchema(profile tableProfile) json.RawMessage {
	schema := privateSnapshotTableSchema
	if profile == snapshotTableProfile {
		schema = strings.ReplaceAll(schema, `"beads"`, `"`+snapshotTableType+`"`)
	}
	return json.RawMessage(schema)
}

// applyTableProfile completes only the declared snapshot-only profile. Legacy
// table schemas retain their indexes. Its schema tag is installed atomically
// with table creation, so core initialization cannot join during provisioning.
func applyTableProfile(ctx context.Context, tr Transport, profile tableProfile) error {
	raw, err := nativeCall(ctx, tr, "schema", "", nil)
	if err != nil {
		return err
	}
	var schema struct {
		DefaultType string `json:"default_type"`
	}
	if err = json.Unmarshal(raw, &schema); err != nil {
		return err
	}
	if schema.DefaultType != snapshotTableType {
		return nil
	}
	if profile != snapshotTableProfile {
		return errUnsupported("core work requires a table separate from snapshot-only coordination")
	}
	raw, err = nativeCall(ctx, tr, "indexes", "", nil)
	if err != nil {
		return err
	}
	names, err := tableIndexNames(raw)
	if err != nil {
		return err
	}
	for _, name := range names {
		if name != "full_text_index_v0" && name != "beads" {
			continue
		}
		if _, err = nativeCall(ctx, tr, "delete-index", name, nil); err != nil {
			return fmt.Errorf("complete snapshot table profile: %w", err)
		}
	}
	return nil
}

func tableIndexNames(raw json.RawMessage) ([]string, error) {
	var entries []struct {
		Name string `json:"name"`
	}
	if json.Unmarshal(raw, &entries) == nil {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			if entry.Name == "" {
				return nil, fmt.Errorf("index entry has no name")
			}
			names = append(names, entry.Name)
		}
		return names, nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(object))
	for name := range object {
		names = append(names, name)
	}
	return names, nil
}
