package antfly

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/steveyegge/beads/internal/storage/backends"
	"strings"
)

// ErrSnapshotConflict reports a positively rejected snapshot publication.
var ErrSnapshotConflict = errConflict

// OpenSnapshot opens a dedicated generic JSON snapshot workspace. Top-level
// object fields are split into immutable per-key records and published with
// one conditional head, without Beads issue projections or lifecycle policy.
// Callers must use their own schema and a workspace separate from Beads work.
func OpenSnapshot(ctx context.Context, target Target, opts backends.OpenOptions) (Transport, error) {
	tr, err := ensureTableWith(ctx, target, opts)
	if err != nil {
		return nil, err
	}
	rows := tr.(*rowTransport)
	rows.domainProjection = false
	return rows, nil
}

// Snapshot keys use a separate row namespace so caller fields such as issues,
// comments, events and leases cannot trigger core Beads reconstruction logic.
func splitSnapshot(data []byte) (map[string]json.RawMessage, map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, nil, err
	}
	if object == nil {
		return nil, nil, fmt.Errorf("snapshot must be a JSON object")
	}
	fields := map[string]json.RawMessage{}
	rows := map[string]json.RawMessage{}
	for kind, value := range object {
		trimmed := strings.TrimSpace(string(value))
		if strings.HasPrefix(trimmed, "{") {
			var entries map[string]json.RawMessage
			if err := json.Unmarshal(value, &entries); err != nil {
				return nil, nil, err
			}
			fields[kind] = json.RawMessage(`{}`)
			for id, v := range entries {
				rows["snapshot-"+base64.RawURLEncoding.EncodeToString([]byte(kind))+"/"+id] = v
			}
		} else {
			fields[kind] = value
		}
	}
	return fields, rows, nil
}
func joinSnapshot(fields, rows map[string]json.RawMessage) ([]byte, error) {
	object := map[string]json.RawMessage{}
	for k, v := range fields {
		object[k] = v
	}
	groups := map[string]map[string]json.RawMessage{}
	for key, value := range rows {
		kind, id, ok := strings.Cut(key, "/")
		if !ok || !strings.HasPrefix(kind, "snapshot-") {
			return nil, fmt.Errorf("invalid generic snapshot row")
		}
		decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(kind, "snapshot-"))
		if err != nil {
			return nil, err
		}
		name := string(decoded)
		if groups[name] == nil {
			groups[name] = map[string]json.RawMessage{}
		}
		groups[name][id] = value
	}
	for name, entries := range groups {
		object[name], _ = json.Marshal(entries)
	}
	return json.Marshal(object)
}
