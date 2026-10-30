package antfly

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/types"
)

const coreIndexField = "_core_point_index"
const coreIndexKind = "_core_lookup"

type coreIssueLookup struct {
	Rows    []string `json:"rows"`
	Parents []string `json:"parents,omitempty"`
}

func addCorePointIndex(fields, rows map[string]json.RawMessage) error {
	lookups := map[string]*coreIssueLookup{}
	for key := range rows {
		if id, ok := strings.CutPrefix(key, "issues/"); ok {
			lookups[id] = &coreIssueLookup{Rows: []string{key}}
		}
	}
	for key, value := range rows {
		var id string
		switch {
		case strings.HasPrefix(key, "leases/"):
			id = strings.TrimPrefix(key, "leases/")
		case strings.HasPrefix(key, "comments/"):
			id = strings.SplitN(strings.TrimPrefix(key, "comments/"), "\x00", 2)[0]
		case strings.HasPrefix(key, "edges/"):
			var edge types.Dependency
			if err := json.Unmarshal(value, &edge); err != nil {
				return err
			}
			id = edge.IssueID
			if incoming := lookups[edge.DependsOnID]; incoming != nil && edge.DependsOnID != id {
				incoming.Rows = append(incoming.Rows, key)
				if edge.Type == types.DepParentChild && lookups[id] != nil {
					lookups[id].Parents = append(lookups[id].Parents, edge.DependsOnID)
				}
			}
		}
		if lookup := lookups[id]; lookup != nil {
			lookup.Rows = append(lookup.Rows, key)
		}
	}
	fields[coreIndexField] = json.RawMessage(`1`)
	for id, lookup := range lookups {
		sort.Strings(lookup.Rows)
		sort.Strings(lookup.Parents)
		raw, err := json.Marshal(lookup)
		if err != nil {
			return err
		}
		rows[coreIndexKind+"/"+id] = raw
	}
	return nil
}

// readLogicalRows fetches only the hash buckets and immutable rows named by a
// captured head. The caller pins all dependent lookups to that head version.
func (r *rowTransport) readLogicalRows(ctx context.Context, head *rowHead, keys []string) (map[string]json.RawMessage, error) {
	out := map[string]json.RawMessage{}
	pages := map[string]*rowPage{}
	for _, key := range keys {
		kind, _, ok := strings.Cut(key, "/")
		if !ok {
			return nil, fmt.Errorf("invalid indexed logical key")
		}
		bucket := kind + "/" + digest([]byte(key))[:2]
		address, exists := head.Pages[bucket]
		if !exists {
			continue
		}
		page := pages[bucket]
		if page == nil {
			raw, err := r.readImmutable(ctx, address)
			if err != nil {
				return nil, err
			}
			page = &rowPage{}
			if err = json.Unmarshal(raw, page); err != nil {
				return nil, err
			}
			if page.Entries == nil {
				return nil, fmt.Errorf("corrupt core lookup page")
			}
			pages[bucket] = page
		}
		address, exists = page.Entries[key]
		if !exists {
			continue
		}
		raw, err := r.readImmutable(ctx, address)
		if err != nil {
			return nil, err
		}
		var row storedRow
		if err = json.Unmarshal(raw, &row); err != nil {
			return nil, err
		}
		if len(row.Value) == 0 {
			return nil, fmt.Errorf("empty core lookup row")
		}
		out[key] = row.Value
	}
	return out, nil
}

func (s *Store) indexedIssue(ctx context.Context, id string) (*types.Issue, bool, error) {
	r, ok := s.transport.(*rowTransport)
	if !ok || !r.domainProjection {
		return nil, false, nil
	}
	for attempt := 0; attempt < 8; attempt++ {
		raw, version, err := r.documentTransport.Load(ctx)
		if err != nil {
			return nil, true, err
		}
		if len(raw) == 0 {
			return nil, false, nil
		}
		head, err := readRowHead(raw)
		if err != nil {
			return nil, true, err
		}
		if head == nil || string(head.Fields[coreIndexField]) != "1" {
			return nil, false, nil
		}
		lookupKey := coreIndexKind + "/" + id
		selected, err := r.readLogicalRows(ctx, head, []string{lookupKey})
		if err != nil {
			return nil, true, err
		}
		keys := []string{"issues/" + id, "config/project_id"}
		if value, exists := selected[lookupKey]; exists {
			var lookup coreIssueLookup
			if err = json.Unmarshal(value, &lookup); err != nil {
				return nil, true, err
			}
			keys = append(keys, lookup.Rows...)
		}
		// An intervening publication invalidates the index-to-record join.
		_, current, err := r.documentTransport.Load(ctx)
		if err != nil {
			return nil, true, err
		}
		if current != version {
			continue
		}
		rows, err := r.readLogicalRows(ctx, head, keys)
		if err != nil {
			return nil, true, err
		}
		if rows["issues/"+id] == nil {
			return nil, true, storage.ErrNotFound
		}
		if selected[lookupKey] == nil {
			return nil, true, fmt.Errorf("missing core issue lookup")
		}
		var lookup coreIssueLookup
		_ = json.Unmarshal(selected[lookupKey], &lookup)
		for _, key := range lookup.Rows {
			if rows[key] == nil {
				return nil, true, fmt.Errorf("missing indexed core record %q", key)
			}
		}
		data, err := joinState(head.Fields, rows, true)
		if err != nil {
			return nil, true, err
		}
		st := emptyState()
		if err = json.Unmarshal(data, st); err != nil {
			return nil, true, err
		}
		st.initialize()
		if st.Format != 1 || s.expectedProject != "" && st.Config["project_id"] != s.expectedProject {
			return nil, true, fmt.Errorf("indexed workspace format/project mismatch")
		}
		for _, record := range st.Issues {
			record.Issue.RowVersion = record.Version
		}
		if st.Issues[id] == nil || st.Issues[id].Issue.ID != id {
			return nil, true, fmt.Errorf("indexed issue identity mismatch")
		}
		return st.hydrate(id), true, nil
	}
	return nil, true, errConflict
}

// indexedConfig validates the header and project identity without scanning
// unrelated work/history when a process opens a row-format workspace.
func (s *Store) indexedConfig(ctx context.Context, keys []string) (map[string]string, bool, error) {
	r, ok := s.transport.(*rowTransport)
	if !ok || !r.domainProjection {
		return nil, false, nil
	}
	raw, _, err := r.documentTransport.Load(ctx)
	if err != nil {
		return nil, true, err
	}
	if len(raw) == 0 {
		return nil, true, storage.ErrNotInitialized
	}
	head, err := readRowHead(raw)
	if err != nil {
		return nil, true, err
	}
	if head == nil {
		return nil, false, nil
	}
	if string(head.Fields["format"]) != "1" {
		return nil, true, fmt.Errorf("unsupported indexed workspace format")
	}
	logical := []string{"config/project_id"}
	for _, key := range keys {
		logical = append(logical, "config/"+key)
	}
	rows, err := r.readLogicalRows(ctx, head, logical)
	if err != nil {
		return nil, true, err
	}
	values := map[string]string{}
	for key, raw := range rows {
		var value string
		if err = json.Unmarshal(raw, &value); err != nil {
			return nil, true, err
		}
		values[strings.TrimPrefix(key, "config/")] = value
	}
	if s.expectedProject != "" && values["project_id"] != s.expectedProject {
		return nil, true, fmt.Errorf("indexed workspace project identity mismatch")
	}
	return values, true, nil
}
