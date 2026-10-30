package antfly

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/steveyegge/beads/internal/types"
)

const coreQueryField = "_core_query_index"
const coreQueryKind = "_core_query_lookup"

// Posting directories name only occupied hash shards. Both directories and
// postings are immutable rows published under the same workspace head.
func coreQueryKey(field, value string) string {
	data, _ := json.Marshal([]string{field, value})
	return coreQueryKind + "/" + digest(data)
}

func addCoreQueryIndex(fields, rows map[string]json.RawMessage) error {
	postings := map[string]map[string][]string{}
	add := func(field, value, id string) {
		key := coreQueryKey(field, value)
		if postings[key] == nil {
			postings[key] = map[string][]string{}
		}
		shard := key + "/" + digest([]byte(id))[:2]
		postings[key][shard] = append(postings[key][shard], id)
	}
	// Even an empty workspace has a complete all-IDs directory.
	postings[coreQueryKey("all", "")] = map[string][]string{}
	for key, raw := range rows {
		if len(key) < 7 || key[:7] != "issues/" {
			continue
		}
		var r record
		if err := json.Unmarshal(raw, &r); err != nil {
			return err
		}
		i := r.Issue
		add("all", "", i.ID)
		add("status", string(i.Status), i.ID)
		add("type", string(i.IssueType), i.ID)
		add("assignee", i.Assignee, i.ID)
		add("priority", strconv.Itoa(i.Priority), i.ID)
		for _, label := range i.Labels {
			add("label", label, i.ID)
		}
		var metadata map[string]any
		if err := json.Unmarshal(i.Metadata, &metadata); len(i.Metadata) > 0 && err != nil {
			return err
		}
		for k, v := range metadata {
			add("metadata:"+k, fmt.Sprint(v), i.ID)
		}
	}
	for key, shards := range postings {
		directory := make([]string, 0, len(shards))
		for shard, ids := range shards {
			sort.Strings(ids)
			rows[shard], _ = json.Marshal(ids)
			directory = append(directory, shard)
		}
		sort.Strings(directory)
		rows[key], _ = json.Marshal(directory)
	}
	fields[coreQueryField] = json.RawMessage(`1`)
	return nil
}

func readyIssueFilter(f types.WorkFilter) types.IssueFilter {
	status := f.Status
	if status == "" {
		status = types.StatusOpen
	}
	filter := types.IssueFilter{Status: &status, Statuses: f.Statuses, Priority: f.Priority, Assignee: f.Assignee, NoAssignee: f.Unassigned, Labels: f.Labels, LabelsAny: f.LabelsAny, ExcludeLabels: f.ExcludeLabels, ExcludeTypes: f.ExcludeTypes, MetadataFields: f.MetadataFields, HasMetadataKey: f.HasMetadataKey}
	if f.Type != "" {
		it := types.IssueType(f.Type)
		filter.IssueType = &it
		filter.ExcludeTypes = nil
	}
	return filter
}

// queryState selects a superset; the existing domain predicates still decide
// filtering, counts, sorting, limits and readiness. Missing support on an older
// snapshot falls back to its full read until the next ordinary publication.
func (s *Store) queryState(ctx context.Context, f types.IssueFilter) (*state, error) {
	st, indexed, err := s.indexedQueryState(ctx, f)
	if indexed || err != nil {
		return st, err
	}
	st, _, err = s.load(ctx)
	return st, err
}

func (s *Store) indexedQueryState(ctx context.Context, f types.IssueFilter) (*state, bool, error) {
	r, ok := s.transport.(*rowTransport)
	if !ok || !r.domainProjection {
		return nil, false, nil
	}
	field, values := "all", []string{""}
	switch {
	case len(f.IDs) > 0:
		field, values = "id", f.IDs
	case f.Status != nil:
		field, values = "status", []string{string(*f.Status)}
	case len(f.Statuses) > 0:
		field, values = "status", []string{}
		for _, v := range f.Statuses {
			values = append(values, string(v))
		}
	case f.Assignee != nil:
		field, values = "assignee", []string{*f.Assignee}
	case f.NoAssignee:
		field, values = "assignee", []string{""}
	case f.IssueType != nil:
		field, values = "type", []string{string(*f.IssueType)}
	case f.Priority != nil:
		field, values = "priority", []string{strconv.Itoa(*f.Priority)}
	case len(f.Labels) > 0:
		field, values = "label", f.Labels[:1]
	case len(f.LabelsAny) > 0:
		field, values = "label", f.LabelsAny
	case len(f.MetadataFields) > 0:
		keys := make([]string, 0, len(f.MetadataFields))
		for k := range f.MetadataFields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		if f.MetadataFields[keys[0]] != "<nil>" {
			field, values = "metadata:"+keys[0], []string{f.MetadataFields[keys[0]]}
		}
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
		if head == nil || string(head.Fields[coreQueryField]) != "1" {
			return nil, false, nil
		}
		ids := map[string]bool{}
		if field == "id" {
			for _, id := range values {
				ids[id] = true
			}
		} else {
			directoryKeys := make([]string, 0, len(values))
			for _, value := range values {
				directoryKeys = append(directoryKeys, coreQueryKey(field, value))
			}
			directories, err := r.readLogicalRows(ctx, head, directoryKeys)
			if err != nil {
				return nil, true, err
			}
			shards := []string{}
			for _, key := range directoryKeys {
				raw, exists := directories[key]
				if !exists {
					if field == "all" {
						return nil, true, fmt.Errorf("missing core query directory")
					}
					continue
				}
				var keys []string
				if err = json.Unmarshal(raw, &keys); err != nil {
					return nil, true, err
				}
				shards = append(shards, keys...)
			}
			postings, err := r.readLogicalRows(ctx, head, shards)
			if err != nil {
				return nil, true, err
			}
			for _, key := range shards {
				raw, exists := postings[key]
				if !exists {
					return nil, true, fmt.Errorf("missing core query posting %q", key)
				}
				var entries []string
				if err = json.Unmarshal(raw, &entries); err != nil {
					return nil, true, err
				}
				for _, id := range entries {
					ids[id] = true
				}
			}
		}
		keySet := map[string]bool{"config/project_id": true}
		visited := map[string]bool{}
		pending := make(map[string]bool, len(ids))
		for id := range ids {
			pending[id] = true
		}
		absentIDs := []string{}
		for len(pending) > 0 {
			lookupKeys := make([]string, 0, len(pending))
			for id := range pending {
				lookupKeys = append(lookupKeys, coreIndexKind+"/"+id)
				visited[id] = true
			}
			lookups, err := r.readLogicalRows(ctx, head, lookupKeys)
			if err != nil {
				return nil, true, err
			}
			next := map[string]bool{}
			for id := range pending {
				raw, exists := lookups[coreIndexKind+"/"+id]
				if !exists {
					if field == "id" && ids[id] {
						absentIDs = append(absentIDs, id)
						continue
					}
					return nil, true, fmt.Errorf("missing indexed issue %q", id)
				}
				var lookup coreIssueLookup
				if err = json.Unmarshal(raw, &lookup); err != nil {
					return nil, true, err
				}
				keySet["issues/"+id] = true
				for _, key := range lookup.Rows {
					keySet[key] = true
				}
				for _, parent := range lookup.Parents {
					if !visited[parent] {
						next[parent] = true
					}
				}
			}
			pending = next
		}
		if len(absentIDs) > 0 {
			keys := make([]string, 0, len(absentIDs))
			for _, id := range absentIDs {
				keys = append(keys, "issues/"+id)
			}
			orphaned, err := r.readLogicalRows(ctx, head, keys)
			if err != nil {
				return nil, true, err
			}
			if len(orphaned) > 0 {
				return nil, true, fmt.Errorf("existing issue missing core query lookup")
			}
		}
		keys := make([]string, 0, len(keySet))
		for key := range keySet {
			keys = append(keys, key)
		}
		rows, err := r.readLogicalRows(ctx, head, keys)
		if err != nil {
			return nil, true, err
		}
		for key := range keySet {
			if rows[key] == nil {
				return nil, true, fmt.Errorf("missing indexed core query record %q", key)
			}
		}
		_, current, err := r.documentTransport.Load(ctx)
		if err != nil {
			return nil, true, err
		}
		if current != version {
			continue
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
		st.queryCandidates = ids
		if st.Format != 1 || s.expectedProject != "" && st.Config["project_id"] != s.expectedProject {
			return nil, true, fmt.Errorf("indexed query workspace format/project mismatch")
		}
		for id, r := range st.Issues {
			if r == nil || r.Issue.ID != id {
				return nil, true, fmt.Errorf("indexed query issue identity mismatch")
			}
			r.Issue.RowVersion = r.Version
		}
		return st, true, nil
	}
	return nil, true, errConflict
}
