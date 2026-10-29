package antfly

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// documentTransport is the engine boundary. Nil version writes only immutable
// staging records; a non-nil version guards publication on the workspace head.
type documentTransport interface {
	Transport
	ReadDocument(context.Context, string) ([]byte, uint64, error)
	WriteDocuments(context.Context, *uint64, map[string]json.RawMessage) error
}

type rowHead struct {
	StorageFormat int                        `json:"storage_format"`
	Fields        map[string]json.RawMessage `json:"fields"`
	Pages         map[string]string          `json:"pages"`
}
type rowPage struct {
	Entries map[string]string `json:"entries"`
}
type storedRow struct {
	Value json.RawMessage `json:"value"`
}
type rowTransport struct {
	documentTransport
	workspace string
}

func newRowTransport(t Transport, workspace string) Transport {
	return &rowTransport{documentTransport: t.(documentTransport), workspace: workspace}
}
func underlyingTransport(t Transport) Transport {
	if rows, ok := t.(*rowTransport); ok {
		return rows.documentTransport
	}
	return t
}
func digest(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func (r *rowTransport) address(b []byte) string {
	return "row:" + digest([]byte(r.workspace))[:32] + ":" + digest(b)
}
func readRowHead(b []byte) (*rowHead, error) {
	var head rowHead
	if err := json.Unmarshal(b, &head); err != nil {
		return nil, err
	}
	if head.StorageFormat == 0 {
		return nil, nil
	} // Format 1 is migrated on its next write.
	if head.StorageFormat != 2 || head.Pages == nil || head.Fields == nil {
		return nil, errors.New("unsupported or corrupt Antfly row head")
	}
	return &head, nil
}
func (r *rowTransport) readImmutable(ctx context.Context, key string) ([]byte, error) {
	b, _, err := r.ReadDocument(ctx, key)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(key, ":")
	if len(parts) == 3 && parts[0] == "row" {
		if digest(b) == parts[2] {
			return b, nil // Original format-2 records.
		}
		// Document APIs may reserialize JSON (including HTML escapes and
		// number spellings). New records retain the exact hashed JSON as a
		// string value, whose bytes survive every valid JSON round trip.
		var envelope struct {
			JSON *string `json:"_beads_immutable_json"`
		}
		if json.Unmarshal(b, &envelope) == nil && envelope.JSON != nil {
			payload := []byte(*envelope.JSON)
			if digest(payload) == parts[2] {
				return payload, nil
			}
		}
		// Recover existing records whose Go HTML escapes were normalized
		// by Antfly. The original hash still has to match; never accept a
		// merely parseable or semantically similar corrupted record.
		var escaped bytes.Buffer
		json.HTMLEscape(&escaped, b)
		if digest(escaped.Bytes()) == parts[2] {
			return escaped.Bytes(), nil
		}
	}
	return nil, fmt.Errorf("missing or corrupt immutable Antfly record %s", key)
}
func (r *rowTransport) references(ctx context.Context, head *rowHead) (map[string]string, error) {
	refs := map[string]string{}
	if head == nil {
		return refs, nil
	}
	for _, key := range head.Pages {
		b, err := r.readImmutable(ctx, key)
		if err != nil {
			return nil, err
		}
		var page rowPage
		if err = json.Unmarshal(b, &page); err != nil {
			return nil, err
		}
		if page.Entries == nil {
			return nil, errors.New("corrupt Antfly row page")
		}
		for id, address := range page.Entries {
			if _, duplicate := refs[id]; duplicate {
				return nil, errors.New("duplicate Antfly row reference")
			}
			refs[id] = address
		}
	}
	return refs, nil
}
func splitState(data []byte) (map[string]json.RawMessage, map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, nil, err
	}
	fields := map[string]json.RawMessage{}
	rows := map[string]json.RawMessage{}
	for kind, value := range object {
		if len(value) > 0 && value[0] == '{' {
			fields[kind] = json.RawMessage(`{}`)
			var entries map[string]json.RawMessage
			if err := json.Unmarshal(value, &entries); err != nil {
				return nil, nil, err
			}
			for id, v := range entries {
				rows[kind+"/"+id] = v
			}
		} else if kind == "events" {
			fields[kind] = json.RawMessage(`null`)
			if len(value) > 0 && value[0] == '[' {
				fields[kind] = json.RawMessage(`[]`)
			}
			var events []json.RawMessage
			if err := json.Unmarshal(value, &events); err != nil {
				return nil, nil, err
			}
			for i, v := range events {
				rows[fmt.Sprintf("events/%020d", i)] = v
			}
		} else {
			fields[kind] = value
		}
	}
	// Comments are independent durable rows; their insertion order is explicit.
	for id, value := range rows {
		if !strings.HasPrefix(id, "issues/") {
			continue
		}
		var record map[string]json.RawMessage
		if err := json.Unmarshal(value, &record); err != nil {
			return nil, nil, err
		}
		var issue map[string]json.RawMessage
		if err := json.Unmarshal(record["issue"], &issue); err != nil {
			return nil, nil, err
		}
		lease := map[string]json.RawMessage{}
		for _, field := range []string{"heartbeat_at", "lease_expires_at"} {
			if v, ok := issue[field]; ok {
				lease[field] = v
				delete(issue, field)
			}
		}
		if len(lease) > 0 {
			rows["leases/"+strings.TrimPrefix(id, "issues/")], _ = json.Marshal(lease)
		}
		record["issue"], _ = json.Marshal(issue)
		rows[id], _ = json.Marshal(record)
		if comments, ok := issue["comments"]; ok {
			var list []json.RawMessage
			if err := json.Unmarshal(comments, &list); err != nil {
				return nil, nil, err
			}
			for i, c := range list {
				rows[fmt.Sprintf("comments/%s\x00%020d", strings.TrimPrefix(id, "issues/"), i)] = c
			}
			delete(issue, "comments")
			record["issue"], _ = json.Marshal(issue)
			rows[id], _ = json.Marshal(record)
		}
	}
	return fields, rows, nil
}
func joinState(fields, rows map[string]json.RawMessage) ([]byte, error) {
	object := map[string]json.RawMessage{}
	for k, v := range fields {
		object[k] = v
	}
	maps := map[string]map[string]json.RawMessage{}
	events := []json.RawMessage{}
	comments := map[string][]json.RawMessage{}
	keys := make([]string, 0, len(rows))
	for key := range rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts := strings.SplitN(key, "/", 2)
		if len(parts) != 2 {
			return nil, errors.New("corrupt logical Antfly row key")
		}
		kind, id := parts[0], parts[1]
		if kind == "events" {
			events = append(events, rows[key])
			continue
		}
		if kind == "comments" {
			issueID, _, ok := strings.Cut(id, "\x00")
			if !ok {
				return nil, errors.New("corrupt comment key")
			}
			comments[issueID] = append(comments[issueID], rows[key])
			continue
		}
		if maps[kind] == nil {
			maps[kind] = map[string]json.RawMessage{}
		}
		maps[kind][id] = rows[key]
	}
	for id, list := range comments {
		var record map[string]json.RawMessage
		if err := json.Unmarshal(maps["issues"][id], &record); err != nil {
			return nil, err
		}
		var issue map[string]json.RawMessage
		if err := json.Unmarshal(record["issue"], &issue); err != nil {
			return nil, err
		}
		issue["comments"], _ = json.Marshal(list)
		record["issue"], _ = json.Marshal(issue)
		maps["issues"][id], _ = json.Marshal(record)
	}
	for id, lease := range maps["leases"] {
		var record map[string]json.RawMessage
		if err := json.Unmarshal(maps["issues"][id], &record); err != nil {
			return nil, err
		}
		var issue, fields map[string]json.RawMessage
		if err := json.Unmarshal(record["issue"], &issue); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(lease, &fields); err != nil {
			return nil, err
		}
		for field, v := range fields {
			issue[field] = v
		}
		record["issue"], _ = json.Marshal(issue)
		maps["issues"][id], _ = json.Marshal(record)
	}
	delete(maps, "leases")
	for kind, entries := range maps {
		object[kind], _ = json.Marshal(entries)
	}
	for _, kind := range []string{"issues", "edges", "notes", "config"} {
		if _, ok := object[kind]; !ok {
			object[kind] = json.RawMessage(`{}`)
		}
	}
	if len(events) > 0 || fields["events"] == nil {
		object["events"], _ = json.Marshal(events)
	}
	return json.Marshal(object)
}
func (r *rowTransport) Load(ctx context.Context) ([]byte, uint64, error) {
	b, v, err := r.documentTransport.Load(ctx)
	if err != nil || len(b) == 0 {
		return b, v, err
	}
	head, err := readRowHead(b)
	if err != nil {
		return nil, 0, err
	}
	if head == nil {
		return b, v, nil
	}
	refs, err := r.references(ctx, head)
	if err != nil {
		return nil, 0, err
	}
	rows := map[string]json.RawMessage{}
	for id, key := range refs {
		b, err = r.readImmutable(ctx, key)
		if err != nil {
			return nil, 0, err
		}
		var row storedRow
		if err = json.Unmarshal(b, &row); err != nil {
			return nil, 0, err
		}
		if len(row.Value) == 0 {
			return nil, 0, errors.New("empty Antfly row")
		}
		rows[id] = row.Value
	}
	data, err := joinState(head.Fields, rows)
	return data, v, err
}
func (r *rowTransport) CompareAndSwap(ctx context.Context, version uint64, data []byte) error {
	return r.compareAndSwapFrom(ctx, version, data, nil)
}
func (r *rowTransport) compareAndSwapFrom(ctx context.Context, version uint64, data []byte, base []byte) error {
	old, v, err := r.documentTransport.Load(ctx)
	if err != nil {
		return err
	}
	if v != version {
		return errConflict
	}
	if len(old) == 0 && version == 0 && len(base) > 0 {
		old = base
	}
	var prior *rowHead
	if len(old) > 0 {
		prior, err = readRowHead(old)
		if err != nil {
			return err
		}
	}
	oldRefs, err := r.references(ctx, prior)
	if err != nil {
		return err
	}
	fields, rows, err := splitState(data)
	if err != nil {
		return err
	}
	head := rowHead{StorageFormat: 2, Fields: fields, Pages: map[string]string{}}
	pages := map[string]map[string]string{}
	staged := map[string]json.RawMessage{}
	batchBytes := 0
	flush := func() error {
		if len(staged) == 0 {
			return nil
		}
		if err := r.WriteDocuments(ctx, nil, staged); err != nil {
			return err
		}
		staged = map[string]json.RawMessage{}
		batchBytes = 0
		return nil
	}
	stage := func(b []byte) error {
		encoded, err := json.Marshal(map[string]string{"_beads_immutable_json": string(b)})
		if err != nil {
			return err
		}
		if len(encoded) > maxStateBytes {
			return errors.New("Antfly record exceeds the 4 MiB per-record limit")
		}
		if batchBytes+len(encoded) > 1<<20 {
			if err := flush(); err != nil {
				return err
			}
		}
		key := r.address(b)
		staged[key] = encoded
		batchBytes += len(encoded)
		return nil
	}
	changedIssues := map[string]bool{}
	markSource := func(id string) {
		for _, kind := range []string{"edges/", "comments/"} {
			if strings.HasPrefix(id, kind) {
				source, _, _ := strings.Cut(strings.TrimPrefix(id, kind), "\x00")
				changedIssues[source] = true
			}
		}
	}
	for id, value := range rows {
		b, err := json.Marshal(storedRow{Value: value})
		if err != nil {
			return err
		}
		address := r.address(b)
		if oldAddress := oldRefs[id]; strings.HasSuffix(oldAddress, ":"+digest(b)) {
			address = oldAddress
		}
		if oldRefs[id] != address {
			markSource(id)
			if err = stage(b); err != nil {
				return err
			}
			if strings.HasPrefix(id, "issues/") {
				changedIssues[strings.TrimPrefix(id, "issues/")] = true
			}
		}
		bucket := strings.SplitN(id, "/", 2)[0] + "/" + digest([]byte(id))[:2]
		if pages[bucket] == nil {
			pages[bucket] = map[string]string{}
		}
		pages[bucket][id] = address
	}
	for id := range oldRefs {
		if _, ok := rows[id]; !ok {
			markSource(id)
		}
		if strings.HasPrefix(id, "issues/") {
			if _, ok := rows[id]; !ok {
				changedIssues[strings.TrimPrefix(id, "issues/")] = true
			}
		}
	}
	for bucket, entries := range pages {
		b, err := json.Marshal(rowPage{Entries: entries})
		if err != nil {
			return err
		}
		address := r.address(b)
		if prior != nil && strings.HasSuffix(prior.Pages[bucket], ":"+digest(b)) {
			address = prior.Pages[bucket]
		}
		if prior == nil || prior.Pages[bucket] != address {
			if err = stage(b); err != nil {
				return err
			}
		}
		head.Pages[bucket] = address
	}
	if err = flush(); err != nil {
		return err
	}
	encoded, err := json.Marshal(head)
	if err != nil {
		return err
	}
	if len(encoded) > maxStateBytes {
		return errors.New("Antfly workspace head exceeds supported page count")
	}
	if version == 0 {
		for id := range rows {
			if strings.HasPrefix(id, "issues/") {
				changedIssues[strings.TrimPrefix(id, "issues/")] = true
			}
		}
	}
	// Materialized ordinary documents are published with the head, never early.
	projections, err := projections(r.workspace, data)
	if err != nil {
		return err
	}
	publish := map[string]json.RawMessage{r.workspace: encoded}
	for id := range changedIssues {
		key := ProjectionKey(r.workspace, id)
		if doc, ok := projections[key]; ok {
			publish[key] = doc
		} else {
			publish[key] = json.RawMessage(`{"beads_document_type":"deleted","_edges":{"beads":{}}}`)
		}
	}
	return r.WriteDocuments(ctx, &version, publish)
}
