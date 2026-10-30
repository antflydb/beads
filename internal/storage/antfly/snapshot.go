package antfly

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/steveyegge/beads/internal/storage/backends"
	"io"
	"strings"
)

// ErrSnapshotConflict reports a positively rejected snapshot publication.
var ErrSnapshotConflict = errConflict

// OpenSnapshot opens a dedicated generic JSON snapshot workspace. Top-level
// object fields are split into immutable per-key records and published with
// one conditional head, without Beads issue projections or lifecycle policy.
// New tables have a snapshot-only profile without default domain indexes.
// Callers must use a dedicated table for new coordination deployments; existing
// untagged tables retain their legacy schema and indexes.
func OpenSnapshot(ctx context.Context, target Target, opts backends.OpenOptions) (Transport, error) {
	tr, err := ensureTableProfileWith(ctx, target, opts, snapshotTableProfile)
	if err != nil {
		return nil, err
	}
	rows := tr.(*rowTransport)
	rows.domainProjection = false
	rows.cacheReads = true
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

// SnapshotRecords is a projection from one committed head. Fields carries the
// head's scalar schema metadata; Records contains only requested existing keys.
type SnapshotRecords struct {
	Fields  map[string]json.RawMessage
	Records map[string]json.RawMessage
	Version uint64
}

// SnapshotRecordReader provides selected records and schema fields from a head.
type SnapshotRecordReader interface {
	ReadSnapshotRecords(context.Context, string, []string, *uint64) (SnapshotRecords, error)
}

// SnapshotRecordCapturer captures one committed head for a compound read.
// Keep the transport open while using the reader. Immutable content is still
// hash-verified; capture does not acquire a garbage-collection lease. Storage
// and integrity errors propagate rather than returning partial results.
type SnapshotRecordCapturer interface {
	CaptureSnapshotRecords(context.Context) (SnapshotRecordReader, error)
}

type capturedSnapshotRecords struct {
	transport *rowTransport
	raw       []byte
	version   uint64
}

func (r *rowTransport) CaptureSnapshotRecords(ctx context.Context) (SnapshotRecordReader, error) {
	if r.domainProjection {
		return nil, errUnsupported("generic indexed snapshot on core work")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, version, err := r.documentTransport.Load(ctx)
	if err != nil {
		return nil, err
	}
	if len(raw) > 0 {
		if _, err = readRowHead(raw); err != nil {
			return nil, err
		}
	}
	return &capturedSnapshotRecords{transport: r, raw: bytes.Clone(raw), version: version}, nil
}

func (r *capturedSnapshotRecords) ReadSnapshotRecords(ctx context.Context, field string, ids []string, expected *uint64) (SnapshotRecords, error) {
	return r.transport.readSnapshotRecordsAt(ctx, r.raw, r.version, field, ids, expected)
}

// ReadSnapshotRecords uses the committed head's hash-bucket index. An optional
// expected version makes a multi-stage index lookup fail rather than mix heads.
func (r *rowTransport) ReadSnapshotRecords(ctx context.Context, field string, ids []string, expected *uint64) (SnapshotRecords, error) {
	if r.domainProjection {
		return SnapshotRecords{}, errUnsupported("generic indexed snapshot on core work")
	}
	if err := ctx.Err(); err != nil {
		return SnapshotRecords{}, err
	}
	raw, v, err := r.documentTransport.Load(ctx)
	if err != nil {
		return SnapshotRecords{}, err
	}
	return r.readSnapshotRecordsAt(ctx, raw, v, field, ids, expected)
}

func (r *rowTransport) readSnapshotRecordsAt(ctx context.Context, raw []byte, v uint64, field string, ids []string, expected *uint64) (SnapshotRecords, error) {
	if err := ctx.Err(); err != nil {
		return SnapshotRecords{}, err
	}
	if expected != nil && *expected != v {
		return SnapshotRecords{}, errConflict
	}
	result := SnapshotRecords{Fields: map[string]json.RawMessage{}, Records: map[string]json.RawMessage{}, Version: v}
	if len(raw) == 0 {
		return result, nil
	}
	head, err := readRowHead(raw)
	if err != nil {
		return SnapshotRecords{}, err
	}
	if head == nil {
		// Legacy snapshots remain readable without an implicit rewrite.
		if err = json.Unmarshal(raw, &result.Fields); err != nil {
			return SnapshotRecords{}, err
		}
		var all map[string]json.RawMessage
		if data, ok := result.Fields[field]; ok {
			if err = json.Unmarshal(data, &all); err != nil {
				return SnapshotRecords{}, err
			}
		}
		for _, id := range ids {
			if value, ok := all[id]; ok {
				result.Records[id] = value
			}
		}
		return result, nil
	}
	result.Fields = head.Fields
	kind := "snapshot-" + base64.RawURLEncoding.EncodeToString([]byte(field))
	pages := map[string]*rowPage{}
	for _, id := range ids {
		key := kind + "/" + id
		bucket := kind + "/" + digest([]byte(key))[:2]
		address, ok := head.Pages[bucket]
		if !ok {
			continue
		}
		page := pages[bucket]
		if page == nil {
			data, e := r.readImmutable(ctx, address)
			if e != nil {
				return SnapshotRecords{}, e
			}
			page = &rowPage{}
			if e = json.Unmarshal(data, page); e != nil {
				return SnapshotRecords{}, e
			}
			if page.Entries == nil {
				return SnapshotRecords{}, fmt.Errorf("corrupt indexed snapshot page")
			}
			pages[bucket] = page
		}
		address, ok = page.Entries[key]
		if !ok {
			continue
		}
		data, e := r.readImmutable(ctx, address)
		if e != nil {
			return SnapshotRecords{}, e
		}
		var row storedRow
		if e = json.Unmarshal(data, &row); e != nil {
			return SnapshotRecords{}, e
		}
		if len(row.Value) == 0 {
			return SnapshotRecords{}, fmt.Errorf("empty indexed snapshot record")
		}
		result.Records[id] = row.Value
	}
	return result, nil
}

// SnapshotBackup stores exact committed workspace bytes in a checksummed,
// transport-independent envelope. Native memory outside the workspace is not
// part of this archive and requires an engine backup of its own.
type SnapshotBackup struct {
	Format  string `json:"format"`
	SHA256  string `json:"sha256"`
	Payload string `json:"payload"`
}
type snapshotBackupTransport interface {
	Load(context.Context) ([]byte, uint64, error)
	CompareAndSwap(context.Context, uint64, []byte) error
}

// BackupSnapshot reads one committed logical snapshot, including all domain
// fields and immutable branch state represented in that workspace.
func BackupSnapshot(ctx context.Context, tr snapshotBackupTransport) ([]byte, error) {
	data, _, err := tr.Load(ctx)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		data = []byte(`{}`)
	}
	var object map[string]json.RawMessage
	if err = json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	if object == nil {
		return nil, fmt.Errorf("snapshot backup requires a JSON object")
	}
	return json.Marshal(SnapshotBackup{Format: "antfly-workspace-backup-v1", SHA256: digest(data), Payload: string(data)})
}

// DecodeSnapshotBackup checks the envelope and returns the exact snapshot bytes
// for caller-owned schema and identity validation before restoring.
func DecodeSnapshotBackup(encoded []byte) ([]byte, error) {
	var archive SnapshotBackup
	if err := json.Unmarshal(encoded, &archive); err != nil {
		return nil, err
	}
	if archive.Format != "antfly-workspace-backup-v1" || archive.SHA256 != digest([]byte(archive.Payload)) {
		return nil, fmt.Errorf("invalid snapshot backup format or checksum")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(archive.Payload), &object); err != nil {
		return nil, err
	}
	if object == nil {
		return nil, fmt.Errorf("backup snapshot is not a JSON object")
	}
	return []byte(archive.Payload), nil
}

// RestoreSnapshotBackup publishes only against the caller's observed head. A
// concurrent writer rejects the restore; unknown outcomes are never replayed.
func RestoreSnapshotBackup(ctx context.Context, tr snapshotBackupTransport, encoded []byte, expected uint64) error {
	data, err := DecodeSnapshotBackup(encoded)
	if err != nil {
		return err
	}
	return tr.CompareAndSwap(ctx, expected, data)
}

// WriteDatabaseBackup delegates a complete physical database archive to the
// engine, including every table and memory namespace beyond this workspace.
func (r *rowTransport) WriteDatabaseBackup(ctx context.Context, writer io.Writer) error {
	engine, ok := r.documentTransport.(interface {
		WriteDatabaseBackup(context.Context, io.Writer) error
	})
	if !ok {
		return errUnsupported("database backup stream; remote engines use server backup jobs")
	}
	return engine.WriteDatabaseBackup(ctx, writer)
}
