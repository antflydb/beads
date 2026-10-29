// Package antfly registers and provisions the native Antfly Beads backend.
// Import this package and call Register once before opening any workspace.
package antfly

import (
	"context"
	"encoding/json"

	"github.com/steveyegge/beads/backend"
	impl "github.com/steveyegge/beads/internal/storage/antfly"
)

type Target = impl.Target
type Transport = impl.Transport

func Register() { impl.Register() }

// New adapts a caller-owned transport. Closing the returned store closes it.
func New(transport Transport, readOnly bool) backend.DoltStorage {
	return impl.New(transport, readOnly)
}

// Initialize provisions or attaches a workspace, recording routing only after
// its identity has been checked. Existing backends are never overwritten.
func Initialize(ctx context.Context, dir string, target Target, prefix string) error {
	return impl.Initialize(ctx, dir, target, prefix)
}

// Memory invokes the native memory plane independently of exact Beads notes.
func Memory(ctx context.Context, dir, space, action, key string, body json.RawMessage) (json.RawMessage, error) {
	return impl.MemoryWorkspace(ctx, dir, space, action, key, body)
}

// Workspace extends the standard Beads engine with Antfly-native operations.
// Gas City can open one per city without changing process-global environment.
type Workspace interface {
	backend.DoltStorage
	EntityMapping(context.Context, string, string) (EntityMapping, error)
	ExecuteFactory(context.Context, string, []string, json.RawMessage, string) (json.RawMessage, error)
	Fork(context.Context, string, string) (Branch, error)
	ListForks(context.Context) ([]Branch, error)
	MergeFork(context.Context, string, string, string, bool) (MergeResult, error)
	ConfigureKnowledge(context.Context, KnowledgeConfig) error
	KnowledgeStatus(context.Context) (KnowledgeStatus, error)
	SyncKnowledge(context.Context, int) (KnowledgeStatus, error)
	KnowledgeOperation(context.Context, string, string, json.RawMessage) (json.RawMessage, error)
	Context(context.Context, ContextRequest) (ContextResult, error)
}
type EntityMapping = impl.EntityMapping
type Branch = impl.Branch
type MergeResult = impl.MergeResult
type MergeConflict = impl.MergeConflict
type KnowledgeConfig = impl.KnowledgeConfig
type KnowledgeStatus = impl.KnowledgeStatus
type ContextRequest = impl.ContextRequest
type ContextResult = impl.ContextResult
type TokenCredential = impl.TokenCredential

func Open(ctx context.Context, dir string, opts backend.OpenOptions) (Workspace, error) {
	return impl.OpenStore(ctx, dir, opts)
}
func InitializeWith(ctx context.Context, dir string, target Target, prefix string, opts backend.OpenOptions) error {
	return impl.InitializeWith(ctx, dir, target, prefix, opts)
}

// SnapshotTransport stores dedicated JSON snapshots using native OCC. Fields
// whose values are objects become per-key immutable records, so the workspace
// has no aggregate-document size limit. Each encoded record is bounded to 4 MiB.
// Use a dedicated workspace and schema; do not open a Beads issue workspace here.
type SnapshotTransport interface {
	Load(context.Context) ([]byte, uint64, error)
	CompareAndSwap(context.Context, uint64, []byte) error
	Close() error
}

// ErrSnapshotConflict means publication was positively rejected and may be
// retried against a freshly loaded snapshot. Other errors may be indeterminate.
var ErrSnapshotConflict = impl.ErrSnapshotConflict

// OpenSnapshot opens a native embedded or remote snapshot workspace. It creates
// the backing table if absent and preserves caller-owned credentials and clients.
func OpenSnapshot(ctx context.Context, target Target, opts backend.OpenOptions) (SnapshotTransport, error) {
	return impl.OpenSnapshot(ctx, target, opts)
}

// SnapshotRecords contains selected records and schema fields from one head.
type SnapshotRecords = impl.SnapshotRecords

// IndexedSnapshotTransport supports bounded point and batch reads without
// reconstructing a whole workspace. Expected versions pin multi-stage lookups.
type IndexedSnapshotTransport interface {
	SnapshotTransport
	ReadSnapshotRecords(context.Context, string, []string, *uint64) (SnapshotRecords, error)
}

// BackupSnapshot archives one committed logical workspace; native memory
// outside that workspace requires a separate engine backup.
func BackupSnapshot(ctx context.Context, tr SnapshotTransport) ([]byte, error) {
	return impl.BackupSnapshot(ctx, tr)
}

// DecodeSnapshotBackup validates the checksum before caller-owned schema checks.
func DecodeSnapshotBackup(encoded []byte) ([]byte, error) { return impl.DecodeSnapshotBackup(encoded) }

// RestoreSnapshotBackup conditionally publishes a previously validated archive.
func RestoreSnapshotBackup(ctx context.Context, tr SnapshotTransport, encoded []byte, expected uint64) error {
	return impl.RestoreSnapshotBackup(ctx, tr, encoded, expected)
}
