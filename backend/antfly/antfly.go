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
