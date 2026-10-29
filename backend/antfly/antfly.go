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
