package antfly

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ErrSnapshotMaintenance is a durable maintenance fence. Readers may retry
// after collection finishes. Interrupted collection is resumed explicitly.
var ErrSnapshotMaintenance = errors.New("Antfly workspace maintenance in progress; resume garbage collection")

// SnapshotGCResult reports physical immutable-document collection, not domain
// record retention. The live head and all its referenced rows remain intact.
type SnapshotGCResult struct{ Scanned, Deleted, Retained int }
type documentScanner interface {
	ScanDocuments(context.Context, string, string, int) ([]string, error)
}

// CollectGarbage collects only this dedicated generic workspace's immutable
// address namespace. Core workspaces are refused: branches can share records.
// Old readers may fail while obsolete rows are removed; they never receive a
// partially reconstructed snapshot. A crashed collector leaves a resumable
// fence. Every delete batch and fence release condition on the owner's head.

func (r *rowTransport) CollectGarbage(ctx context.Context) (SnapshotGCResult, error) {
	return r.collectGarbage(ctx, nil)
}

// CollectGarbageChecked validates caller-owned schema metadata even when a
// crashed collector's fence prevents ordinary snapshot reads.
func (r *rowTransport) CollectGarbageChecked(ctx context.Context, validate func(map[string]json.RawMessage) error) (SnapshotGCResult, error) {
	if validate == nil {
		return SnapshotGCResult{}, fmt.Errorf("nil snapshot maintenance validator")
	}
	return r.collectGarbage(ctx, validate)
}
func (r *rowTransport) collectGarbage(ctx context.Context, validate func(map[string]json.RawMessage) error) (result SnapshotGCResult, err error) {
	if r.domainProjection {
		return result, errUnsupported("garbage collection of branching workspaces")
	}
	scanner, ok := r.documentTransport.(documentScanner)
	if !ok {
		return result, errUnsupported("snapshot scan")
	}
	raw, v, err := r.documentTransport.Load(ctx)
	if err != nil || len(raw) == 0 {
		return result, err
	}
	var head rowHead
	if err = json.Unmarshal(raw, &head); err != nil {
		return result, err
	}
	if head.StorageFormat != 2 || head.Fields == nil || head.Pages == nil {
		return result, fmt.Errorf("snapshot must use row format 2 before collection")
	}
	if head.Fields[coreIndexField] != nil || head.Fields["format"] != nil && head.Fields["revision"] != nil {
		return result, errUnsupported("core history requires catalog-wide collection")
	}
	if validate != nil {
		if err = validate(head.Fields); err != nil {
			return result, err
		}
	}
	var token [16]byte
	if _, err = rand.Read(token[:]); err != nil {
		return result, err
	}
	head.Maintenance = hex.EncodeToString(token[:])
	locked, _ := json.Marshal(head)
	if err = r.WriteDocuments(ctx, &v, map[string]json.RawMessage{r.workspace: locked}); err != nil {
		return result, err
	}
	raw, v, err = r.documentTransport.Load(ctx)
	if err != nil {
		return result, err
	}
	if !bytes.Equal(raw, locked) {
		var actual rowHead
		if json.Unmarshal(raw, &actual) != nil || actual.Maintenance != head.Maintenance {
			return result, errConflict
		}
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		head.Maintenance = ""
		released, _ := json.Marshal(head)
		releaseErr := r.WriteDocuments(releaseCtx, &v, map[string]json.RawMessage{r.workspace: released})
		if releaseErr != nil {
			err = errors.Join(err, fmt.Errorf("releasing maintenance fence: %w", releaseErr))
		}
	}()
	live := map[string]bool{}
	prefix := "row:" + digest([]byte(r.workspace))[:32] + ":"
	for _, address := range head.Pages {
		live[address] = true
		pageBytes, e := r.readImmutableUncached(ctx, address)
		if e != nil {
			return result, e
		}
		var page rowPage
		if e = json.Unmarshal(pageBytes, &page); e != nil {
			return result, e
		}
		if page.Entries == nil {
			return result, fmt.Errorf("corrupt maintenance page")
		}
		for _, address := range page.Entries {
			if !strings.HasPrefix(address, prefix) {
				return result, fmt.Errorf("shared immutable rows require multi-workspace collection")
			}
			if _, e = r.readImmutableUncached(ctx, address); e != nil {
				return result, e
			}
			live[address] = true
		}
	}
	after := prefix
	upper := strings.TrimSuffix(prefix, ":") + ";"
	for {
		keys, e := scanner.ScanDocuments(ctx, after, upper, 256)
		if e != nil {
			return result, e
		}
		if len(keys) == 0 {
			break
		}
		deletes := map[string]json.RawMessage{}
		previous := after
		for _, key := range keys {
			if key <= previous || !strings.HasPrefix(key, prefix) {
				return result, fmt.Errorf("invalid snapshot scan ordering or namespace")
			}
			previous = key
			result.Scanned++
			if live[key] {
				result.Retained++
			} else {
				deletes[key] = nil
			}
		}
		if len(deletes) > 0 {
			if e = r.WriteDocuments(ctx, &v, deletes); e != nil {
				return result, e
			}
			result.Deleted += len(deletes)
		}
		after = keys[len(keys)-1]
	}
	return result, nil
}

func (r *remote) ScanDocuments(ctx context.Context, from, to string, limit int) ([]string, error) {
	raw, _, _, e := r.call(ctx, http.MethodPost, "/db/v1/tables/"+url.PathEscape(r.target.Table)+"/documents", map[string]any{"from": from, "to": to, "exclusive_to": true, "limit": limit})
	if e != nil {
		return nil, e
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	keys := []string{}
	for {
		var row struct {
			ID string `json:"_id"`
		}
		if e = decoder.Decode(&row); e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
		if row.ID == "" {
			return nil, fmt.Errorf("scan omitted document key")
		}
		keys = append(keys, row.ID)
	}
	return keys, nil
}
