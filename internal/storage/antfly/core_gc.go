package antfly

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/storage/backends"
)

// CollectWorkspaceGarbage fences the main catalog and every registered fork
// before collecting unreachable immutable history. Maintenance-aware clients
// refuse those heads. Interrupted maintenance is resumed through this same door.
// Pending fork creation must finish before collection can start.
func CollectWorkspaceGarbage(ctx context.Context, dir string, opts backends.OpenOptions) (SnapshotGCResult, error) {
	target, err := LoadTarget(dir)
	if err != nil {
		return SnapshotGCResult{}, err
	}
	cfg, err := configfile.LoadForDiscovery(dir)
	if err != nil || cfg == nil || cfg.Backend != Backend || cfg.ProjectID == "" {
		return SnapshotGCResult{}, fmt.Errorf("core collection requires native workspace metadata and project identity")
	}
	transport, err := OpenTransportWith(target, opts)
	if err != nil {
		return SnapshotGCResult{}, err
	}
	defer func() { _ = transport.Close() }()
	rows, ok := transport.(*rowTransport)
	if !ok {
		return SnapshotGCResult{}, errUnsupported("core collection requires row storage")
	}
	return rows.collectCoreGarbage(ctx, cfg.ProjectID)
}

type coreMaintenanceHead struct {
	key     string
	head    rowHead
	version uint64
}

func (r *rowTransport) maintenanceState(ctx context.Context, raw []byte, project string) (rowHead, *state, error) {
	var head rowHead
	if err := json.Unmarshal(raw, &head); err != nil {
		return head, nil, err
	}
	if head.StorageFormat != 2 || head.Fields == nil || head.Pages == nil {
		return head, nil, fmt.Errorf("core collection requires row format 2")
	}
	copy := head
	copy.Maintenance = ""
	readable, _ := json.Marshal(copy)
	data, err := r.restoreSnapshot(ctx, readable)
	if err != nil {
		return head, nil, err
	}
	st := emptyState()
	if err = json.Unmarshal(data, st); err != nil {
		return head, nil, err
	}
	if st.Format != 1 || st.Config == nil || st.Issues == nil || st.Edges == nil || st.Notes == nil || st.Config["project_id"] != project {
		return head, nil, fmt.Errorf("core collection format/project identity mismatch")
	}
	return head, st, nil
}

func (r *rowTransport) collectCoreGarbage(ctx context.Context, project string) (result SnapshotGCResult, err error) {
	if !r.domainProjection {
		return result, errUnsupported("core collection of generic workspace")
	}
	scanner, ok := r.documentTransport.(documentScanner)
	if !ok {
		return result, errUnsupported("core snapshot scan")
	}
	raw, version, err := r.documentTransport.Load(ctx)
	if err != nil {
		return result, err
	}
	head, catalog, err := r.maintenanceState(ctx, raw, project)
	if err != nil {
		return result, err
	}
	if catalog.Config["antfly.branch.parent"] != "" {
		return result, fmt.Errorf("collect core history from its main workspace")
	}
	branches := make([]Branch, 0, len(catalog.Branches))
	for name, branch := range catalog.Branches {
		if branch.Pending {
			return result, fmt.Errorf("finish pending fork %s before collection", name)
		}
		if branch.Name != name || branch.ID == "" || branch.Workspace == "" || branch.ProjectID != project || branch.Workspace == r.workspace {
			return result, fmt.Errorf("invalid fork catalog identity")
		}
		branches = append(branches, branch)
	}
	sort.Slice(branches, func(i, j int) bool { return branches[i].Workspace < branches[j].Workspace })
	token, err := randomIdentity()
	if err != nil {
		return result, err
	}
	held := []coreMaintenanceHead{}
	// Release forks before main. A lost collector can release only its own
	// unchanged head, and every delete is conditioned on the main owner version.
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		for i := len(held) - 1; i >= 0; i-- {
			lease := held[i]
			lease.head.Maintenance = ""
			encoded, _ := json.Marshal(lease.head)
			if e := writeSource(releaseCtx, r.documentTransport, lease.key, lease.version, encoded); e != nil {
				err = errors.Join(err, fmt.Errorf("release core maintenance %s: %w", lease.key, e))
			}
		}
	}()
	lock := func(key string, head rowHead, version uint64) error {
		head.Maintenance = token
		encoded, _ := json.Marshal(head)
		if e := writeSource(ctx, r.documentTransport, key, version, encoded); e != nil {
			return e
		}
		current, v, e := r.ReadDocument(ctx, key)
		if e != nil {
			return e
		}
		if !bytes.Equal(current, encoded) {
			return errConflict
		}
		held = append(held, coreMaintenanceHead{key: key, head: head, version: v})
		return nil
	}
	if err = lock(r.workspace, head, version); err != nil {
		return result, err
	}
	mainVersion := held[0].version
	seenWorkspaces := map[string]bool{r.workspace: true}
	snapshots := [][]byte{}
	for _, branch := range branches {
		if seenWorkspaces[branch.Workspace] {
			return result, fmt.Errorf("duplicate fork workspace")
		}
		seenWorkspaces[branch.Workspace] = true
		branchRaw, v, e := r.ReadDocument(ctx, branch.Workspace)
		if e != nil {
			return result, e
		}
		forkHead, fork, e := r.maintenanceState(ctx, branchRaw, project)
		if e != nil {
			return result, e
		}
		if fork.Config["antfly.branch.parent"] != r.workspace || fork.Config["antfly.branch.id"] != branch.ID {
			return result, fmt.Errorf("fork owner identity mismatch")
		}
		if err = lock(branch.Workspace, forkHead, v); err != nil {
			return result, err
		}
		snapshots = append(snapshots, branch.Base)
	}
	for _, lease := range held {
		copy := lease.head
		copy.Maintenance = ""
		b, _ := json.Marshal(copy)
		snapshots = append(snapshots, b)
	}
	live := map[string]bool{}
	seenHeads := map[string]bool{}
	// Retain bases recursively: a base's catalog may itself contain older bases.
	// Read uncached so a previously open reader cannot hide missing history.
	for len(snapshots) > 0 {
		snapshot := snapshots[len(snapshots)-1]
		snapshots = snapshots[:len(snapshots)-1]
		hash := digest(snapshot)
		if seenHeads[hash] {
			continue
		}
		seenHeads[hash] = true
		if len(seenHeads) > 10000 {
			return result, fmt.Errorf("core snapshot ancestry exceeds collection bound")
		}
		snapshotHead, e := readRowHead(snapshot)
		if e != nil {
			return result, fmt.Errorf("invalid core base snapshot: %w", e)
		}
		if snapshotHead == nil {
			return result, fmt.Errorf("core base snapshot must use row format 2")
		}
		for _, address := range snapshotHead.Pages {
			live[address] = true
			pageRaw, e := r.readImmutableUncached(ctx, address)
			if e != nil {
				return result, e
			}
			var page rowPage
			if e = json.Unmarshal(pageRaw, &page); e != nil || page.Entries == nil {
				return result, fmt.Errorf("invalid core maintenance page")
			}
			for key, address := range page.Entries {
				if live[address] && !strings.HasPrefix(key, "branches/") {
					continue
				}
				live[address] = true
				rowRaw, e := r.readImmutableUncached(ctx, address)
				if e != nil {
					return result, e
				}
				if strings.HasPrefix(key, "branches/") {
					var row storedRow
					var branch Branch
					if e = json.Unmarshal(rowRaw, &row); e != nil {
						return result, e
					}
					if e = json.Unmarshal(row.Value, &branch); e != nil {
						return result, e
					}
					if len(branch.Base) > 0 {
						snapshots = append(snapshots, branch.Base)
					}
				}
			}
		}
	}
	for workspace := range seenWorkspaces {
		prefix := "row:" + digest([]byte(workspace))[:32] + ":"
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
					return result, fmt.Errorf("invalid core collection scan ordering or namespace")
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
				if e = r.WriteDocuments(ctx, &mainVersion, deletes); e != nil {
					return result, e
				}
				result.Deleted += len(deletes)
			}
			after = keys[len(keys)-1]
		}
	}
	return result, nil
}
