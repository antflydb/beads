package antfly

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
)

type Branch struct {
	Name         string          `json:"name"`
	ID           string          `json:"id"`
	Workspace    string          `json:"workspace"`
	ProjectID    string          `json:"project_id"`
	BaseRevision uint64          `json:"base_revision"`
	Base         json.RawMessage `json:"base"`
	CreatedBy    string          `json:"created_by,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	Pending      bool            `json:"pending"`
	Directory    string          `json:"directory,omitempty"`
}
type Integration struct {
	Branch   string `json:"branch"`
	Digest   string `json:"digest"`
	Revision uint64 `json:"revision"`
}
type MergeResult struct {
	Branch         string   `json:"branch"`
	Revision       uint64   `json:"revision"`
	Applied        bool     `json:"applied"`
	AlreadyApplied bool     `json:"already_applied"`
	Conflicts      []string `json:"conflicts"`
}
type MergeConflict struct{ Paths []string }

func (e *MergeConflict) Error() string {
	return "branch merge conflicts: " + strings.Join(e.Paths, ", ")
}

var branchName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func randomIdentity() (string, error) {
	var b [16]byte
	_, err := rand.Read(b[:])
	return hex.EncodeToString(b[:]), err
}
func (s *Store) ListForks(ctx context.Context) ([]Branch, error) {
	st, _, e := s.load(ctx)
	if e != nil {
		return nil, e
	}
	out := []Branch{}
	for _, branch := range st.Branches {
		if s.localDir != "" {
			branch.Directory = filepath.Join(s.localDir, "branches", branch.Name)
		}
		out = append(out, branch)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
func (s *Store) Fork(ctx context.Context, name, actor string) (Branch, error) {
	if s.readOnly {
		return Branch{}, errors.New("Antfly store is read-only")
	}
	if !branchName.MatchString(name) || name == "main" || name == "." || name == ".." {
		return Branch{}, errors.New("branch name must contain 1–64 letters, digits, dots, underscores or dashes and must not be main")
	}
	if s.target == nil {
		return Branch{}, errUnsupported("Fork: caller-owned transport has no catalog routing")
	}
	rows, ok := s.transport.(*rowTransport)
	if !ok {
		return Branch{}, errUnsupported("Fork: requires row storage")
	}
	var branch Branch
	for attempt := 0; attempt < 16; attempt++ {
		st, v, err := s.load(ctx)
		if err != nil {
			return branch, err
		}
		if err = mainClaims(st); err != nil {
			return branch, err
		}
		if existing, ok := st.Branches[name]; ok {
			branch = existing
			break
		}
		base, version, err := rows.documentTransport.Load(ctx)
		if err != nil {
			return branch, err
		}
		if version != v {
			continue
		}
		head, err := readRowHead(base)
		if err != nil {
			return branch, err
		}
		if head == nil {
			if err = s.SetConfig(ctx, "antfly.row-format", "2"); err != nil {
				return branch, err
			}
			continue
		}
		id, err := randomIdentity()
		if err != nil {
			return branch, err
		}
		branch = Branch{Name: name, ID: id, Workspace: "fork:" + digest([]byte(st.Config["project_id"]))[:16] + ":" + id, ProjectID: st.Config["project_id"], BaseRevision: st.Revision, Base: base, CreatedBy: actor, CreatedAt: time.Now().UTC(), Pending: true}
		if s.localDir != "" {
			branch.Directory = filepath.Join(s.localDir, "branches", name)
		}
		err = s.mutate(ctx, func(current *state) error {
			if current.Revision != st.Revision {
				return errConflict
			}
			if _, exists := current.Branches[name]; exists {
				return errConflict
			}
			stored := branch
			stored.Directory = ""
			current.Branches[name] = stored
			return nil
		})
		if errors.Is(err, errConflict) {
			continue
		}
		if err != nil {
			return branch, err
		}
		break
	}
	if branch.ID == "" {
		return branch, errConflict
	}
	if s.localDir != "" {
		branch.Directory = filepath.Join(s.localDir, "branches", name)
	}
	target := *s.target
	target.Workspace = branch.Workspace
	tr, err := s.openTarget(ctx, target, false)
	if err != nil {
		return branch, err
	}
	defer func() { _ = tr.Close() }()
	raw, _, err := tr.Load(ctx)
	if err != nil {
		return branch, err
	}
	if len(raw) == 0 {
		source, err := rows.restoreSnapshot(ctx, branch.Base)
		if err != nil {
			return branch, err
		}
		var seed state
		if err = json.Unmarshal(source, &seed); err != nil {
			return branch, err
		}
		seed.initialize()
		seed.Config["antfly.branch.parent"] = s.target.Workspace
		seed.Config["antfly.branch.id"] = branch.ID
		seed.Branches = map[string]Branch{}
		seed.Integrations = map[string]Integration{}
		seed.Events = nil
		seed.Knowledge = nil
		seed.KnowledgePreparing = nil
		for id, record := range seed.Issues {
			if storageops.IsWisp(&record.Issue) {
				delete(seed.Issues, id)
				continue
			}
			record.Issue.HeartbeatAt = nil
			record.Issue.LeaseExpiresAt = nil
		}
		for key, edge := range seed.Edges {
			if seed.Issues[edge.IssueID] == nil || seed.Issues[edge.DependsOnID] == nil {
				delete(seed.Edges, key)
			}
		}
		payload, err := json.Marshal(seed)
		if err != nil {
			return branch, err
		}
		err = tr.(*rowTransport).compareAndSwapFrom(ctx, 0, payload, branch.Base)
		if err != nil && !errors.Is(err, errConflict) {
			return branch, err
		}
	}
	raw, _, err = tr.Load(ctx)
	if err != nil {
		return branch, err
	}
	var existing state
	if err = json.Unmarshal(raw, &existing); err != nil {
		return branch, err
	}
	if existing.Config["project_id"] != branch.ProjectID || existing.Config["antfly.branch.id"] != branch.ID {
		return branch, errors.New("branch destination identity mismatch")
	}
	branch.Pending = false
	if err = s.mutate(ctx, func(st *state) error {
		current, ok := st.Branches[name]
		if !ok || current.ID != branch.ID {
			return errors.New("branch identity changed")
		}
		stored := branch
		stored.Directory = ""
		st.Branches[name] = stored
		return nil
	}); err != nil {
		return branch, err
	}
	if branch.Directory != "" {
		// Initialize is resumable here: an existing matching routing file is read,
		// never overwritten. It cannot retarget a checkout under an active worker.
		if routing, err := LoadTarget(branch.Directory); err == nil {
			if routing.Workspace != target.Workspace || routing.Table != target.Table || routing.Path != target.Path || routing.URL != target.URL || routing.Database != target.Database || routing.Namespace != target.Namespace {
				return branch, errors.New("existing branch checkout selects a different target")
			}
			loaded, err := openWorkspaceWith(ctx, branch.Directory, s.openOptions)
			if err != nil {
				return branch, err
			}
			_ = loaded.Close()
		} else if err = InitializeWith(ctx, branch.Directory, target, existing.Config["issue_prefix"], s.openOptions); err != nil {
			return branch, err
		}
	}
	return branch, nil
}
func (r *rowTransport) restoreSnapshot(ctx context.Context, snapshot []byte) ([]byte, error) {
	head, err := readRowHead(snapshot)
	if err != nil {
		return nil, err
	}
	if head == nil {
		return nil, errors.New("branch base must use row storage")
	}
	refs, err := r.references(ctx, head)
	if err != nil {
		return nil, err
	}
	values := map[string]json.RawMessage{}
	for id, key := range refs {
		b, err := r.readImmutable(ctx, key)
		if err != nil {
			return nil, err
		}
		var row storedRow
		if err = json.Unmarshal(b, &row); err != nil {
			return nil, err
		}
		values[id] = row.Value
	}
	return joinState(head.Fields, values, r.domainProjection)
}
func (s *Store) MergeFork(ctx context.Context, name, key, actor string, dryRun bool) (out MergeResult, err error) {
	if key == "" || strings.TrimSpace(actor) == "" {
		return out, errors.New("merge requires an idempotency key and actor")
	}
	if s.target == nil {
		return out, errUnsupported("Merge: missing catalog routing")
	}
	initial, _, err := s.load(ctx)
	if err != nil {
		return out, err
	}
	if err = mainClaims(initial); err != nil {
		return out, err
	}
	branch, ok := initial.Branches[name]
	if !ok || branch.Pending {
		return out, errors.New("branch does not exist or is still being created")
	}
	rows, ok := s.transport.(*rowTransport)
	if !ok {
		return out, errUnsupported("Merge: requires row storage")
	}
	target := *s.target
	target.Workspace = branch.Workspace
	tr, err := s.openTarget(ctx, target, false)
	if err != nil {
		return out, err
	}
	defer func() { _ = tr.Close() }()
	var data, snapshot []byte
	consistent := false
	for attempt := 0; attempt < 16; attempt++ {
		var version uint64
		data, version, err = tr.Load(ctx)
		if err != nil {
			return out, err
		}
		var snapshotVersion uint64
		snapshot, snapshotVersion, err = underlyingTransport(tr).Load(ctx)
		if err != nil {
			return out, err
		}
		if version == snapshotVersion {
			consistent = true
			break
		}
	}
	if !consistent {
		return out, errConflict
	}
	var fork state
	if err = json.Unmarshal(data, &fork); err != nil {
		return out, err
	}
	fork.initialize()
	if fork.Config["project_id"] != branch.ProjectID || fork.Config["antfly.branch.id"] != branch.ID {
		return out, errors.New("branch lineage mismatch")
	}
	fingerprint := digest(data)
	apply := func(main *state) error {
		out = MergeResult{Branch: name, Revision: main.Revision, Conflicts: []string{}}
		if receipt, ok := main.Integrations[key]; ok {
			if receipt.Branch != branch.ID || receipt.Digest != fingerprint {
				return errors.New("merge idempotency key was already used for a different branch state")
			}
			out.AlreadyApplied = true
			out.Revision = receipt.Revision
			return nil
		}
		if main.Config["project_id"] != branch.ProjectID {
			return errors.New("merge project identity mismatch")
		}
		currentBranch, exists := main.Branches[name]
		if !exists || currentBranch.ID != branch.ID {
			return errors.New("branch integration lineage changed")
		}
		baseData, e := rows.restoreSnapshot(ctx, currentBranch.Base)
		if e != nil {
			return e
		}
		var base state
		if e = json.Unmarshal(baseData, &base); e != nil {
			return e
		}
		base.initialize()
		candidate, conflicts, e := mergeStates(&base, main, &fork, actor)
		if e != nil {
			return e
		}
		if len(conflicts) > 0 {
			out.Conflicts = conflicts
			if dryRun {
				return nil
			}
			return &MergeConflict{Paths: conflicts}
		}
		if dryRun {
			return nil
		}
		previousIssues, previousNotes := main.Issues, main.Notes
		main.Issues = candidate.Issues
		main.Edges = candidate.Edges
		main.Notes = candidate.Notes
		main.Config = candidate.Config
		for id, record := range main.Issues {
			if old, ok := previousIssues[id]; !ok || !sameDomainIssue(&old.Issue, &record.Issue) {
				main.touch(&record.Issue)
				main.audit("merge", id, actor)
			}
		}
		for id := range previousIssues {
			if main.Issues[id] == nil {
				main.audit("delete", id, actor)
			}
		}
		// Exact-note changes carry replayable accepted guidance / forgetting events.
		for id, text := range main.Notes {
			if previousNotes[id] != text {
				main.audit("remember", id, actor)
			}
		}
		for id := range previousNotes {
			if _, ok := main.Notes[id]; !ok {
				main.audit("forget", id, actor)
			}
		}
		main.reblock()
		currentBranch.Base = snapshot
		currentBranch.BaseRevision = fork.Revision
		main.Branches[name] = currentBranch
		main.Integrations[key] = Integration{Branch: branch.ID, Digest: fingerprint, Revision: main.Revision + 1}
		out.Applied = true
		out.Revision = main.Revision + 1
		return nil
	}
	if dryRun {
		err = apply(initial)
	} else {
		err = s.mutate(ctx, apply)
	}
	return out, err
}

// Domain equality deliberately excludes lease and revision bookkeeping.
func issueObject(issue *types.Issue) map[string]json.RawMessage {
	b, _ := json.Marshal(issue)
	var object map[string]json.RawMessage
	_ = json.Unmarshal(b, &object)
	for _, key := range []string{"updated_at", "heartbeat_at", "lease_expires_at", "started_at", "closed_at", "is_blocked", "dependencies", "assignee"} {
		delete(object, key)
	}
	// Comments merge by their globally unique identity, not array position.
	var comments []*types.Comment
	if raw, ok := object["comments"]; ok {
		_ = json.Unmarshal(raw, &comments)
	}
	byID := map[string]*types.Comment{}
	for _, c := range comments {
		byID[c.ID] = c
	}
	object["comments"], _ = json.Marshal(byID)
	labels := map[string]bool{}
	for _, label := range issue.Labels {
		labels[label] = true
	}
	object["labels"], _ = json.Marshal(labels)

	return object
}
func sameDomainIssue(a, b *types.Issue) bool {
	left, _ := json.Marshal(issueObject(a))
	right, _ := json.Marshal(issueObject(b))
	return string(left) == string(right)
}
func mergeStates(base, main, fork *state, actor string) (*state, []string, error) {
	b, _ := json.Marshal(main)
	out := emptyState()
	if err := json.Unmarshal(b, out); err != nil {
		return nil, nil, err
	}
	out.initialize()
	conflicts := []string{}
	ids := map[string]bool{}
	for id := range base.Issues {
		ids[id] = true
	}
	for id := range main.Issues {
		ids[id] = true
	}
	for id := range fork.Issues {
		ids[id] = true
	}
	for id := range ids {
		br, mr, fr := base.Issues[id], main.Issues[id], fork.Issues[id]
		if (br != nil && storageops.IsWisp(&br.Issue)) || (mr != nil && storageops.IsWisp(&mr.Issue)) {
			continue
		}
		encode := func(record *record) json.RawMessage {
			if record == nil {
				return nil
			}
			b, _ := json.Marshal(issueObject(&record.Issue))
			return b
		}
		merged, paths := mergeValue("issues/"+id, encode(br), encode(mr), encode(fr))
		conflicts = append(conflicts, paths...)
		if len(paths) > 0 {
			continue
		}
		if merged == nil {
			if mr != nil && mr.Issue.Status == types.StatusInProgress && mr.Issue.LeaseExpiresAt != nil {
				conflicts = append(conflicts, "issues/"+id+"/live_claim")
				continue
			}
			delete(out.Issues, id)
			continue
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal(merged, &object); err != nil {
			return nil, nil, err
		}
		var comments map[string]*types.Comment
		if raw, ok := object["comments"]; ok {
			if err := json.Unmarshal(raw, &comments); err != nil {
				return nil, nil, err
			}
			delete(object, "comments")
		}
		var labels map[string]bool
		if raw, ok := object["labels"]; ok {
			if err := json.Unmarshal(raw, &labels); err != nil {
				return nil, nil, err
			}
			delete(object, "labels")
		}
		payload, _ := json.Marshal(object)
		var issue types.Issue
		if err := json.Unmarshal(payload, &issue); err != nil {
			return nil, nil, err
		}
		for label := range labels {
			issue.Labels = append(issue.Labels, label)
		}
		sort.Strings(issue.Labels)
		for _, c := range comments {
			issue.Comments = append(issue.Comments, c)
		}
		sort.Slice(issue.Comments, func(i, j int) bool {
			a, b := issue.Comments[i], issue.Comments[j]
			if a.CreatedAt.Equal(b.CreatedAt) {
				return a.ID < b.ID
			}
			return a.CreatedAt.Before(b.CreatedAt)
		})
		version := int64(0)
		if mr != nil {
			issue.Assignee = mr.Issue.Assignee
			issue.StartedAt = mr.Issue.StartedAt
			issue.HeartbeatAt = mr.Issue.HeartbeatAt
			issue.LeaseExpiresAt = mr.Issue.LeaseExpiresAt
			issue.UpdatedAt = mr.Issue.UpdatedAt
			version = mr.Version
		}
		if issue.Status == types.StatusClosed {
			if mr != nil && mr.Issue.Status == types.StatusClosed {
				issue.ClosedAt = mr.Issue.ClosedAt
			} else if fr != nil {
				issue.ClosedAt = fr.Issue.ClosedAt
			}
			issue.HeartbeatAt = nil
			issue.LeaseExpiresAt = nil
		}
		issue.RowVersion = version
		out.Issues[id] = &record{Issue: issue, Version: version}
	}
	mergeMap := func(kind string, a, b, c any, destination any) error {
		ar, _ := json.Marshal(a)
		mr, _ := json.Marshal(b)
		fr, _ := json.Marshal(c)
		merged, paths := mergeValue(kind, ar, mr, fr)
		conflicts = append(conflicts, paths...)
		if len(paths) > 0 {
			return nil
		}
		return json.Unmarshal(merged, destination)
	}
	domainEdges := func(st *state) map[string]*types.Dependency {
		out := map[string]*types.Dependency{}
		for key, edge := range st.Edges {
			a, b := st.Issues[edge.IssueID], st.Issues[edge.DependsOnID]
			if (a != nil && storageops.IsWisp(&a.Issue)) || (b != nil && storageops.IsWisp(&b.Issue)) {
				continue
			}
			out[key] = edge
		}
		return out
	}
	if err := mergeMap("edges", domainEdges(base), domainEdges(main), domainEdges(fork), &out.Edges); err != nil {
		return nil, nil, err
	}
	for key, edge := range main.Edges {
		a, b := main.Issues[edge.IssueID], main.Issues[edge.DependsOnID]
		if (a != nil && storageops.IsWisp(&a.Issue)) || (b != nil && storageops.IsWisp(&b.Issue)) {
			out.Edges[key] = edge
		}
	}
	if err := mergeMap("notes", base.Notes, main.Notes, fork.Notes, &out.Notes); err != nil {
		return nil, nil, err
	}
	settings := func(st *state) map[string]string {
		out := map[string]string{}
		for k, v := range st.Config {
			if !strings.HasPrefix(k, "antfly.") && k != "project_id" && k != "issue_prefix" {
				out[k] = v
			}
		}
		return out
	}
	config := map[string]string{}
	if err := mergeMap("config", settings(base), settings(main), settings(fork), &config); err != nil {
		return nil, nil, err
	}
	for k := range settings(main) {
		delete(out.Config, k)
	}
	for k, v := range config {
		out.Config[k] = v
	}
	if len(conflicts) > 0 {
		sort.Strings(conflicts)
		return out, conflicts, nil
	}
	check := emptyState()
	check.Issues = out.Issues
	for _, edge := range out.Edges {
		if e := check.addEdge(edge.IssueID, edge.DependsOnID, edge.Type, actor); e != nil {
			return nil, nil, fmt.Errorf("merged graph invariant: %w", e)
		}
	}
	for id, record := range out.Issues {
		if e := record.Issue.Validate(); e != nil {
			return nil, nil, e
		}
		if old := main.Issues[id]; old != nil && old.Issue.Status != types.StatusClosed && record.Issue.Status == types.StatusClosed {
			if e := out.canClose(&old.Issue, actor, false); e != nil {
				return nil, nil, e
			}
		}
	}
	out.reblock()
	return out, nil, nil
}
func mergeValue(path string, base, main, branch json.RawMessage) (json.RawMessage, []string) {
	equal := func(a, b json.RawMessage) bool {
		if a == nil || b == nil {
			return a == nil && b == nil
		}
		return jsonEqual(a, b)
	}
	if equal(branch, base) {
		return main, nil
	}
	if equal(main, base) || equal(main, branch) {
		return branch, nil
	}
	if len(base) > 0 && base[0] == '{' && len(main) > 0 && main[0] == '{' && len(branch) > 0 && branch[0] == '{' {
		var b, m, f map[string]json.RawMessage
		_ = json.Unmarshal(base, &b)
		_ = json.Unmarshal(main, &m)
		_ = json.Unmarshal(branch, &f)
		keys := map[string]bool{}
		for k := range b {
			keys[k] = true
		}
		for k := range m {
			keys[k] = true
		}
		for k := range f {
			keys[k] = true
		}
		out := map[string]json.RawMessage{}
		conflicts := []string{}
		for key := range keys {
			v, paths := mergeValue(path+"/"+key, b[key], m[key], f[key])
			conflicts = append(conflicts, paths...)
			if v != nil {
				out[key] = v
			}
		}
		raw, _ := json.Marshal(out)
		return raw, conflicts
	}
	return nil, []string{path}
}
