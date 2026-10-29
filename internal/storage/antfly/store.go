package antfly

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/backends"
	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/internal/workapi/storeversionreconciler"
	"github.com/steveyegge/beads/internal/workapi/storeworkspaceconfig"
	"github.com/steveyegge/beads/issueops"
	"github.com/steveyegge/beads/memoryops"
)

var errIndeterminate = storage.ErrCommitIndeterminate

// State is a transaction-local domain view. Separate immutable rows are
// published with one workspace OCC fence by the transport.
type state struct {
	Branches           map[string]Branch            `json:"branches"`
	Integrations       map[string]Integration       `json:"integrations"`
	Knowledge          *KnowledgeState              `json:"knowledge,omitempty"`
	KnowledgePreparing *KnowledgePreparation        `json:"knowledge_preparing,omitempty"`
	Format             int                          `json:"format"`
	Revision           uint64                       `json:"revision"`
	Config             map[string]string            `json:"config"`
	Issues             map[string]*record           `json:"issues"`
	Edges              map[string]*types.Dependency `json:"edges"`
	Notes              map[string]string            `json:"notes"`
	Events             []event                      `json:"events"`
}

type record struct {
	Issue   types.Issue `json:"issue"`
	Version int64       `json:"version"`
}
type event struct {
	Sequence  uint64       `json:"sequence"`
	Operation string       `json:"operation"`
	ID        string       `json:"id"`
	Actor     string       `json:"actor"`
	At        time.Time    `json:"at"`
	Issue     *types.Issue `json:"issue,omitempty"`
	Content   string       `json:"content,omitempty"`
}

func (st *state) initialize() {
	if st.Branches == nil {
		st.Branches = map[string]Branch{}
	}
	if st.Integrations == nil {
		st.Integrations = map[string]Integration{}
	}
}
func emptyState() *state {
	return &state{Branches: map[string]Branch{}, Integrations: map[string]Integration{}, Format: 1, Config: map[string]string{}, Issues: map[string]*record{}, Edges: map[string]*types.Dependency{}, Notes: map[string]string{}}
}

type Store struct {
	unsupportedDoltStorage
	transport       Transport
	target          *Target
	openOptions     backends.OpenOptions
	readOnly        bool
	localDir        string
	expectedProject string
	memorySpace     string
	localMu         sync.Mutex
	local           map[string]string
}

var _ storage.DoltStorage = (*Store)(nil)

func New(transport Transport, readOnly bool) *Store {
	return &Store{transport: transport, readOnly: readOnly}
}

// OpenReadOnly is the classified-command hook, which must allow ready --claim.
// A caller that needs a write-refusing store can use New(transport, true).
func Register() {
	backends.Register(Backend, backends.Backend{Open: openWorkspace, OpenReadOnly: openWorkspace, OpenWith: openWorkspaceWith, WorkspaceIsBeadsDir: true})
}

func openWorkspace(ctx context.Context, dir string) (storage.DoltStorage, error) {
	return openWorkspaceWith(ctx, dir, backends.OpenOptions{})
}
func openWorkspaceWith(ctx context.Context, dir string, opts backends.OpenOptions) (storage.DoltStorage, error) {
	t, err := LoadTarget(dir)
	if err != nil {
		return nil, err
	}
	transport, err := OpenTransportWith(t, opts)
	if err != nil {
		return nil, err
	}
	s := New(transport, false)
	s.localDir = dir
	s.target = &t
	s.openOptions = opts
	cfg, err := configfile.LoadForDiscovery(dir)
	if err != nil || cfg == nil || cfg.Backend != Backend || cfg.ProjectID == "" {
		_ = transport.Close()
		return nil, errors.New("Antfly workspace requires valid metadata.json with a project identity")
	}
	s.expectedProject = cfg.ProjectID
	database, namespace := t.Database, t.Namespace
	if database == "" {
		database = "default"
	}
	if namespace == "" {
		namespace = "public"
	}
	sum := sha256.Sum256([]byte(database + "\x00" + namespace + "\x00" + t.Table + "\x00" + t.Workspace + "\x00" + cfg.ProjectID))
	s.memorySpace = fmt.Sprintf("beads-%x", sum[:16])
	st, _, err := s.load(ctx)
	if err == nil && st.Config["issue_prefix"] == "" {
		err = storage.ErrNotInitialized
	}
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) load(ctx context.Context) (*state, uint64, error) {
	b, v, err := s.transport.Load(ctx)
	if err != nil {
		return nil, 0, err
	}
	st := emptyState()
	if len(b) == 0 {
		if s.expectedProject != "" {
			return nil, 0, storage.ErrNotInitialized
		}
		return st, v, nil
	}
	if err = json.Unmarshal(b, st); err != nil {
		return nil, 0, fmt.Errorf("decode Antfly workspace: %w", err)
	}
	st.initialize()
	if st.Format != 1 || st.Config == nil || st.Issues == nil || st.Edges == nil || st.Notes == nil {
		return nil, 0, errors.New("unsupported or corrupt Antfly workspace format")
	}
	if s.expectedProject != "" && st.Config["project_id"] != s.expectedProject {
		return nil, 0, errors.New("Antfly workspace project identity does not match metadata.json")
	}
	for _, r := range st.Issues {
		if r == nil {
			return nil, 0, errors.New("corrupt Antfly issue record")
		}
		r.Issue.RowVersion = r.Version
	}
	return st, v, nil
}

func (s *Store) mutate(ctx context.Context, fn func(*state) error) error {
	if s.readOnly {
		return errors.New("Antfly store is read-only")
	}
	for attempt := 0; attempt < 16; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		st, v, err := s.load(ctx)
		if err != nil {
			return err
		}
		before, err := json.Marshal(st)
		if err != nil {
			return err
		}
		if err = fn(st); err != nil {
			return err
		}
		for _, r := range st.Issues {
			r.Version = r.Issue.RowVersion
		}
		after, err := json.Marshal(st)
		if err != nil {
			return err
		}
		if string(before) == string(after) {
			return nil
		}
		st.Revision++
		after, err = json.Marshal(st)
		if err != nil {
			return err
		}
		err = s.transport.CompareAndSwap(ctx, v, after)
		if !errors.Is(err, errConflict) {
			return err
		}
		// Retry only a proved abort, never an ambiguous transport outcome.
		timer := time.NewTimer(time.Duration(attempt+1) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("Antfly workspace contention: %w", errConflict)
}

func errUnsupported(op string) error                                  { return &storage.ErrUnsupported{Op: op, Backend: Backend} }
func (s *Store) Close() error                                         { return s.transport.Close() }
func (s *Store) Flush(context.Context) error                          { return nil }
func (s *Store) Commit(context.Context, string) error                 { return nil }
func (s *Store) CommitWithConfig(context.Context, string) error       { return nil }
func (s *Store) CommitConfigOnly(context.Context, string) error       { return nil }
func (s *Store) CommitConfigUserKVOnly(context.Context, string) error { return nil }
func (s *Store) CommitAll(context.Context, string) (bool, error)      { return false, nil }
func (s *Store) CommitPending(context.Context, string) (bool, error)  { return false, nil }
func (s *Store) GetCurrentCommit(ctx context.Context) (string, error) {
	st, _, err := s.load(ctx)
	if err != nil {
		return "", err
	}
	return "antfly:" + strconv.FormatUint(st.Revision, 10), nil
}
func (s *Store) GetUnderlyingDB() any { return nil }

func (s *Store) GetConfig(ctx context.Context, key string) (string, error) {
	st, _, err := s.load(ctx)
	if err != nil {
		return "", err
	}
	return st.Config[key], nil
}
func (s *Store) GetAllConfig(ctx context.Context) (map[string]string, error) {
	st, _, err := s.load(ctx)
	if err != nil {
		return nil, err
	}
	return st.Config, nil
}
func (s *Store) SetConfig(ctx context.Context, key, value string) error {
	if key == "project_id" || key == "status.custom" || key == "types.custom" || key == "claim.pools" {
		return errUnsupported("SetConfig." + key)
	}
	return s.mutate(ctx, func(st *state) error { st.Config[key] = value; return nil })
}
func (s *Store) DeleteConfig(ctx context.Context, key string) error {
	if key == "project_id" || key == "issue_prefix" {
		return errUnsupported("DeleteConfig." + key)
	}
	return s.mutate(ctx, func(st *state) error { delete(st.Config, key); return nil })
}
func (s *Store) WorkspaceConfig() (issueops.WorkspaceConfig, error) {
	return storeworkspaceconfig.New(s)
}
func (s *Store) Memories() (memoryops.Memories, error)        { return &notes{s}, nil }
func (s *Store) InitVerifier() (issueops.InitVerifier, error) { return &identity{s}, nil }
func (s *Store) VersionReconciler() (issueops.VersionReconciler, error) {
	return storeversionreconciler.New(s)
}
func (s *Store) Bootstrapper() (issueops.Bootstrapper, error)         { return &identity{s}, nil }
func (s *Store) IssueLifecycle() (issueops.Lifecycle, error)          { return &lifecycle{s}, nil }
func (s *Store) IssueReader() (issueops.Reader, error)                { return &reader{s}, nil }
func (s *Store) IssueClaimer() (issueops.Claimer, error)              { return &lifecycle{s}, nil }
func (s *Store) DependencyEditor() (issueops.DependencyEditor, error) { return &graph{s}, nil }
func (s *Store) IssueRelations() (issueops.Relations, error)          { return &graph{s}, nil }
func (s *Store) Commenter() (issueops.Commenter, error)               { return &comments{s}, nil }

// Memory calls the native service without changing the exact keyed-note plane.
func (s *Store) Memory(ctx context.Context, space, action, key string, body json.RawMessage) (json.RawMessage, error) {
	if s.readOnly && action != "recall" && action != "inspect" {
		return nil, errors.New("Antfly store is read-only")
	}
	if _, _, err := s.load(ctx); err != nil {
		return nil, err
	}
	if space == "" {
		space = s.memorySpace
	}
	if space == "" {
		return nil, errors.New("memory space is required for a caller-owned transport")
	}
	if action != "recall" && action != "inspect" && key == "" {
		return nil, errors.New("memory mutations require an idempotency key")
	}
	return s.transport.Memory(ctx, space, action, key, body)
}

func (st *state) issue(id string) (*types.Issue, error) {
	r, ok := st.Issues[id]
	if !ok {
		return nil, fmt.Errorf("%w: %s", storage.ErrNotFound, id)
	}
	return &r.Issue, nil
}
func (st *state) audit(op, id, actor string) {
	e := event{Sequence: uint64(len(st.Events) + 1), Operation: op, ID: id, Actor: actor, At: time.Now().UTC()}
	if r := st.Issues[id]; r != nil && !storageops.IsWisp(&r.Issue) && !r.Issue.NoHistory {
		b, _ := json.Marshal(r.Issue)
		var issue types.Issue
		_ = json.Unmarshal(b, &issue)
		e.Issue = &issue
	}
	if op == "remember" {
		e.Content = st.Notes[id]
	}
	st.Events = append(st.Events, e)
}
func (st *state) touch(i *types.Issue) {
	i.UpdatedAt = time.Now().UTC()
	i.RowVersion = int64(st.Revision + 1)
}

type identity struct{ s *Store }

func (i *identity) VerifyIdentity(ctx context.Context, _ issueops.VerifyIdentityRequest) (issueops.VerifyIdentityResult, error) {
	st, _, err := i.s.load(ctx)
	if err != nil {
		return issueops.VerifyIdentityResult{}, err
	}
	return issueops.VerifyIdentityResult{Prefix: st.Config["issue_prefix"], ProjectID: st.Config["project_id"]}, nil
}
func (i *identity) Bootstrap(ctx context.Context, req issueops.BootstrapRequest) (issueops.BootstrapResult, error) {
	if req.Prefix == "" || req.ProjectID == "" {
		return issueops.BootstrapResult{}, storage.ErrValidation
	}
	err := i.s.mutate(ctx, func(st *state) error {
		if st.Config["issue_prefix"] != "" || st.Config["project_id"] != "" {
			return storage.ErrAlreadyIdentified
		}
		st.Config["issue_prefix"] = req.Prefix
		st.Config["project_id"] = req.ProjectID
		return nil
	})
	return issueops.BootstrapResult{Prefix: req.Prefix, ProjectID: req.ProjectID}, err
}

// MemoryWorkspace checks project identity before accessing the native memory plane.
func MemoryWorkspace(ctx context.Context, dir, space, action, key string, body json.RawMessage) (json.RawMessage, error) {
	raw, err := openWorkspace(ctx, dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = raw.Close() }()
	return raw.(*Store).Memory(ctx, space, action, key, body)
}

func resolved(status types.Status) bool {
	return status == types.StatusClosed || status == types.StatusPinned
}

func OpenStore(ctx context.Context, dir string, opts backends.OpenOptions) (*Store, error) {
	raw, err := openWorkspaceWith(ctx, dir, opts)
	if err != nil {
		return nil, err
	}
	return raw.(*Store), nil
}
func (s *Store) openTarget(ctx context.Context, target Target, provision bool) (Transport, error) {
	if provision {
		return ensureTableWith(ctx, target, s.openOptions)
	}
	return OpenTransportWith(target, s.openOptions)
}
