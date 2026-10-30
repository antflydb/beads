//go:build cgo && (darwin || linux)

package antfly

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/steveyegge/beads/backend"
	"github.com/steveyegge/beads/backend/conformance"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage/backends"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/issueops"
	"github.com/steveyegge/beads/memoryops"
)

// Run with ANTFLY_TEST_LIBRARY pointing to a matching libantfly ABI 3 build.
// This checks the actual native persistence boundary, not a mocked C API.
func TestNativeAntflyPersistence(t *testing.T) {
	library := os.Getenv("ANTFLY_TEST_LIBRARY")
	if library == "" {
		t.Skip("set ANTFLY_TEST_LIBRARY to run native persistence integration")
	}
	ctx := context.Background()
	target := Target{Path: filepath.Join(t.TempDir(), "beads.aflite"), Library: library, Table: "beads", Workspace: "test"}
	transport, err := OpenTransport(target)
	if err != nil {
		t.Fatal(err)
	}
	s := New(transport, false)
	_, err = (&identity{s}).Bootstrap(ctx, issueops.BootstrapRequest{Prefix: "bd", ProjectID: "native-test"})
	if err != nil {
		_ = transport.Close()
		t.Fatal(err)
	}
	createTestIssue(t, s, "bd-persist")
	if _, err = (&lifecycle{s}).Claim(ctx, issueops.ClaimRequest{Actor: "worker", IssueID: "bd-persist"}); err != nil {
		_ = transport.Close()
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	transport, err = OpenTransport(target)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = transport.Close() }()
	s = New(transport, false)
	i, err := s.GetIssue(ctx, "bd-persist")
	if err != nil {
		t.Fatal(err)
	}
	if i.Status != types.StatusInProgress || i.Assignee != "worker" || i.RowVersion == 0 || i.LeaseExpiresAt == nil {
		t.Fatalf("native reopened state lost: %+v", i)
	}
	if _, err = (&lifecycle{s}).Close(ctx, issueops.CloseRequest{Actor: "worker", IssueID: i.ID}); err != nil {
		t.Fatal(err)
	}
	// The same commit publishes an ordinary graph/enrichment source document.
	native := underlyingTransport(transport).(*embedded)
	native.key = ProjectionKey(target.Workspace, i.ID)
	document, _, err := native.Load(ctx)
	native.key = target.Workspace
	if err != nil || !strings.Contains(string(document), `"beads_document_type":"issue"`) || !strings.Contains(string(document), `"status":"closed"`) {
		t.Fatalf("native issue projection = %s, %v", document, err)
	}
	if _, err = transport.Memory(ctx, "native", "create", "create-native", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	observation := json.RawMessage(`{"evidence":[{"id":"ev1","text":"Run regression tests before merging"}],"assertions":[{"id":"claim1","subject":"project","predicate":"guidance","value":"run tests","statement":"Run regression tests before merging","evidence":[{"id":"ev1"}]}]}`)
	if _, err = transport.Memory(ctx, "native", "observe", "observe-native", observation); err != nil {
		t.Fatal(err)
	}
	recall, err := transport.Memory(ctx, "native", "recall", "", json.RawMessage(`{"query":"regression tests"}`))
	if err != nil || !strings.Contains(string(recall), "Run regression tests before merging") {
		t.Fatalf("native memory recall = %s %v", recall, err)
	}
}

func TestNativeBranchesAndKnowledge(t *testing.T) {
	library := os.Getenv("ANTFLY_TEST_LIBRARY")
	if library == "" {
		t.Skip("set ANTFLY_TEST_LIBRARY for native branch and knowledge integration")
	}
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), ".beads")
	target := Target{Path: filepath.Join(dir, "data.aflite"), Library: library, Table: "work", Workspace: "main"}
	if err := Initialize(ctx, dir, target, "bd"); err != nil {
		t.Fatal(err)
	}
	s, err := OpenStore(ctx, dir, backends.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	createTestIssue(t, s, "bd-branch")
	fork, err := s.Fork(ctx, "feature", "test")
	if err != nil {
		t.Fatal(err)
	}
	if fork.Pending || fork.Directory == "" {
		t.Fatalf("incomplete fork: %+v", fork)
	}
	branch, err := OpenStore(ctx, fork.Directory, backends.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = branch.Close() }()
	_, err = (&lifecycle{branch}).Update(ctx, issueops.UpdateRequest{Actor: "test", IssueID: "bd-branch", Patch: issueops.IssuePatch{Title: issueops.Field[string]{Set: true, Value: "branch edit"}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = (&lifecycle{s}).Update(ctx, issueops.UpdateRequest{Actor: "test", IssueID: "bd-branch", Patch: issueops.IssuePatch{Description: issueops.Field[string]{Set: true, Value: "main edit"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = (&lifecycle{branch}).Claim(ctx, issueops.ClaimRequest{Actor: "worker", IssueID: "bd-branch"}); err == nil {
		t.Fatal("branch granted a live claim")
	}
	merged, err := s.MergeFork(ctx, "feature", "integration-1", "test", false)
	if err != nil || !merged.Applied {
		t.Fatalf("merge: %+v %v", merged, err)
	}
	merged, err = s.MergeFork(ctx, "feature", "integration-1", "test", false)
	if err != nil || !merged.AlreadyApplied {
		t.Fatalf("retry: %+v %v", merged, err)
	}
	issue, err := s.GetIssue(ctx, "bd-branch")
	if err != nil || issue.Title != "branch edit" || issue.Description != "main edit" {
		t.Fatalf("post-merge: %+v %v", issue, err)
	}
	// A second integration uses the previously accepted branch snapshot as
	// its base, so an unchanged branch field cannot undo a later main edit.
	_, err = (&lifecycle{s}).Update(ctx, issueops.UpdateRequest{Actor: "test", IssueID: "bd-branch", Patch: issueops.IssuePatch{Title: issueops.Field[string]{Set: true, Value: "later main title"}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = (&lifecycle{branch}).Update(ctx, issueops.UpdateRequest{Actor: "test", IssueID: "bd-branch", Patch: issueops.IssuePatch{Design: issueops.Field[string]{Set: true, Value: "later branch design"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.MergeFork(ctx, "feature", "integration-2", "test", false); err != nil {
		t.Fatal(err)
	}
	issue, err = s.GetIssue(ctx, "bd-branch")
	if err != nil || issue.Title != "later main title" || issue.Design != "later branch design" {
		t.Fatalf("linear integration: %+v %v", issue, err)
	}
	if err = s.ConfigureKnowledge(ctx, KnowledgeConfig{Revision: "native-test-v1"}); err != nil {
		t.Fatal(err)
	}
	_, err = (&notes{s}).Remember(ctx, memoryops.RememberRequest{Key: "policy", Content: "Always run the regression suite"})
	if err != nil {
		t.Fatal(err)
	}
	// An accepted observation whose response is lost must leave the durable
	// checkpoint untouched. A retry sends identical bytes and native key.
	before, err := s.KnowledgeStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	original := s.transport
	fault := &lostObservationTransport{Transport: original}
	s.transport = fault
	if _, err = s.SyncKnowledge(ctx, 100); err == nil || !fault.accepted {
		t.Fatalf("expected accepted observation with lost acknowledgement: %v", err)
	}
	s.transport = original
	after, err := s.KnowledgeStatus(ctx)
	if err != nil || after.State.Checkpoint != before.State.Checkpoint {
		t.Fatalf("unknown outcome advanced checkpoint: %+v %v", after, err)
	}
	// A different worker cannot take an unexpired consumer lease.
	if err = s.mutate(ctx, func(st *state) error {
		st.Knowledge.Worker = "other-worker"
		st.Knowledge.WorkerUntil = time.Now().Add(time.Minute)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SyncKnowledge(ctx, 100); err == nil {
		t.Fatal("stole consumer lease")
	}
	if err = s.mutate(ctx, func(st *state) error {
		st.Knowledge.WorkerUntil = time.Now().Add(-time.Minute)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	status, err := s.SyncKnowledge(ctx, 100)
	if err != nil || status.Pending != 0 {
		t.Fatalf("consumer: %+v %v", status, err)
	}
	recalled, err := s.Memory(ctx, "", "recall", "", json.RawMessage(`{"query":"regression suite"}`))
	if err != nil || !strings.Contains(string(recalled), "Always run the regression suite") {
		t.Fatalf("projected memory: %s %v", recalled, err)
	}
	contextResult, contextErr := s.Context(ctx, ContextRequest{Query: "regression suite", MaxTokens: 2048})
	if contextErr != nil || len(contextResult.Sources) == 0 {
		t.Fatalf("native context=%+v err=%v", contextResult, contextErr)
	}
	rendered, renderErr := s.KnowledgeOperation(ctx, "render", "entities", json.RawMessage(`{"text":"regression","label":"concept"}`))
	if renderErr != nil || !strings.Contains(string(rendered), "entity/") {
		t.Fatalf("resolver=%s err=%v", rendered, renderErr)
	}
	_, err = (&notes{s}).Forget(ctx, memoryops.ForgetRequest{Key: "policy"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.SyncKnowledge(ctx, 100); err != nil {
		t.Fatal(err)
	}
	recalled, err = s.Memory(ctx, "", "recall", "", json.RawMessage(`{"query":"regression suite"}`))
	if err != nil || strings.Contains(string(recalled), "Always run the regression suite") {
		t.Fatalf("forgotten memory resurrected: %s %v", recalled, err)
	}
	// Replaying from the beginning must honor the native source forget fences.
	if err = s.mutate(ctx, func(st *state) error { st.Knowledge.Checkpoint = 0; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SyncKnowledge(ctx, 100); err != nil {
		t.Fatal(err)
	}
	recalled, err = s.Memory(ctx, "", "recall", "", json.RawMessage(`{"query":"regression suite"}`))
	if err != nil || strings.Contains(string(recalled), "Always run the regression suite") {
		t.Fatalf("replay resurrected memory: %s %v", recalled, err)
	}
}

func TestNativeWorkspaceLargerThanFourMiB(t *testing.T) {
	library := os.Getenv("ANTFLY_TEST_LIBRARY")
	if library == "" {
		t.Skip("set ANTFLY_TEST_LIBRARY for large row storage integration")
	}
	target := Target{Path: filepath.Join(t.TempDir(), "large.aflite"), Library: library, Table: "work", Workspace: "large"}
	tr, err := OpenTransport(target)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	state := emptyState()
	state.Config["issue_prefix"] = "bd"
	state.Config["project_id"] = "large-project"
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		state.Notes[key] = strings.Repeat(key, 1<<20)
	}
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err = tr.CompareAndSwap(context.Background(), 0, data); err != nil {
		t.Fatal(err)
	}
	restored, _, err := tr.Load(context.Background())
	if err != nil || !jsonEqual(data, restored) {
		t.Fatalf("large native snapshot: %v", err)
	}
}

func TestNativePortableTransactionalContract(t *testing.T) {
	library := os.Getenv("ANTFLY_TEST_LIBRARY")
	if library == "" {
		t.Skip("set ANTFLY_TEST_LIBRARY for the native portable contract")
	}
	conformance.RunTransactionalWorkflowContract(t, func(t *testing.T) conformance.TransactionalWorkflowFixture {
		dir := filepath.Join(t.TempDir(), ".beads")
		target := Target{Path: filepath.Join(dir, "data.aflite"), Library: library, Table: "work", Workspace: "contract"}
		ctx := context.Background()
		if e := Initialize(ctx, dir, target, "bd"); e != nil {
			t.Fatal(e)
		}
		s, e := OpenStore(ctx, dir, backends.OpenOptions{})
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = s.Close() })
		return conformance.TransactionalWorkflowFixture{Store: s, IndependentStore: func() backend.DoltStorage {
			other, e := OpenStore(ctx, dir, backends.OpenOptions{})
			if e != nil {
				t.Fatal(e)
			}
			t.Cleanup(func() { _ = other.Close() })
			return other
		}, SetLeaseExpiry: func(ctx context.Context, id string, expiry time.Time) error {
			return s.mutate(ctx, func(st *state) error { st.Issues[id].Issue.LeaseExpiresAt = &expiry; return nil })
		}, AuditCount: func(ctx context.Context) (int, error) {
			st, _, e := s.load(ctx)
			if e != nil {
				return 0, e
			}
			return len(st.Events), nil
		}}
	})
}

// Drop only the first successfully accepted observe response; the native
// destination still commits it, exercising idempotency across checkpoint replay.
type lostObservationTransport struct {
	Transport
	accepted bool
}

func (f *lostObservationTransport) Memory(ctx context.Context, space, action, key string, body json.RawMessage) (json.RawMessage, error) {
	out, err := f.Transport.Memory(ctx, space, action, key, body)
	if err == nil && action == "observe" && !f.accepted {
		f.accepted = true
		return nil, errors.New("injected lost observation acknowledgement")
	}
	return out, err
}

func TestNativeGenericSnapshot(t *testing.T) {
	library := os.Getenv("ANTFLY_TEST_LIBRARY")
	if library == "" {
		t.Skip("ANTFLY_TEST_LIBRARY is required for native snapshots")
	}
	ctx := context.Background()
	target := Target{Path: filepath.Join(t.TempDir(), "generic.aflite"), Library: library, Table: "control", Workspace: "gascity"}
	transport, err := OpenSnapshot(ctx, target, backends.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = transport.Close() }()
	values := map[string]string{}
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		values[key] = strings.Repeat("<exact> & -> ", 100000)
	}
	data, _ := json.Marshal(map[string]any{"schema": "gascity", "records": values})
	if len(data) <= maxStateBytes {
		t.Fatal("fixture does not exceed aggregate limit")
	}
	if err = transport.CompareAndSwap(ctx, 0, data); err != nil {
		t.Fatal(err)
	}
	restored, version, err := transport.Load(ctx)
	if err != nil || !jsonEqual(restored, data) {
		t.Fatalf("generic snapshot lost data: %v", err)
	}
	if err = transport.CompareAndSwap(ctx, 0, data); !errors.Is(err, ErrSnapshotConflict) {
		t.Fatalf("stale publication: %v", err)
	}
	if err = transport.CompareAndSwap(ctx, version, data); err != nil {
		t.Fatal(err)
	}
}

func TestNativeIndexedSnapshotBackup(t *testing.T) {
	lib := os.Getenv("ANTFLY_TEST_LIBRARY")
	if lib == "" {
		t.Skip("ANTFLY_TEST_LIBRARY required")
	}
	ctx := context.Background()
	target := Target{Path: filepath.Join(t.TempDir(), "archive.aflite"), Library: lib, Table: "snapshots", Workspace: "source"}
	src, e := OpenSnapshot(ctx, target, backends.OpenOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer src.Close()
	data := []byte(`{"schema":"native-v1","records":{"one":{"text":"<exact>"},"two":{"text":"other"}}}`)
	if e = src.CompareAndSwap(ctx, 0, data); e != nil {
		t.Fatal(e)
	}
	selected, e := src.(*rowTransport).ReadSnapshotRecords(ctx, "records", []string{"one"}, nil)
	if e != nil || len(selected.Records) != 1 {
		t.Fatalf("indexed read: %+v %v", selected, e)
	}
	archive, e := BackupSnapshot(ctx, src)
	if e != nil {
		t.Fatal(e)
	}
	target.Workspace = "restored"
	dst, e := OpenSnapshot(ctx, target, backends.OpenOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer dst.Close()
	if e = RestoreSnapshotBackup(ctx, dst, archive, 0); e != nil {
		t.Fatal(e)
	}
	raw, _, e := dst.Load(ctx)
	if e != nil {
		t.Fatal(e)
	}
	var got, want any
	if e = json.Unmarshal(raw, &got); e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(data, &want); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("restore differs: %s", raw)
	}
	for n := 0; n < 3; n++ {
		_, v, e := src.Load(ctx)
		if e != nil {
			t.Fatal(e)
		}
		next, _ := json.Marshal(map[string]any{"schema": "native-v1", "records": map[string]any{"one": n}})
		if e = src.CompareAndSwap(ctx, v, next); e != nil {
			t.Fatal(e)
		}
	}
	gc, e := src.(*rowTransport).CollectGarbage(ctx)
	if e != nil || gc.Deleted == 0 {
		t.Fatalf("native collection: %+v %v", gc, e)
	}
	if _, _, e = src.Load(ctx); e != nil {
		t.Fatal(e)
	}
}

func TestNativeDatabaseArchivePreservesSeparateWorkspaces(t *testing.T) {
	lib := os.Getenv("ANTFLY_TEST_LIBRARY")
	if lib == "" {
		t.Skip("ANTFLY_TEST_LIBRARY required")
	}
	ctx := context.Background()
	dir := t.TempDir()
	target := Target{Path: filepath.Join(dir, "source.aflite"), Library: lib, Table: "infra", Workspace: "one"}
	source, e := OpenSnapshot(ctx, target, backends.OpenOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer source.Close()
	if e = source.CompareAndSwap(ctx, 0, []byte(`{"schema":"one","records":{"a":1}}`)); e != nil {
		t.Fatal(e)
	}
	target.Workspace = "two"
	other, e := OpenSnapshot(ctx, target, backends.OpenOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	if e = other.CompareAndSwap(ctx, 0, []byte(`{"schema":"two","records":{"b":2}}`)); e != nil {
		t.Fatal(e)
	}

	if _, e = source.Memory(ctx, "backup-space", "create", "create", json.RawMessage(`{}`)); e != nil {
		t.Fatal(e)
	}
	observation := json.RawMessage(`{"evidence":[{"id":"e1","text":"Backup memory sentinel"}],"assertions":[{"id":"a1","subject":"project","predicate":"guidance","value":"retained","statement":"Backup memory sentinel","evidence":[{"id":"e1"}]}]}`)
	if _, e = source.Memory(ctx, "backup-space", "observe", "observe", observation); e != nil {
		t.Fatal(e)
	}
	archivePath := filepath.Join(dir, "database.afb")
	file, e := os.Create(archivePath)
	if e != nil {
		t.Fatal(e)
	}
	if e = source.(*rowTransport).WriteDatabaseBackup(ctx, file); e != nil {
		_ = file.Close()
		t.Fatal(e)
	}
	if e = file.Close(); e != nil {
		t.Fatal(e)
	}
	restoredPath := filepath.Join(dir, "restored.aflite")
	if e = RestoreDatabaseBackupFile(ctx, lib, restoredPath, archivePath); e != nil {
		t.Fatal(e)
	}
	if e = RestoreDatabaseBackupFile(ctx, lib, restoredPath, archivePath); e == nil {
		t.Fatal("engine restore replaced an existing destination")
	}
	target.Path = restoredPath
	for _, workspace := range []string{"one", "two"} {
		target.Workspace = workspace
		copy, e := OpenSnapshot(ctx, target, backends.OpenOptions{})
		if e != nil {
			t.Fatal(e)
		}
		raw, _, e := copy.Load(ctx)
		memory, memoryErr := copy.Memory(ctx, "backup-space", "recall", "", json.RawMessage(`{"query":"Backup memory sentinel"}`))
		if memoryErr != nil || !strings.Contains(string(memory), "Backup memory sentinel") {
			_ = copy.Close()
			t.Fatalf("restored memory: %s %v", memory, memoryErr)
		}
		_ = copy.Close()
		if e != nil || !strings.Contains(string(raw), `"schema":"`+workspace+`"`) {
			t.Fatalf("restored %s: %s %v", workspace, raw, e)
		}
	}
}
