package antfly

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/steveyegge/beads/internal/storage/backends"
	"github.com/steveyegge/beads/issueops"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

type documentService struct {
	mu          sync.Mutex
	docs        map[string][]byte
	versions    map[string]uint64
	clock       uint64
	reads       int
	anchor      string
	failPublish bool
}

func (s *documentService) Load(ctx context.Context) ([]byte, uint64, error) {
	return s.ReadDocument(ctx, s.anchor)
}
func (s *documentService) ReadDocument(_ context.Context, key string) ([]byte, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	return append([]byte(nil), s.docs[key]...), s.versions[key], nil
}
func (s *documentService) WriteDocuments(_ context.Context, v *uint64, docs map[string]json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v != nil {
		if s.failPublish {
			return errIndeterminate
		}
		if *v != s.versions[s.anchor] {
			return errConflict
		}
	}
	s.clock++
	for key, data := range docs {
		if data == nil {
			delete(s.docs, key)
			delete(s.versions, key)
			continue
		}
		s.docs[key] = append([]byte(nil), data...)
		s.versions[key] = s.clock
	}
	return nil
}
func (s *documentService) CompareAndSwap(ctx context.Context, v uint64, b []byte) error {
	return s.WriteDocuments(ctx, &v, map[string]json.RawMessage{s.anchor: b})
}
func (s *documentService) Memory(context.Context, string, string, string, json.RawMessage) (json.RawMessage, error) {
	return nil, errUnsupported("memory")
}
func (s *documentService) Close() error { return nil }
func testRows() (*rowTransport, *documentService) {
	service := &documentService{docs: map[string][]byte{}, versions: map[string]uint64{}, anchor: "team"}
	return newRowTransport(service, "team").(*rowTransport), service
}
func TestRowsExceedAggregateLimitAndFencePublication(t *testing.T) {
	ctx := context.Background()
	tr, service := testRows()
	st := emptyState()
	st.Config["issue_prefix"] = "bd"
	st.Config["project_id"] = "test"
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		st.Notes[key] = strings.Repeat(key, 1<<20)
	}
	data, _ := json.Marshal(st)
	if len(data) <= maxStateBytes {
		t.Fatal("fixture is not larger than the old aggregate limit")
	}
	if err := tr.CompareAndSwap(ctx, 0, data); err != nil {
		t.Fatal(err)
	}
	restored, v, err := tr.Load(ctx)
	if err != nil || !sameJSON(restored, data) {
		t.Fatalf("row restore: %v", err)
	}
	var head rowHead
	if err = json.Unmarshal(service.docs["team"], &head); err != nil || head.StorageFormat != 2 {
		t.Fatalf("head=%+v err=%v", head, err)
	}
	if len(service.docs["team"]) > 4096 {
		t.Fatal("large workspace was embedded in head")
	}
	st.Notes["a"] = "changed"
	changed, _ := json.Marshal(st)
	service.failPublish = true
	if err = tr.CompareAndSwap(ctx, v, changed); !errors.Is(err, errIndeterminate) {
		t.Fatal(err)
	}
	restored, _, err = tr.Load(ctx)
	if err != nil || !sameJSON(restored, data) {
		t.Fatal("unpublished staged rows leaked into snapshot")
	}
	service.failPublish = false
	if err = tr.CompareAndSwap(ctx, v+1, changed); !errors.Is(err, errConflict) {
		t.Fatal(err)
	}
}
func TestRowsMigrateLegacyStateAndPreserveComments(t *testing.T) {
	ctx := context.Background()
	tr, service := testRows()
	st := emptyState()
	data, _ := json.Marshal(st)
	service.docs["team"] = data
	service.versions["team"] = 1
	s := New(tr, false)
	_, err := (&identity{s}).Bootstrap(ctx, issueops.BootstrapRequest{Prefix: "bd", ProjectID: "test"})
	if err != nil {
		t.Fatal(err)
	}
	createTestIssue(t, s, "bd-comment")
	if _, err := (&comments{s}).AddComment(ctx, issueops.AddCommentRequest{IssueID: "bd-comment", Author: "test", Text: "preserved"}); err != nil {
		t.Fatal(err)
	}
	issue, err := s.GetIssue(ctx, "bd-comment")
	if err != nil || len(issue.Comments) != 1 || issue.Comments[0].Text != "preserved" {
		t.Fatalf("comments=%v err=%v", issue, err)
	}
	var head rowHead
	_ = json.Unmarshal(service.docs["team"], &head)
	if head.StorageFormat != 2 {
		t.Fatal("legacy workspace did not migrate")
	}
}

func sameJSON(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}

func TestImmutableRowsSurviveDocumentReserialization(t *testing.T) {
	ctx := context.Background()
	tr, service := testRows()
	st := emptyState()
	st.Notes["format"] = "close -> accepted <review> & tested \u2028 Unicode é"
	data, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if err = tr.CompareAndSwap(ctx, 0, data); err != nil {
		t.Fatal(err)
	}
	// Simulate a JSON document service changing whitespace and escapes. Exact
	// inner payloads, including large numbers and number spellings, survive.
	for key, raw := range service.docs {
		var object any
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if err = decoder.Decode(&object); err != nil {
			t.Fatal(err)
		}
		var out strings.Builder
		encoder := json.NewEncoder(&out)
		encoder.SetEscapeHTML(false)
		encoder.SetIndent("", "  ")
		if err = encoder.Encode(object); err != nil {
			t.Fatal(err)
		}
		service.docs[key] = []byte(out.String())
	}
	restored, _, err := tr.Load(ctx)
	if err != nil || !sameJSON(restored, data) {
		t.Fatalf("round trip: %s %v", restored, err)
	}
	payload := []byte(`{"value":{"number":9007199254740993,"decimal":1.000,"text":"a -> b & c"}}`)
	key := tr.address(payload)
	encoded, _ := json.Marshal(map[string]string{"_beads_immutable_json": string(payload)})
	service.docs[key] = encoded
	actual, err := tr.readImmutable(ctx, key)
	if err != nil || string(actual) != string(payload) {
		t.Fatalf("exact bytes: %s %v", actual, err)
	}
	service.docs[key] = []byte(`{"_beads_immutable_json":"changed"}`)
	if _, err = tr.readImmutable(ctx, key); err == nil {
		t.Fatal("corrupt envelope accepted")
	}
}

func TestImmutableLegacyHTMLEscapesRemainVerifiable(t *testing.T) {
	ctx := context.Background()
	tr, service := testRows()
	original, _ := json.Marshal(storedRow{Value: json.RawMessage(`{"close_reason":"tests -> passed <ok> & verified"}`)})
	key := tr.address(original)
	service.docs[key] = []byte(strings.NewReplacer(`\u003e`, ">", `\u003c`, "<", `\u0026`, "&").Replace(string(original)))
	actual, err := tr.readImmutable(ctx, key)
	if err != nil || string(actual) != string(original) {
		t.Fatalf("legacy recovery: %s %v", actual, err)
	}
	service.docs[key] = []byte(`{"value":{"close_reason":"changed"}}`)
	if _, err = tr.readImmutable(ctx, key); err == nil {
		t.Fatal("corrupt legacy record accepted")
	}
}

func TestGenericSnapshotPreservesCallerSchema(t *testing.T) {
	ctx := context.Background()
	tr, service := testRows()
	tr.domainProjection = false
	data := []byte(`{"issues":{"a":{"custom":true}},"comments":{"x":"opaque"},"leases":{"y":3},"events":["one","two"],"a/b":{"key/with/slash":{"v":"<exact> & ->"}},"empty":{},"nil":null}`)
	if err := tr.CompareAndSwap(ctx, 0, data); err != nil {
		t.Fatal(err)
	}
	actual, version, err := tr.Load(ctx)
	if err != nil || !sameJSON(actual, data) {
		t.Fatalf("caller schema changed: %s %v", actual, err)
	}
	// Generic writes must not emit core issue projections.
	for key := range service.docs {
		if strings.HasPrefix(key, "bead:") {
			t.Fatalf("unexpected domain projection %s", key)
		}
	}
	service.failPublish = true
	if err = tr.CompareAndSwap(ctx, version, []byte(`{"new":"unpublished"}`)); !errors.Is(err, errIndeterminate) {
		t.Fatal(err)
	}
	actual, _, err = tr.Load(ctx)
	if err != nil || !sameJSON(actual, data) {
		t.Fatalf("partial publication: %s %v", actual, err)
	}
}

func TestGenericSnapshotCacheFollowsHeadAndDoesNotAlias(t *testing.T) {
	ctx := context.Background()
	tr, service := testRows()
	tr.domainProjection = false
	tr.cacheReads = true
	original := []byte(`{"schema":"generic","records":{"a":{"value":"first"},"b":{"value":"stable"}}}`)
	if err := tr.CompareAndSwap(ctx, 0, original); err != nil {
		t.Fatal(err)
	}
	first, v, err := tr.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	reads := service.reads
	service.mu.Unlock()
	first[0] = '!'
	again, _, err := tr.Load(ctx)
	if err != nil || !sameJSON(again, original) {
		t.Fatalf("caller mutated cached snapshot: %s %v", again, err)
	}
	service.mu.Lock()
	delta := service.reads - reads
	service.mu.Unlock()
	if delta != 1 {
		t.Fatalf("unchanged snapshot read %d documents, want only fresh head", delta)
	}
	changed := []byte(`{"schema":"generic","records":{"a":{"value":"next"},"b":{"value":"stable"}}}`)
	if err = tr.CompareAndSwap(ctx, v, changed); err != nil {
		t.Fatal(err)
	}
	actual, _, err := tr.Load(ctx)
	if err != nil || !sameJSON(actual, changed) {
		t.Fatalf("cache hid publication: %s %v", actual, err)
	}
}

func TestGenericIndexedSnapshotReadsBoundedRecords(t *testing.T) {
	ctx := context.Background()
	svc := &documentService{docs: map[string][]byte{}, versions: map[string]uint64{}, anchor: "infra"}
	tr := &rowTransport{documentTransport: svc, workspace: "infra"}
	records := map[string]any{}
	for n := 0; n < 1000; n++ {
		records[fmt.Sprintf("id-%d", n)] = map[string]any{"title": fmt.Sprintf("row %d", n)}
	}
	data, _ := json.Marshal(map[string]any{"schema": "test-v1", "records": records})
	if e := tr.CompareAndSwap(ctx, 0, data); e != nil {
		t.Fatal(e)
	}
	svc.reads = 0
	got, e := tr.ReadSnapshotRecords(ctx, "records", []string{"id-123", "absent"}, nil)
	if e != nil {
		t.Fatal(e)
	}
	if len(got.Records) != 1 || !strings.Contains(string(got.Records["id-123"]), "row 123") {
		t.Fatalf("records=%s", got.Records)
	}
	if svc.reads > 4 {
		t.Fatalf("indexed lookup read %d documents", svc.reads)
	}
	old := got.Version
	if e = tr.CompareAndSwap(ctx, old, data); e != nil {
		t.Fatal(e)
	}
	if _, e = tr.ReadSnapshotRecords(ctx, "records", []string{"id-123"}, &old); !errors.Is(e, ErrSnapshotConflict) {
		t.Fatalf("stale pinned read: %v", e)
	}
}

func TestSnapshotBackupRestoreChecksIntegrityAndConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	source := &testTransport{data: []byte(`{"schema":"one","records":{"a":{"text":"<exact>"}}}`), version: 1}
	backup, e := BackupSnapshot(ctx, source)
	if e != nil {
		t.Fatal(e)
	}
	dest := &testTransport{}
	if e = RestoreSnapshotBackup(ctx, dest, backup, 0); e != nil {
		t.Fatal(e)
	}
	if string(dest.data) != string(source.data) {
		t.Fatalf("restore changed bytes: %s", dest.data)
	}
	if e = RestoreSnapshotBackup(ctx, dest, backup, 0); !errors.Is(e, ErrSnapshotConflict) {
		t.Fatalf("stale restore: %v", e)
	}
	var envelope map[string]any
	if e = json.Unmarshal(backup, &envelope); e != nil {
		t.Fatal(e)
	}
	envelope["payload"] = `{"schema":"corrupt"}`
	corrupt, _ := json.Marshal(envelope)
	before := string(dest.data)
	if e = RestoreSnapshotBackup(ctx, dest, corrupt, dest.version); e == nil {
		t.Fatal("corrupt archive accepted")
	}
	if before != string(dest.data) {
		t.Fatal("invalid archive wrote data")
	}
}

func (s *documentService) ScanDocuments(_ context.Context, from, to string, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := []string{}
	for key := range s.docs {
		if key > from && key < to {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	return keys, nil
}
func TestGenericSnapshotGarbageCollectionRetainsLiveRows(t *testing.T) {
	ctx := context.Background()
	svc := &documentService{docs: map[string][]byte{}, versions: map[string]uint64{}, anchor: "infra"}
	tr := &rowTransport{documentTransport: svc, workspace: "infra"}
	for n := 0; n < 5; n++ {
		_, v, e := tr.Load(ctx)
		if e != nil {
			t.Fatal(e)
		}
		raw, _ := json.Marshal(map[string]any{"schema": "test", "records": map[string]any{"a": n}})
		if e = tr.CompareAndSwap(ctx, v, raw); e != nil {
			t.Fatal(e)
		}
	}
	before, _, e := tr.Load(ctx)
	if e != nil {
		t.Fatal(e)
	}
	svc.docs["unrelated"] = []byte(`{"keep":true}`)
	result, e := tr.CollectGarbage(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if result.Deleted == 0 {
		t.Fatal("unreachable history was not collected")
	}
	after, _, e := tr.Load(ctx)
	if e != nil || string(before) != string(after) {
		t.Fatalf("live snapshot changed: %s %v", after, e)
	}
	if _, ok := svc.docs["unrelated"]; !ok {
		t.Fatal("deleted another workspace")
	}
	// A crashed collector's durable head marker is explicitly resumable.
	var head rowHead
	if e = json.Unmarshal(svc.docs[svc.anchor], &head); e != nil {
		t.Fatal(e)
	}
	head.Maintenance = "crashed"
	svc.docs[svc.anchor], _ = json.Marshal(head)
	if _, _, e = tr.Load(ctx); !errors.Is(e, ErrSnapshotMaintenance) {
		t.Fatalf("maintenance read did not fail closed: %v", e)
	}
	if _, e = tr.CollectGarbage(ctx); e != nil {
		t.Fatal(e)
	}
	if _, _, e = tr.Load(ctx); e != nil {
		t.Fatal(e)
	}
}

type stealingCollector struct {
	*documentService
	once sync.Once
}

func (s *stealingCollector) ScanDocuments(ctx context.Context, from, to string, limit int) ([]string, error) {
	keys, e := s.documentService.ScanDocuments(ctx, from, to, limit)
	s.once.Do(func() {
		raw, v, _ := s.documentService.Load(ctx)
		var head rowHead
		_ = json.Unmarshal(raw, &head)
		head.Maintenance = "another-owner"
		raw, _ = json.Marshal(head)
		_ = s.documentService.WriteDocuments(ctx, &v, map[string]json.RawMessage{s.anchor: raw})
	})
	return keys, e
}
func TestGenericSnapshotCollectorCannotDeleteAfterLosingFence(t *testing.T) {
	ctx := context.Background()
	svc := &documentService{docs: map[string][]byte{}, versions: map[string]uint64{}, anchor: "infra"}
	wrapped := &stealingCollector{documentService: svc}
	tr := &rowTransport{documentTransport: wrapped, workspace: "infra"}
	for n := 0; n < 2; n++ {
		_, v, _ := tr.Load(ctx)
		data, _ := json.Marshal(map[string]any{"records": map[string]any{"a": n}})
		if e := tr.CompareAndSwap(ctx, v, data); e != nil {
			t.Fatal(e)
		}
	}
	count := len(svc.docs)
	if _, e := tr.CollectGarbage(ctx); !errors.Is(e, ErrSnapshotConflict) {
		t.Fatalf("lost maintenance fence: %v", e)
	}
	if len(svc.docs) != count {
		t.Fatal("old collector deleted after losing ownership")
	}
	var head rowHead
	if e := json.Unmarshal(svc.docs[svc.anchor], &head); e != nil {
		t.Fatal(e)
	}
	if head.Maintenance != "another-owner" {
		t.Fatal("old collector cleared another owner's fence")
	}
	core := newRowTransport(svc, "infra").(*rowTransport)
	if _, e := core.CollectGarbage(ctx); e == nil {
		t.Fatal("branching work GC was admitted")
	}
}

func TestRemoteSnapshotIndexedArchiveAndCollection(t *testing.T) {
	endpoint := os.Getenv("ANTFLY_TEST_URL")
	if endpoint == "" {
		t.Skip("ANTFLY_TEST_URL required for disposable remote server")
	}
	ctx := context.Background()
	target := Target{URL: endpoint, Table: fmt.Sprintf("snapshot_qualification_%d", time.Now().UnixNano()), Workspace: "infra", Database: "default", Namespace: "public"}
	tr, e := OpenSnapshot(ctx, target, backends.OpenOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer tr.Close()
	for n := 0; n < 3; n++ {
		_, v, e := tr.Load(ctx)
		if e != nil {
			t.Fatal(e)
		}
		raw, _ := json.Marshal(map[string]any{"schema": "remote-v1", "records": map[string]any{"one": n}})
		if e = tr.CompareAndSwap(ctx, v, raw); e != nil {
			t.Fatal(e)
		}
	}
	selected, e := tr.(*rowTransport).ReadSnapshotRecords(ctx, "records", []string{"one"}, nil)
	if e != nil || len(selected.Records) != 1 {
		t.Fatalf("remote index: %+v %v", selected, e)
	}
	archive, e := BackupSnapshot(ctx, tr)
	if e != nil {
		t.Fatal(e)
	}
	target.Workspace = "restored"
	restored, e := OpenSnapshot(ctx, target, backends.OpenOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer restored.Close()
	if e = RestoreSnapshotBackup(ctx, restored, archive, 0); e != nil {
		t.Fatal(e)
	}
	result, e := tr.(*rowTransport).CollectGarbage(ctx)
	if e != nil || result.Deleted == 0 {
		t.Fatalf("remote collection: %+v %v", result, e)
	}
	if _, _, e = tr.Load(ctx); e != nil {
		t.Fatal(e)
	}
	if _, _, e = restored.Load(ctx); e != nil {
		t.Fatal(e)
	}
}
