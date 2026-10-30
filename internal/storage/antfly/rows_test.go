package antfly

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/backends"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/issueops"
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

type immutableReadService struct {
	*documentService
	immutableReads int
}

func (s *immutableReadService) ReadImmutableDocument(ctx context.Context, key string) ([]byte, error) {
	s.immutableReads++
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.docs[key]...), ctx.Err()
}

func TestImmutableLookupPreservesContentVerificationAndHeadFence(t *testing.T) {
	ctx := context.Background()
	_, service := testRows()
	fast := &immutableReadService{documentService: service}
	tr := newRowTransport(fast, "team").(*rowTransport)
	tr.domainProjection = false
	payload := []byte(`{"sessions":{"one":{"state":"active"}}}`)
	if err := tr.CompareAndSwap(ctx, 0, payload); err != nil {
		t.Fatal(err)
	}
	service.reads, fast.immutableReads = 0, 0
	got, version, err := tr.Load(ctx)
	if err != nil || !sameJSON(got, payload) || version == 0 {
		t.Fatalf("snapshot read: version=%d err=%v data=%s", version, err, got)
	}
	if service.reads != 1 || fast.immutableReads == 0 {
		t.Fatalf("want one fenced head read and immutable lookups: fenced=%d immutable=%d", service.reads, fast.immutableReads)
	}
	if err := tr.CompareAndSwap(ctx, version+1, payload); !errors.Is(err, errConflict) {
		t.Fatalf("stale publication was not fenced: %v", err)
	}
	for key := range service.docs {
		if strings.HasPrefix(key, "row:") {
			service.docs[key] = []byte(`{"corrupt":true}`)
			if _, _, err := tr.Load(ctx); err == nil {
				t.Fatal("immutable lookup accepted a corrupt content address")
			}
			break
		}
	}
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

func TestCapturedSnapshotRecordsStayConsistentWithoutRefreshingHead(t *testing.T) {
	ctx := context.Background()
	_, service := testRows()
	fast := &immutableReadService{documentService: service}
	tr := newRowTransport(fast, "team").(*rowTransport)
	tr.domainProjection = false
	original := []byte(`{"schema":"first","records":{"one":{"value":"old"}},"indexes":{"owner":["one"]}}`)
	if err := tr.CompareAndSwap(ctx, 0, original); err != nil {
		t.Fatal(err)
	}
	reader, err := tr.CaptureSnapshotRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	posting, err := reader.ReadSnapshotRecords(ctx, "indexes", []string{"owner"}, nil)
	if err != nil || string(posting.Records["owner"]) != `["one"]` {
		t.Fatalf("captured posting: %+v %v", posting, err)
	}
	changed := []byte(`{"schema":"next","records":{"one":{"value":"new"},"added":true},"indexes":{"owner":["added"]}}`)
	if err = tr.CompareAndSwap(ctx, posting.Version, changed); err != nil {
		t.Fatal(err)
	}
	reads := service.reads
	// A caller may own and edit the returned JSON without altering the view.
	posting.Fields["schema"][0] = '!'
	rows, err := reader.ReadSnapshotRecords(ctx, "records", []string{"one", "added"}, &posting.Version)
	if err != nil || string(rows.Records["one"]) != `{"value":"old"}` || len(rows.Records["added"]) != 0 || string(rows.Fields["schema"]) != `"first"` {
		t.Fatalf("captured read mixed publications or aliased fields: %+v %v", rows, err)
	}
	if service.reads != reads {
		t.Fatal("captured continuation refreshed the mutable head")
	}
	wrong := posting.Version + 1
	if _, err = reader.ReadSnapshotRecords(ctx, "records", []string{"one"}, &wrong); !errors.Is(err, ErrSnapshotConflict) {
		t.Fatalf("mismatched captured version: %v", err)
	}
	if _, err = tr.ReadSnapshotRecords(ctx, "records", []string{"one"}, &posting.Version); !errors.Is(err, ErrSnapshotConflict) {
		t.Fatalf("ordinary current-head read accepted a stale version: %v", err)
	}
	next, err := tr.CaptureSnapshotRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rows, err = next.ReadSnapshotRecords(ctx, "records", []string{"one", "added"}, nil)
	if err != nil || string(rows.Records["one"]) != `{"value":"new"}` || string(rows.Records["added"]) != "true" || rows.Version == posting.Version {
		t.Fatalf("fresh capture missed publication: %+v %v", rows, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = reader.ReadSnapshotRecords(canceled, "records", []string{"one"}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("captured read ignored cancellation: %v", err)
	}
}

func TestCapturedSnapshotRecordsPreserveLegacyAndMaintenanceBoundaries(t *testing.T) {
	ctx := context.Background()
	tr, service := testRows()
	tr.domainProjection = false
	legacy := []byte(`{"schema":"legacy","records":{"one":"original"}}`)
	if err := service.CompareAndSwap(ctx, 0, legacy); err != nil {
		t.Fatal(err)
	}
	reader, err := tr.CaptureSnapshotRecords(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = tr.CompareAndSwap(ctx, service.versions[service.anchor], []byte(`{"schema":"current","records":{"one":"new"}}`)); err != nil {
		t.Fatal(err)
	}
	got, err := reader.ReadSnapshotRecords(ctx, "records", []string{"one"}, nil)
	if err != nil || string(got.Records["one"]) != `"original"` {
		t.Fatalf("legacy capture changed: %+v %v", got, err)
	}
	raw, version, err := service.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	head, err := readRowHead(raw)
	if err != nil {
		t.Fatal(err)
	}
	head.Maintenance = "collector"
	raw, err = json.Marshal(head)
	if err != nil {
		t.Fatal(err)
	}
	if err = service.CompareAndSwap(ctx, version, raw); err != nil {
		t.Fatal(err)
	}
	if _, err = tr.CaptureSnapshotRecords(ctx); !errors.Is(err, ErrSnapshotMaintenance) {
		t.Fatalf("capture bypassed collection fence: %v", err)
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

func TestSnapshotCollectorValidatesSchemaBeforeAdoptingFence(t *testing.T) {
	svc := &documentService{docs: map[string][]byte{}, versions: map[string]uint64{}, anchor: "infra"}
	tr := &rowTransport{documentTransport: svc, workspace: "infra"}
	ctx := context.Background()
	if e := tr.CompareAndSwap(ctx, 0, []byte(`{"schema":"foreign","records":{"one":1}}`)); e != nil {
		t.Fatal(e)
	}
	var head rowHead
	if e := json.Unmarshal(svc.docs[svc.anchor], &head); e != nil {
		t.Fatal(e)
	}
	head.Maintenance = "crashed"
	svc.docs[svc.anchor], _ = json.Marshal(head)
	before := string(svc.docs[svc.anchor])
	refusal := errors.New("wrong caller schema")
	if _, e := tr.CollectGarbageChecked(ctx, func(map[string]json.RawMessage) error { return refusal }); !errors.Is(e, refusal) {
		t.Fatal(e)
	}
	if string(svc.docs[svc.anchor]) != before {
		t.Fatal("adopted a foreign workspace before validation")
	}
}

func TestCoreIndexedIssueReadsBoundedRowsAndPreservesAggregate(t *testing.T) {
	ctx := context.Background()
	svc := &documentService{docs: map[string][]byte{}, versions: map[string]uint64{}, anchor: "work"}
	transport := newRowTransport(svc, "work")
	st := emptyState()
	st.Config["project_id"] = "indexed-project"
	st.Config["issue_prefix"] = "bd"
	for n := 0; n < 1000; n++ {
		id := fmt.Sprintf("bd-indexed-%04d", n)
		st.Issues[id] = &record{Issue: types.Issue{ID: id, Title: id, Status: types.StatusOpen, IssueType: types.TypeTask, Priority: 2, RowVersion: 7}, Version: 7}
	}
	id := "bd-indexed-0123"
	expiry := time.Now().UTC().Add(time.Hour)
	st.Issues[id].Issue.LeaseExpiresAt = &expiry
	st.Issues[id].Issue.Comments = []*types.Comment{{ID: "comment-one", IssueID: id, Text: "indexed evidence"}}
	if err := st.addEdge(id, "bd-indexed-0000", types.DepBlocks, "test"); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if err = transport.CompareAndSwap(ctx, 0, encoded); err != nil {
		t.Fatal(err)
	}
	store := New(transport, false)
	store.expectedProject = "indexed-project"
	svc.reads = 0
	config, indexed, configErr := store.indexedConfig(ctx, []string{"issue_prefix"})
	if configErr != nil || !indexed || config["issue_prefix"] != "bd" || svc.reads > 6 {
		t.Fatalf("unbounded indexed open/config: %v %v %v reads=%d", config, indexed, configErr, svc.reads)
	}
	want := st.hydrate(id)
	svc.reads = 0
	got, err := store.GetIssue(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	left, _ := json.Marshal(want)
	right, _ := json.Marshal(got)
	if string(left) != string(right) {
		t.Fatalf("indexed aggregate differs: want=%s got=%s", left, right)
	}
	if svc.reads > 18 {
		t.Fatalf("point lookup read unrelated workspace/history: %d document reads", svc.reads)
	}
	got.Comments[0].Text = "caller mutation"
	again, err := store.GetIssue(ctx, id)
	if err != nil || again.Comments[0].Text != "indexed evidence" {
		t.Fatalf("caller alias: %+v %v", again, err)
	}
	store.expectedProject = "foreign-project"
	if _, err = store.GetIssue(ctx, id); err == nil {
		t.Fatal("indexed lookup bypassed project identity")
	}
	store.expectedProject = "indexed-project"
	if _, err = store.GetIssue(ctx, "bd-absent"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing indexed issue: %v", err)
	}
}

func TestGenericSnapshotCacheRetainsSmallRecordsAcrossLargeLiveSet(t *testing.T) {
	ctx := context.Background()
	tr, service := testRows()
	tr.domainProjection = false
	tr.cacheReads = true
	records := map[string]any{}
	for n := 0; n < 3000; n++ {
		records[fmt.Sprintf("id-%d", n)] = map[string]any{"title": fmt.Sprintf("row %d", n)}
	}
	data, _ := json.Marshal(map[string]any{"schema": "cache-v1", "records": records})
	if err := tr.CompareAndSwap(ctx, 0, data); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tr.Load(ctx); err != nil {
		t.Fatal(err)
	}
	_, v, err := tr.documentTransport.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	records["id-0"] = map[string]any{"title": "changed"}
	data, _ = json.Marshal(map[string]any{"schema": "cache-v1", "records": records})
	if err = tr.CompareAndSwap(ctx, v, data); err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	before := service.reads
	service.mu.Unlock()
	actual, _, err := tr.Load(ctx)
	if err != nil || !sameJSON(actual, data) {
		t.Fatalf("changed snapshot: %v", err)
	}
	service.mu.Lock()
	reads := service.reads - before
	service.mu.Unlock()
	if reads > 3 {
		t.Fatalf("one changed row reread %d documents; want head, one page and one row", reads)
	}
	if tr.immutableBytes > 8<<20 {
		t.Fatalf("cache exceeded memory budget: %d", tr.immutableBytes)
	}
}
