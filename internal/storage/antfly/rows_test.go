package antfly

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/steveyegge/beads/issueops"
	"reflect"
	"strings"
	"sync"
	"testing"
)

type documentService struct {
	mu          sync.Mutex
	docs        map[string][]byte
	versions    map[string]uint64
	clock       uint64
	anchor      string
	failPublish bool
}

func (s *documentService) Load(ctx context.Context) ([]byte, uint64, error) {
	return s.ReadDocument(ctx, s.anchor)
}
func (s *documentService) ReadDocument(_ context.Context, key string) ([]byte, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
