//go:build cgo && (darwin || linux)

package antfly

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/issueops"
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
	native := transport.(*embedded)
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
