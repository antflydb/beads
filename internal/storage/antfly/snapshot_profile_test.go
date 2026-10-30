package antfly

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/steveyegge/beads/internal/storage"
	"github.com/steveyegge/beads/internal/storage/backends"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCapturedSnapshotRecordsAcrossPublicationNativeAndRemote(t *testing.T) {
	for _, mode := range []string{"embedded", "remote"} {
		t.Run(mode, func(t *testing.T) {
			target := Target{Table: "snapshot_capture_" + digest([]byte(t.Name() + time.Now().String()))[:16], Workspace: "control"}
			if mode == "embedded" {
				target.Library = os.Getenv("ANTFLY_TEST_LIBRARY")
				if target.Library == "" {
					t.Skip("ANTFLY_TEST_LIBRARY required")
				}
				target.Path = filepath.Join(t.TempDir(), "control.aflite")
			} else {
				target.URL = os.Getenv("ANTFLY_TEST_URL")
				if target.URL == "" {
					t.Skip("ANTFLY_TEST_URL required")
				}
			}
			ctx := context.Background()
			tr, err := OpenSnapshot(ctx, target, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer tr.Close()
			original := []byte(`{"schema":"control","records":{"one":{"fence":7,"revision":19}},"indexes":{"ready":["one"]}}`)
			if err = tr.CompareAndSwap(ctx, 0, original); err != nil {
				t.Fatal(err)
			}
			reader, err := tr.(SnapshotRecordCapturer).CaptureSnapshotRecords(ctx)
			if err != nil {
				t.Fatal(err)
			}
			posting, err := reader.ReadSnapshotRecords(ctx, "indexes", []string{"ready"}, nil)
			if err != nil || string(posting.Records["ready"]) != `["one"]` {
				t.Fatalf("captured posting: %+v %v", posting, err)
			}
			writer, err := OpenSnapshot(ctx, target, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			changed := []byte(`{"schema":"control","records":{"one":{"fence":8,"revision":20}},"indexes":{"ready":[]}}`)
			if err = writer.CompareAndSwap(ctx, posting.Version, changed); err != nil {
				t.Fatal(err)
			}
			rows, err := reader.ReadSnapshotRecords(ctx, "records", []string{"one"}, &posting.Version)
			if err != nil || string(rows.Records["one"]) != `{"fence":7,"revision":19}` {
				t.Fatalf("captured read mixed generations: %+v %v", rows, err)
			}
			if err = tr.CompareAndSwap(ctx, posting.Version, original); !errors.Is(err, ErrSnapshotConflict) {
				t.Fatalf("captured reader bypassed current write fence: %v", err)
			}
			actual, _, err := writer.Load(ctx)
			if err != nil || !jsonEqual(actual, changed) {
				t.Fatalf("stale publication changed live state: %s %v", actual, err)
			}
		})
	}
}

func TestSnapshotTablesAvoidDomainIndexesNativeAndRemote(t *testing.T) {
	for _, mode := range []string{"embedded", "remote"} {
		t.Run(mode, func(t *testing.T) {
			target := Target{Table: "snapshot_profile_" + digest([]byte(t.Name() + time.Now().String()))[:16], Workspace: "control"}
			if mode == "embedded" {
				target.Library = os.Getenv("ANTFLY_TEST_LIBRARY")
				if target.Library == "" {
					t.Skip("ANTFLY_TEST_LIBRARY required")
				}
				target.Path = filepath.Join(t.TempDir(), "control.aflite")
			} else {
				target.URL = os.Getenv("ANTFLY_TEST_URL")
				if target.URL == "" {
					t.Skip("ANTFLY_TEST_URL required")
				}
			}
			ctx := context.Background()
			tr, err := OpenSnapshot(ctx, target, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer tr.Close()
			raw, err := nativeCall(ctx, tr, "indexes", "", nil)
			if err != nil {
				t.Fatal(err)
			}
			var array []json.RawMessage
			var object map[string]json.RawMessage
			if json.Unmarshal(raw, &array) == nil {
				if len(array) != 0 {
					t.Fatalf("snapshot table has domain indexes: %s", raw)
				}
			} else if json.Unmarshal(raw, &object) == nil {
				if len(object) != 0 {
					t.Fatalf("snapshot table has domain indexes: %s", raw)
				}
			} else {
				t.Fatalf("index response: %s", raw)
			}
			payload := []byte(`{"schema":"control","records":{"one":{"fence":19,"revision":7,"text":"stored exactly"}}}`)
			if err = tr.CompareAndSwap(ctx, 0, payload); err != nil {
				t.Fatal(err)
			}
			got, version, err := tr.Load(ctx)
			if err != nil || !jsonEqual(got, payload) {
				t.Fatalf("snapshot data: %s %v", got, err)
			}
			work := target
			work.Workspace = "work"
			var unsupported *storage.ErrUnsupported
			if attached, err := ensureTableWith(ctx, work, backends.OpenOptions{}); err == nil {
				attached.Close()
				t.Fatal("core work attached to a snapshot-only table")
			} else if !errors.As(err, &unsupported) {
				t.Fatalf("wrong core attachment error: %v", err)
			}
			after, next, err := tr.Load(ctx)
			if err != nil || next != version || !jsonEqual(after, payload) {
				t.Fatalf("rejected core attachment changed state: %v", err)
			}
			// Reopening completes the declared profile idempotently and retains
			// its revision/fence values without reintroducing domain indexes.
			reopened, err := OpenSnapshot(ctx, target, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			after, next, err = reopened.Load(ctx)
			if err != nil || next != version || !jsonEqual(after, payload) {
				t.Fatalf("profile reopen changed state: %v", err)
			}
			indexes, err := nativeCall(ctx, reopened, "indexes", "", nil)
			if err != nil {
				t.Fatal(err)
			}
			names, err := tableIndexNames(indexes)
			if err != nil || len(names) != 0 {
				t.Fatalf("profile indexes reappeared: %s %v", indexes, err)
			}
			// Existing core tables are a compatibility path. Opening a generic
			// workspace there must leave every existing index intact.
			legacy := target
			legacy.Table += "_legacy"
			legacy.Workspace = "legacy"
			prior, err := ensureTableWith(ctx, legacy, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			beforeIndexes, err := nativeCall(ctx, prior, "indexes", "", nil)
			if err != nil {
				t.Fatal(err)
			}
			prior.Close()
			attached, err := OpenSnapshot(ctx, legacy, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer attached.Close()
			afterIndexes, err := nativeCall(ctx, attached, "indexes", "", nil)
			if err != nil || !jsonEqual(beforeIndexes, afterIndexes) {
				t.Fatalf("legacy indexes changed: %s -> %s %v", beforeIndexes, afterIndexes, err)
			}
		})
	}
}
