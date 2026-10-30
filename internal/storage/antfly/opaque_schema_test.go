package antfly

import (
	"context"
	"encoding/json"
	"github.com/steveyegge/beads/internal/storage/backends"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPrivateSnapshotTextIsOpaqueNativeAndRemote(t *testing.T) {
	for _, mode := range []string{"embedded", "remote"} {
		t.Run(mode, func(t *testing.T) {
			target := Target{Table: "opaque_schema_" + digest([]byte(t.Name() + time.Now().String()))[:16], Workspace: "main"}
			if mode == "embedded" {
				target.Library = os.Getenv("ANTFLY_TEST_LIBRARY")
				if target.Library == "" {
					t.Skip("ANTFLY_TEST_LIBRARY required")
				}
				target.Path = filepath.Join(t.TempDir(), "work.aflite")
			} else {
				target.URL = os.Getenv("ANTFLY_TEST_URL")
				if target.URL == "" {
					t.Skip("ANTFLY_TEST_URL required")
				}
			}
			ctx := context.Background()
			tr, err := ensureTableWith(ctx, target, backends.OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer tr.Close()
			raw := underlyingTransport(tr)
			// A projection may carry both searchable public issue text and an opaque
			// private payload. Head fields and page references are private as well.
			body := []byte(`{"title":"visiblezapsentinel","_beads_immutable_json":"privatezapsentinel","fields":{"secret":"fieldzapsentinel"},"pages":{"secret":"pagezapsentinel"}}`)
			if err = raw.CompareAndSwap(ctx, 0, body); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(20 * time.Second)
			for {
				values := map[string]int{}
				for field, term := range map[string]string{"title": "visiblezapsentinel", "_beads_immutable_json": "privatezapsentinel", "fields.secret": "fieldzapsentinel", "pages.secret": "pagezapsentinel"} {
					request, _ := json.Marshal(map[string]any{"full_text_search": map[string]any{"match": term, "field": field}, "limit": 1})
					result, err := nativeCall(ctx, tr, "query", "", request)
					if err != nil {
						t.Fatal(err)
					}
					var response struct {
						Responses []struct {
							Hits struct {
								Total struct {
									Value int `json:"value"`
								} `json:"total"`
							} `json:"hits"`
						} `json:"responses"`
					}
					if err = json.Unmarshal(result, &response); err != nil || len(response.Responses) != 1 {
						t.Fatalf("query response: %s %v", result, err)
					}
					values[field] = response.Responses[0].Hits.Total.Value
				}
				if values["title"] == 1 {
					if values["_beads_immutable_json"] != 0 || values["fields.secret"] != 0 || values["pages.secret"] != 0 {
						t.Fatalf("private snapshot fields indexed: %+v", values)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("public title never searchable: %+v", values)
				}
				time.Sleep(20 * time.Millisecond)
			}
			got, _, err := raw.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var wantDoc, gotDoc map[string]any
			if err = json.Unmarshal(body, &wantDoc); err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(got, &gotDoc); err != nil {
				t.Fatal(err)
			}
			for field, want := range wantDoc {
				a, _ := json.Marshal(want)
				b, _ := json.Marshal(gotDoc[field])
				if string(a) != string(b) {
					t.Fatalf("opaque field not stored: %s", field)
				}
			}
		})
	}
}
