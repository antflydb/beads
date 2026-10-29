package antfly

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/steveyegge/beads/internal/storage/backends"
)

type foreignCredential struct{}

func (foreignCredential) BackendCredential() {}
func TestPerWorkspaceCredentialsDoNotUseAmbientEnvironment(t *testing.T) {
	t.Setenv("ANTFLY_WRONG_TOKEN", "ambient")
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		workspace := "a"
		if strings.HasSuffix(r.URL.Path, "/b") {
			workspace = "b"
		}
		if r.Header.Get("Authorization") != "Bearer city-"+workspace || r.Header.Get("User-Agent") != "factory/1" {
			t.Errorf("workspace %s received the wrong per-open credential or agent", workspace)
			w.WriteHeader(401)
			return
		}
		w.Header().Set("X-Antfly-Version", "1")
		state := emptyState()
		state.Config["project_id"] = workspace
		state.Config["issue_prefix"] = "bd"
		_ = json.NewEncoder(w).Encode(state)
	}))
	defer server.Close()
	for _, workspace := range []string{"a", "b"} {
		target := Target{URL: server.URL, Table: "work", Workspace: workspace, TokenEnv: "ANTFLY_WRONG_TOKEN"}
		tr, e := OpenTransportWith(target, backends.OpenOptions{Credential: TokenCredential("city-" + workspace), HTTPClient: server.Client(), UserAgent: "factory/1"})
		if e != nil {
			t.Fatal(e)
		}
		data, _, e := tr.Load(context.Background())
		_ = tr.Close()
		if e != nil || !strings.Contains(string(data), `"project_id":"`+workspace+`"`) {
			t.Fatalf("workspace %s load=%s %v", workspace, data, e)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("requests=%d", calls.Load())
	}
	if _, e := OpenTransportWith(Target{URL: server.URL, Table: "work", Workspace: "a"}, backends.OpenOptions{Credential: foreignCredential{}}); !errors.Is(e, backends.ErrUnsupportedCredential) {
		t.Fatalf("foreign credential=%v", e)
	}
	if _, e := OpenTransportWith(Target{URL: server.URL, Table: "work", Workspace: "a"}, backends.OpenOptions{Credential: TokenCredential("")}); e == nil {
		t.Fatal("explicit empty credential fell back to ambient authentication")
	}
}
func TestInjectedHTTPClientCannotRedirectCredentials(t *testing.T) {
	var leaked atomic.Bool
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Store(true); w.WriteHeader(200) }))
	defer destination.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, http.StatusFound) }))
	defer origin.Close()
	client := origin.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return nil }
	tr, e := OpenTransportWith(Target{URL: origin.URL, Table: "work", Workspace: "a"}, backends.OpenOptions{Credential: TokenCredential("secret"), HTTPClient: client})
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = tr.Close() }()
	if _, _, e = tr.Load(context.Background()); e == nil {
		t.Fatal("redirect was accepted")
	}
	if leaked.Load() {
		t.Fatal("credential-bearing request followed a redirect")
	}
}
