// Package antfly implements the experimental native Antfly Beads backend.
package antfly

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const Backend = "antfly"
const maxStateBytes = 4 << 20

var errConflict = errors.New("antfly version conflict")

// Transport deliberately exposes a conditional document write, not a local
// mutex: independent CLI processes and remote clients share the same fence.
type Transport interface {
	Load(context.Context) ([]byte, uint64, error)
	CompareAndSwap(context.Context, uint64, []byte) error
	Memory(context.Context, string, string, string, json.RawMessage) (json.RawMessage, error)
	Close() error
}

// Target contains routing only. Credentials are resolved from TokenEnv at open
// time and never written to metadata.json or antfly.json.
type Target struct {
	URL       string `json:"url,omitempty"`
	Path      string `json:"path,omitempty"`
	Library   string `json:"library,omitempty"`
	Table     string `json:"table"`
	Workspace string `json:"workspace"`
	TokenEnv  string `json:"token_env,omitempty"`
	Database  string `json:"database,omitempty"`
	Namespace string `json:"namespace,omitempty"`
}

func LoadTarget(dir string) (Target, error) {
	var t Target
	b, err := os.ReadFile(filepath.Join(dir, "antfly.json")) // #nosec G304 -- workspace routing file selected by the caller.
	if err != nil {
		return t, err
	}
	err = json.Unmarshal(b, &t)
	return t, err
}

func (t Target) Validate() error {
	if (t.URL == "") == (t.Path == "") {
		return errors.New("choose exactly one Antfly url or embedded path")
	}
	if t.Table == "" || t.Workspace == "" {
		return errors.New("Antfly table and workspace are required")
	}
	if strings.HasPrefix(t.Workspace, "bead:") {
		return errors.New("Antfly workspace keys beginning bead: are reserved for issue projections")
	}
	if t.Path != "" && ((t.Database != "" && t.Database != "default") || (t.Namespace != "" && t.Namespace != "public") || t.TokenEnv != "") {
		return errors.New("embedded Antfly supports default/public scope and no remote credentials")
	}
	if t.URL != "" && t.Library != "" {
		return errors.New("library is only valid for embedded Antfly")
	}
	if t.URL != "" {
		u, err := url.Parse(t.URL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return errors.New("Antfly URL must be an HTTP(S) base URL without credentials, query, or fragment")
		}
		if u.Scheme == "http" && u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1" {
			return errors.New("remote Antfly requires HTTPS")
		}
	}
	return nil
}

func OpenTransport(t Target) (Transport, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	if t.Path != "" {
		return openEmbedded(t)
	}
	token := ""
	if t.TokenEnv != "" {
		var ok bool
		token, ok = os.LookupEnv(t.TokenEnv)
		if !ok || token == "" {
			return nil, fmt.Errorf("Antfly credential environment variable %s is empty", t.TokenEnv)
		}
	}
	return &remote{target: t, token: token, client: &http.Client{Timeout: 60_000_000_000, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

type remote struct {
	target Target
	token  string
	client *http.Client
}

func (r *remote) call(ctx context.Context, method, path string, body any) ([]byte, http.Header, int, error) {
	var in io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, nil, 0, err
		}
		in = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(r.target.URL, "/")+path, in)
	if err != nil {
		return nil, nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if r.token != "" {
		req.Header.Set("Authorization", "Bearer "+r.token)
	}
	if r.target.Database != "" {
		req.Header.Set("X-Antfly-Database", r.target.Database)
	}
	if r.target.Namespace != "" {
		req.Header.Set("X-Antfly-Namespace", r.target.Namespace)
	}
	resp, err := r.client.Do(req) // #nosec G704 -- explicit user-selected, validated Antfly endpoint.
	if err != nil {
		return nil, nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxStateBytes+1))
	if err != nil {
		return nil, nil, resp.StatusCode, err
	}
	if len(b) > maxStateBytes {
		return nil, nil, resp.StatusCode, errors.New("Antfly response exceeds workspace size limit")
	}
	if resp.StatusCode >= 300 {
		return b, resp.Header, resp.StatusCode, fmt.Errorf("Antfly HTTP %d: %s", resp.StatusCode, b)
	}
	return b, resp.Header, resp.StatusCode, nil
}

func (r *remote) Load(ctx context.Context) ([]byte, uint64, error) {
	b, h, status, err := r.call(ctx, http.MethodGet, "/db/v1/tables/"+url.PathEscape(r.target.Table)+"/documents/"+url.PathEscape(r.target.Workspace), nil)
	if status == http.StatusNotFound {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	v, err := strconv.ParseUint(h.Get("X-Antfly-Version"), 10, 64)
	if err != nil || v == 0 {
		return nil, 0, errors.New("Antfly lookup did not return a nonzero X-Antfly-Version")
	}
	return b, v, nil
}

func (r *remote) CompareAndSwap(ctx context.Context, version uint64, data []byte) error {
	docs, err := projections(r.target.Workspace, data)
	if err != nil {
		return err
	}
	body := map[string]any{"read_set": []any{map[string]any{"table": r.target.Table, "key": r.target.Workspace, "version": strconv.FormatUint(version, 10)}}, "tables": map[string]any{r.target.Table: map[string]any{"inserts": docs, "sync_level": "write"}}}
	b, _, status, err := r.call(ctx, http.MethodPost, "/db/v1/transactions/commit", body)
	if status == http.StatusConflict {
		return errConflict
	}
	// A lost response can follow a durable commit. Never replay network errors.
	if err != nil {
		return fmt.Errorf("%w: %v", errIndeterminate, err)
	}
	var result struct {
		Status string `json:"status"`
	}
	if json.Unmarshal(b, &result) != nil {
		return errIndeterminate
	}
	if result.Status == "aborted" {
		return errConflict
	}
	if result.Status != "committed" && result.Status != "committed_pending" {
		return errIndeterminate
	}
	if result.Status == "committed_pending" || status == http.StatusAccepted {
		// The durable write is never replayed. Wait for read-after-write visibility;
		// a later workspace version necessarily includes this serialized mutation.
		visibleCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		for {
			if _, v, e := r.Load(visibleCtx); e == nil && v > version {
				return nil
			}
			timer := time.NewTimer(20 * time.Millisecond)
			select {
			case <-visibleCtx.Done():
				timer.Stop()
				return fmt.Errorf("%w: committed but visibility unconfirmed", errIndeterminate)
			case <-timer.C:
			}
		}
	}
	return nil
}

func (r *remote) Memory(ctx context.Context, space, action, key string, body json.RawMessage) (json.RawMessage, error) {
	paths := map[string]string{"create": "", "observe": "/observations", "recall": "/recall", "forget": "/forget"}
	p, ok := paths[action]
	if !ok {
		return nil, fmt.Errorf("unsupported memory action %q", action)
	}
	path := "/memory/v1/spaces/" + url.PathEscape(space) + p
	if action == "create" {
		path = "/memory/v1/spaces"
		var cfg map[string]any
		if err := json.Unmarshal(body, &cfg); err != nil {
			return nil, err
		}
		cfg = map[string]any{"space": space, "config": cfg}
		body, _ = json.Marshal(cfg)
	}
	// Memory uses a separate transport operation to carry its idempotency header.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(r.target.URL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	if r.token != "" {
		req.Header.Set("Authorization", "Bearer "+r.token)
	}
	if r.target.Database != "" {
		req.Header.Set("X-Antfly-Database", r.target.Database)
	}
	if r.target.Namespace != "" {
		req.Header.Set("X-Antfly-Namespace", r.target.Namespace)
	}
	resp, err := r.client.Do(req) // #nosec G704 -- explicit user-selected, validated Antfly endpoint.
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxStateBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxStateBytes {
		return nil, errors.New("memory response too large")
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("Antfly memory HTTP %d: %s", resp.StatusCode, b)
	}
	return b, nil
}

func (r *remote) Close() error { r.client.CloseIdleConnections(); return nil }
