package antfly

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/issueops"
)

func Initialize(ctx context.Context, dir string, target Target, prefix string) error {
	if err := target.Validate(); err != nil {
		return err
	}
	prefix = strings.TrimSuffix(prefix, "-")
	if strings.TrimSpace(prefix) == "" {
		return errors.New("issue prefix is required")
	}
	if _, err := os.Stat(filepath.Join(dir, configfile.ConfigFileName)); err == nil {
		return errors.New("workspace already has metadata.json; initialize a new workspace to avoid switching existing data")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if target.Path != "" {
		p, e := filepath.Abs(target.Path)
		if e != nil {
			return e
		}
		target.Path = p
		if e = os.MkdirAll(filepath.Dir(p), 0o700); e != nil {
			return e
		}
	}
	t, e := OpenTransport(target)
	if e != nil {
		return e
	}
	defer func() { _ = t.Close() }()
	if r, ok := t.(*remote); ok {
		path := "/db/v1/tables/" + url.PathEscape(target.Table)
		_, _, status, err := r.call(ctx, http.MethodGet, path, nil)
		if status == http.StatusNotFound {
			_, _, _, err = r.call(ctx, http.MethodPost, path, map[string]any{"num_shards": 1, "storage": map[string]any{"engine": "local", "dense_embeddings": "primary_lsm"}, "indexes": map[string]any{"beads": map[string]any{"type": "graph"}}})
		}
		if err != nil {
			return fmt.Errorf("provision Antfly table: %w", err)
		}
	}
	s := New(t, false)
	st, _, e := s.load(ctx)
	if e != nil {
		return e
	}
	project := st.Config["project_id"]
	if st.Config["issue_prefix"] != "" {
		if st.Config["issue_prefix"] != prefix {
			return errors.New("remote workspace prefix does not match requested prefix")
		}
		if project == "" {
			return errors.New("Antfly workspace is missing its project identity")
		}
	} else {
		project = configfile.GenerateProjectID()
		_, e = (&identity{s}).Bootstrap(ctx, issueops.BootstrapRequest{Prefix: prefix, ProjectID: project})
		if e != nil {
			return e
		}
	}
	if e = os.MkdirAll(dir, 0o700); e != nil {
		return e
	}
	b, e := json.MarshalIndent(target, "", "  ")
	if e != nil {
		return e
	}
	if e = os.WriteFile(filepath.Join(dir, "antfly.json"), b, 0o600); e != nil {
		return e
	}
	cfg := &configfile.Config{Backend: Backend, Database: target.Workspace, ProjectID: project}
	return cfg.Save(dir)
}
