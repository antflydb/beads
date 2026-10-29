package antfly

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

func (s *Store) loadLocal() error {
	if s.local != nil {
		return nil
	}
	s.local = map[string]string{}
	if s.localDir == "" {
		return nil
	}
	b, e := os.ReadFile(filepath.Join(s.localDir, "antfly_local.json"))
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	return json.Unmarshal(b, &s.local)
}
func (s *Store) GetLocalMetadata(_ context.Context, key string) (string, error) {
	s.localMu.Lock()
	defer s.localMu.Unlock()
	if e := s.loadLocal(); e != nil {
		return "", e
	}
	return s.local[key], nil
}
func (s *Store) SetLocalMetadata(_ context.Context, key, value string) error {
	s.localMu.Lock()
	defer s.localMu.Unlock()
	if e := s.loadLocal(); e != nil {
		return e
	}
	s.local[key] = value
	if s.localDir == "" {
		return nil
	}
	b, e := json.Marshal(s.local)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(s.localDir, ".antfly-local-*")
	if e != nil {
		return e
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if _, e = f.Write(b); e != nil {
		_ = f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	return os.Rename(f.Name(), filepath.Join(s.localDir, "antfly_local.json"))
}
func (s *Store) GetMetadata(ctx context.Context, key string) (string, error) {
	if key == "_project_id" {
		return s.GetConfig(ctx, "project_id")
	}
	return s.GetConfig(ctx, "metadata."+key)
}
func (s *Store) SetMetadata(ctx context.Context, key, value string) error {
	return s.SetConfig(ctx, "metadata."+key, value)
}
