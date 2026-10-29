package antfly

import (
	"context"
	"strings"

	"github.com/steveyegge/beads/internal/memoryapi"
	"github.com/steveyegge/beads/memoryops"
)

type notes struct{ s *Store }

func (n *notes) Remember(ctx context.Context, req memoryops.RememberRequest) (out memoryops.RememberResult, err error) {
	if err = memoryapi.ValidateContent(req.Content); err != nil {
		return out, err
	}
	key, err := memoryapi.ResolveKey(req.Key, req.Content)
	if err != nil {
		return out, err
	}
	err = n.s.mutate(ctx, func(st *state) error {
		_, exists := st.Notes[key]
		out = memoryops.RememberResult{Key: key, Value: req.Content, Replaced: exists}
		st.Notes[key] = req.Content
		st.audit("remember", key, "")
		return nil
	})
	return out, err
}
func (n *notes) Recall(ctx context.Context, req memoryops.RecallRequest) (memoryops.RecallResult, error) {
	if _, err := memoryapi.ValidateKey(req.Key); err != nil {
		return memoryops.RecallResult{}, err
	}
	st, _, err := n.s.load(ctx)
	if err != nil {
		return memoryops.RecallResult{}, err
	}
	v, ok := st.Notes[req.Key]
	return memoryops.RecallResult{Key: req.Key, Value: v, Found: ok}, nil
}
func (n *notes) Forget(ctx context.Context, req memoryops.ForgetRequest) (out memoryops.ForgetResult, err error) {
	if _, err = memoryapi.ValidateKey(req.Key); err != nil {
		return out, err
	}
	err = n.s.mutate(ctx, func(st *state) error {
		v, ok := st.Notes[req.Key]
		out = memoryops.ForgetResult{Key: req.Key, Value: v, Found: ok}
		if ok {
			delete(st.Notes, req.Key)
			st.audit("forget", req.Key, "")
		}
		return nil
	})
	return out, err
}
func (n *notes) List(ctx context.Context, req memoryops.ListRequest) (memoryops.ListResult, error) {
	st, _, err := n.s.load(ctx)
	if err != nil {
		return memoryops.ListResult{}, err
	}
	out := map[string]string{}
	q := strings.ToLower(req.Search)
	for k, v := range st.Notes {
		if strings.Contains(strings.ToLower(k), q) || strings.Contains(strings.ToLower(v), q) {
			out[k] = v
		}
	}
	return memoryops.ListResult{Memories: out}, nil
}
