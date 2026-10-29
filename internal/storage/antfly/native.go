package antfly

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/steveyegge/beads/internal/storage/backends"
	"net/http"
	"net/url"
)

type nativeTransport interface {
	Native(context.Context, string, string, json.RawMessage) (json.RawMessage, error)
}

func nativeCall(ctx context.Context, t Transport, op, name string, body json.RawMessage) (json.RawMessage, error) {
	native, ok := underlyingTransport(t).(nativeTransport)
	if !ok {
		return nil, errUnsupported("native." + op)
	}
	out, err := native.Native(ctx, op, name, body)
	if err == nil && op == "query" {
		var shape map[string]json.RawMessage
		if json.Unmarshal(out, &shape) == nil && shape["responses"] == nil {
			out, _ = json.Marshal(map[string]any{"responses": []json.RawMessage{out}})
		}
	}
	return out, err
}
func (r *remote) Native(ctx context.Context, op, name string, body json.RawMessage) (json.RawMessage, error) {
	base := "/db/v1/tables/" + url.PathEscape(r.target.Table)
	method, path := http.MethodPost, ""
	switch op {
	case "index":
		var cfg map[string]any
		if err := json.Unmarshal(body, &cfg); err != nil {
			return nil, err
		}
		delete(cfg, "name")
		body, _ = json.Marshal(cfg)
		path = base + "/indexes/" + url.PathEscape(name)
	case "enrichment":
		method = http.MethodPut
		path = base + "/artifacts/" + url.PathEscape(name) + "/enrichment"
	case "query":
		path = base + "/query"
	case "resolve", "render":
		path = base + "/resolvers/" + url.PathEscape(name) + ":" + op
	case "reviews":
		method = http.MethodGet
		path = base + "/resolvers/" + url.PathEscape(name) + "/reviews"
		body = nil
	case "review":
		resolver, review, err := resolverReview(name)
		if err != nil {
			return nil, err
		}
		path = base + "/resolvers/" + url.PathEscape(resolver) + "/reviews/" + url.PathEscape(review)
	case "curation":
		method = http.MethodGet
		path = base + "/resolvers/" + url.PathEscape(name) + "/curation"
		body = nil
	case "merge":
		path = base + "/resolvers/" + url.PathEscape(name) + "/redirects"
	case "suppress":
		path = base + "/resolvers/" + url.PathEscape(name) + "/suppressions"
	default:
		return nil, errUnsupported("native." + op)
	}
	out, _, _, err := r.call(ctx, method, path, body)
	return out, err
}
func resolverReview(name string) (string, string, error) {
	for i := range name {
		if name[i] == '/' && i > 0 && i < len(name)-1 {
			return name[:i], name[i+1:], nil
		}
	}
	return "", "", fmt.Errorf("review name must be resolver/review-id")
}
func ensureTableWith(ctx context.Context, t Target, opts backends.OpenOptions) (Transport, error) {
	tr, err := OpenTransportWith(t, opts)
	if err != nil {
		return nil, err
	}
	if r, ok := underlyingTransport(tr).(*remote); ok {
		path := "/db/v1/tables/" + url.PathEscape(t.Table)
		_, _, status, e := r.call(ctx, http.MethodGet, path, nil)
		if status == http.StatusNotFound {
			_, _, _, e = r.call(ctx, http.MethodPost, path, map[string]any{"num_shards": 1, "storage": map[string]any{"engine": "local", "dense_embeddings": "primary_lsm"}})
		}
		if e != nil {
			_ = tr.Close()
			return nil, e
		}
	}
	return tr, nil
}
