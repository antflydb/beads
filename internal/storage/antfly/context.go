package antfly

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/steveyegge/beads/internal/remedy"
	"github.com/steveyegge/beads/internal/types"
	"net/url"
	"runtime"
	"sort"
	"strings"
	"unicode/utf8"
)

type ContextRequest struct {
	Query      string               `json:"query"`
	Limit      int                  `json:"limit,omitempty"`
	MaxTokens  int                  `json:"max_tokens,omitempty"`
	Embeddings map[string][]float64 `json:"embeddings,omitempty"`
}
type ContextSource struct {
	CitationNote        string `json:"citation_note,omitempty"`
	ConfigurationDigest string `json:"configuration_digest,omitempty"`
	ProducerRevision    string `json:"producer_revision,omitempty"`
	Kind                string `json:"kind"`
	ID                  string `json:"id"`
	Citation            string `json:"citation"`
	Text                string `json:"text"`
}
type ContextResult struct {
	Sources   []ContextSource `json:"sources"`
	Truncated bool            `json:"truncated"`
}

func (s *Store) Context(ctx context.Context, req ContextRequest) (ContextResult, error) {
	out := ContextResult{Sources: []ContextSource{}}
	if req.Query == "" {
		return out, errors.New("context query is required")
	}
	if req.Limit == 0 {
		req.Limit = 10
	}
	if req.Limit < 1 || req.Limit > 100 {
		return out, errors.New("context limit must be 1..100")
	}
	if req.MaxTokens == 0 {
		req.MaxTokens = 4096
	}
	if req.MaxTokens < 128 || req.MaxTokens > 65536 {
		return out, errors.New("context max_tokens must be 128..65536")
	}
	// One UTF-8 byte per token is a conservative budget for byte-based model
	// tokenizers. Account for the complete encoded result, including citations.
	appendSource := func(source ContextSource) {
		candidate := append(out.Sources, source)
		b, _ := json.Marshal(ContextResult{Sources: candidate, Truncated: out.Truncated})
		if len(b) <= req.MaxTokens {
			out.Sources = candidate
			return
		}
		out.Truncated = true
		overflow := len(b) - req.MaxTokens + 8
		if overflow >= len(source.Text) {
			return
		}
		source.Text = source.Text[:len(source.Text)-overflow]
		for !utf8.ValidString(source.Text) {
			source.Text = source.Text[:len(source.Text)-1]
		}
		candidate = append(out.Sources, source)
		b, _ = json.Marshal(ContextResult{Sources: candidate, Truncated: true})
		if len(b) <= req.MaxTokens {
			out.Sources = candidate
		}
	}
	st, _, err := s.load(ctx)
	if err != nil {
		return out, err
	}
	// Exact task state remains independent of inference and recall.
	rows, err := st.search(req.Query, types.IssueFilter{SkipWisps: true})
	if err != nil {
		return out, err
	}
	for i, row := range rows {
		if i >= req.Limit {
			break
		}
		argument, note := remedy.CommandArg(runtime.GOOS, row.ID, "<issue-id>")
		appendSource(ContextSource{Kind: "issue", ID: row.ID, Citation: "bd show " + argument, CitationNote: note, Text: row.Title + "\n" + row.Description + "\n" + row.Notes})
	}
	if st.Knowledge == nil {
		return out, nil
	}
	status, err := s.KnowledgeStatus(ctx)
	if err != nil {
		return out, err
	}
	query := map[string]any{"full_text_search": map[string]any{"match": req.Query, "field": "text"}, "limit": req.Limit, "filter_query": map[string]any{"term": st.Knowledge.Digest, "field": "configuration_digest"}}
	if len(req.Embeddings) > 0 {
		query["embeddings"] = req.Embeddings
		indexes := make([]string, 0, len(req.Embeddings))
		for index := range req.Embeddings {
			indexes = append(indexes, index)
		}
		sort.Strings(indexes)
		query["indexes"] = indexes
	}
	if len(req.Embeddings) == 0 {
		indexes := []string{}
		for name, raw := range st.Knowledge.Config.Indexes {
			var index struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(raw, &index) == nil && index.Type == "embeddings" {
				indexes = append(indexes, name)
			}
		}
		if len(indexes) > 0 {
			sort.Strings(indexes)
			query["semantic_search"] = req.Query
			query["indexes"] = indexes
		}
	}
	body, _ := json.Marshal(query)
	result, err := s.KnowledgeOperation(ctx, "query", "", body)
	if err != nil {
		return out, err
	}
	var response struct {
		Responses []struct {
			Hits struct {
				Hits []struct {
					ID     string `json:"_id"`
					Source struct {
						Text                string `json:"text"`
						Forgotten           bool   `json:"forgotten"`
						ConfigurationDigest string `json:"configuration_digest"`
						ProducerRevision    string `json:"producer_revision"`
					} `json:"_source"`
				} `json:"hits"`
			} `json:"hits"`
		} `json:"responses"`
	}
	if err = json.Unmarshal(result, &response); err != nil {
		return out, err
	}
	seeds := []string{}
	for _, r := range response.Responses {
		for _, hit := range r.Hits.Hits {
			if hit.Source.Forgotten {
				continue
			}
			seeds = append(seeds, hit.ID)
			appendSource(ContextSource{Kind: "evidence", ID: hit.ID, Citation: StableSourceURL(status.SourceTable, hit.ID), Text: hit.Source.Text, ConfigurationDigest: hit.Source.ConfigurationDigest, ProducerRevision: hit.Source.ProducerRevision})
		}
	}
	// Graph expansion returns identities and source citations, never workflow
	// blockers. The engine rejects unsupported cross-shard seeded placements.
	if len(seeds) > 0 {
		graph, _ := json.Marshal(map[string]any{"graph_queries": map[string]any{"context": map[string]any{"index": "knowledge", "traverse": map[string]any{"start": map[string]any{"keys": seeds}, "edge_types": []string{"mentions", "records"}, "max_depth": 2, "limit": req.Limit}}}, "limit": req.Limit})
		expanded, e := s.KnowledgeOperation(ctx, "query", "", graph)
		if e != nil {
			return out, e
		}
		var envelope struct {
			Responses []struct {
				Graph map[string]struct {
					Nodes []struct {
						Key   string `json:"key"`
						Table string `json:"table"`
					} `json:"nodes"`
				} `json:"graph_results"`
			} `json:"responses"`
		}
		if err = json.Unmarshal(expanded, &envelope); err != nil {
			return out, err
		}
		for _, r := range envelope.Responses {
			for _, g := range r.Graph {
				for _, node := range g.Nodes {
					table := node.Table
					if table == "" {
						table = status.SourceTable
					}
					if node.Table == "" && strings.HasPrefix(node.Key, "entity/") {
						table = status.EntityTable
					} else if node.Table == "" && strings.HasPrefix(node.Key, "event/") {
						table = status.EventTable
					}
					appendSource(ContextSource{Kind: "entity", ID: node.Key, Citation: StableSourceURL(table, node.Key), Text: node.Key})
				}
			}
		}
	}
	recall, _ := json.Marshal(map[string]any{"query": req.Query})
	memory, err := s.Memory(ctx, "", "recall", "", recall)
	if err != nil {
		return out, err
	}
	var recalled struct {
		Items []json.RawMessage `json:"items"`
	}
	if err = json.Unmarshal(memory, &recalled); err != nil {
		return out, err
	}
	for _, raw := range recalled.Items {
		var item struct {
			ID        string `json:"id"`
			Statement string `json:"statement"`
			Memory    struct {
				Assertion struct {
					ID        string `json:"id"`
					Statement string `json:"statement"`
				} `json:"assertion"`
			} `json:"memory"`
		}
		if err = json.Unmarshal(raw, &item); err != nil {
			return out, err
		}
		if item.ID == "" {
			item.ID = item.Memory.Assertion.ID
			item.Statement = item.Memory.Assertion.Statement
		}
		if item.ID == "" {
			return out, errors.New("memory recall omitted its assertion identity")
		}
		appendSource(ContextSource{Kind: "memory", ID: item.ID, Citation: "/memory/v1/spaces/" + url.PathEscape(status.Space) + "/assertions/" + url.PathEscape(item.ID), Text: item.Statement})
	}

	return out, nil
}
