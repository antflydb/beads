package antfly

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// ProjectionKey keeps separate workspaces' equal issue IDs distinct in a shared
// table. The aggregate owns the plain workspace key; bead: keys are reserved.
func ProjectionKey(workspace, id string) string {
	sum := sha256.Sum256([]byte(workspace))
	return "bead:" + hex.EncodeToString(sum[:16]) + ":" + id
}

// projections are published in the SAME Antfly transaction as the authority.
// Enrichments consume ordinary issue documents without decoding an aggregate.
// Their inferred relationships never participate in Beads readiness.
func projections(workspace string, data []byte) (map[string]json.RawMessage, error) {
	var st state
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	out := map[string]json.RawMessage{workspace: data}
	for id, r := range st.Issues {
		b, err := json.Marshal(r.Issue)
		if err != nil {
			return nil, err
		}
		var doc map[string]any
		if err = json.Unmarshal(b, &doc); err != nil {
			return nil, err
		}
		delete(doc, "heartbeat_at")
		delete(doc, "lease_expires_at")
		doc["beads_workspace"] = workspace
		doc["beads_revision"] = r.Version
		doc["beads_document_type"] = "issue"
		edges := map[string][]map[string]any{}
		for _, edge := range st.Edges {
			if edge.IssueID == id {
				kind := string(edge.Type)
				edges[kind] = append(edges[kind], map[string]any{"target": ProjectionKey(workspace, edge.DependsOnID), "weight": 1})
			}
		}
		doc["_edges"] = map[string]any{"beads": edges}
		encoded, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}
		out[ProjectionKey(workspace, id)] = encoded
	}
	return out, nil
}
