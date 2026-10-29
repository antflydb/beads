package antfly

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"
)

type KnowledgeConfig struct {
	ManagedObservations    bool                       `json:"managed_observations,omitempty"`
	Revision               string                     `json:"revision"`
	ReplacePending         string                     `json:"replace_pending,omitempty"`
	ExpectedMemoryRevision *uint64                    `json:"expected_memory_revision,omitempty"`
	EntityResolver         string                     `json:"entity_resolver,omitempty"`
	SpaceConfig            json.RawMessage            `json:"space_config,omitempty"`
	Enrichments            map[string]json.RawMessage `json:"enrichments,omitempty"`
	Indexes                map[string]json.RawMessage `json:"indexes,omitempty"`
}
type KnowledgePreparation struct {
	Digest         string    `json:"digest"`
	PreviousDigest string    `json:"previous_digest"`
	ExpiresAt      time.Time `json:"expires_at"`
}

type KnowledgeState struct {
	Config      KnowledgeConfig `json:"config"`
	Digest      string          `json:"digest"`
	Generations []string        `json:"generations"`
	Checkpoint  uint64          `json:"checkpoint"`
	Worker      string          `json:"worker,omitempty"`
	WorkerUntil time.Time       `json:"worker_until,omitempty"`
}
type KnowledgeStatus struct {
	Preparing   *KnowledgePreparation `json:"preparing,omitempty"`
	Configured  bool                  `json:"configured"`
	SourceTable string                `json:"source_table"`
	EntityTable string                `json:"entity_table"`
	EventTable  string                `json:"event_table"`
	Space       string                `json:"space"`
	Pending     uint64                `json:"pending"`
	State       *KnowledgeState       `json:"state,omitempty"`
}

func (s *Store) knowledgeTarget() (Target, error) {
	if s.target == nil {
		return Target{}, errUnsupported("knowledge: missing catalog routing")
	}
	t := *s.target
	if t.Database == "" {
		t.Database = "default"
	}
	if t.Namespace == "" {
		t.Namespace = "public"
	}
	t.Table = "beads_kg_" + digest([]byte(t.Database + "\x00" + t.Namespace + "\x00" + t.Table + "\x00" + t.Workspace + "\x00" + s.expectedProject))[:32]
	return t, nil
}
func (s *Store) KnowledgeStatus(ctx context.Context) (KnowledgeStatus, error) {
	st, _, err := s.load(ctx)
	if err != nil {
		return KnowledgeStatus{}, err
	}
	target, err := s.knowledgeTarget()
	if err != nil {
		return KnowledgeStatus{}, err
	}
	out := KnowledgeStatus{Configured: st.Knowledge != nil, SourceTable: target.Table, EntityTable: target.Table + "_entities", EventTable: target.Table + "_events", Space: s.memorySpace, State: st.Knowledge, Preparing: st.KnowledgePreparing}
	if st.Knowledge != nil && uint64(len(st.Events)) >= st.Knowledge.Checkpoint {
		out.Pending = uint64(len(st.Events)) - st.Knowledge.Checkpoint
	}
	return out, nil
}
func rejectInlineCredentials(value any) error {
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			normalized := strings.ToLower(key)
			if normalized == "api_key" || normalized == "token" || normalized == "password" || normalized == "secret" {
				if str, ok := item.(string); ok && str != "" && !strings.HasPrefix(str, "${secret:") && !strings.HasPrefix(str, "${env:") {
					return errors.New("knowledge configuration must use environment defaults or secret references for credentials")
				}
			}
			if err := rejectInlineCredentials(item); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range v {
			if err := rejectInlineCredentials(item); err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *Store) ConfigureKnowledge(ctx context.Context, cfg KnowledgeConfig) error {
	if strings.TrimSpace(cfg.Revision) == "" {
		return errors.New("knowledge configuration requires an explicit revision")
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	var generic any
	if err = json.Unmarshal(encoded, &generic); err != nil {
		return err
	}
	if err = rejectInlineCredentials(generic); err != nil {
		return err
	}
	st, _, err := s.load(ctx)
	if err != nil {
		return err
	}
	if err = mainClaims(st); err != nil {
		return err
	}
	if s.readOnly {
		return errors.New("Antfly store is read-only")
	}
	target, err := s.knowledgeTarget()
	if err != nil {
		return err
	}
	// Deterministic entity/event promotion works without a model. An explicitly
	// configured extractor can replace this source with a generated artifact.
	graph := map[string]any{"type": "graph", "source": map[string]any{"artifact": "beads_kg_v1", "format": "extraction_graph", "mention_edge_type": "mentions"}, "artifact": map[string]any{"name": "beads_kg_v1", "kind": "asset", "source": map[string]any{"type": "field", "value": "kg"}, "content_type": "application/json"}, "edge_types": []any{map[string]any{"name": "mentions"}, map[string]any{"name": "records"}}, "resolvers": []any{map[string]any{"name": "entities", "table": target.Table + "_entities", "source_artifact": "beads_kg_v1", "resolution_artifact": "beads_entities_v1", "key_template": "entity/{{hash _entity.text}}", "config_generation": 1}, map[string]any{"name": "events", "labels": []string{"event"}, "table": target.Table + "_events", "source_artifact": "beads_kg_v1", "resolution_artifact": "beads_events_v1", "key_template": "event/{{hash _entity.text}}", "config_generation": 1}}}
	indexes := map[string]json.RawMessage{}
	indexes["knowledge"], _ = json.Marshal(graph)
	indexes["text"] = json.RawMessage(`{"type":"full_text","field":"text"}`)
	for name, index := range cfg.Indexes {
		indexes[name] = index
	}
	for name, raw := range indexes {
		var index struct {
			Resolvers []struct {
				Table string `json:"table"`
			} `json:"resolvers"`
		}
		if err := json.Unmarshal(raw, &index); err != nil {
			return err
		}
		for _, resolver := range index.Resolvers {
			if resolver.Table == s.target.Table {
				return fmt.Errorf("knowledge index %s must not promote entities into the authoritative work table", name)
			}
		}
	}
	resolved, _ := json.Marshal(map[string]any{"config": cfg, "indexes": indexes, "defaults": "beads-kg-v1", "source_table": target.Table})
	hash := digest(resolved)
	if st.Knowledge != nil && st.Knowledge.Digest == hash {
		if st.KnowledgePreparing != nil {
			return errors.New("a knowledge configuration is pending; resume it or explicitly replace its digest")
		}
		return nil
	}
	previous := ""
	if st.Knowledge != nil {
		previous = st.Knowledge.Digest
	}
	normalizeSpace := func(raw json.RawMessage) json.RawMessage {
		if len(raw) == 0 {
			return json.RawMessage(`{}`)
		}
		return raw
	}
	changeSpace := st.Knowledge != nil && !jsonEqual(normalizeSpace(st.Knowledge.Config.SpaceConfig), normalizeSpace(cfg.SpaceConfig))
	if changeSpace && cfg.ExpectedMemoryRevision == nil {
		return errors.New("changing native memory configuration requires expected_memory_revision from memory inspect")
	}
	if err = s.mutate(ctx, func(current *state) error {
		if current.Knowledge != nil && current.Knowledge.Worker != "" && current.Knowledge.WorkerUntil.After(time.Now()) {
			return errors.New("knowledge consumer is running; retry configuration after its lease is released")
		}
		if p := current.KnowledgePreparing; p != nil && p.Digest != hash && p.ExpiresAt.After(time.Now()) && cfg.ReplacePending != p.Digest {
			return errors.New("another knowledge configuration is pending")
		}
		observed := ""
		if current.Knowledge != nil {
			observed = current.Knowledge.Digest
		}
		if observed != previous {
			return errors.New("knowledge configuration changed during preparation")
		}
		current.KnowledgePreparing = &KnowledgePreparation{Digest: hash, PreviousDigest: previous, ExpiresAt: time.Now().UTC().Add(5 * time.Minute)}
		return nil
	}); err != nil {
		return err
	}
	for _, name := range []string{target.Table, target.Table + "_entities", target.Table + "_events"} {
		table := target
		table.Table = name
		tr, err := s.openTarget(ctx, table, true)
		if err != nil {
			return err
		}
		_ = tr.Close()
	}
	tr, err := s.openTarget(ctx, target, false)
	if err != nil {
		return err
	}
	defer func() { _ = tr.Close() }()
	names := make([]string, 0, len(cfg.Enrichments))
	for name := range cfg.Enrichments {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if _, err = nativeCall(ctx, tr, "enrichment", name, cfg.Enrichments[name]); err != nil {
			return fmt.Errorf("configure enrichment %s: %w", name, err)
		}
	}
	names = names[:0]
	for name := range indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if _, err = nativeCall(ctx, tr, "index", name, indexes[name]); err != nil {
			return fmt.Errorf("configure index %s: %w", name, err)
		}
	}
	space := normalizeSpace(cfg.SpaceConfig)
	if cfg.ExpectedMemoryRevision != nil {
		body, _ := json.Marshal(map[string]any{"expected_revision": *cfg.ExpectedMemoryRevision, "config": space})
		if _, err = s.Memory(ctx, "", "configure", "beads-configure-"+hash, body); err != nil {
			return err
		}
	} else if st.Knowledge == nil {
		if _, err = s.Memory(ctx, "", "create", "beads-space-"+hash, space); err != nil {
			return err
		}
	}
	return s.mutate(ctx, func(st *state) error {
		if st.KnowledgePreparing == nil || st.KnowledgePreparing.Digest != hash {
			return errors.New("knowledge configuration preparation fence changed")
		}
		observed := ""
		if st.Knowledge != nil {
			observed = st.Knowledge.Digest
		}
		if observed != previous && observed != hash {
			return errors.New("knowledge configuration was superseded")
		}
		st.KnowledgePreparing = nil
		if st.Knowledge != nil && st.Knowledge.Digest == hash {
			return nil
		}
		generations := []string{}
		if st.Knowledge != nil {
			generations = append(generations, st.Knowledge.Generations...)
			generations = append(generations, st.Knowledge.Digest)
		}
		st.Knowledge = &KnowledgeState{Config: cfg, Digest: hash, Generations: generations}
		return nil
	})
}
func eventPlane(e event) string {
	if e.Operation == "remember" || e.Operation == "forget" {
		return "note"
	}
	return "issue"
}
func eventSource(project, configuration string, e event) string {
	return "beads:" + digest([]byte(project + "\x00" + configuration + "\x00" + eventPlane(e) + "\x00" + e.ID))[:32] + fmt.Sprintf(":%020d", e.Sequence)
}
func (s *Store) SyncKnowledge(ctx context.Context, limit int) (KnowledgeStatus, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 10000 {
		return KnowledgeStatus{}, errors.New("knowledge sync limit must be at most 10000")
	}
	owner, err := randomIdentity()
	if err != nil {
		return KnowledgeStatus{}, err
	}
	err = s.mutate(ctx, func(st *state) error {
		if err := mainClaims(st); err != nil {
			return err
		}
		if st.Knowledge == nil {
			return errors.New("knowledge is not configured")
		}
		if st.KnowledgePreparing != nil {
			return errors.New("knowledge configuration is pending; resume its exact configuration request")
		}
		k := st.Knowledge
		if k.Worker != "" && k.WorkerUntil.After(time.Now()) {
			return errors.New("another knowledge consumer holds the lease")
		}
		k.Worker = owner
		k.WorkerUntil = time.Now().UTC().Add(2 * time.Minute)
		return nil
	})
	if err != nil {
		return KnowledgeStatus{}, err
	}
	defer func() {
		release, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		_ = s.mutate(release, func(st *state) error {
			if st.Knowledge != nil && st.Knowledge.Worker == owner {
				st.Knowledge.Worker = ""
				st.Knowledge.WorkerUntil = time.Time{}
			}
			return nil
		})
	}()
	target, err := s.knowledgeTarget()
	if err != nil {
		return KnowledgeStatus{}, err
	}
	transport, err := s.openTarget(ctx, target, false)
	if err != nil {
		return KnowledgeStatus{}, err
	}
	defer func() { _ = transport.Close() }()
	documents := underlyingTransport(transport).(documentTransport)
	for processed := 0; processed < limit; processed++ {
		st, _, err := s.load(ctx)
		if err != nil {
			return KnowledgeStatus{}, err
		}
		k := st.Knowledge
		if k == nil || k.Worker != owner {
			return KnowledgeStatus{}, errors.New("knowledge consumer lease was superseded")
		}
		if k.Checkpoint > uint64(len(st.Events)) {
			return KnowledgeStatus{}, errors.New("knowledge history has a retention gap; an explicit source resnapshot is required")
		}
		if k.Checkpoint == uint64(len(st.Events)) {
			break
		}
		e := st.Events[k.Checkpoint]
		if e.Sequence != k.Checkpoint+1 {
			return KnowledgeStatus{}, errors.New("knowledge event sequence gap; checkpoint not advanced")
		}
		if err = s.projectEvent(ctx, documents, st, e); err != nil {
			return KnowledgeStatus{}, fmt.Errorf("knowledge event %d: %w", e.Sequence, err)
		}
		if err = s.mutate(ctx, func(current *state) error {
			if current.Knowledge == nil || current.Knowledge.Worker != owner || current.Knowledge.Checkpoint != e.Sequence-1 {
				return errors.New("knowledge consumer fence changed")
			}
			current.Knowledge.Checkpoint = e.Sequence
			current.Knowledge.WorkerUntil = time.Now().UTC().Add(2 * time.Minute)
			return nil
		}); err != nil {
			return KnowledgeStatus{}, err
		}
	}
	return s.KnowledgeStatus(ctx)
}
func (s *Store) projectEvent(ctx context.Context, documents documentTransport, st *state, e event) error {
	k := st.Knowledge
	source := eventSource(st.Config["project_id"], k.Digest, e)
	erase := func(previous event, configuration string) error {
		id := eventSource(st.Config["project_id"], configuration, previous)
		// Native memory's source fence prevents an expired worker from resurrecting
		// evidence even if it resumes after another consumer performs forgetting.
		body, _ := json.Marshal(map[string]any{"source_ids": []string{id}})
		if _, err := s.Memory(ctx, "", "forget", "beads-forget-"+digest([]byte(source+"\x00"+id)), body); err != nil {
			return err
		}
		for attempt := 0; attempt < 16; attempt++ {
			b, v, err := documents.ReadDocument(ctx, id)
			if err != nil {
				return err
			}
			if strings.Contains(string(b), `"forgotten":true`) {
				return nil
			}
			// Fence creation at zero as well: a slow pre-forget publisher must fail.
			tombstone, _ := json.Marshal(map[string]any{"forgotten": true, "source_id": id, "text": "", "kg": map[string]any{"entities": []any{}, "relations": []any{}}})
			err = writeSource(ctx, documents, id, v, tombstone)
			if errors.Is(err, errConflict) {
				continue
			}
			return err
		}
		return errConflict
	}
	if e.Operation == "forget" || e.Operation == "delete" || e.Operation == "remember" {
		generations := append(append([]string{}, k.Generations...), k.Digest)
		for _, previous := range st.Events {
			if previous.Sequence >= e.Sequence {
				break
			}
			if previous.ID == e.ID && eventPlane(previous) == eventPlane(e) {
				for _, generation := range generations {
					if err := erase(previous, generation); err != nil {
						return err
					}
				}
			}
		}
	}
	if e.Operation == "forget" || e.Operation == "delete" {
		return nil
	}
	text := e.Content
	if e.Issue != nil {
		b, err := json.Marshal(e.Issue)
		if err != nil {
			return err
		}
		text = string(b)
	}
	if text == "" {
		return nil
	}
	subject := st.Config["project_id"] + ":" + eventPlane(e) + ":" + e.ID
	kg := map[string]any{"entities": []any{map[string]any{"id": "subject", "label": eventPlane(e), "text": subject}, map[string]any{"id": "event", "label": "event", "text": source, "predicate": e.Operation}}, "relations": []any{map[string]any{"type": "records", "source": "event", "target": "subject"}}}
	docValue := map[string]any{"source_id": source, "project_id": st.Config["project_id"], "beads_id": e.ID, "beads_document_type": "source_event", "sequence": e.Sequence, "operation": e.Operation, "author": e.Actor, "event_time": e.At, "text": text, "kg": kg, "configuration_digest": k.Digest, "producer_revision": k.Config.Revision}
	existing, v, err := documents.ReadDocument(ctx, source)
	if err != nil {
		return err
	}
	if len(existing) == 0 {
		resolver := k.Config.EntityResolver
		if resolver == "" {
			resolver = "entities"
		}
		mention, _ := json.Marshal(map[string]any{"text": subject, "label": eventPlane(e)})
		rendered, err := nativeCall(ctx, documents, "render", resolver, mention)
		if err != nil {
			return err
		}
		var resolved struct {
			Key   string `json:"key"`
			Table string `json:"table"`
		}
		if err = json.Unmarshal(rendered, &resolved); err != nil {
			return err
		}
		if resolved.Key == "" || resolved.Table == "" {
			return errors.New("resolver omitted the canonical entity identity")
		}
		mapping := s.entityMapping(resolved.Table, resolved.Key)
		docValue["entity_mapping"] = mapping
		doc, err := json.Marshal(docValue)
		if err != nil {
			return err
		}
		if err = writeSource(ctx, documents, source, 0, doc); err != nil && !errors.Is(err, errConflict) {
			return err
		}
		existing, v, err = documents.ReadDocument(ctx, source)
		if err != nil {
			return err
		}
	}
	var frozen struct {
		Forgotten bool           `json:"forgotten"`
		Mapping   *EntityMapping `json:"entity_mapping"`
	}
	if err = json.Unmarshal(existing, &frozen); err != nil {
		return err
	}
	if frozen.Forgotten {
		return nil
	}
	if frozen.Mapping != nil {
		docValue["entity_mapping"] = frozen.Mapping
	}
	doc, err := json.Marshal(docValue)
	if err != nil {
		return err
	}
	if !jsonEqual(existing, doc) {
		return fmt.Errorf("knowledge source identity %s already contains different bytes at version %d", source, v)
	}
	evidenceID := "evidence:" + source
	evidence := map[string]any{"id": evidenceID, "source_id": source, "source_version": fmt.Sprint(e.Sequence), "text": text}
	if k.Config.ManagedObservations && e.Issue != nil {
		evidence["managed"] = true
	}
	if e.Actor != "" {
		evidence["author"] = e.Actor
	}
	observation := map[string]any{"evidence": []any{evidence}}
	// A crosswalk is not a native memory Entity declaration. Entity records
	// have immutable evidence/lifecycle contracts; curation stays with memory.
	assertionSubject := subject
	if frozen.Mapping != nil {
		assertionSubject = frozen.Mapping.MemoryEntityID
	}
	if e.Operation == "remember" {
		observation["assertions"] = []any{map[string]any{"id": "guidance:" + source, "subject": assertionSubject, "predicate": "guidance", "value": text, "statement": text, "evidence": []any{map[string]any{"id": evidenceID}}}}
	}
	body, err := json.Marshal(observation)
	if err != nil {
		return err
	}
	_, err = s.Memory(ctx, "", "observe", "beads-observe-"+digest([]byte(source)), body)
	return err
}
func jsonEqual(a, b []byte) bool {
	decode := func(raw []byte) (any, error) {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		var value any
		err := decoder.Decode(&value)
		return value, err
	}
	x, err := decode(a)
	if err != nil {
		return false
	}
	y, err := decode(b)
	if err != nil {
		return false
	}
	left, _ := json.Marshal(x)
	right, _ := json.Marshal(y)
	return string(left) == string(right)
}

// Source publication fences its own key, rather than the workflow head.
func writeSource(ctx context.Context, t documentTransport, key string, version uint64, body []byte) error {
	switch raw := t.(type) {
	case *remote:
		copy := *raw
		copy.target.Workspace = key
		return copy.WriteDocuments(ctx, &version, map[string]json.RawMessage{key: body})
	default:
		if native, ok := t.(interface {
			writeSource(context.Context, string, uint64, []byte) error
		}); ok {
			return native.writeSource(ctx, key, version, body)
		}
		return errUnsupported("knowledge source OCC")
	}
}
func (s *Store) KnowledgeOperation(ctx context.Context, op, name string, body json.RawMessage) (json.RawMessage, error) {
	target, err := s.knowledgeTarget()
	if err != nil {
		return nil, err
	}
	if _, _, err = s.load(ctx); err != nil {
		return nil, err
	}
	if s.readOnly && op != "query" && op != "resolve" && op != "render" && op != "reviews" && op != "curation" {
		return nil, errors.New("Antfly store is read-only")
	}
	tr, err := s.openTarget(ctx, target, false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tr.Close() }()
	return nativeCall(ctx, tr, op, name, body)
}

// StableSourceURL is suitable for tools that need a citation without exposing credentials.
func StableSourceURL(table, key string) string {
	return "/db/v1/tables/" + url.PathEscape(table) + "/documents/" + url.PathEscape(key)
}

type EntityMapping struct {
	Database       string `json:"database"`
	Namespace      string `json:"namespace"`
	Table          string `json:"table"`
	CanonicalKey   string `json:"canonical_key"`
	MemoryEntityID string `json:"memory_entity_id"`
}

func (s *Store) entityMapping(table, key string) EntityMapping {
	database, namespace := "default", "public"
	if s.target != nil {
		if s.target.Database != "" {
			database = s.target.Database
		}
		if s.target.Namespace != "" {
			namespace = s.target.Namespace
		}
	}
	return EntityMapping{Database: database, Namespace: namespace, Table: table, CanonicalKey: key, MemoryEntityID: "antfly:" + digest([]byte(database+"\x00"+namespace+"\x00"+table+"\x00"+key))}
}
func (s *Store) EntityMapping(ctx context.Context, table, key string) (EntityMapping, error) {
	if _, _, err := s.load(ctx); err != nil {
		return EntityMapping{}, err
	}
	if table == "" {
		target, err := s.knowledgeTarget()
		if err != nil {
			return EntityMapping{}, err
		}
		table = target.Table + "_entities"
	}
	if key == "" {
		return EntityMapping{}, errors.New("canonical entity key is required")
	}
	return s.entityMapping(table, key), nil
}
