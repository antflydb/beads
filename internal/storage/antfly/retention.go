package antfly

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/internal/workapi"
	"github.com/steveyegge/beads/issueops"
)

type retentionOptions struct {
	policy, storeRef                   string
	now                                time.Time
	staleAge, closedAge, staleIssueAge time.Duration
	cityScope, dryRun                  bool
	limit                              int
}
type retentionResult struct {
	Scanned  int      `json:"scanned"`
	Closed   []string `json:"closed"`
	Deleted  []string `json:"deleted"`
	Promoted []string `json:"promoted"`
	Held     int      `json:"held"`
}

func (s *Store) factoryRetention(ctx context.Context, args []string, actor string) (json.RawMessage, error) {
	options := retentionOptions{now: time.Now().UTC()}
	flags := flag.NewFlagSet("retention", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&options.policy, "policy", "reap", "")
	flags.StringVar(&options.storeRef, "store-ref", "", "")
	flags.DurationVar(&options.staleAge, "stale-age", 24*time.Hour, "")
	flags.DurationVar(&options.closedAge, "closed-age", 7*24*time.Hour, "")
	flags.DurationVar(&options.staleIssueAge, "stale-issue-age", 30*24*time.Hour, "")
	flags.BoolVar(&options.cityScope, "city-scope", false, "")
	flags.BoolVar(&options.dryRun, "dry-run", false, "")
	flags.IntVar(&options.limit, "limit", 500, "")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	if len(flags.Args()) != 0 {
		return nil, fmt.Errorf("retention takes flags only")
	}
	result, err := s.retain(ctx, options, actor)
	if err != nil {
		return nil, err
	}
	return json.Marshal(result)
}

// retain evaluates typed policy and invokes the ordinary lifecycle roles in
// one isolated snapshot. The final publication is conditional and never
// replays selection after a conflict. A preview discards the same operation.
func (s *Store) retain(ctx context.Context, options retentionOptions, actor string) (retentionResult, error) {
	result := retentionResult{Closed: []string{}, Deleted: []string{}, Promoted: []string{}}
	if (options.policy != "reap" && options.policy != "compact") || options.limit <= 0 || options.staleAge <= 0 || options.closedAge < 7*24*time.Hour || options.staleIssueAge <= 0 || options.now.IsZero() || actor == "" {
		return retentionResult{}, fmt.Errorf("retention requires a known policy, actor, clock, positive ages/limit and at least seven days of closed history")
	}
	run := func(staged *Store) error {
		st, _, err := staged.load(ctx)
		if err != nil {
			return err
		}
		result.Scanned = len(st.Issues)
		metadata := map[string]map[string]string{}
		ttlStamped := map[string]bool{}
		ids := make([]string, 0, len(st.Issues))
		for id, r := range st.Issues {
			ids = append(ids, id)
			values := map[string]any{}
			if len(r.Issue.Metadata) > 0 {
				if err = json.Unmarshal(r.Issue.Metadata, &values); err != nil {
					return err
				}
			}
			metadata[id] = map[string]string{}
			if expires, ok := values["expires_at"]; ok && expires != nil && expires != "" {
				ttlStamped[id] = true
			}
			for key, value := range values {
				if text, ok := value.(string); ok {
					metadata[id][key] = text
				}
			}
		}
		sort.Strings(ids)
		local := func(id string) bool {
			ref := metadata[id]["gc.root_store_ref"]
			return ref == "" || ref == options.storeRef
		}
		protected := func(id string) bool {
			i := &st.Issues[id].Issue
			if i.Pinned || i.Status == types.StatusPinned || domainInfraOrQueue(i) {
				return true
			}
			return !local(id)
		}
		changed := func() int { return len(result.Closed) + len(result.Deleted) + len(result.Promoted) }
		children := map[string][]string{}
		incoming := map[string][]string{}
		parents := map[string][]string{}
		for _, d := range st.Edges {
			incoming[d.DependsOnID] = append(incoming[d.DependsOnID], d.IssueID)
			if d.Type == types.DepParentChild || d.Type == types.DependencyType("tracks") && metadata[d.IssueID]["gc.root_bead_id"] == d.DependsOnID {
				children[d.DependsOnID] = append(children[d.DependsOnID], d.IssueID)
				parents[d.IssueID] = append(parents[d.IssueID], d.DependsOnID)
			}
		}
		for id := range st.Issues {
			root := metadata[id]["gc.root_bead_id"]
			if root != "" && root != id {
				children[root] = append(children[root], id)
				incoming[root] = append(incoming[root], id)
			}
		}
		if options.policy == "reap" {
			candidates := map[string]bool{}
			eligible := func(id string) bool {
				i := &st.Issues[id].Issue
				return !protected(id) && i.CreatedAt.Before(options.now.Add(-options.staleAge)) && slices.Contains([]types.Status{types.StatusOpen, types.StatusInProgress, types.Status("hooked")}, i.Status)
			}
			for _, id := range ids {
				i := &st.Issues[id].Issue
				if !eligible(id) {
					continue
				}
				if storageops.IsWisp(i) {
					for _, parent := range parents[id] {
						if owner := st.Issues[parent]; owner != nil && owner.Issue.Status == types.StatusClosed && local(parent) {
							candidates[id] = true
						}
					}
				}
				root := metadata[id]["gc.kind"] == "workflow" || metadata[id]["gc.formula_contract"] == "graph.v2" || i.IssueType == types.TypeMolecule
				if root && i.Assignee == "" && i.UpdatedAt.Before(options.now.Add(-options.staleAge)) && (storageops.IsWisp(i) || options.cityScope) && (metadata[id]["gc.root_bead_id"] == "" || metadata[id]["gc.root_bead_id"] == id) {
					candidates[id] = true
				}
				if options.cityScope && !storageops.IsWisp(i) && i.IssueType != types.TypeEpic && i.Priority > 1 && !ttlStamped[id] && i.UpdatedAt.Before(options.now.Add(-options.staleIssueAge)) {
					activeEdge := false
					for _, d := range st.Edges {
						neighbor := ""
						if d.IssueID == id {
							neighbor = d.DependsOnID
						} else if d.DependsOnID == id {
							neighbor = d.IssueID
						}
						if neighbor != "" {
							r := st.Issues[neighbor]
							if r == nil || !resolved(r.Issue.Status) {
								activeEdge = true
							}
						}
					}
					if !activeEdge {
						candidates[id] = true
					}
				}
			}
			for progress := true; progress; {
				progress = false
				for _, id := range ids {
					if candidates[id] || !eligible(id) || !storageops.IsWisp(&st.Issues[id].Issue) {
						continue
					}
					for _, parent := range parents[id] {
						if candidates[parent] {
							candidates[id] = true
							progress = true
						}
					}
				}
			}
			for progress := true; progress && changed() < options.limit; {
				progress = false
				for _, id := range ids {
					i := &st.Issues[id].Issue
					if !candidates[id] || i.Status == types.StatusClosed {
						continue
					}
					safe := st.canClose(i, i.Assignee, false) == nil
					for _, child := range children[id] {
						if row := st.Issues[child]; row == nil || !resolved(row.Issue.Status) {
							safe = false
						}
					}
					if !safe {
						continue
					}
					current, loadErr := staged.GetIssue(ctx, id)
					if loadErr != nil {
						return loadErr
					}
					_, err = (&lifecycle{staged}).Close(ctx, issueops.CloseRequest{IssueID: id, Actor: actor, ExpectedVersion: &current.RowVersion, Force: true, Reason: "retention: stale ownership or drained workflow"})
					if err != nil {
						return err
					}
					i.Status = types.StatusClosed
					result.Closed = append(result.Closed, id)
					progress = true
					if changed() >= options.limit {
						break
					}
				}
			}
			for id := range candidates {
				if st.Issues[id].Issue.Status != types.StatusClosed {
					result.Held++
				}
			}
		}
		// Removal never rewrites a surviving edge or live textual citation.
		possible := map[string]bool{}
		for _, id := range ids {
			possible[id] = true
		}
		matcher := workapi.NewCandidateIDMatcher(possible)
		cited := map[string]bool{}
		for _, r := range st.Issues {
			if !resolved(r.Issue.Status) {
				matcher.FindAll(r.Issue.Description, cited)
				matcher.FindAll(r.Issue.Notes, cited)
				for _, c := range r.Issue.Comments {
					if c != nil {
						matcher.FindAll(c.Text, cited)
					}
				}
			}
		}
		for _, id := range ids {
			if changed() >= options.limit {
				break
			}
			i := &st.Issues[id].Issue
			if protected(id) || !storageops.IsWisp(i) || slices.Contains(result.Closed, id) {
				continue
			}
			if options.policy == "compact" {
				if !i.Ephemeral && i.WispType == "" {
					continue
				}
				ttl := 24 * time.Hour
				kind := string(i.WispType)
				for _, label := range i.Labels {
					if strings.HasPrefix(label, "wisp_type:") {
						kind = strings.TrimPrefix(label, "wisp_type:")
					}
				}
				switch kind {
				case "heartbeat", "ping":
					ttl = 6 * time.Hour
				case "recovery", "error", "escalation":
					ttl = 7 * 24 * time.Hour
				}
				keep := slices.Contains(i.Labels, "keep")
				if !keep && !i.UpdatedAt.Before(options.now.Add(-ttl)) {
					continue
				}
				if keep || i.Status != types.StatusClosed || len(i.Comments) > 0 {
					_, err = (&lifecycle{staged}).Update(ctx, issueops.UpdateRequest{IssueID: id, Actor: actor, ExpectedVersion: &i.RowVersion, Patch: issueops.IssuePatch{Persistence: issueops.Field[issueops.PersistenceMode]{Set: true, Value: issueops.PersistenceModePersistent}}})
					if err != nil {
						return err
					}
					result.Promoted = append(result.Promoted, id)
					continue
				}
			} else if i.Status != types.StatusClosed || i.ClosedAt == nil || !i.ClosedAt.Before(options.now.Add(-options.closedAge)) {
				continue
			}
			if i.ClosedAt == nil {
				result.Held++
				continue
			}
			if len(incoming[id]) > 0 || cited[id] {
				result.Held++
				continue
			}
			current, loadErr := staged.GetIssue(ctx, id)
			if loadErr != nil {
				return loadErr
			}
			_, err = (&deleter{staged}).Delete(ctx, issueops.DeleteRequest{IDs: []string{id}, Actor: actor, Force: true, ExpectedVersion: &current.RowVersion})
			if err != nil {
				return err
			}
			if err = staged.mutate(ctx, func(current *state) error { current.Retired[id] = *i.ClosedAt; return nil }); err != nil {
				return err
			}
			result.Deleted = append(result.Deleted, id)
		}
		return nil
	}
	var err error
	if options.dryRun {
		var raw []byte
		raw, _, err = s.transport.Load(ctx)
		if err == nil {
			staged := New(&stagedTransport{data: raw, version: 1}, false)
			staged.expectedProject = s.expectedProject
			err = run(staged)
		}
	} else {
		err = s.stage(ctx, run)
	}
	if err != nil {
		return retentionResult{}, err
	}
	return result, nil
}

func domainInfraOrQueue(i *types.Issue) bool {
	if slices.Contains([]string{"op", "session", "message", "nudge", "agent", "role"}, string(i.IssueType)) {
		return true
	}
	for _, label := range i.Labels {
		if strings.HasPrefix(label, "gc:extmsg-") || slices.Contains([]string{"gc:nudge", "gc:nudge-queue", "gc:session", "gt:slot"}, label) {
			return true
		}
	}
	return false
}
