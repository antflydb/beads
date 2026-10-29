package antfly

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"

	storageops "github.com/steveyegge/beads/internal/storage/issueops"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/issueops"
)

// ExecuteFactory implements Gas City's executable Beads provider protocol.
// All writes enter the same lifecycle/graph roles as ordinary bd commands.
func (s *Store) ExecuteFactory(ctx context.Context, op string, args []string, body json.RawMessage, actor string) (json.RawMessage, error) {
	encode := func(value any) (json.RawMessage, error) { return json.Marshal(value) }
	getID := func() (string, error) {
		if len(args) != 1 || args[0] == "" {
			return "", errors.New("operation requires one canonical issue ID")
		}
		return args[0], nil
	}
	wire := func(st *state, i *types.Issue) map[string]any {
		b, _ := json.Marshal(i)
		out := map[string]any{}
		_ = json.Unmarshal(b, &out)
		out["type"] = i.IssueType
		out["parent_id"] = ""
		out["ref"] = ""
		if i.ExternalRef != nil {
			out["ref"] = *i.ExternalRef
		}
		needs := []string{}
		for _, d := range st.Edges {
			if d.IssueID == i.ID && d.Type == types.DepParentChild {
				out["parent_id"] = d.DependsOnID
			}
			if d.IssueID == i.ID && d.Type == types.DepBlocks {
				needs = append(needs, d.DependsOnID)
			}
		}
		sort.Strings(needs)
		out["needs"] = needs
		return out
	}
	switch op {
	case "health":
		st, _, err := s.load(ctx)
		if err != nil {
			return nil, err
		}
		return encode(map[string]any{"backend": Backend, "project_id": st.Config["project_id"], "ready": true})
	case "get":
		id, err := getID()
		if err != nil {
			return nil, err
		}
		st, _, err := s.load(ctx)
		if err != nil {
			return nil, err
		}
		i, err := st.issue(id)
		if err != nil {
			return nil, err
		}
		return encode(wire(st, i))
	case "list", "children", "list-by-label", "ready":
		flags := flag.NewFlagSet(op, flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		status := flags.String("status", "", "")
		assignee := flags.String("assignee", "", "")
		kind := flags.String("type", "", "")
		limit := flags.Int("limit", 0, "")
		ephemeral := flags.Bool("include-ephemeral", false, "")
		parent, label := "", ""
		if op == "children" || op == "list-by-label" {
			if len(args) < 1 {
				return nil, errors.New("operation requires a parent or label")
			}
			if op == "children" {
				parent = args[0]
			} else {
				label = args[0]
			}
			args = args[1:]
			if len(args) > 0 && args[0] == "0" {
				args = args[1:]
			}
		}
		if err := flags.Parse(args); err != nil {
			return nil, err
		}
		if len(flags.Args()) > 0 {
			return nil, errors.New("unexpected provider arguments")
		}
		st, _, err := s.load(ctx)
		if err != nil {
			return nil, err
		}
		f := types.IssueFilter{Limit: *limit}
		if *status != "" {
			v := types.Status(*status)
			f.Status = &v
		}
		if *assignee != "" {
			f.Assignee = assignee
		}
		if *kind != "" {
			v := types.IssueType(*kind)
			f.IssueType = &v
		}
		if parent != "" {
			f.ParentID = &parent
		}
		if label != "" {
			f.Labels = []string{label}
		}
		rows, err := st.search("", f)
		if op == "ready" {
			rows, err = st.ready(types.WorkFilter{Limit: *limit, IncludeEphemeral: *ephemeral})
		}
		if err != nil {
			return nil, err
		}
		out := []map[string]any{}
		for _, row := range rows {
			out = append(out, wire(st, row.Issue))
		}
		return encode(out)
	case "create":
		var object map[string]json.RawMessage
		if err := json.Unmarshal(body, &object); err != nil {
			return nil, err
		}
		raw, _ := json.Marshal(object)
		var i types.Issue
		if err := json.Unmarshal(raw, &i); err != nil {
			return nil, err
		}
		i.IssueType = types.TypeTask
		if value := object["type"]; value != nil {
			if err := json.Unmarshal(value, &i.IssueType); err != nil {
				return nil, err
			}
		}
		var parent, ref, from string
		var needs []string
		for key, into := range map[string]any{"parent_id": &parent, "ref": &ref, "from": &from, "needs": &needs} {
			if value := object[key]; value != nil {
				if err := json.Unmarshal(value, into); err != nil {
					return nil, err
				}
			}
		}
		if ref != "" {
			i.ExternalRef = &ref
		}
		if from != "" {
			i.CreatedBy = from
		}
		req := issueops.CreateRequest{Actor: actor, Issue: &i, ParentID: parent}
		for _, id := range needs {
			req.Dependencies = append(req.Dependencies, issueops.CreateDependency{TargetID: id, Type: types.DepBlocks})
		}
		result, err := (&lifecycle{s}).Create(ctx, req)
		if err != nil {
			return nil, err
		}
		st, _, err := s.load(ctx)
		if err != nil {
			return nil, err
		}
		return encode(wire(st, result.Issue))
	case "update":
		id, err := getID()
		if err != nil {
			return nil, err
		}
		var object map[string]json.RawMessage
		if err = json.Unmarshal(body, &object); err != nil {
			return nil, err
		}
		before, err := s.GetIssue(ctx, id)
		if err != nil {
			return nil, err
		}
		req := issueops.UpdateRequest{Actor: actor, IssueID: id, ExpectedVersion: &before.RowVersion}
		fields := map[string]any{"title": &req.Patch.Title.Value, "description": &req.Patch.Description.Value, "assignee": &req.Patch.Assignee.Value, "parent_id": &req.Patch.ParentID.Value, "status": &req.Patch.Status.Value, "priority": &req.Patch.Priority.Value, "type": &req.Patch.IssueType.Value, "labels": &req.Patch.Labels.Add, "remove_labels": &req.Patch.Labels.Remove, "metadata": &req.Patch.Metadata.Set, "defer_until": &req.Patch.DeferUntil.Value}
		for key, value := range object {
			into, ok := fields[key]
			if !ok {
				return nil, fmt.Errorf("unsupported provider update field %s", key)
			}
			if err = json.Unmarshal(value, into); err != nil {
				return nil, err
			}
		}
		_, req.Patch.Title.Set = object["title"]
		_, req.Patch.Description.Set = object["description"]
		_, req.Patch.Assignee.Set = object["assignee"]
		_, req.Patch.ParentID.Set = object["parent_id"]
		_, req.Patch.Status.Set = object["status"]
		_, req.Patch.Priority.Set = object["priority"]
		_, req.Patch.IssueType.Set = object["type"]
		_, req.Patch.DeferUntil.Set = object["defer_until"]
		if req.Patch.Assignee.Set {
			req.ExpectedAssignee = &before.Assignee
		}
		if req.Patch.Status.Set && req.Patch.Status.Value == types.StatusInProgress && req.Patch.Assignee.Set && req.Patch.Assignee.Value != "" && !storageops.IsWisp(before) {
			req.Actor = req.Patch.Assignee.Value
			req.Claim = true
			req.ExpectedAssignee = nil
			req.Patch.Assignee = issueops.Field[string]{}
			req.Patch.Status = issueops.Field[types.Status]{}
		}
		_, err = (&lifecycle{s}).Update(ctx, req)
		return json.RawMessage(`{}`), err
	case "close", "reopen":
		id, err := getID()
		if err != nil {
			return nil, err
		}
		if op == "reopen" {
			_, err = (&lifecycle{s}).Reopen(ctx, issueops.ReopenRequest{IssueID: id, Actor: actor})
			return json.RawMessage(`{}`), err
		}
		i, err := s.GetIssue(ctx, id)
		if err != nil {
			return nil, err
		}
		owner := actor
		if i.Assignee != "" {
			owner = i.Assignee
		}
		var metadata map[string]any
		_ = json.Unmarshal(i.Metadata, &metadata)
		reason, _ := metadata["close_reason"].(string)
		_, err = (&lifecycle{s}).Close(ctx, issueops.CloseRequest{IssueID: id, Actor: owner, Reason: reason})
		return json.RawMessage(`{}`), err
	case "delete":
		force := false
		ids := []string{}
		for _, arg := range args {
			if arg == "--force" {
				force = true
			} else {
				ids = append(ids, arg)
			}
		}
		_, err := (&deleter{s}).Delete(ctx, issueops.DeleteRequest{IDs: ids, Actor: actor, Force: force})
		return json.RawMessage(`{}`), err
	case "set-metadata":
		if len(args) != 2 {
			return nil, errors.New("set-metadata requires issue ID and key")
		}
		value := body
		if !json.Valid(value) {
			value, _ = json.Marshal(string(body))
		}
		_, err := (&lifecycle{s}).Update(ctx, issueops.UpdateRequest{Actor: actor, IssueID: args[0], Patch: issueops.IssuePatch{Metadata: issueops.MetadataPatch{Set: map[string]json.RawMessage{args[1]: value}}}})
		return json.RawMessage(`{}`), err
	case "dep-add":
		if len(args) != 3 {
			return nil, errors.New("dep-add requires issue, target and type")
		}
		_, err := (&graph{s}).AddDependencies(ctx, issueops.AddDependenciesRequest{Actor: actor, Edges: []issueops.DependencyEdge{{IssueID: args[0], DependsOnID: args[1], Type: types.DependencyType(args[2])}}})
		return json.RawMessage(`{}`), err
	case "dep-remove":
		if len(args) != 2 {
			return nil, errors.New("dep-remove requires issue and target")
		}
		_, err := (&graph{s}).RemoveDependency(ctx, issueops.RemoveDependencyRequest{Actor: actor, IssueID: args[0], DependsOnID: args[1]})
		return json.RawMessage(`{}`), err
	case "dep-list":
		if len(args) != 2 || (args[1] != "down" && args[1] != "up") {
			return nil, errors.New("dep-list requires issue and down/up")
		}
		st, _, err := s.load(ctx)
		if err != nil {
			return nil, err
		}
		out := []*types.Dependency{}
		for _, edge := range st.Edges {
			if args[1] == "down" && edge.IssueID == args[0] || args[1] == "up" && edge.DependsOnID == args[0] {
				copy := *edge
				out = append(out, &copy)
			}
		}
		return encode(out)
	default:
		return nil, fmt.Errorf("unsupported factory provider operation %q", op)
	}
}
func (s *Store) ListWisps(ctx context.Context, f types.WispFilter) ([]*types.Issue, error) {
	st, _, err := s.load(ctx)
	if err != nil {
		return nil, err
	}
	out := []*types.Issue{}
	for id, row := range st.Issues {
		i := &row.Issue
		if !storageops.IsWisp(i) {
			continue
		}
		if f.Type != nil && i.IssueType != *f.Type || f.Status != nil && i.Status != *f.Status || f.Status == nil && !f.IncludeClosed && i.Status == types.StatusClosed {
			continue
		}
		if f.UpdatedAfter != nil && !i.UpdatedAt.After(*f.UpdatedAfter) || f.UpdatedBefore != nil && !i.UpdatedAt.Before(*f.UpdatedBefore) {
			continue
		}
		out = append(out, st.hydrate(id))
	}
	sortIssues(out)
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}
func sortIssues(issues []*types.Issue) {
	sort.Slice(issues, func(i, j int) bool { return issues[i].ID < issues[j].ID })
}
