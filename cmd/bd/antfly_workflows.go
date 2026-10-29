package main

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/spf13/cobra"
	"github.com/steveyegge/beads/backend"
	bantfly "github.com/steveyegge/beads/backend/antfly"
)

func openAntflyWorkspace(cmd *cobra.Command) (bantfly.Workspace, error) {
	return bantfly.Open(cmd.Context(), resolveInitBeadsDir(), backend.OpenOptions{})
}
func antflyBody(cmd *cobra.Command) (json.RawMessage, error) {
	b, e := io.ReadAll(io.LimitReader(cmd.InOrStdin(), (4<<20)+1))
	if e != nil {
		return nil, e
	}
	if len(b) == 0 {
		return json.RawMessage(`{}`), nil
	}
	if len(b) > 4<<20 || !json.Valid(b) {
		return nil, fmt.Errorf("stdin must contain JSON of at most 4 MiB")
	}
	return b, nil
}
func antflyJSON(cmd *cobra.Command, value any) error {
	return json.NewEncoder(cmd.OutOrStdout()).Encode(value)
}
func branchView(branch bantfly.Branch) map[string]any {
	return map[string]any{"name": branch.Name, "id": branch.ID, "workspace": branch.Workspace, "base_revision": branch.BaseRevision, "directory": branch.Directory, "pending": branch.Pending}
}
func addAntflyWorkflows(group *cobra.Command) {
	branch := &cobra.Command{Use: "branch", Short: "Create immutable workspace snapshots and merge isolated work"}
	create := &cobra.Command{Use: "create <name>", Short: "Create a branch checkout from a consistent workspace snapshot", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		CheckReadonly("antfly branch create")
		workspace, e := openAntflyWorkspace(cmd)
		if e != nil {
			return e
		}
		defer func() { _ = workspace.Close() }()
		result, e := workspace.Fork(cmd.Context(), args[0], getActorWithGit())
		if e != nil {
			return e
		}
		return antflyJSON(cmd, branchView(result))
	}}
	list := &cobra.Command{Use: "list", Short: "List branch snapshots", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		workspace, e := openAntflyWorkspace(cmd)
		if e != nil {
			return e
		}
		defer func() { _ = workspace.Close() }()
		branches, e := workspace.ListForks(cmd.Context())
		if e != nil {
			return e
		}
		out := []map[string]any{}
		for _, branch := range branches {
			out = append(out, branchView(branch))
		}
		return antflyJSON(cmd, out)
	}}
	var key string
	var dry bool
	merge := &cobra.Command{Use: "merge <name>", Short: "Integrate branch changes with three-way conflict checks", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if !dry {
			CheckReadonly("antfly branch merge")
		}
		workspace, e := openAntflyWorkspace(cmd)
		if e != nil {
			return e
		}
		defer func() { _ = workspace.Close() }()
		result, e := workspace.MergeFork(cmd.Context(), args[0], key, getActorWithGit(), dry)
		if e != nil {
			return e
		}
		return antflyJSON(cmd, result)
	}}
	merge.Flags().StringVar(&key, "idempotency-key", "", "Stable integration key, required even for a preview")
	merge.Flags().BoolVar(&dry, "dry-run", false, "Report conflicts without changing main")
	branch.AddCommand(create, list, merge)
	knowledge := &cobra.Command{Use: "knowledge", Short: "Project source events into native memory and entity resolution"}
	configure := &cobra.Command{Use: "configure", Short: "Configure memory and knowledge indexes from JSON on stdin", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		CheckReadonly("antfly knowledge configure")
		body, e := antflyBody(cmd)
		if e != nil {
			return e
		}
		var cfg bantfly.KnowledgeConfig
		if e = json.Unmarshal(body, &cfg); e != nil {
			return e
		}
		workspace, e := openAntflyWorkspace(cmd)
		if e != nil {
			return e
		}
		defer func() { _ = workspace.Close() }()
		if e = workspace.ConfigureKnowledge(cmd.Context(), cfg); e != nil {
			return e
		}
		status, e := workspace.KnowledgeStatus(cmd.Context())
		if e != nil {
			return e
		}
		return antflyJSON(cmd, status)
	}}
	status := &cobra.Command{Use: "status", Short: "Show consumer progress and knowledge destinations", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		workspace, e := openAntflyWorkspace(cmd)
		if e != nil {
			return e
		}
		defer func() { _ = workspace.Close() }()
		result, e := workspace.KnowledgeStatus(cmd.Context())
		if e != nil {
			return e
		}
		return antflyJSON(cmd, result)
	}}
	var limit int
	sync := &cobra.Command{Use: "sync", Short: "Replay committed source events through the durable consumer", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		CheckReadonly("antfly knowledge sync")
		workspace, e := openAntflyWorkspace(cmd)
		if e != nil {
			return e
		}
		defer func() { _ = workspace.Close() }()
		result, e := workspace.SyncKnowledge(cmd.Context(), limit)
		if e != nil {
			return e
		}
		return antflyJSON(cmd, result)
	}}
	sync.Flags().IntVar(&limit, "limit", 100, "Maximum source events to process")
	var batch int
	var interval time.Duration
	watch := &cobra.Command{Use: "watch", Short: "Continuously project new source events until interrupted", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		CheckReadonly("antfly knowledge watch")
		if interval < 100*time.Millisecond {
			return fmt.Errorf("interval must be at least 100ms")
		}
		workspace, e := openAntflyWorkspace(cmd)
		if e != nil {
			return e
		}
		defer func() { _ = workspace.Close() }()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			result, e := workspace.SyncKnowledge(cmd.Context(), batch)
			if e != nil {
				return e
			}
			if e = antflyJSON(cmd, result); e != nil {
				return e
			}
			select {
			case <-cmd.Context().Done():
				return cmd.Context().Err()
			case <-ticker.C:
			}
		}
	}}
	watch.Flags().IntVar(&batch, "limit", 100, "Maximum source events per iteration")
	watch.Flags().DurationVar(&interval, "interval", 2*time.Second, "Delay between consumer iterations")
	var mappingTable string
	mapping := &cobra.Command{Use: "mapping <canonical-key>", Short: "Show the explicit graph-to-memory entity identity mapping", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		workspace, e := openAntflyWorkspace(cmd)
		if e != nil {
			return e
		}
		defer func() { _ = workspace.Close() }()
		out, e := workspace.EntityMapping(cmd.Context(), mappingTable, args[0])
		if e != nil {
			return e
		}
		return antflyJSON(cmd, out)
	}}
	mapping.Flags().StringVar(&mappingTable, "table", "", "Canonical entity table (default the workspace entity table)")
	knowledge.AddCommand(configure, status, sync, watch, mapping)
	for _, operation := range []struct {
		name, use, description string
		read                   bool
	}{
		{"query", "query", "Search knowledge using a native Antfly JSON query", true},
		{"resolve", "resolve <resolver>", "Resolve entities using the configured resolver", true},
		{"render", "render <resolver>", "Render a canonical entity identity", true},
		{"reviews", "reviews <resolver>", "List unresolved identity proposals", true},
		{"review", "review <resolver/review-id>", "Decide an identity proposal with native revision guards", false},
		{"curation", "curation <resolver>", "Inspect persistent resolver curation", true},
		{"merge", "merge <resolver>", "Apply an explicitly reviewed entity merge", false},
		{"suppress", "suppress <resolver>", "Suppress or unsuppress canonical entity identities", false},
	} {
		op := operation
		args := cobra.ExactArgs(1)
		if op.name == "query" {
			args = cobra.NoArgs
		}
		command := &cobra.Command{Use: op.use, Short: op.description, Args: args, RunE: func(cmd *cobra.Command, args []string) error {
			if !op.read {
				CheckReadonly("antfly knowledge " + op.name)
			}
			body := json.RawMessage(`{}`)
			var e error
			if op.name != "reviews" && op.name != "curation" {
				body, e = antflyBody(cmd)
				if e != nil {
					return e
				}
			}
			workspace, e := openAntflyWorkspace(cmd)
			if e != nil {
				return e
			}
			defer func() { _ = workspace.Close() }()
			name := ""
			if len(args) > 0 {
				name = args[0]
			}
			out, e := workspace.KnowledgeOperation(cmd.Context(), op.name, name, body)
			if e != nil {
				return e
			}
			_, e = fmt.Fprintln(cmd.OutOrStdout(), string(out))
			return e
		}}
		knowledge.AddCommand(command)
	}
	var maxTokens, contextLimit int
	contextCommand := &cobra.Command{Use: "context <query>", Short: "Return cited work, memory and graph context under a token budget", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		workspace, e := openAntflyWorkspace(cmd)
		if e != nil {
			return e
		}
		defer func() { _ = workspace.Close() }()
		out, e := workspace.Context(cmd.Context(), bantfly.ContextRequest{Query: args[0], Limit: contextLimit, MaxTokens: maxTokens})
		if e != nil {
			return e
		}
		return antflyJSON(cmd, out)
	}}
	contextCommand.Flags().IntVar(&maxTokens, "max-tokens", 4096, "Maximum context budget")
	contextCommand.Flags().IntVar(&contextLimit, "limit", 10, "Maximum matches from each source")
	provider := &cobra.Command{Use: "provider <operation> [arguments...]", Short: "Serve the Gas City executable Beads provider protocol", DisableFlagParsing: true, Args: cobra.MinimumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 1 && (args[0] == "--help" || args[0] == "-h") {
			return cmd.Help()
		}
		switch args[0] {
		case "get", "list", "children", "list-by-label", "ready", "dep-list", "health":
		default:
			CheckReadonly("antfly provider")
		}
		body := []byte(`{}`)
		if args[0] == "create" || args[0] == "update" || args[0] == "set-metadata" {
			var e error
			body, e = io.ReadAll(io.LimitReader(cmd.InOrStdin(), (4<<20)+1))
			if e != nil {
				return e
			}
			if len(body) > 4<<20 {
				return fmt.Errorf("provider request exceeds 4 MiB")
			}
			if len(body) == 0 && args[0] != "set-metadata" {
				body = []byte(`{}`)
			}
		}
		workspace, e := openAntflyWorkspace(cmd)
		if e != nil {
			return e
		}
		defer func() { _ = workspace.Close() }()
		out, e := workspace.ExecuteFactory(cmd.Context(), args[0], args[1:], body, getActorWithGit())
		if e != nil {
			return e
		}
		_, e = fmt.Fprintln(cmd.OutOrStdout(), string(out))
		return e
	}}
	group.AddCommand(branch, knowledge, contextCommand, provider)
}
