package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/steveyegge/beads/backend"
	bantfly "github.com/steveyegge/beads/backend/antfly"
)

func init() {
	bantfly.Register()
	group := &cobra.Command{Use: "antfly", Short: "Configure native Antfly storage and access its memory service", GroupID: "setup", Annotations: map[string]string{skipStoreAnnotation: "1"}}
	var target bantfly.Target
	var prefix string
	initialize := &cobra.Command{Use: "init", Short: "Create or attach an Antfly workspace", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		CheckReadonly("antfly init")
		dir := resolveInitBeadsDir()
		if dir == "" {
			return fmt.Errorf("cannot resolve .beads directory")
		}
		if target.URL == "" && target.Path == "" {
			target.Path = filepath.Join(dir, "antfly.aflite")
		}
		if e := bantfly.Initialize(cmd.Context(), dir, target, prefix); e != nil {
			return e
		}
		if jsonOutput {
			return outputJSON(map[string]string{"backend": "antfly", "workspace": target.Workspace, "beads_dir": dir})
		}
		_, e := fmt.Fprintln(cmd.OutOrStdout(), "Initialized Antfly workspace in "+dir)
		return e
	}}
	initialize.Flags().StringVar(&target.URL, "url", "", "Antfly server origin, for example http://127.0.0.1:8080")
	initialize.Flags().StringVar(&target.Path, "path", "", "Embedded .aflite database path (default .beads/antfly.aflite)")
	initialize.Flags().StringVar(&target.Library, "library", "", "Path to libantfly shared library (or ANTFLY_LIBRARY_PATH)")
	initialize.Flags().StringVar(&target.Table, "table", "beads_workspaces", "Antfly table")
	initialize.Flags().StringVar(&target.Workspace, "workspace", "default", "Workspace key within the table")
	initialize.Flags().StringVar(&target.TokenEnv, "token-env", "", "Environment variable containing the remote bearer token")
	initialize.Flags().StringVar(&target.Database, "database", "default", "Antfly database")
	initialize.Flags().StringVar(&target.Namespace, "namespace", "public", "Antfly namespace")
	initialize.Flags().StringVar(&prefix, "prefix", "bd", "Issue ID prefix")
	var space, key string
	memory := &cobra.Command{Use: "memory <create|inspect|configure|observe|recall|correct|curate-entity|outcome|forget>", Short: "Call native Antfly memory with a JSON body from stdin", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if args[0] != "recall" && args[0] != "inspect" {
			CheckReadonly("antfly memory")
		}
		if args[0] != "create" && args[0] != "observe" && args[0] != "recall" && args[0] != "forget" && args[0] != "correct" && args[0] != "curate-entity" && args[0] != "outcome" && args[0] != "inspect" && args[0] != "configure" {
			return fmt.Errorf("unknown memory action %q", args[0])
		}
		if args[0] != "recall" && args[0] != "inspect" && key == "" {
			return fmt.Errorf("memory mutations require --idempotency-key")
		}
		b := []byte(`{}`)
		var e error
		if args[0] != "inspect" {
			b, e = io.ReadAll(io.LimitReader(cmd.InOrStdin(), (4<<20)+1))
			if e != nil {
				return e
			}
			if len(b) > 4<<20 || !json.Valid(b) {
				return fmt.Errorf("stdin must contain a JSON body of at most 4 MiB")
			}
		}
		dir := resolveInitBeadsDir()
		if _, e = os.Stat(filepath.Join(dir, "antfly.json")); e != nil {
			return e
		}
		action := args[0]
		if action == "curate-entity" {
			action = "curate_entity"
		}
		out, e := bantfly.Memory(cmd.Context(), dir, space, action, key, b)
		if e != nil {
			return e
		}
		_, e = fmt.Fprintln(cmd.OutOrStdout(), string(out))
		return e
	}}
	memory.Flags().StringVar(&space, "space", "", "Memory space (default derived from workspace routing)")
	memory.Flags().StringVar(&key, "idempotency-key", "", "Stable operation key, required for mutations")

	collect := &cobra.Command{Use: "gc", Short: "Collect unreachable core history while fencing main and all forks", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		CheckReadonly("antfly gc")
		result, err := bantfly.CollectWorkspaceGarbage(cmd.Context(), resolveInitBeadsDir(), backend.OpenOptions{})
		if err != nil {
			return err
		}
		return antflyJSON(cmd, result)
	}}
	group.AddCommand(collect)
	group.AddCommand(initialize, memory)
	addAntflyWorkflows(group)
	rootCmd.AddCommand(group)
}
