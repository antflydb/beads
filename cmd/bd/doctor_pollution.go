package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/steveyegge/beads/internal/remedy"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/internal/ui"
	"github.com/steveyegge/beads/internal/workapi"
	"github.com/steveyegge/beads/issueops"
)

// runPollutionCheck runs detailed test pollution detection
// This integrates the detect-pollution command functionality into doctor.
//
//nolint:unparam // path reserved for future use
func runPollutionCheck(_ string, clean bool, yes bool) error {
	if err := ensureDirectMode("pollution check requires direct mode"); err != nil {
		return HandleError("%v", err)
	}

	ctx := rootCtx

	// Get all issues (env-only cap via BEADS_MAX_ROWS; designer §4: doctor
	// family is env opt-in, no operator-facing flag).
	maxRows, maxRowsSource := resolveMaxRowsEnvOnly()
	allIssues, err := store.SearchIssues(ctx, "", types.IssueFilter{
		MaxRows:       maxRows,
		MaxRowsSource: maxRowsSource,
	})
	if err != nil {
		if capErr := handleMaxRowsError(err); capErr != nil {
			return capErr
		}
		return HandleError("fetching issues: %v", err)
	}

	polluted := detectTestPollution(allIssues)

	if len(polluted) == 0 {
		if jsonOutput {
			return outputJSON(map[string]interface{}{
				"polluted_count": 0,
				"issues":         []interface{}{},
			})
		}
		fmt.Println("No test pollution detected!")
		return nil
	}

	// Categorize by confidence
	highConfidence := []pollutionResult{}
	mediumConfidence := []pollutionResult{}

	for _, p := range polluted {
		if p.score >= 0.9 {
			highConfidence = append(highConfidence, p)
		} else {
			mediumConfidence = append(mediumConfidence, p)
		}
	}

	if jsonOutput {
		result := map[string]interface{}{
			"polluted_count":    len(polluted),
			"high_confidence":   len(highConfidence),
			"medium_confidence": len(mediumConfidence),
			"issues":            []map[string]interface{}{},
		}

		for _, p := range polluted {
			result["issues"] = append(result["issues"].([]map[string]interface{}), map[string]interface{}{
				"id":         p.issue.ID,
				"title":      p.issue.Title,
				"score":      p.score,
				"reasons":    p.reasons,
				"created_at": p.issue.CreatedAt,
			})
		}

		return outputJSON(result)
	}

	// Human-readable output
	fmt.Printf("Found %d potential test issues:\n\n", len(polluted))

	if len(highConfidence) > 0 {
		fmt.Printf("High Confidence (score ≥ 0.9):\n")
		for _, p := range highConfidence {
			fmt.Printf("  %s: %q (score: %.2f)\n", p.issue.ID, p.issue.Title, p.score)
			for _, reason := range p.reasons {
				fmt.Printf("    - %s\n", reason)
			}
		}
		fmt.Printf("  (Total: %d issues)\n\n", len(highConfidence))
	}

	if len(mediumConfidence) > 0 {
		fmt.Printf("Medium Confidence (score 0.7-0.9):\n")
		for _, p := range mediumConfidence {
			fmt.Printf("  %s: %q (score: %.2f)\n", p.issue.ID, p.issue.Title, p.score)
			for _, reason := range p.reasons {
				fmt.Printf("    - %s\n", reason)
			}
		}
		fmt.Printf("  (Total: %d issues)\n\n", len(mediumConfidence))
	}

	if !clean {
		fmt.Printf("Run 'bd doctor --check=pollution --clean' to delete these issues (with confirmation).\n")
		return nil
	}

	// The retained types are read from the workspace database the deletes go
	// to, and a read error fails the clean: it deletes row by row with no
	// tombstone, so deleting on a guess could take a bead whose id must be
	// retired.
	warnIgnoredYAMLRetainedTypes()
	retainedSetting, err := store.GetConfig(ctx, issueops.SettingKeyTypesRetained)
	if err != nil {
		return HandleError("reading %s: %v", issueops.SettingKeyTypesRetained, err)
	}
	polluted, retained := selectPollutionDeletable(polluted, retainedSetting)
	if retained > 0 {
		fmt.Println(retainedTypesKeptLine(retained, "test issue(s)"))
	}
	if len(polluted) == 0 {
		fmt.Println("Nothing to delete.")
		return nil
	}

	if !yes {
		fmt.Printf("\nDelete %d test issues? [y/N] ", len(polluted))
		var response string
		_, _ = fmt.Scanln(&response)
		if strings.ToLower(response) != "y" {
			fmt.Println("Canceled.")
			return nil
		}
	}

	backupPath := ".beads/pollution-backup.jsonl"
	if err := backupPollutedIssues(polluted, backupPath); err != nil {
		return HandleError("backing up issues: %v", err)
	}
	fmt.Printf("Backed up %d issues to %s\n", len(polluted), backupPath)

	fmt.Printf("\nDeleting %d issues...\n", len(polluted))
	deleted := 0
	for _, p := range polluted {
		if err := deleteIssue(ctx, p.issue.ID); err != nil {
			fmt.Fprintf(os.Stderr, "Error deleting %s: %v\n", p.issue.ID, err)
			continue
		}
		deleted++
	}

	fmt.Printf("%s Deleted %d test issues\n", ui.RenderPass("✓"), deleted)
	arg, note := remedy.CommandArg(runtime.GOOS, backupPath, "<backup>")
	fmt.Printf("\nCleanup complete. To restore, run: bd init --from-jsonl %s%s\n", arg, note)
	return nil
}

func init() {
	rootCmd.AddCommand(doctorCmd)
	doctorCmd.Flags().BoolVar(&perfMode, "perf", false, "Run performance diagnostics and generate CPU profile")
	doctorCmd.Flags().BoolVar(&checkHealthMode, "check-health", false, "Quick health check for git hooks (silent on success)")
	doctorCmd.Flags().StringVarP(&doctorOutput, "output", "o", "", "Export diagnostics to JSON file")
	doctorCmd.Flags().StringVar(&doctorCheckFlag, "check", "", "Run specific check in detail (e.g., 'pollution')")
	doctorCmd.Flags().BoolVar(&doctorClean, "clean", false, "For pollution check: delete detected test issues")
	doctorCmd.Flags().BoolVar(&doctorDeep, "deep", false, "Validate full graph integrity")
}

// selectPollutionDeletable holds back the detected test issues of a type the
// workspace retains (retainedSetting is the stored value of types.retained)
// and counts them. The pollution heuristic ranges over the whole workspace
// and the clean deletes with no tombstone, so a retained bead's id — an
// idempotency key — is never its to free.
func selectPollutionDeletable(polluted []pollutionResult, retainedSetting string) ([]pollutionResult, int) {
	retainedTypes := workapi.RetainedTypesFromSetting(retainedSetting)
	if len(retainedTypes) == 0 {
		return polluted, 0
	}
	deletable := make([]pollutionResult, 0, len(polluted))
	retained := 0
	for _, p := range polluted {
		if retainedTypes[string(p.issue.IssueType)] {
			retained++
			continue
		}
		deletable = append(deletable, p)
	}
	return deletable, retained
}
