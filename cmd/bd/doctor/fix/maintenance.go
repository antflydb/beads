package fix

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/configfile"
	"github.com/steveyegge/beads/internal/types"
	"github.com/steveyegge/beads/internal/workapi"
	"github.com/steveyegge/beads/issueops"
)

// StaleClosedIssues deletes stale closed issues.
// This is the fix handler for the "Stale Closed Issues" doctor check.
//
// This fix is DISABLED by default (stale_closed_issues_days=0). Users must
// explicitly set a positive threshold in metadata.json to enable cleanup.
func StaleClosedIssues(path string) error {
	beadsDir, err := resolvedWorkspaceBeadsDir(path)
	if err != nil {
		return err
	}

	// Load config and check if cleanup is enabled
	cfg, err := configfile.Load(beadsDir)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Get threshold; 0 means disabled
	var thresholdDays int
	if cfg != nil {
		thresholdDays = cfg.GetStaleClosedIssuesDays()
	}

	if thresholdDays == 0 {
		fmt.Println("  Stale closed issues cleanup disabled (set stale_closed_issues_days to enable)")
		return nil
	}

	// Open database using factory to respect backend configuration (bd-m2jr:
	// SQLite fallback fix). This handler DELETES issues, so it opens through
	// the bead-mutating factory: the deletes are journaled like any other.
	ctx := context.Background()
	store, err := openBeadMutatingStore(ctx, beadsDir)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer func() { _ = store.Close() }()

	// Find closed issues older than configured threshold
	cutoff := time.Now().AddDate(0, 0, -thresholdDays)
	statusClosed := types.StatusClosed
	filter := types.IssueFilter{
		Status:       &statusClosed,
		ClosedBefore: &cutoff,
	}

	issues, err := store.SearchIssues(ctx, "", filter)
	if err != nil {
		return fmt.Errorf("failed to query issues: %w", err)
	}

	// The retained types are read from the workspace database the deletes go
	// to. A read error fails the fix: deleting on a guess would take beads
	// whose ids must be retired, and this fix retires none.
	retainedSetting, err := store.GetConfig(ctx, issueops.SettingKeyTypesRetained)
	if err != nil {
		return fmt.Errorf("reading %s: %w", issueops.SettingKeyTypesRetained, err)
	}
	deletable, skipped, retained := selectStaleClosed(issues, retainedSetting)

	var deleted int
	for _, issue := range deletable {
		if err := store.DeleteIssue(ctx, issue.ID); err != nil {
			fmt.Printf("  Warning: failed to delete %s: %v\n", issue.ID, err)
			continue
		}
		deleted++
	}

	if deleted == 0 && skipped == 0 && retained == 0 {
		fmt.Println("  No stale closed issues to clean up")
	} else {
		if deleted > 0 {
			fmt.Printf("  Cleaned up %d stale closed issue(s) (older than %d days)\n", deleted, thresholdDays)
		}
		if skipped > 0 {
			fmt.Printf("  Skipped %d pinned issue(s)\n", skipped)
		}
		if retained > 0 {
			fmt.Printf("  Kept %d closed issue(s) of a retained type (types.retained) — only bd prune/purge --retain-ids deletes them\n", retained)
		}
	}

	return nil
}

// selectStaleClosed narrows the stale closed issues to the ones the fix may
// delete: pinned issues are skipped, and so are issues of a type the
// workspace retains (retainedSetting is the stored value of types.retained).
// The fix deletes row by row and writes no tombstone, so a retained issue's id
// — an idempotency key — is never its to free.
func selectStaleClosed(issues []*types.Issue, retainedSetting string) (deletable []*types.Issue, pinned, retained int) {
	unpinned := make([]*types.Issue, 0, len(issues))
	for _, issue := range issues {
		if issue.Pinned {
			pinned++
			continue
		}
		unpinned = append(unpinned, issue)
	}
	deletable, retained = workapi.PartitionSweepRetainedTypes(unpinned, workapi.RetainedTypesFromSetting(retainedSetting))
	return deletable, pinned, retained
}

// PatrolPollution deletes patrol digest and session ended beads that pollute the database.
// This is the fix handler for the "Patrol Pollution" doctor check.
//
// It removes beads matching:
// - Patrol digests: titles matching "Digest: mol-*-patrol"
// - Session ended beads: titles matching "Session ended: *"
//
// After deletion, cleans up any orphaned data.
func PatrolPollution(path string) error {
	beadsDir, err := resolvedWorkspaceBeadsDir(path)
	if err != nil {
		return err
	}

	// Open database using factory to respect backend configuration (bd-m2jr:
	// SQLite fallback fix). This handler DELETES issues, so it opens through
	// the bead-mutating factory: the deletes are journaled like any other.
	ctx := context.Background()
	store, err := openBeadMutatingStore(ctx, beadsDir)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer func() { _ = store.Close() }()

	// Get all issues and identify pollution
	ephemeral := false
	issues, err := store.SearchIssues(ctx, "", types.IssueFilter{Ephemeral: &ephemeral})
	if err != nil {
		return fmt.Errorf("failed to query issues: %w", err)
	}

	// The retained types are read from the workspace database the deletes go
	// to. A read error fails the fix: it deletes by title with no tombstone,
	// so deleting on a guess could take a bead whose id must be retired.
	retainedSetting, err := store.GetConfig(ctx, issueops.SettingKeyTypesRetained)
	if err != nil {
		return fmt.Errorf("reading %s: %w", issueops.SettingKeyTypesRetained, err)
	}
	sel := selectPatrolPollution(issues, retainedSetting)
	if sel.retained > 0 {
		fmt.Printf("  Kept %d pollution bead(s) of a retained type (types.retained) — only bd prune/purge --retain-ids deletes them\n", sel.retained)
	}

	if len(sel.ids) == 0 {
		fmt.Println("  No patrol pollution beads to delete")
		return nil
	}

	// Delete all pollution beads
	var deleted int
	for _, id := range sel.ids {
		if err := store.DeleteIssue(ctx, id); err != nil {
			fmt.Printf("  Warning: failed to delete %s: %v\n", id, err)
			continue
		}
		deleted++
	}

	// Report results
	if sel.digests > 0 {
		fmt.Printf("  Deleted %d patrol digest bead(s)\n", sel.digests)
	}
	if sel.sessions > 0 {
		fmt.Printf("  Deleted %d session ended bead(s)\n", sel.sessions)
	}
	fmt.Printf("  Total: %d pollution bead(s) removed\n", deleted)

	return nil
}

// patrolPollutionSelection is what the Patrol Pollution fix deletes (ids) and
// how many of each kind it matched, plus how many matching beads it held back
// because their type is retained.
type patrolPollutionSelection struct {
	ids                         []string
	digests, sessions, retained int
}

// selectPatrolPollution picks the patrol digest and session-ended beads by
// title, then holds back the matches of a type the workspace retains
// (retainedSetting is the stored value of types.retained): the fix deletes row
// by row and writes no tombstone, so a retained bead's id — an idempotency key
// — is never its to free.
func selectPatrolPollution(issues []*types.Issue, retainedSetting string) patrolPollutionSelection {
	var matched []*types.Issue
	for _, issue := range issues {
		if isPatrolDigestTitle(issue.Title) || isSessionEndedTitle(issue.Title) {
			matched = append(matched, issue)
		}
	}
	deletable, retained := workapi.PartitionSweepRetainedTypes(matched, workapi.RetainedTypesFromSetting(retainedSetting))
	sel := patrolPollutionSelection{retained: retained}
	for _, issue := range deletable {
		if isPatrolDigestTitle(issue.Title) {
			sel.digests++
		} else {
			sel.sessions++
		}
		sel.ids = append(sel.ids, issue.ID)
	}
	return sel
}

// isPatrolDigestTitle matches a patrol digest: "Digest: mol-*-patrol".
func isPatrolDigestTitle(title string) bool {
	return strings.HasPrefix(title, "Digest: mol-") && strings.HasSuffix(title, "-patrol")
}

// isSessionEndedTitle matches a session-ended bead: "Session ended: *".
func isSessionEndedTitle(title string) bool {
	return strings.HasPrefix(title, "Session ended:")
}
