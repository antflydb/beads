//go:build cgo

package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// TestProxiedServerRetainedTypes is TestEmbeddedRetainedTypes on the proxied
// route, whose `bd gc` decay reads through its own unit of work rather than
// the store: with types.retained naming a type, a plain `bd prune`, `bd gc`
// and `bd mol wisp gc` keep that type's beads and report them as
// retention_required under --json.
func TestProxiedServerRetainedTypes(t *testing.T) {
	requireSharedProxiedServer(t)
	t.Parallel()

	bd := buildEmbeddedBD(t)
	p := newSharedProxiedProject(t, bd, "pk")
	// set-many writes the batch on its own unit of work on this route; it
	// refuses a malformed value as `bd config set` does.
	if out, err := bdProxiedRun(t, bd, p.dir, "config", "set-many", "types.retained=[chore]"); err == nil {
		t.Errorf("bd config set-many types.retained=[chore] succeeded:\n%s\nwant the malformed value refused", out)
	}
	if out, err := bdProxiedRun(t, bd, p.dir, "config", "set", "types.retained", "chore"); err != nil {
		t.Fatalf("bd config set types.retained: %v\n%s", err, out)
	}
	keyed := bdProxiedCreate(t, bd, p.dir, "keyed", "--id", "pk-keyed1", "--type", "chore").ID
	if out, err := bdProxiedRun(t, bd, p.dir, "close", keyed); err != nil {
		t.Fatalf("bd close %s: %v\n%s", keyed, err, out)
	}
	decayJSON := func(args ...string) map[string]any {
		t.Helper()
		stdout, stderr, err := bdProxiedRunBuffers(t, bd, p.dir, append(args, "--json")...)
		if err != nil {
			t.Fatalf("bd %s --json: %v\n%s\n%s", strings.Join(args, " "), err, stdout, stderr)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(stdout[strings.Index(stdout, "{"):]), &payload); err != nil {
			t.Fatalf("bd %s --json output is not JSON: %v\n%s", strings.Join(args, " "), err, stdout)
		}
		return payload
	}

	pruned := decayJSON("prune", "--pattern", keyed, "--force")
	if pruned["pruned_count"] != float64(0) || pruned["retention_required"] != float64(1) {
		t.Errorf("bd prune --force --json = %v, want pruned_count 0 and retention_required 1", pruned)
	}
	// --older-than 0 compares closed_at against now; let the second-precision
	// close stamp fall strictly before it.
	time.Sleep(1100 * time.Millisecond)
	report := decayJSON("gc", "--older-than", "0", "--force", "--skip-dolt")
	if report["retention_required"] != float64(1) {
		t.Errorf("bd gc --json = %v, want retention_required 1", report)
	}
	if out, err := bdProxiedRun(t, bd, p.dir, "show", keyed, "--json"); err != nil {
		t.Errorf("bd show %s after a plain prune and gc: %v\n%s; a retained bead was deleted", keyed, err, out)
	}

	// `bd mol wisp gc` reads the setting on its own unit of work on this
	// route, in both modes.
	keyedWisp := bdProxiedCreate(t, bd, p.dir, "keyed wisp", "--id", "pk-keyed2", "--type", "chore", "--ephemeral").ID
	if out, err := bdProxiedRun(t, bd, p.dir, "close", keyedWisp); err != nil {
		t.Fatalf("bd close %s: %v\n%s", keyedWisp, err, out)
	}
	purged := decayJSON("mol", "wisp", "gc", "--closed", "--force")
	if purged["retention_required"] != float64(1) {
		t.Errorf("bd mol wisp gc --closed --force --json = %v, want retention_required 1", purged)
	}
	openWisp := bdProxiedCreate(t, bd, p.dir, "abandoned keyed wisp", "--id", "pk-keyed3", "--type", "chore", "--ephemeral").ID
	// --age 0s makes every wisp abandoned without a clock comparison
	// (wispIsAbandoned). A small positive age is not enough: updated_at is a
	// DATETIME(0), which Dolt rounds half-up, so a wisp created a few
	// milliseconds ago can read back as not yet that old.
	aged := decayJSON("mol", "wisp", "gc", "--age", "0s", "--all")
	if aged["retention_required"] != float64(2) {
		t.Errorf("bd mol wisp gc --all --json = %v, want retention_required 2", aged)
	}
	for _, id := range []string{keyedWisp, openWisp} {
		if out, err := bdProxiedRun(t, bd, p.dir, "show", id, "--json"); err != nil {
			t.Errorf("bd show %s after bd mol wisp gc: %v\n%s; a retained wisp was deleted", id, err, out)
		}
	}
}
