//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/schema"
)

// TestInFlightReindexTrackers covers every phase QA Claude reproduced in
// 0-weaviate-issues#215: no migration (P0), started-only (P1),
// reindexed (P2), swapped (P3), tidied (P4/P5), merged-without-tidied
// (recovery), plus negative-shape cases that must not be mistaken for
// in-flight: pre-generation legacy entries, payload.mig only, an empty
// .migrations dir, a missing .migrations dir, and non-dir entries.
func TestInFlightReindexTrackers(t *testing.T) {
	tests := []struct {
		name string
		// setup creates the on-disk shape inside <root>/lsm/.migrations/
		setup       func(t *testing.T, migsDir string)
		expectDirs  []string // dirnames returned (sorted); empty = no entries returned
		expectError bool
	}{
		{
			name:       "no migrations dir",
			setup:      func(t *testing.T, migsDir string) {},
			expectDirs: nil,
		},
		{
			name: "empty migrations dir",
			setup: func(t *testing.T, migsDir string) {
				require.NoError(t, os.MkdirAll(migsDir, 0o755))
			},
			expectDirs: nil,
		},
		{
			name: "P1 — started only, no reindexed",
			setup: func(t *testing.T, migsDir string) {
				makeTracker(t, migsDir, "searchable_retokenize_body_1", "started.mig")
			},
			expectDirs: []string{"searchable_retokenize_body_1"},
		},
		{
			name: "P2 — started + reindexed, no swapped/tidied",
			setup: func(t *testing.T, migsDir string) {
				makeTracker(t, migsDir, "filterable_retokenize_body_1", "started.mig", "reindexed.mig")
			},
			expectDirs: []string{"filterable_retokenize_body_1"},
		},
		{
			name: "P3 — started + reindexed + swapped, no tidied (still in-flight)",
			setup: func(t *testing.T, migsDir string) {
				makeTracker(t, migsDir, "searchable_retokenize_body_2",
					"started.mig", "reindexed.mig", "swapped.mig")
			},
			expectDirs: []string{"searchable_retokenize_body_2"},
		},
		{
			name: "P4/P5 — tidied present, not in-flight",
			setup: func(t *testing.T, migsDir string) {
				makeTracker(t, migsDir, "searchable_retokenize_body_1",
					"started.mig", "reindexed.mig", "swapped.mig", "tidied.mig")
			},
			expectDirs: nil,
		},
		{
			name: "recovery — merged.mig present without tidied: not in-flight (finalize will promote)",
			setup: func(t *testing.T, migsDir string) {
				makeTracker(t, migsDir, "searchable_retokenize_body_1",
					"started.mig", "reindexed.mig", "merged.mig")
			},
			expectDirs: nil,
		},
		{
			name: "no started — pre-iteration scratch, not in-flight",
			setup: func(t *testing.T, migsDir string) {
				makeTracker(t, migsDir, "searchable_retokenize_body_1", "payload.mig")
			},
			expectDirs: nil,
		},
		{
			name: "missing gen suffix — pre-generation legacy state, skipped defensively",
			setup: func(t *testing.T, migsDir string) {
				makeTracker(t, migsDir, "searchable_retokenize_body", "started.mig")
			},
			expectDirs: nil,
		},
		{
			name: "regular file inside .migrations — skipped",
			setup: func(t *testing.T, migsDir string) {
				require.NoError(t, os.MkdirAll(migsDir, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(migsDir, "stray.txt"), []byte("x"), 0o600))
			},
			expectDirs: nil,
		},
		{
			name: "multiple trackers — sorted result, mix of in-flight and finished",
			setup: func(t *testing.T, migsDir string) {
				// In-flight (P1, P2, P3) — should appear.
				makeTracker(t, migsDir, "searchable_retokenize_body_1", "started.mig")
				makeTracker(t, migsDir, "filterable_retokenize_body_1",
					"started.mig", "reindexed.mig")
				makeTracker(t, migsDir, "searchable_retokenize_body_2",
					"started.mig", "reindexed.mig", "swapped.mig")
				// Finished (P4/P5) — should NOT appear.
				makeTracker(t, migsDir, "filterable_retokenize_body_2",
					"started.mig", "reindexed.mig", "tidied.mig")
				// Merged recovery state — should NOT appear.
				makeTracker(t, migsDir, "enable_searchable_other_1",
					"started.mig", "merged.mig")
			},
			expectDirs: []string{
				// Sorted alphabetically.
				"filterable_retokenize_body_1",
				"searchable_retokenize_body_1",
				"searchable_retokenize_body_2",
			},
		},
		{
			name: "every recognized strategy prefix, one in-flight each",
			setup: func(t *testing.T, migsDir string) {
				for _, name := range []string{
					"searchable_map_to_blockmax_1",
					"filterable_roaringset_refresh_1",
					"filterable_to_rangeable_p1_1",
					"searchable_retokenize_p1_1",
					"filterable_retokenize_p1_1",
					"enable_filterable_p1_1",
					"enable_searchable_p1_1",
				} {
					makeTracker(t, migsDir, name, "started.mig")
				}
			},
			expectDirs: []string{
				"enable_filterable_p1_1",
				"enable_searchable_p1_1",
				"filterable_retokenize_p1_1",
				"filterable_roaringset_refresh_1",
				"filterable_to_rangeable_p1_1",
				"searchable_map_to_blockmax_1",
				"searchable_retokenize_p1_1",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			lsm := filepath.Join(root, "lsm")
			migs := filepath.Join(lsm, ".migrations")
			tc.setup(t, migs)

			got, err := inFlightReindexTrackers(lsm)
			if tc.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			// Treat nil and an empty slice as equivalent for the "nothing
			// in flight" case — both correctly represent the expected
			// outcome and the function's return semantics don't promise
			// one over the other.
			var names []string
			for _, tr := range got {
				names = append(names, tr.DirName)
			}
			assert.Equal(t, tc.expectDirs, names)
		})
	}
}

func TestInFlightReindexTrackers_EmptyPath(t *testing.T) {
	got, err := inFlightReindexTrackers("")
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestInFlightReindexTrackers_TrackerEntryFields(t *testing.T) {
	root := t.TempDir()
	migs := filepath.Join(root, "lsm", ".migrations")
	makeTracker(t, migs, "searchable_retokenize_body_42",
		"started.mig", "reindexed.mig")

	got, err := inFlightReindexTrackers(filepath.Join(root, "lsm"))
	require.NoError(t, err)
	require.Len(t, got, 1)
	tr := got[0]
	assert.Equal(t, "searchable_retokenize_body_42", tr.DirName)
	assert.Equal(t, "searchable_retokenize_body", tr.Prefix)
	assert.Equal(t, 42, tr.Generation)
	assert.True(t, tr.Started)
	assert.True(t, tr.Reindexed)
	assert.False(t, tr.Tidied)
}

func TestInFlightReindexTracker_String(t *testing.T) {
	tr := InFlightReindexTracker{
		DirName:    "searchable_retokenize_body_1",
		Prefix:     "searchable_retokenize_body",
		Generation: 1,
		Started:    true,
		Reindexed:  false,
		Tidied:     false,
	}
	assert.Equal(t,
		"searchable_retokenize_body_1 [started=true reindexed=false tidied=false]",
		tr.String())
}

func TestReindexInFlightError_NoTrackers_NoError(t *testing.T) {
	require.NoError(t, reindexInFlightError("shard0", nil))
	require.NoError(t, reindexInFlightError("shard0", []InFlightReindexTracker{}))
}

func TestReindexInFlightError_Wraps_Sentinel(t *testing.T) {
	err := reindexInFlightError("shard0", []InFlightReindexTracker{
		{DirName: "searchable_retokenize_body_1", Started: true},
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrBackupBlockedByInFlightReindex),
		"error must wrap the sentinel so REST handlers can map via errors.Is")
	// Sanity-check the human-readable message includes the shard name + tracker dir,
	// so an operator log line surfaces both.
	assert.Contains(t, err.Error(), "shard0")
	assert.Contains(t, err.Error(), "searchable_retokenize_body_1")
}

func TestReindexInFlightError_ListsEveryTracker(t *testing.T) {
	trackers := []InFlightReindexTracker{
		{DirName: "filterable_retokenize_body_1", Started: true},
		{DirName: "searchable_retokenize_body_1", Started: true, Reindexed: true},
	}
	err := reindexInFlightError("shard1", trackers)
	require.Error(t, err)
	for _, tr := range trackers {
		assert.Contains(t, err.Error(), tr.DirName,
			"error message must mention every active tracker; operator needs the full picture to decide which migration to wait for / cancel")
	}
}

// TestRefuseIfReindexInFlight_OnRealIndex builds a minimal *Index instance
// with a real shard directory layout so the inactive-shard backup path's
// call to refuseIfReindexInFlight exercises the same shardPathLSM
// resolution as production.
func TestRefuseIfReindexInFlight_OnRealIndex(t *testing.T) {
	root := t.TempDir()
	className := schema.ClassName("JourneyClass")
	shardName := "ABC123"
	lsmPath := filepath.Join(root, indexID(className), shardName, "lsm")
	require.NoError(t, os.MkdirAll(filepath.Join(lsmPath, ".migrations"), 0o755))

	idx := &Index{Config: IndexConfig{RootPath: root, ClassName: className}}

	// Clean shard — no .migrations entries — must not be refused.
	require.NoError(t, idx.refuseIfReindexInFlight(shardName))

	// Add an in-flight tracker and verify the refusal kicks in.
	makeTracker(t, filepath.Join(lsmPath, ".migrations"),
		"searchable_retokenize_body_1", "started.mig")
	err := idx.refuseIfReindexInFlight(shardName)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrBackupBlockedByInFlightReindex))
	require.True(t, strings.Contains(err.Error(), shardName))
}

// makeTracker creates a `.migrations/<dirName>/` directory containing the
// listed sentinel files. The migrations dir is mkdir'd if needed.
func makeTracker(t *testing.T, migsDir, dirName string, sentinels ...string) {
	t.Helper()
	dir := filepath.Join(migsDir, dirName)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	for _, s := range sentinels {
		require.NoError(t, os.WriteFile(filepath.Join(dir, s), nil, 0o600))
	}
}

// TestShard_HaltForTransfer_RefusesWhenReindexInFlight wires the refusal
// behavior end-to-end through a real *Shard. This pins the QA Claude
// repro from 0-weaviate-issues#215: the backup path observed a "halt
// shard for backup: pause compaction" failure roughly 30%–40% of the
// time when a runtime-reindex was in flight. After this fix, the same
// situation deterministically returns ErrBackupBlockedByInFlightReindex
// before any flush is attempted.
//
// The test uses two phases on the same shard:
//   - First: HaltForTransfer with an in-flight tracker on disk → refused.
//   - Second: tidied.mig written → tracker is no longer in-flight, halt
//     succeeds. Resume restores the shard to the live state.
//
// Offloading=true is explicitly NOT exercised in either phase, matching
// the production gate that scopes the refusal to backup callers.
func TestShard_HaltForTransfer_RefusesWhenReindexInFlight(t *testing.T) {
	ctx := testCtx()
	className := "ShardHaltRefuseClass"
	shd, _ := testShard(t, ctx, className)

	migsDir := filepath.Join(shd.(*Shard).pathLSM(), ".migrations")
	dirName := "searchable_retokenize_body_1"

	// Phase 1: in-flight tracker on disk — halt is refused.
	makeTracker(t, migsDir, dirName, "started.mig", "reindexed.mig")
	err := shd.HaltForTransfer(ctx, false, 100*time.Millisecond)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrBackupBlockedByInFlightReindex),
		"halt must wrap the sentinel so REST handlers can map via errors.Is")
	require.Contains(t, err.Error(), dirName,
		"error message must surface the blocking tracker so operators know which migration to wait for")

	// Confirm the refusal did not leave the halt counter incremented:
	// a subsequent halt after the tracker is finished must NOT short-circuit
	// via the "shard was already halted" branch and must do the full work.
	require.NoError(t, os.WriteFile(
		filepath.Join(migsDir, dirName, "tidied.mig"), nil, 0o600))

	require.NoError(t, shd.HaltForTransfer(ctx, false, 100*time.Millisecond))
	// Pair the halt with a resume so the test teardown can proceed cleanly.
	require.NoError(t, shd.(*Shard).resumeMaintenanceCycles(ctx))
}

// TestShard_HaltForTransfer_OffloadIgnoresInFlightReindex documents
// that the refusal is intentionally scoped to backup callers. Tenant
// offload's HaltForTransfer(ctx, true, ...) does not exercise the same
// race surface (the tenant is moved off this node entirely), so the
// gate intentionally lets it through. A regression that flipped this
// would break offload of any tenant whose property happens to be in
// the middle of a reindex; pinning it explicitly prevents accidental
// scope creep on the offload path.
func TestShard_HaltForTransfer_OffloadIgnoresInFlightReindex(t *testing.T) {
	ctx := testCtx()
	className := "ShardHaltOffloadClass"
	shd, _ := testShard(t, ctx, className)

	migsDir := filepath.Join(shd.(*Shard).pathLSM(), ".migrations")
	makeTracker(t, migsDir, "searchable_retokenize_body_1", "started.mig", "reindexed.mig")

	require.NoError(t, shd.HaltForTransfer(ctx, true, 100*time.Millisecond),
		"offload caller must not be blocked by an in-flight reindex; that scenario is out of scope for this fix and needs its own treatment if the underlying race is reproduced in offload")
	require.NoError(t, shd.(*Shard).resumeMaintenanceCycles(ctx))
}
