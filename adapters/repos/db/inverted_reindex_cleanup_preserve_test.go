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
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// TestCompletedMigrationGens_PinsR2DataLoss pins the gating logic that
// prevents the R2/R2b silent data loss (#10675 family): a back-to-back
// submit on the same property must NOT wipe the prior migration's
// tracker dir when that tracker has tidied.mig (= successfully
// completed, ingest dir is live data).
//
// The tests below construct a synthetic .migrations/ directory layout
// and assert which generations [completedMigrationGens] reports as
// preserved.
func TestCompletedMigrationGens(t *testing.T) {
	type setup struct {
		// trackerDir name (e.g. "searchable_retokenize_text_1") → sentinels to write.
		trackers map[string][]string
	}
	tests := []struct {
		name     string
		prefixes []string
		setup    setup
		want     []int
	}{
		{
			name:     "empty migrations dir → no preserved gens",
			prefixes: []string{"searchable_retokenize_text", "filterable_retokenize_text"},
			setup:    setup{trackers: map[string][]string{}},
			want:     []int{},
		},
		{
			name:     "only started.mig → not preserved (partial state)",
			prefixes: []string{"searchable_retokenize_text"},
			setup: setup{trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "payload.mig"},
			}},
			want: []int{},
		},
		{
			name:     "tidied.mig present → preserved",
			prefixes: []string{"searchable_retokenize_text"},
			setup: setup{trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "tidied.mig"},
			}},
			want: []int{1},
		},
		{
			name:     "merged.mig only (untidied; recovery path) → preserved",
			prefixes: []string{"searchable_retokenize_text"},
			setup: setup{trackers: map[string][]string{
				"searchable_retokenize_text_2": {"started.mig", "merged.mig"},
			}},
			want: []int{2},
		},
		{
			name:     "mix: tidied gen 1, started gen 2 → only gen 1 preserved",
			prefixes: []string{"searchable_retokenize_text"},
			setup: setup{trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "tidied.mig"},
				"searchable_retokenize_text_2": {"started.mig"},
			}},
			want: []int{1},
		},
		{
			name:     "different prefix → not matched",
			prefixes: []string{"searchable_retokenize_text"},
			setup: setup{trackers: map[string][]string{
				"filterable_retokenize_text_1": {"tidied.mig"},
			}},
			want: []int{},
		},
		{
			name:     "different prop suffix → not matched",
			prefixes: []string{"searchable_retokenize_text"},
			setup: setup{trackers: map[string][]string{
				"searchable_retokenize_other_1": {"tidied.mig"},
			}},
			want: []int{},
		},
		{
			name: "two matching prefixes (searchable + filterable for same prop) " +
				"both tidied → both preserved",
			prefixes: []string{
				"searchable_retokenize_text",
				"filterable_retokenize_text",
			},
			setup: setup{trackers: map[string][]string{
				"searchable_retokenize_text_1": {"tidied.mig"},
				"filterable_retokenize_text_1": {"tidied.mig"},
			}},
			want: []int{1},
		},
		{
			name:     "no .migrations dir → empty result, no error",
			prefixes: []string{"searchable_retokenize_text"},
			setup:    setup{trackers: nil},
			want:     []int{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			if tc.setup.trackers != nil {
				migsDir := filepath.Join(tmp, ".migrations")
				require.NoError(t, os.MkdirAll(migsDir, 0o755))
				for trackerName, sentinels := range tc.setup.trackers {
					dir := filepath.Join(migsDir, trackerName)
					require.NoError(t, os.MkdirAll(dir, 0o755))
					for _, s := range sentinels {
						require.NoError(t,
							os.WriteFile(filepath.Join(dir, s), []byte("x"), 0o644))
					}
				}
			}

			got := completedMigrationGens(tmp, tc.prefixes)
			gens := make([]int, 0, len(got))
			for g := range got {
				gens = append(gens, g)
			}
			sort.Ints(gens)
			require.Equal(t, tc.want, gens, "preserved gens mismatch")
		})
	}
}

// TestCleanStaleMigrationDirsAt_PreservesCompletedGens pins the
// end-to-end behavior of the cleanup helper: tracker dirs with
// tidied.mig (or merged.mig) survive; tracker dirs with only started.mig
// are removed.
//
// This is the R2/R2b regression: before the fix, the pre-submit
// CleanStalePartialReindexState wiped tracker_1 (which had tidied.mig)
// out from under the in-memory ingest bucket pointer → next migration
// picked gen=1 again → previous data overwritten → silent #10675-shape
// loss on the controller node.
func TestCleanStaleMigrationDirsAt_PreservesCompletedGens(t *testing.T) {
	tests := []struct {
		name     string
		propName string
		idxType  string
		// Pre-cleanup trackers: name → sentinels.
		trackers map[string][]string
		// Post-cleanup expected trackers still on disk.
		wantSurvivors []string
	}{
		{
			name:     "tidied tracker survives, started-only tracker removed",
			propName: "text",
			idxType:  "searchable",
			trackers: map[string][]string{
				// Completed migration (T1) — must survive.
				"searchable_retokenize_text_1": {"started.mig", "tidied.mig"},
				// Cancelled / partial migration (T2 cancelled mid-flight) — wipe.
				"searchable_retokenize_text_2": {"started.mig"},
			},
			wantSurvivors: []string{"searchable_retokenize_text_1"},
		},
		{
			name:     "untidied-merged tracker survives (recovery path)",
			propName: "text",
			idxType:  "searchable",
			trackers: map[string][]string{
				"searchable_retokenize_text_3": {"started.mig", "merged.mig"},
			},
			wantSurvivors: []string{"searchable_retokenize_text_3"},
		},
		{
			name:     "non-matching prop survives (different propName)",
			propName: "text",
			idxType:  "searchable",
			trackers: map[string][]string{
				// Cleaning prop=text MUST NOT touch prop=other.
				"searchable_retokenize_other_1": {"started.mig"},
				// Stale state on the target prop — wipe.
				"searchable_retokenize_text_1": {"started.mig"},
			},
			wantSurvivors: []string{"searchable_retokenize_other_1"},
		},
		{
			name:     "different indexType survives (filterable when searchable is cleaned)",
			propName: "text",
			idxType:  "searchable",
			trackers: map[string][]string{
				// Filterable tracker for the same prop — not touched.
				"filterable_retokenize_text_1": {"started.mig"},
				"searchable_retokenize_text_1": {"started.mig"},
			},
			wantSurvivors: []string{"filterable_retokenize_text_1"},
		},
		{
			name:     "all started-only → all removed",
			propName: "text",
			idxType:  "searchable",
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig"},
				"searchable_retokenize_text_2": {"started.mig"},
			},
			wantSurvivors: []string{},
		},
		{
			name:     "R2 repro: T1 tidied at gen 1, T2 tidied at gen 2 → both survive",
			propName: "text",
			idxType:  "searchable",
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "tidied.mig"},
				"searchable_retokenize_text_2": {"started.mig", "tidied.mig"},
			},
			wantSurvivors: []string{
				"searchable_retokenize_text_1",
				"searchable_retokenize_text_2",
			},
		},
	}

	logger := logrus.New()
	logger.SetOutput(io.Discard)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			migsDir := filepath.Join(tmp, ".migrations")
			require.NoError(t, os.MkdirAll(migsDir, 0o755))
			for trackerName, sentinels := range tc.trackers {
				dir := filepath.Join(migsDir, trackerName)
				require.NoError(t, os.MkdirAll(dir, 0o755))
				for _, s := range sentinels {
					require.NoError(t,
						os.WriteFile(filepath.Join(dir, s), []byte("x"), 0o644))
				}
			}

			cleanStaleMigrationDirsAt(tmp, tc.propName, tc.idxType, logger)

			survivors, err := os.ReadDir(migsDir)
			require.NoError(t, err)
			var got []string
			for _, e := range survivors {
				got = append(got, e.Name())
			}
			sort.Strings(got)
			want := append([]string(nil), tc.wantSurvivors...)
			sort.Strings(want)
			require.Equal(t, want, got)
		})
	}
}

// TestCompletedMigrationGens_R2Repro pins the exact R2 scenario where the
// pre-submit defense-in-depth cleanup would otherwise wipe a successfully
// completed migration's tracker dir. T1 finishes (tracker_1 has
// tidied.mig). T2 is submitted. completedMigrationGens MUST report gen=1
// as preserved so the cleanup leaves the live ingest dir alone.
func TestCompletedMigrationGens_R2Repro(t *testing.T) {
	tmp := t.TempDir()
	migsDir := filepath.Join(tmp, ".migrations")
	require.NoError(t, os.MkdirAll(migsDir, 0o755))

	// Simulate post-T1 disk state: tracker_1 has all sentinels through
	// markTidied (started, reindexed, prepended, merged, swapped, tidied).
	for _, sub := range []string{
		"searchable_retokenize_text_1",
		"filterable_retokenize_text_1",
	} {
		dir := filepath.Join(migsDir, sub)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		for _, sentinel := range []string{
			"started.mig", "payload.mig", "reindexed.mig",
			"prepended.mig", "merged.mig", "swapped.mig", "tidied.mig",
		} {
			require.NoError(t,
				os.WriteFile(filepath.Join(dir, sentinel), []byte("x"), 0o644))
		}
	}

	prefixes := []string{"searchable_retokenize_text", "filterable_retokenize_text"}
	got := completedMigrationGens(tmp, prefixes)
	require.True(t, got[1],
		"R2 repro: gen=1 MUST be preserved (T1 successfully tidied); else pre-submit cleanup wipes live ingest_1 dir → silent data loss on the controller node")
	require.Len(t, got, 1, "only gen=1 should be reported, got %v", got)
}
