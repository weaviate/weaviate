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
		name string
		// indexType picks the strategy prefixes through the production table;
		// "searchable" unless set.
		indexType string
		setup     setup
		want      []int
	}{
		{
			name:  "empty migrations dir → no preserved gens",
			setup: setup{trackers: map[string][]string{}},
			want:  []int{},
		},
		{
			name: "only started.mig → not preserved (partial state)",
			setup: setup{trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "payload.mig"},
			}},
			want: []int{},
		},
		{
			name: "tidied.mig present → preserved",
			setup: setup{trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "tidied.mig"},
			}},
			want: []int{1},
		},
		{
			name: "merged.mig only (untidied; recovery path) → preserved",
			setup: setup{trackers: map[string][]string{
				"searchable_retokenize_text_2": {"started.mig", "merged.mig"},
			}},
			want: []int{2},
		},
		{
			name: "mix: tidied gen 1, started gen 2 → only gen 1 preserved",
			setup: setup{trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "tidied.mig"},
				"searchable_retokenize_text_2": {"started.mig"},
			}},
			want: []int{1},
		},
		{
			name: "different prefix → not matched",
			setup: setup{trackers: map[string][]string{
				"filterable_retokenize_text_1": {"tidied.mig"},
			}},
			want: []int{},
		},
		{
			name: "different prop suffix → not matched",
			setup: setup{trackers: map[string][]string{
				"searchable_retokenize_other_1": {"tidied.mig"},
			}},
			want: []int{},
		},
		{
			name: "two of this index type's prefixes at the same gen, " +
				"both tidied → the gen is preserved once",
			setup: setup{trackers: map[string][]string{
				"searchable_retokenize_text_1": {"tidied.mig"},
				"enable_searchable_text_1":     {"tidied.mig"},
			}},
			want: []int{1},
		},
		{
			name:  "no .migrations dir → empty result, no error",
			setup: setup{trackers: nil},
			want:  []int{},
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

			indexType := tc.indexType
			if indexType == "" {
				indexType = "searchable"
			}
			got := completedMigrationGens(migrationDirsOf(tmp, nil, "text", indexType))
			gens := make([]int, 0, len(got))
			for g := range got {
				gens = append(gens, g)
			}
			sort.Ints(gens)
			require.Equal(t, tc.want, gens, "preserved gens mismatch")
		})
	}
}

// TestCleanStaleMigrationDirsAtRetiresWhatTheDeleteRemoves pins the end-to-end
// behavior of the helper an index DELETE sweeps with: it removes the trackers of
// the bucket the DELETE just dropped, whatever stage they reached, and touches
// nothing outside that bucket. A completed tracker outliving its bucket is what
// has the next load re-open a deleted index.
//
// The opposite contract — a completed tracker survives the sweep, since its
// ingest dir is what the in-memory bucket pointer is on (#10675) — belongs to
// the submit and cancel path, [Shard.CleanStalePartialReindexState], which does
// not retire.
func TestCleanStaleMigrationDirsAtRetiresWhatTheDeleteRemoves(t *testing.T) {
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
			name:     "every generation of the removed bucket goes, completed or not",
			propName: "text",
			idxType:  "searchable",
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "tidied.mig"},
				// The recovery shape: the swap died between merging and tidying.
				"searchable_retokenize_text_2": {"started.mig", "merged.mig"},
				"searchable_retokenize_text_3": {"started.mig"},
			},
			wantSurvivors: []string{},
		},
		{
			// The rangeable strategy is in the filterable sweep's scope, but the
			// bucket it promotes is not the one this DELETE removes.
			name:     "a completed tracker of a neighbouring index's bucket survives",
			propName: "text",
			idxType:  "filterable",
			trackers: map[string][]string{
				"filterable_to_rangeable_text_1": {"started.mig", "tidied.mig"},
				"enable_filterable_text_1":       {"started.mig", "tidied.mig"},
			},
			wantSurvivors: []string{"filterable_to_rangeable_text_1"},
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

			cleanStaleMigrationDirsAt(t.Context(), tmp, tc.propName, tc.idxType, logger, nil)

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

	got := completedMigrationGens(migrationDirsOf(tmp, nil, "text", "searchable"))
	require.True(t, got[1],
		"R2 repro: gen=1 MUST be preserved (T1 successfully tidied); else pre-submit cleanup wipes live ingest_1 dir → silent data loss on the controller node")
	require.Len(t, got, 1, "only gen=1 should be reported, got %v", got)
}
