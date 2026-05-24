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

package reindex

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHasUntidiedTracker pins the on-disk recovery-detection signal:
// a tracker dir matching the property/index prefix without tidied.mig
// or merged.mig is a half-applied swap that needs OnGroupCompleted to
// re-fire. Without this detection, the scheduler bootstrap pre-mark
// silently suppresses the retry and the affected shard stays at the
// old tokenization (#10675-family RollingRestartMidMigration repro).
func TestHasUntidiedTracker(t *testing.T) {
	tests := []struct {
		name     string
		prefixes []string
		// tracker dir name → sentinels in it.
		trackers map[string][]string
		want     bool
	}{
		{
			name:     "no .migrations dir → no recovery needed",
			prefixes: []string{"searchable_retokenize_text"},
			trackers: nil,
			want:     false,
		},
		{
			name:     "empty .migrations dir → no recovery needed",
			prefixes: []string{"searchable_retokenize_text"},
			trackers: map[string][]string{},
			want:     false,
		},
		{
			name:     "tracker with tidied.mig → completed, no recovery",
			prefixes: []string{"searchable_retokenize_text"},
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "tidied.mig"},
			},
			want: false,
		},
		{
			name:     "tracker with merged.mig only → recovery-eligible, NO recovery (will be promoted by finalize)",
			prefixes: []string{"searchable_retokenize_text"},
			trackers: map[string][]string{
				"searchable_retokenize_text_2": {"started.mig", "merged.mig"},
			},
			want: false,
		},
		{
			name:     "started only → recovery NEEDED",
			prefixes: []string{"searchable_retokenize_text"},
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig"},
			},
			want: true,
		},
		{
			name:     "started + reindexed but no merged/tidied → recovery NEEDED",
			prefixes: []string{"searchable_retokenize_text"},
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "reindexed.mig"},
			},
			want: true,
		},
		{
			name:     "RollingRestartMid repro: prepended but not merged/tidied → recovery NEEDED",
			prefixes: []string{"searchable_retokenize_text"},
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "reindexed.mig", "prepended.mig"},
			},
			want: true,
		},
		{
			name:     "non-matching prefix → no recovery (different property)",
			prefixes: []string{"searchable_retokenize_text"},
			trackers: map[string][]string{
				"searchable_retokenize_other_1": {"started.mig"},
			},
			want: false,
		},
		{
			name:     "non-matching prefix → no recovery (different indexType)",
			prefixes: []string{"searchable_retokenize_text"},
			trackers: map[string][]string{
				"filterable_retokenize_text_1": {"started.mig"},
			},
			want: false,
		},
		{
			name: "mixed: gen 1 tidied, gen 2 started → recovery NEEDED " +
				"(in-flight follow-up migration interrupted)",
			prefixes: []string{"searchable_retokenize_text"},
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "tidied.mig"},
				"searchable_retokenize_text_2": {"started.mig"},
			},
			want: true,
		},
		{
			name: "two matching prefixes, one tidied + one started → recovery NEEDED",
			prefixes: []string{
				"searchable_retokenize_text",
				"filterable_retokenize_text",
			},
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "tidied.mig"},
				"filterable_retokenize_text_1": {"started.mig"},
			},
			want: true,
		},
		{
			name: "two matching prefixes, both tidied → no recovery",
			prefixes: []string{
				"searchable_retokenize_text",
				"filterable_retokenize_text",
			},
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "tidied.mig"},
				"filterable_retokenize_text_1": {"started.mig", "tidied.mig"},
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			if tc.trackers != nil {
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
			}
			got := hasUntidiedTracker(tmp, tc.prefixes)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestSemanticMigrationIndexTypes pins the migration-type → index-type
// mapping. Format-only migrations (repair-*, enable-rangeable) MUST
// return nil here — they don't go through the swap barrier, so
// LocalCallbacksDone has nothing to check for them.
func TestSemanticMigrationIndexTypes(t *testing.T) {
	tests := []struct {
		name string
		mt   ReindexMigrationType
		want []string
	}{
		{
			name: "change-tokenization → searchable + filterable",
			mt:   ReindexTypeChangeTokenization,
			want: []string{"searchable", "filterable"},
		},
		{
			name: "change-tokenization-filterable → filterable only",
			mt:   ReindexTypeChangeTokenizationFilterable,
			want: []string{"filterable"},
		},
		{
			name: "enable-searchable → searchable",
			mt:   ReindexTypeEnableSearchable,
			want: []string{"searchable"},
		},
		{
			name: "enable-filterable → filterable",
			mt:   ReindexTypeEnableFilterable,
			want: []string{"filterable"},
		},
		{
			name: "repair-searchable → empty (format-only, no swap barrier)",
			mt:   ReindexTypeChangeAlgorithm,
			want: nil,
		},
		{
			name: "repair-filterable → empty (format-only)",
			mt:   ReindexTypeRepairFilterable,
			want: nil,
		},
		{
			name: "enable-rangeable → empty (format-only)",
			mt:   ReindexTypeEnableRangeable,
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := semanticMigrationIndexTypes(tc.mt)
			require.Equal(t, tc.want, got)
		})
	}
}
