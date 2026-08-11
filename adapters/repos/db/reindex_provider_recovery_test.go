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
		name string
		// indexType picks the strategy prefixes through the production table;
		// "searchable" unless set.
		indexType string
		// tracker dir name → sentinels in it.
		trackers map[string][]string
		// payloads is the property list a tracker's task recorded.
		payloads map[string][]string
		// corruptPayloads name trackers whose payload.mig exists but does
		// not parse.
		corruptPayloads []string
		// unlistable removes read permission from .migrations, so listing it
		// fails without the dir being absent.
		unlistable bool
		want       bool
	}{
		{
			name:     "no .migrations dir → no recovery needed",
			trackers: nil,
			want:     false,
		},
		{
			name:     "empty .migrations dir → no recovery needed",
			trackers: map[string][]string{},
			want:     false,
		},
		{
			name: "tracker with tidied.mig → completed, no recovery",
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "tidied.mig"},
			},
			want: false,
		},
		{
			name: "tracker with merged.mig only → recovery-eligible, NO recovery (will be promoted by finalize)",
			trackers: map[string][]string{
				"searchable_retokenize_text_2": {"started.mig", "merged.mig"},
			},
			want: false,
		},
		{
			name: "started only → recovery NEEDED",
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig"},
			},
			want: true,
		},
		{
			name: "started + reindexed but no merged/tidied → recovery NEEDED",
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "reindexed.mig"},
			},
			want: true,
		},
		{
			name: "RollingRestartMid repro: prepended but not merged/tidied → recovery NEEDED",
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "reindexed.mig", "prepended.mig"},
			},
			want: true,
		},
		{
			name: "non-matching prefix → no recovery (different property)",
			trackers: map[string][]string{
				"searchable_retokenize_other_1": {"started.mig"},
			},
			want: false,
		},
		{
			name: "non-matching prefix → no recovery (different indexType)",
			trackers: map[string][]string{
				"filterable_retokenize_text_1": {"started.mig"},
			},
			want: false,
		},
		{
			name: "mixed: gen 1 tidied, gen 2 started → recovery NEEDED " +
				"(in-flight follow-up migration interrupted)",
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "tidied.mig"},
				"searchable_retokenize_text_2": {"started.mig"},
			},
			want: true,
		},
		// One tracker serves both properties; the payload says which.
		// (enable_searchable, because retokenize payloads are rejected unless
		// they name exactly one property — see [migrationDirScope.matches].)
		{
			name: "a two-property task, started only → recovery NEEDED",
			trackers: map[string][]string{
				"enable_searchable_other_text_1": {"started.mig"},
			},
			payloads: map[string][]string{
				"enable_searchable_other_text_1": {"other", "text"},
			},
			want: true,
		},
		{
			name: "a two-property task this property is not part of",
			trackers: map[string][]string{
				"enable_searchable_other_third_1": {"started.mig"},
			},
			payloads: map[string][]string{
				"enable_searchable_other_third_1": {"other", "third"},
			},
			want: false,
		},
		// A payload that exists but doesn't parse could name this property;
		// reporting "done" on it would deregister the local callbacks while
		// the untidied tracker remains. Fails toward recovery, like the
		// unloaded-shard gate on identical input.
		{
			name: "an untidied multi-property tracker with a corrupt payload → recovery NEEDED",
			trackers: map[string][]string{
				"enable_searchable_other_text_1": {"started.mig"},
			},
			corruptPayloads: []string{"enable_searchable_other_text_1"},
			want:            true,
		},
		{
			name: "a tidied tracker with a corrupt payload → completed, no recovery",
			trackers: map[string][]string{
				"enable_searchable_other_text_1": {"started.mig", "tidied.mig"},
			},
			corruptPayloads: []string{"enable_searchable_other_text_1"},
			want:            false,
		},
		{
			name: "a corrupt payload on another index type's tracker → no recovery",
			trackers: map[string][]string{
				"filterable_retokenize_text_1": {"started.mig"},
			},
			corruptPayloads: []string{"filterable_retokenize_text_1"},
			want:            false,
		},
		// A dir from before [genSuffix]: the sweep deletes it, so the
		// recovery probe must see it too.
		{
			name: "a generation-less tracker, started only → recovery NEEDED",
			trackers: map[string][]string{
				"searchable_retokenize_text": {"started.mig"},
			},
			want: true,
		},
		{
			name: "two of this index type's prefixes, one tidied + one started → recovery NEEDED",
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "tidied.mig"},
				"enable_searchable_text_1":     {"started.mig"},
			},
			want: true,
		},
		{
			name: "two of this index type's prefixes, both tidied → no recovery",
			trackers: map[string][]string{
				"searchable_retokenize_text_1": {"started.mig", "tidied.mig"},
				"enable_searchable_text_1":     {"started.mig", "tidied.mig"},
			},
			want: false,
		},
		// A .migrations dir that exists but can't be listed could hold an
		// untidied tracker; reporting "done" would deregister the local
		// callbacks while it remains. Fails toward recovery, like the
		// unloaded-shard gate on the identical condition.
		{
			name:       "an unlistable .migrations dir → recovery NEEDED",
			trackers:   map[string][]string{},
			unlistable: true,
			want:       true,
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
					if props, ok := tc.payloads[trackerName]; ok {
						mkRecoveryPayload(t, tmp, trackerName, props...)
					}
				}
				for _, trackerName := range tc.corruptPayloads {
					require.NoError(t, os.WriteFile(
						filepath.Join(migsDir, trackerName, reindexRecoveryPayloadFile),
						[]byte("not a recovery record"), 0o644))
				}
				if tc.unlistable {
					require.NoError(t, os.Chmod(migsDir, 0o000))
					t.Cleanup(func() { _ = os.Chmod(migsDir, 0o755) })
				}
			}
			indexType := tc.indexType
			if indexType == "" {
				indexType = "searchable"
			}
			got := hasUntidiedTracker(migrationDirsOf(tmp, nil, "text", indexType))
			require.Equal(t, tc.want, got)
		})
	}
}

// TestIsSemanticMigration pins the semantic/format-only classification
// (weaviate/0-weaviate-issues#254 promoted change-algorithm to semantic).
func TestIsSemanticMigration(t *testing.T) {
	semantic := []ReindexMigrationType{
		ReindexTypeChangeTokenization,
		ReindexTypeChangeTokenizationFilterable,
		ReindexTypeEnableFilterable,
		ReindexTypeEnableSearchable,
		ReindexTypeChangeAlgorithm,
	}
	formatOnly := []ReindexMigrationType{
		ReindexTypeRebuildSearchable,
		ReindexTypeRepairFilterable,
		ReindexTypeEnableRangeable,
		ReindexTypeRepairRangeable,
	}
	for _, mt := range semantic {
		t.Run(string(mt)+" → semantic", func(t *testing.T) {
			require.True(t, IsSemanticMigration(mt))
		})
	}
	for _, mt := range formatOnly {
		t.Run(string(mt)+" → format-only", func(t *testing.T) {
			require.False(t, IsSemanticMigration(mt))
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
			name: "change-algorithm → searchable (semantic, cluster-wide flag flip)",
			mt:   ReindexTypeChangeAlgorithm,
			want: []string{"searchable"},
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
