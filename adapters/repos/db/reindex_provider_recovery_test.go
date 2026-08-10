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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
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

// TestIsSemanticMigration pins the semantic/format-only classification:
// semantic iff the migration requires a global schema flip
// (weaviate/0-weaviate-issues#254, #465).
func TestIsSemanticMigration(t *testing.T) {
	semantic := []ReindexMigrationType{
		ReindexTypeChangeTokenization,
		ReindexTypeChangeTokenizationFilterable,
		ReindexTypeEnableFilterable,
		ReindexTypeEnableSearchable,
		ReindexTypeChangeAlgorithm,
		ReindexTypeEnableRangeable,
	}
	formatOnly := []ReindexMigrationType{
		ReindexTypeRebuildSearchable,
		ReindexTypeRepairFilterable,
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
// mapping. Format-only migrations MUST return nil here — they don't go
// through the swap barrier, so LocalCallbacksDone has nothing to check
// for them.
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
			name: "enable-rangeable → rangeable",
			mt:   ReindexTypeEnableRangeable,
			want: []string{"rangeable"},
		},
		{
			name: "repair-rangeable → empty (format-only, no schema flip)",
			mt:   ReindexTypeRepairRangeable,
			want: nil,
		},
		{
			name: "rebuild-searchable → empty (format-only)",
			mt:   ReindexTypeRebuildSearchable,
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

// A change-tokenization tracker dir found on disk must revive only its own
// sub-task, not its sibling.
func TestTasksOwningMigrationDir(t *testing.T) {
	rec := reindexRecoveryRecord{
		TaskID: "task-1", TaskVersion: 3, UnitID: "unit-0",
		Payload: ReindexTaskPayload{
			MigrationType:      ReindexTypeChangeTokenization,
			Collection:         "Articles",
			Properties:         []string{"text"},
			TargetTokenization: models.PropertyTokenizationLowercase,
			BucketStrategy:     lsmkv.StrategyMapCollection,
		},
	}
	logger, _ := test.NewNullLogger()
	built, err := buildRecoveryTasks(rec, "shard-0", 1, logger, nil)
	require.NoError(t, err)
	require.Len(t, built, 2, "change-tokenization has a searchable and a filterable sub-task")

	for _, dirName := range []string{
		MigrationDirPrefixFilterableRetokenize + "_text_1",
		MigrationDirPrefixSearchableRetokenize + "_text_1",
	} {
		t.Run(dirName, func(t *testing.T) {
			owned := tasksOwningMigrationDir(built, dirName, logger)
			require.Len(t, owned, 1, "only the sub-task whose tracker dir this is may be revived")
			require.Equal(t, dirName, owned[0].MigrationDirName())
		})
	}

	t.Run("unrecognized dir keeps every sub-task", func(t *testing.T) {
		require.Equal(t, built, tasksOwningMigrationDir(built, "something_else_1", logger))
	})
}

// Same rule from the startup entry point: with both sub-task trackers
// in flight, each dir must revive only its own sub-task.
func TestDiscoverInFlightReindexTasks_ChangeTokenizationRevivesOnlyOwnSubTask(t *testing.T) {
	rec := reindexRecoveryRecord{
		TaskID: "task-1", TaskVersion: 3, UnitID: "unit-0",
		Payload: ReindexTaskPayload{
			MigrationType:      ReindexTypeChangeTokenization,
			Collection:         "Articles",
			Properties:         []string{"text"},
			TargetTokenization: models.PropertyTokenizationLowercase,
			BucketStrategy:     lsmkv.StrategyMapCollection,
		},
	}
	payload, err := json.Marshal(rec)
	require.NoError(t, err)

	dirNames := []string{
		MigrationDirPrefixSearchableRetokenize + "_text_1",
		MigrationDirPrefixFilterableRetokenize + "_text_1",
	}

	rootPath := t.TempDir()
	migsDir := filepath.Join(rootPath, "articles", "shard-0", "lsm", ".migrations")
	for _, dirName := range dirNames {
		dir := filepath.Join(migsDir, dirName)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		// started + reindexed without tidied is the recovery window.
		for _, s := range []string{"started.mig", "reindexed.mig"} {
			require.NoError(t, os.WriteFile(filepath.Join(dir, s), []byte("x"), 0o644))
		}
		require.NoError(t, os.WriteFile(
			filepath.Join(dir, reindexRecoveryPayloadFile), payload, 0o644))
	}

	logger, _ := test.NewNullLogger()
	recovered, err := DiscoverInFlightReindexTasks(rootPath, logger, nil)
	require.NoError(t, err)
	require.Len(t, recovered, 2, "one entry per tracker dir on disk")

	var revived []string
	for _, rr := range recovered {
		for _, task := range rr.Tasks {
			revived = append(revived, task.MigrationDirName())
		}
	}
	require.ElementsMatch(t, dirNames, revived,
		"each tracker dir may revive only the sub-task that owns it")
}
