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
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	entschema "github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
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
		// unlistable makes .migrations a file instead of a directory, so
		// listing it fails without the dir being absent. Only meaningful
		// with trackers empty (nothing a file substitution could hide).
		unlistable bool
		// classLevel uses change-algorithm's scope: one tracker dir the whole
		// collection shares, with no property in its name.
		classLevel bool
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
		// they name exactly one property — see [migrationDirScope.inScope].)
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
		// A dir from before [genSuffix]: the sweep deletes it, so the recovery
		// probe must see it too. The class-level blockmax tracker shipped before
		// generations existed, so it is the shape a real disk can still hold.
		{
			name: "a generation-less tracker, started only → recovery NEEDED",
			trackers: map[string][]string{
				MigrationDirSearchableMapToBlockmax: {"started.mig"},
			},
			classLevel: true,
			want:       true,
		},
		{
			name: "a class-level tracker, started only → recovery NEEDED",
			trackers: map[string][]string{
				MigrationDirSearchableMapToBlockmax + "_1": {"started.mig"},
			},
			classLevel: true,
			want:       true,
		},
		{
			name: "a class-level tracker with tidied.mig → completed, no recovery",
			trackers: map[string][]string{
				MigrationDirSearchableMapToBlockmax + "_1": {"started.mig", "tidied.mig"},
			},
			classLevel: true,
			want:       false,
		},
		// A dir with no sentinels: the probe cannot tell "never started" from
		// an interrupted swap, and reports recovery either way.
		{
			name: "a class-level tracker with no sentinels at all → recovery NEEDED",
			trackers: map[string][]string{
				MigrationDirSearchableMapToBlockmax + "_1": {},
			},
			classLevel: true,
			want:       true,
		},
		{
			name: "a per-property tracker is not the class-level scope's → no recovery",
			trackers: map[string][]string{
				"enable_searchable_text_1": {"started.mig"},
			},
			classLevel: true,
			want:       false,
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
			if tc.unlistable {
				// See unlistable above for why a file, not chmod.
				require.NoError(t,
					os.WriteFile(filepath.Join(tmp, ".migrations"), []byte("x"), 0o644))
			} else if tc.trackers != nil {
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
			}
			indexType := tc.indexType
			if indexType == "" {
				indexType = "searchable"
			}
			scope := migrationDirsOf(tmp, nil, "text", indexType)
			if tc.classLevel {
				scope = classLevelMigrationDirsOf(tmp, MigrationDirSearchableMapToBlockmax)
			}
			require.Equal(t, tc.want, hasUntidiedTracker(scope))
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
		ReindexTypeEnableRangeable,
		ReindexTypeChangeAlgorithm,
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
// mapping. Format-only migrations (repair-*, rebuild-*) MUST return nil
// here — they don't go through the swap barrier, so LocalCallbacksDone has
// nothing to check for them.
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
			name: "repair-rangeable → empty (format-only)",
			mt:   ReindexTypeRepairRangeable,
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

// Pins that LocalCallbacksDone, firing at bootstrap with every tenant cold,
// reads the tracker dir at a path it can join without loading the shard.
func TestLocalCallbacksDoneLeavesUnloadedShardsAlone(t *testing.T) {
	const (
		prop   = "title"
		tenant = "cold-tenant"
		node   = "n1"
	)

	for _, tc := range []struct {
		name string
		// sentinels are written into the tenant's tracker dir; nil writes
		// no tracker dir at all.
		sentinels []string
		// hostedElsewhere maps the unit to a node that is not this one.
		hostedElsewhere bool
		// changeAlgorithm runs the migration whose tracker is class-level, so
		// the probe has to look somewhere the per-property scope never does.
		changeAlgorithm bool
		// absentFromShardMap leaves the shard out of this node's map while
		// the payload still assigns its unit here.
		absentFromShardMap bool
		want               bool
	}{
		{
			name:      "a cold tenant whose swap started and never committed",
			sentinels: []string{"started.mig"},
			want:      false,
		},
		{
			name:      "a cold tenant whose swap tidied",
			sentinels: []string{"started.mig", "tidied.mig"},
			want:      true,
		},
		{
			name: "a cold tenant carrying nothing",
			want: true,
		},
		{
			name:            "an interrupted swap on another node's unit",
			sentinels:       []string{"started.mig"},
			hostedElsewhere: true,
			want:            true,
		},
		{
			name:            "a cold tenant whose class-level blockmax swap started and never committed",
			sentinels:       []string{"started.mig"},
			changeAlgorithm: true,
			want:            false,
		},
		{
			name:            "a cold tenant whose class-level blockmax swap tidied",
			sentinels:       []string{"started.mig", "tidied.mig"},
			changeAlgorithm: true,
			want:            true,
		},
		// The unit is assigned here, so the payload's node filter passes and
		// the empty-set early return does not fire. Membership in this node's
		// shard map is what has to reject it.
		{
			name:               "a unit assigned to this node whose shard the map does not hold",
			sentinels:          []string{"started.mig"},
			absentFromShardMap: true,
			want:               true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "LocalCallbacksDone_" + uuid.NewString()[:8]
			class := newTestClassWithProps(className, []string{prop})
			hot, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			defer hot.Shutdown(context.Background())

			migrationType := ReindexTypeChangeTokenization
			trackerDir := postMergeTrackerDir(t, prop)
			if tc.changeAlgorithm {
				migrationType = ReindexTypeChangeAlgorithm
				trackerDir = MigrationDirSearchableMapToBlockmax + "_1"
			}
			if tc.sentinels != nil {
				mkTrackerDir(t, shardPathLSM(idx.path(), tenant), trackerDir, tc.sentinels...)
			}
			cold := NewLazyLoadShard(ctx, nil, tenant, idx, class, idx.centralJobQueue,
				idx.indexCheckpoints, idx.allocChecker, idx.shardLoadLimiter, idx.shardReindexer,
				false, idx.bitmapBufPool)
			if !tc.absentFromShardMap {
				idx.shards.Store(tenant, cold)
			}
			defer func() {
				if cold.isLoaded() {
					require.NoError(t, cold.Shutdown(context.Background()))
				}
			}()

			owner := node
			if tc.hostedElsewhere {
				owner = "n2"
			}
			payload, err := json.Marshal(ReindexTaskPayload{
				Collection:    className,
				MigrationType: migrationType,
				Properties:    []string{prop},
				UnitToShard:   map[string]string{"u1": tenant},
				UnitToNode:    map[string]string{"u1": owner},
			})
			require.NoError(t, err)

			logger, _ := logrustest.NewNullLogger()
			p := NewReindexProvider(
				&DB{indices: map[string]*Index{indexID(entschema.ClassName(className)): idx}},
				nil, nil, logger, node, nil, ctx)

			got := p.LocalCallbacksDone(&distributedtask.Task{
				Namespace:      ReindexNamespace,
				TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_bootstrap", Version: 1},
				Status:         distributedtask.TaskStatusSwapping,
				Payload:        payload,
			}, node)

			require.Equal(t, tc.want, got)
			require.False(t, cold.isLoaded(),
				"the bootstrap check reads a tracker dir at a path this node joins itself; "+
					"loading a tenant to ask it for that path is what startup cannot afford")
		})
	}
}

// A tracker dir whose name already excludes "category" can't have that
// decided by a corrupt payload next to it — the name settles it first.
func TestLocalCallbacksDoneOnACorruptPayloadUnderAnotherPropertysTracker(t *testing.T) {
	ctx := testCtx()
	className := "OtherPropTracker_" + uuid.NewString()[:8]
	shard, idx := testShard(t, ctx, className)
	concrete, err := unwrapShard(ctx, shard)
	require.NoError(t, err)

	const tracker = MigrationDirPrefixEnableFilterable + "_other_1"
	mkTrackerDir(t, concrete.pathLSM(), tracker)
	require.NoError(t, os.WriteFile(
		filepath.Join(concrete.pathLSM(), ".migrations", tracker, reindexRecoveryPayloadFile),
		[]byte("not a recovery record"), 0o644))

	payload, err := json.Marshal(ReindexTaskPayload{
		Collection:    className,
		MigrationType: ReindexTypeEnableFilterable,
		Properties:    []string{"category"},
		UnitToShard:   map[string]string{"u1": shard.Name()},
		UnitToNode:    map[string]string{"u1": "n1"},
	})
	require.NoError(t, err)

	logger, _ := logrustest.NewNullLogger()
	p := NewReindexProvider(
		&DB{indices: map[string]*Index{indexID(entschema.ClassName(className)): idx}},
		nil, nil, logger, "n1", nil, ctx)

	require.True(t, p.LocalCallbacksDone(&distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: "T_other_prop", Version: 1},
		Status:         distributedtask.TaskStatusFinished,
		Payload:        payload,
	}, "n1"))
}

// Pins the bootstrap probe's cost per shard: every tuple it walks asks the
// same tracker dirs whose properties they name, and answering that from disk
// is a full payload.mig parse — 126ms on an 8 MB payload.
func TestLocalCallbacksDoneReadsAShardsPayloadOnce(t *testing.T) {
	lsm := t.TempDir()
	// A two-property name no shortcut can settle, so every tuple pays for the
	// payload. Tidied, so the walk runs past it instead of stopping there.
	const tracker = MigrationDirPrefixEnableFilterable + "_alpha_beta_1"
	mkTrackerDir(t, lsm, tracker, "started.mig", "tidied.mig")
	mkRecoveryPayload(t, lsm, tracker, "alpha", "beta")

	payload := &ReindexTaskPayload{
		MigrationType: ReindexTypeEnableFilterable,
		Properties:    []string{"alpha", "beta"},
	}
	indexTypes := semanticMigrationIndexTypes(payload.MigrationType)
	require.Equal(t, []string{"filterable"}, indexTypes)

	// A memo per tuple is what the probe cost before they shared one.
	var perTuple int
	for _, propName := range payload.Properties {
		own := &taskPropsCache{}
		hasUntidiedTracker(migrationDirsOf(lsm, nil, propName, indexTypes[0]).cachingProps(own))
		perTuple += own.count()
	}
	require.Equal(t, 2, perTuple, "a memo per tuple reads the same payload once each")

	shared := &taskPropsCache{}
	require.False(t, shardHasUntidiedTracker(lsm, payload, indexTypes, shared))
	require.Equal(t, 1, shared.count(), "one memo per shard covers the whole walk")
}
