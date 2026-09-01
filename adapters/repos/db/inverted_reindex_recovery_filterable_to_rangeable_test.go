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
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/filters"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// -----------------------------------------------------------------------------
// Recovery-convergence baseline for FilterableToRangeable
// -----------------------------------------------------------------------------
//
// Regression test for weaviate/0-weaviate-issues#246: a restart could
// strand a replica because its rangeable bucket — created by
// [FilterableToRangeableStrategy.PreReindexHook], not
// createPropertyValueIndex — was never reloaded into memory.
//
// Source data is int64 (rangeable applies only to numeric props); the
// fingerprint helper queries each known value via ReaderRoaringSetRange.Read
// with OperatorEqual.

// filterableToRangeablePropName is the numeric property name used by every
// case. Centralized so the cycling-value math (modulo arithmetic in
// makeFilterableToRangeableTestObjects) is in one place.
const filterableToRangeablePropName = "score"

// filterableToRangeableNumDistinctValues controls the modulus the cycling
// generator uses. Smaller than numObjects (25) so several docs share the
// same value — the fingerprint then verifies that the recovery code path
// produces the correct multi-doc posting list per value, which is the
// failure shape the #240-style divergence would land on.
const filterableToRangeableNumDistinctValues = 5

// makeFilterableToRangeableTestObjects builds a deterministic list of test
// objects with an int property cycling through a small set of distinct
// values. Sibling of makeConvergenceTestObjects (which generates text);
// numeric data is required because FilterableToRangeable only applies to
// int / number / date properties — the analyzer's HasRangeableIndex check
// rejects text props (inverted/objects.go:561).
//
// Each docID i gets value (i % filterableToRangeableNumDistinctValues),
// so for n=25 every distinct value gets 5 docs. This is what the
// fingerprint verifies post-recovery: value→sorted-docIDs equality.
func makeFilterableToRangeableTestObjects(t *testing.T, n int, className string) []*storobj.Object {
	t.Helper()
	out := make([]*storobj.Object, n)
	for i := 0; i < n; i++ {
		out[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
				Properties: map[string]interface{}{
					filterableToRangeablePropName: int64(i % filterableToRangeableNumDistinctValues),
				},
			},
		}
	}
	return out
}

// newFilterableToRangeableTestClass builds a class with a single numeric
// property in the pre-migration state: IndexFilterable defaults to true
// (filterable bucket exists), IndexRangeFilters is nil so HasRangeableIndex
// returns false (no rangeable bucket pre-migration). PreReindexHook will
// create the rangeable bucket; the backfill populates it.
//
// Mirrors newTestClassWithProps but for a numeric prop. We cannot reuse
// newTestClassWithProps directly because it hard-codes text/word and the
// rangeable strategy would reject the data type at write time.
func newFilterableToRangeableTestClass(className string) *models.Class {
	return &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexNullState:         true,
			IndexPropertyLength:    true,
			UsingBlockMaxWAND:      false,
		},
		Properties: []*models.Property{
			{
				Name:     filterableToRangeablePropName,
				DataType: schema.DataTypeInt.PropString(),
				// IndexFilterable nil → defaults to true (filterable
				// bucket gets created on shard init).
				// IndexRangeFilters nil → defaults to false (rangeable
				// bucket does NOT exist pre-migration — strategy's
				// PreReindexHook creates it).
			},
		},
	}
}

// filterableToRangeableFingerprint snapshots a RoaringSetRange bucket
// as (lex-key → sorted docIDs). Query-per-value (instead of cursor
// iteration) because RoaringSetRange exposes only Read on the public
// API. Key encoding matches WriteToReindexBucket's storage form so the
// comparison is bit-equality, not production-read-path equivalence.
func filterableToRangeableFingerprint(t *testing.T, b *lsmkv.Bucket) map[uint64][]uint64 {
	t.Helper()
	out := map[uint64][]uint64{}
	if b == nil {
		return out
	}
	require.Equal(t, lsmkv.StrategyRoaringSetRange, b.Strategy(),
		"fingerprint helper requires a RoaringSetRange bucket")
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()
	for v := int64(0); v < int64(filterableToRangeableNumDistinctValues); v++ {
		lex, err := entinverted.LexicographicallySortableInt64(v)
		require.NoError(t, err)
		key := binary.BigEndian.Uint64(lex)
		bm, release, err := reader.Read(context.Background(), key, filters.OperatorEqual)
		require.NoError(t, err)
		var ids []uint64
		if bm != nil {
			ids = bm.ToArray()
		}
		if release != nil {
			release()
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		out[key] = ids
	}
	return out
}

// newFilterableToRangeableTask wraps a FilterableToRangeableStrategy in
// the test infrastructure. Mirrors NewRuntimeFilterableToRangeableTask
// (the production constructor in inverted_reindexer_filterable_to_rangeable.go)
// but with two test-side adaptations:
//
//  1. schemaManager is nil — the test wrapper overrides OnMigrationComplete
//     so the schema-flag flip never runs, and the strategy doesn't touch
//     schemaManager outside that call.
//  2. The OnMigrationComplete observer is a flag setter, so the baseline
//     test can assert the hook fired without needing a real RAFT/schema
//     wire-up.
func newFilterableToRangeableTask(t *testing.T, idx *Index, className, propName string) (*ShardReindexTaskGeneric, *testFilterableToRangeableStrategyWrapper) {
	t.Helper()
	wrapped := &testFilterableToRangeableStrategyWrapper{
		FilterableToRangeableStrategy: FilterableToRangeableStrategy{
			schemaManager: nil, // OnMigrationComplete is overridden below
			propNames:     []string{propName},
			generation:    1,
		},
	}

	selectedProps := map[string]struct{}{propName: {}}
	cfg := reindexTaskConfig{
		concurrency:                   2,
		memtableOptFactor:             4,
		backupMemtableOptFactor:       1,
		processingDuration:            10 * time.Minute,
		pauseDuration:                 1 * time.Second,
		checkProcessingEveryNoObjects: 1000,

		selectionEnabled: true,
		selectedPropsByCollection: map[string]map[string]struct{}{
			className: selectedProps,
		},
		selectedShardsByCollection: map[string]map[string]struct{}{
			className: nil, // nil = all shards
		},
	}

	task := NewShardReindexTaskGeneric(
		"FilterableToRangeable", idx.logger, wrapped, cfg,
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
	return task, wrapped
}

// testFilterableToRangeableStrategyWrapper overrides OnMigrationComplete
// with a flag-setter so the test can assert the hook fired without
// needing a real schema manager. Mirrors testMigrationStrategy and
// testFilterableRetokenizeStrategyWrapper. The wrapper also intentionally
// avoids the setRangeableLocallyReady side effect that the production
// hook does; that flag is a query-path optimization, not a correctness
// invariant for the bucket-content fingerprint we're testing.
//
// preReindexHookCount counts every PreReindexHook fire.
type testFilterableToRangeableStrategyWrapper struct {
	FilterableToRangeableStrategy
	migrationCompleted  bool
	preReindexHookCount int
}

func (s *testFilterableToRangeableStrategyWrapper) OnMigrationComplete(_ context.Context, _ ShardLike) error {
	s.migrationCompleted = true
	return nil
}

func (s *testFilterableToRangeableStrategyWrapper) PreReindexHook(shard *Shard, props []string) {
	s.preReindexHookCount++
	s.FilterableToRangeableStrategy.PreReindexHook(shard, props)
}

// TestRecoveryConvergence_FilterableToRangeable_Baseline drives the
// strategy from no rangeable bucket to a fully populated one.
func TestRecoveryConvergence_FilterableToRangeable_Baseline(t *testing.T) {
	const numObjects = 25
	propName := filterableToRangeablePropName

	ctx := testCtx()
	className := "FilterToRangeBaseline_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	// Pre-migration: filterable bucket exists (IndexFilterable defaults
	// to true), rangeable bucket does NOT yet exist
	// (IndexRangeFilters=nil → false).
	filtBucketName := helpers.BucketFromPropNameLSM(propName)
	require.NotNil(t, shard.store.Bucket(filtBucketName),
		"pre-migration filterable bucket must exist (defaults to true for int prop)")
	rangeBucketName := helpers.BucketRangeableFromPropNameLSM(propName)
	require.Nil(t, shard.store.Bucket(rangeBucketName),
		"pre-migration rangeable bucket must NOT exist (IndexRangeFilters defaults to false)")

	task, wrapped := newFilterableToRangeableTask(t, idx, className, propName)
	require.NoError(t, task.RunOnShard(ctx, shard))
	require.True(t, wrapped.migrationCompleted,
		"OnMigrationComplete must fire post-migration")

	// Post-migration: rangeable bucket exists and holds the full posting
	// set, one term per distinct value with cardinality numObjects /
	// filterableToRangeableNumDistinctValues.
	postBucket := shard.store.Bucket(rangeBucketName)
	require.NotNil(t, postBucket, "post-migration rangeable bucket must exist")
	require.Equal(t, lsmkv.StrategyRoaringSetRange, postBucket.Strategy(),
		"post-migration rangeable bucket must be StrategyRoaringSetRange")

	fp := filterableToRangeableFingerprint(t, postBucket)
	require.Lenf(t, fp, filterableToRangeableNumDistinctValues,
		"post-migration rangeable bucket should have %d distinct terms",
		filterableToRangeableNumDistinctValues)
	expectedPerValue := numObjects / filterableToRangeableNumDistinctValues
	for term, ids := range fp {
		require.Lenf(t, ids, expectedPerValue,
			"term %d should have %d docIDs, got %d", term, expectedPerValue, len(ids))
	}

	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	require.True(t, rt.IsReindexed())
	require.True(t, rt.IsPrepended())
	require.True(t, rt.IsMerged())
	require.True(t, rt.IsSwapped())
	require.True(t, rt.IsTidied())
}

// computeFilterableToRangeableBaseline runs a clean migration on a
// throw-away shard and returns its post-migration rangeable fingerprint.
// The matrix cells below compare against this as ground truth.
func computeFilterableToRangeableBaseline(t *testing.T, propName string, numObjects int) map[uint64][]uint64 {
	t.Helper()
	ctx := testCtx()
	className := "FilterToRangeBaselineRef_" + uuid.NewString()[:8]
	class := newFilterableToRangeableTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	task, _ := newFilterableToRangeableTask(t, idx, className, propName)
	require.NoError(t, task.RunOnShard(ctx, shard))

	return filterableToRangeableFingerprint(t,
		shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName)))
}

// TestRecoveryConvergence_FilterableToRangeable_FromEachState pins the
// same invariant the RebuildSearchable matrix pins, for the rangeable
// family (enable-rangeable and repair-rangeable both run through
// FilterableToRangeableStrategy): from any on-disk state a replica can
// land in after a mid-migration restart, one relaunch drives the
// migration to the terminal state and the rangeable bucket content
// matches a clean baseline run.
//
// Five sentinel states, reached through the production entry points
// except for the two states that live inside an atomic method
// (IsPrepended, IsSwapped), which are synthesized by removing the later
// sentinel file.
func TestRecoveryConvergence_FilterableToRangeable_FromEachState(t *testing.T) {
	const numObjects = 25
	propName := filterableToRangeablePropName

	baseline := computeFilterableToRangeableBaseline(t, propName, numObjects)
	require.NotEmpty(t, baseline, "baseline fingerprint must be non-empty")

	cases := []recoveryConvergenceCase{
		{
			name: "FilterableToRangeable_IsReindexed_via_RunReindexOnlyOnShard",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": false, "merged": false, "swapped": false, "tidied": false,
			},
		},
		{
			name: "FilterableToRangeable_IsPrepended_synthetic_merged_removed",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
				rt, err := task.newReindexTracker(shard.pathLSM())
				require.NoError(t, err)
				ftr := rt.(*fileReindexTracker)
				require.NoError(t, os.Remove(
					filepath.Join(ftr.config.migrationPath, ftr.config.filenameMerged)))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": false, "swapped": false, "tidied": false,
			},
		},
		{
			name: "FilterableToRangeable_IsSwapped_synthetic_tidied_removed",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.RunOnShard(ctx, shard))
				rt, err := task.newReindexTracker(shard.pathLSM())
				require.NoError(t, err)
				ftr := rt.(*fileReindexTracker)
				require.NoError(t, os.Remove(
					filepath.Join(ftr.config.migrationPath, ftr.config.filenameTidied)))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": true, "swapped": true, "tidied": false,
			},
		},
		{
			name: "FilterableToRangeable_IsMerged_via_RunPrepareOnShard",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": true, "swapped": false, "tidied": false,
			},
		},
		{
			name: "FilterableToRangeable_IsTidied_via_RunOnShard",
			driveToState: func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
				require.NoError(t, task.RunOnShard(ctx, shard))
			},
			expectedPostStateSentinels: map[string]bool{
				"reindexed": true, "prepended": true, "merged": true, "swapped": true, "tidied": true,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "FilterToRangeCase_" + uuid.NewString()[:8]
			class := newFilterableToRangeableTestClass(className)

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeFilterableToRangeableTestObjects(t, numObjects, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			task, _ := newFilterableToRangeableTask(t, idx, className, propName)
			tc.driveToState(t, ctx, shard, task)

			// Verify driveToState actually landed at the intended on-disk
			// state. Without this guard a buggy driveToState would let
			// recovery from a different state appear to "converge".
			rt, err := task.newReindexTracker(shard.pathLSM())
			require.NoError(t, err)
			got := map[string]bool{
				"reindexed": rt.IsReindexed(),
				"prepended": rt.IsPrepended(),
				"merged":    rt.IsMerged(),
				"swapped":   rt.IsSwapped(),
				"tidied":    rt.IsTidied(),
			}
			for name, want := range tc.expectedPostStateSentinels {
				assert.Equalf(t, want, got[name], "after driveToState, sentinel %q (case %q)", name, tc.name)
			}

			// Simulated restart: graceful shutdown, fresh task, then
			// idx.initShard re-runs FinalizeCompletedMigrations → LSM
			// init → OnAfterLsmInit.
			shardName := shard.Name()
			require.NoError(t, shard.Shutdown(ctx))

			task2, wrapped2 := newFilterableToRangeableTask(t, idx, className, propName)
			idx.shardReindexer = &testShardReindexer{task: task2}

			shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
			require.NoErrorf(t, err, "shard re-init must succeed (case %q)", tc.name)
			shard2 := shd2.(*Shard)
			defer shard2.Shutdown(ctx)
			idx.shards.Store(shardName, shd2)

			// Relaunch the task the way the provider does after a
			// restart: one RunOnShard drives whatever the previous run
			// left unfinished through to the terminal state.
			require.NoErrorf(t, task2.RunOnShard(ctx, shard2),
				"recovery RunOnShard must not error (case %q)", tc.name)

			rt2, err := task2.newReindexTracker(shard2.pathLSM())
			require.NoErrorf(t, err, "post-recovery tracker init (case %q)", tc.name)
			require.Truef(t, rt2.IsTidied(),
				"recovery must reach the terminal tidied state (case %q)", tc.name)
			require.Truef(t, wrapped2.migrationCompleted,
				"recovery must run OnMigrationComplete (case %q)", tc.name)

			bucket := shard2.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
			require.NotNilf(t, bucket, "post-recovery rangeable bucket must exist (case %q)", tc.name)

			gotFP := filterableToRangeableFingerprint(t, bucket)
			assert.Equalf(t, len(baseline), len(gotFP),
				"post-recovery rangeable term count diverges from baseline (case %q)", tc.name)
			for term, expectedIDs := range baseline {
				gotIDs, ok := gotFP[term]
				if !ok {
					assert.Failf(t, "missing term",
						"term %d present in baseline but missing post-recovery (case %q)", term, tc.name)
					continue
				}
				assert.Equalf(t, expectedIDs, gotIDs,
					"term %d post-recovery doc-id list diverges from baseline (case %q)", term, tc.name)
			}
		})
	}
}
