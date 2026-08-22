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
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
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
	// Without an identity the task's record key is incomplete and every
	// transition would refuse to write itself.
	task.setMigrationIdentity(
		distributedtask.TaskDescriptor{ID: "test-filterable-to-rangeable", Version: 1},
		"shard-1__node-0",
		&ReindexTaskPayload{MigrationType: ReindexTypeEnableRangeable},
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
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}
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
}
