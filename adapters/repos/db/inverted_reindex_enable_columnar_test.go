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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// -----------------------------------------------------------------------------
// enable-columnar backfill migration — end-to-end at the shard level
// -----------------------------------------------------------------------------
//
// Mirrors the FilterableToRangeable baseline test shape (see
// inverted_reindex_recovery_filterable_to_rangeable_test.go): create a class
// with an int property WITHOUT indexColumnar, import N objects, run the
// enable-columnar migration through the production task lifecycle
// (OnAfterLsmInit + OnAfterLsmInitAsync loop), and assert the columnar
// bucket serves all N values.
//
// The schema-flag flip (IndexColumnar=true via the RAFT fieldmask
// api.PropertyFieldIndexColumnar) is NOT exercised at this level — same as
// the rangeable tests, the wrapper overrides OnMigrationComplete because a
// real schema.Manager/RAFT wire-up is out of scope for a shard-level test.
// The wrapper preserves the production hook's setColumnarLocallyReady side
// effect so the read-gating contract is still pinned.

const (
	enableColumnarPropName          = "score"
	enableColumnarNumDistinctValues = 5
)

// makeEnableColumnarTestObjects builds deterministic objects with an int
// property cycling through enableColumnarNumDistinctValues distinct values,
// so for n=25 every distinct value gets 5 docs.
func makeEnableColumnarTestObjects(t *testing.T, n int, className string) []*storobj.Object {
	t.Helper()
	out := make([]*storobj.Object, n)
	for i := 0; i < n; i++ {
		out[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
				Properties: map[string]interface{}{
					enableColumnarPropName: int64(i % enableColumnarNumDistinctValues),
				},
			},
		}
	}
	return out
}

// newEnableColumnarTestClass builds a class with a single int property in
// the pre-migration state: IndexColumnar is nil so HasColumnarIndex returns
// false (no columnar bucket pre-migration). The strategy's PreReindexHook
// creates the bucket; the backfill populates it.
func newEnableColumnarTestClass(className string) *models.Class {
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
				Name:     enableColumnarPropName,
				DataType: schema.DataTypeInt.PropString(),
				// IndexFilterable nil → defaults to true (filterable bucket
				// exists — needed by the filtered-aggregation fallback path).
				// IndexColumnar nil → false (columnar bucket does NOT exist
				// pre-migration).
			},
		},
	}
}

// enableColumnarFingerprint snapshots a columnar bucket as docID → int64
// value via ColumnarScan over column 0.
func enableColumnarFingerprint(t *testing.T, b *lsmkv.Bucket) map[uint64]int64 {
	t.Helper()
	out := map[uint64]int64{}
	if b == nil {
		return out
	}
	require.Equal(t, lsmkv.StrategyColumnar, b.Strategy(),
		"fingerprint helper requires a Columnar bucket")
	require.NoError(t, b.ColumnarScan(0, nil, func(docID uint64, bits uint64) bool {
		out[docID] = int64(bits)
		return true
	}))
	return out
}

// testEnableColumnarStrategyWrapper overrides OnMigrationComplete with a
// flag-setter so the test can assert the hook fired without a real schema
// manager. Unlike the rangeable test wrapper it KEEPS the production
// setColumnarLocallyReady side effect — the locally-ready lifecycle is part
// of what this file pins.
type testEnableColumnarStrategyWrapper struct {
	EnableColumnarStrategy
	migrationCompleted  bool
	preReindexHookCount int
}

func (s *testEnableColumnarStrategyWrapper) OnMigrationComplete(ctx context.Context, shard ShardLike) error {
	// Mirror the production hook's ordering contract: locally-ready flips
	// BEFORE the (skipped) schema update.
	if concrete, err := unwrapShard(ctx, shard); err == nil && concrete != nil {
		for _, propName := range s.propNames {
			concrete.setColumnarLocallyReady(propName, true)
		}
	}
	s.migrationCompleted = true
	return nil
}

func (s *testEnableColumnarStrategyWrapper) PreReindexHook(shard *Shard, props []string) {
	s.preReindexHookCount++
	s.EnableColumnarStrategy.PreReindexHook(shard, props)
}

// newEnableColumnarTask wraps an EnableColumnarStrategy in the test
// infrastructure. Mirrors NewRuntimeEnableColumnarTask (the production
// constructor) but with a nil schemaManager + overridden OnMigrationComplete.
func newEnableColumnarTask(t *testing.T, idx *Index, className, propName string) (*ShardReindexTaskGeneric, *testEnableColumnarStrategyWrapper) {
	t.Helper()
	wrapped := &testEnableColumnarStrategyWrapper{
		EnableColumnarStrategy: EnableColumnarStrategy{
			schemaManager: nil, // OnMigrationComplete is overridden above
			propNames:     []string{propName},
			generation:    1,
		},
	}

	cfg := reindexTaskConfig{
		swapBuckets:                   true,
		tidyBuckets:                   true,
		concurrency:                   2,
		memtableOptFactor:             4,
		backupMemtableOptFactor:       1,
		processingDuration:            10 * time.Minute,
		pauseDuration:                 1 * time.Second,
		checkProcessingEveryNoObjects: 1000,

		selectionEnabled: true,
		selectedPropsByCollection: map[string]map[string]struct{}{
			className: {propName: {}},
		},
		selectedShardsByCollection: map[string]map[string]struct{}{
			className: nil, // nil = all shards
		},
	}

	task := NewShardReindexTaskGeneric(
		"EnableColumnar", idx.logger, wrapped, cfg,
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
	return task, wrapped
}

// runEnableColumnarTask drives a task through the production inline
// (non-semantic) lifecycle to completion.
func runEnableColumnarTask(t *testing.T, ctx context.Context, task *ShardReindexTaskGeneric, shard *Shard) {
	t.Helper()
	require.NoError(t, task.OnAfterLsmInit(ctx, shard))
	for {
		rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
		require.NoError(t, err)
		if rerunAt.IsZero() {
			break
		}
	}
}

// TestEnableColumnar_BackfillBaseline pins the core contract: enabling
// columnar on a property with existing objects backfills the columnar
// bucket with every object's value via the property-reindex framework.
func TestEnableColumnar_BackfillBaseline(t *testing.T) {
	const numObjects = 25
	propName := enableColumnarPropName

	ctx := testCtx()
	className := "EnableColumnarBaseline_" + uuid.NewString()[:8]
	class := newEnableColumnarTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeEnableColumnarTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	// Pre-migration: no columnar bucket, not locally ready (bucket-missing
	// default).
	bucketName := helpers.BucketColumnarFromPropNameLSM(propName)
	require.Nil(t, shard.store.Bucket(bucketName),
		"pre-migration columnar bucket must NOT exist (IndexColumnar defaults to false)")
	require.False(t, shard.IsColumnarLocallyReady(propName),
		"pre-migration: not ready (no bucket)")

	task, wrapped := newEnableColumnarTask(t, idx, className, propName)
	runEnableColumnarTask(t, ctx, task, shard)

	require.True(t, wrapped.migrationCompleted, "OnMigrationComplete must fire post-migration")
	require.GreaterOrEqual(t, wrapped.preReindexHookCount, 1, "PreReindexHook must fire")
	require.True(t, shard.IsColumnarLocallyReady(propName),
		"post-migration: locally ready")

	// Post-migration: columnar bucket exists and serves all N values.
	postBucket := shard.store.Bucket(bucketName)
	require.NotNil(t, postBucket, "post-migration columnar bucket must exist")
	require.Equal(t, lsmkv.StrategyColumnar, postBucket.Strategy())

	fp := enableColumnarFingerprint(t, postBucket)
	require.Len(t, fp, numObjects, "columnar bucket must hold one row per object")
	countsPerValue := map[int64]int{}
	for docID, v := range fp {
		require.GreaterOrEqualf(t, v, int64(0), "docID %d value out of range", docID)
		require.Lessf(t, v, int64(enableColumnarNumDistinctValues), "docID %d value out of range", docID)
		countsPerValue[v]++
		// Cross-check the point-lookup path against the scan.
		got, ok := postBucket.ColumnarLookupInt64(docID, 0)
		require.Truef(t, ok, "point lookup for docID %d must succeed", docID)
		require.Equalf(t, v, got, "point lookup for docID %d diverges from scan", docID)
	}
	expectedPerValue := numObjects / enableColumnarNumDistinctValues
	for v, n := range countsPerValue {
		require.Equalf(t, expectedPerValue, n, "value %d should back %d docs", v, expectedPerValue)
	}

	// Full lifecycle sentinels present.
	rt, err := task.newReindexTracker(shard.pathLSM())
	require.NoError(t, err)
	require.True(t, rt.IsReindexed())
	require.True(t, rt.IsPrepended())
	require.True(t, rt.IsMerged())
	require.True(t, rt.IsSwapped())
	require.True(t, rt.IsTidied())
}

// TestEnableColumnar_AggregationGatedDuringMigrationWindow pins the
// silent-data-loss guard: during the window where the cluster-wide
// IndexColumnar flag is already true but THIS shard's swap hasn't landed,
// the columnar bucket exists but is empty/partial — aggregations MUST fall
// back to the object path instead of silently returning partial results.
func TestEnableColumnar_AggregationGatedDuringMigrationWindow(t *testing.T) {
	const numObjects = 25
	propName := enableColumnarPropName

	ctx := testCtx()
	className := "EnableColumnarGating_" + uuid.NewString()[:8]
	class := newEnableColumnarTestClass(className)

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(ctx)

	for _, obj := range makeEnableColumnarTestObjects(t, numObjects, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}

	// Drive the migration up to (but not including) the swap. The
	// PreReindexHook has created an empty main columnar bucket and marked
	// the prop not-locally-ready; the backfill landed only in the
	// __columnar_reindex sidecar.
	task, _ := newEnableColumnarTask(t, idx, className, propName)
	task.skipSwapOnFinish.Store(true)
	runEnableColumnarTask(t, ctx, task, shard)

	bucketName := helpers.BucketColumnarFromPropNameLSM(propName)
	require.NotNil(t, shard.store.Bucket(bucketName),
		"mid-migration: PreReindexHook must have created the (empty) main columnar bucket")
	require.False(t, shard.IsColumnarLocallyReady(propName),
		"mid-migration: NOT locally ready even though the bucket exists")

	// Simulate the cluster-wide schema flip arriving from another replica
	// that already completed its swap: flip IndexColumnar=true on the live
	// class. The fake schema getter shares this pointer, so the aggregator
	// now sees a columnar-flagged property whose local bucket is empty.
	trueVal := true
	class.Properties[0].IndexColumnar = &trueVal

	aggregate := func() *aggregation.Result {
		res, err := shard.Aggregate(ctx, aggregation.Params{
			ClassName:        schema.ClassName(className),
			IncludeMetaCount: true,
			Filters: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorGreaterThanEqual,
					Value:    &filters.Value{Type: schema.DataTypeInt, Value: 0},
					On:       &filters.Path{Class: schema.ClassName(className), Property: schema.PropertyName(propName)},
				},
			},
			Properties: []aggregation.ParamProperty{
				{Name: schema.PropertyName(propName), Aggregators: []aggregation.Aggregator{
					aggregation.CountAggregator,
					aggregation.SumAggregator,
					aggregation.MeanAggregator,
				}},
			},
		}, nil)
		require.NoError(t, err)
		require.Len(t, res.Groups, 1)
		return res
	}

	assertCorrect := func(res *aggregation.Result, phase string) {
		group := res.Groups[0]
		aggs := group.Properties[propName].NumericalAggregations
		require.NotNilf(t, aggs, "%s: numerical aggregations missing", phase)
		// values 0..4, 5 docs each: count=25, sum=5*(0+1+2+3+4)=50, mean=2.0
		assert.Equalf(t, float64(numObjects), aggs["count"], "%s: count", phase)
		assert.Equalf(t, float64(50), aggs["sum"], "%s: sum", phase)
		assert.Equalf(t, float64(2), aggs["mean"], "%s: mean", phase)
	}

	// Mid-migration: without the IsColumnarLocallyReady gate in
	// columnarBucketFor, this would aggregate over the EMPTY main bucket
	// and report count=0 — the silent partial-result failure mode.
	assertCorrect(aggregate(), "mid-migration (gated, object-path fallback)")

	// Persist the recovery payload exactly as the production
	// ReindexProvider does before iteration starts — the shard-init
	// pessimistic scan reads the property names from payload.mig.
	payloadBytes, err := json.Marshal(reindexRecoveryRecord{
		TaskID: "test-enable-columnar",
		UnitID: shard.Name() + "__node1",
		Payload: ReindexTaskPayload{
			MigrationType: ReindexTypeEnableColumnar,
			Collection:    className,
			Properties:    []string{propName},
		},
	})
	require.NoError(t, err)
	require.NoError(t, task.SaveRecoveryPayload(shard.pathLSM(), payloadBytes))

	// Pin the shard-init pessimistic scan in isolation: simulate a fresh
	// process by clearing the in-memory ready map (it is in-memory only,
	// so a real restart starts empty), confirm the dangerous default
	// (bucket exists → true), then run the scan and assert it restores
	// the explicit false from the on-disk in-flight tracker + payload.mig.
	shard.columnarLocalReadyMu.Lock()
	shard.columnarLocalReady = nil
	shard.columnarLocalReadyMu.Unlock()
	require.True(t, shard.IsColumnarLocallyReady(propName),
		"sanity: with the in-memory map cleared, bucket existence defaults to ready")
	markInFlightLocalReadyMigrationsNotReady(shard)
	require.False(t, shard.IsColumnarLocallyReady(propName),
		"pessimistic init scan must flag the in-flight enable-columnar migration")

	// Complete the migration via the recovery path: simulated restart
	// (graceful shutdown, fresh task, idx.initShard re-runs
	// FinalizeCompletedMigrations → OnBeforeLsmInit → LSM init →
	// OnAfterLsmInit) — the same restart primitive the rangeable
	// convergence matrix uses.
	shardName := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))

	task2, wrapped2 := newEnableColumnarTask(t, idx, className, propName)
	idx.shardReindexer = &testShardReindexer{task: task2}

	shd2, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err, "shard re-init must succeed")
	shard2 := shd2.(*Shard)
	defer shard2.Shutdown(ctx)
	idx.shards.Store(shardName, shd2)

	// Drive the recovery to completion (inline runtimeSwap path). Note:
	// initShard already ran one async pass via testShardReindexer, so the
	// migration may already be complete when this loop starts.
	for {
		rerunAt, _, err := task2.OnAfterLsmInitAsync(ctx, shard2)
		require.NoError(t, err, "recovery OnAfterLsmInitAsync must not error")
		if rerunAt.IsZero() {
			break
		}
	}
	require.True(t, wrapped2.migrationCompleted, "recovery run must complete the migration")
	require.True(t, shard2.IsColumnarLocallyReady(propName), "post-migration: locally ready")

	// Now the columnar bucket holds all rows and serves the aggregation.
	fp := enableColumnarFingerprint(t, shard2.store.Bucket(bucketName))
	require.Len(t, fp, numObjects, "post-migration columnar bucket must hold every row")

	aggregate2 := func() *aggregation.Result {
		res, err := shard2.Aggregate(ctx, aggregation.Params{
			ClassName:        schema.ClassName(className),
			IncludeMetaCount: true,
			Filters: &filters.LocalFilter{
				Root: &filters.Clause{
					Operator: filters.OperatorGreaterThanEqual,
					Value:    &filters.Value{Type: schema.DataTypeInt, Value: 0},
					On:       &filters.Path{Class: schema.ClassName(className), Property: schema.PropertyName(propName)},
				},
			},
			Properties: []aggregation.ParamProperty{
				{Name: schema.PropertyName(propName), Aggregators: []aggregation.Aggregator{
					aggregation.CountAggregator,
					aggregation.SumAggregator,
					aggregation.MeanAggregator,
				}},
			},
		}, nil)
		require.NoError(t, err)
		require.Len(t, res.Groups, 1)
		return res
	}
	assertCorrect(aggregate2(), "post-migration (columnar fast path)")
}

// TestBucketTouchPredicates_CoverEveryMigrationType pins that every known
// ReindexMigrationType is enumerated in the TouchesSearchable /
// TouchesFilterable exhaustive switches — a missing type panics on the
// first submit that exercises it (typesConflictReason calls both as a
// sanity check). Found the hard way: ReindexTypeRebuildSearchable was
// missing from both switches, so submitting {"searchable":{"rebuild":true}}
// panicked at the conflict-check boundary; enable-columnar is the newest
// addition this guards.
func TestBucketTouchPredicates_CoverEveryMigrationType(t *testing.T) {
	allTypes := []ReindexMigrationType{
		ReindexTypeChangeAlgorithm,
		ReindexTypeRebuildSearchable,
		ReindexTypeRepairFilterable,
		ReindexTypeEnableRangeable,
		ReindexTypeRepairRangeable,
		ReindexTypeEnableFilterable,
		ReindexTypeEnableSearchable,
		ReindexTypeChangeTokenization,
		ReindexTypeChangeTokenizationFilterable,
		ReindexTypeEnableColumnar,
	}
	for _, mt := range allTypes {
		t.Run(string(mt), func(t *testing.T) {
			assert.NotPanics(t, func() { TouchesSearchable(mt) }, "TouchesSearchable must enumerate %s", mt)
			assert.NotPanics(t, func() { TouchesFilterable(mt) }, "TouchesFilterable must enumerate %s", mt)
		})
	}
}

// TestGetPropNameAndIndexTypeFromBucketName_Columnar pins the bucket-name
// decoding for columnar buckets: "property_<p>_columnar" must parse as
// (p, IndexTypePropColumnarValue), NOT as ("<p>_columnar",
// IndexTypePropValue) via the greedy plain-value pattern.
func TestGetPropNameAndIndexTypeFromBucketName_Columnar(t *testing.T) {
	cases := []struct {
		bucketName   string
		expectedProp string
		expectedType PropertyIndexType
	}{
		{helpers.BucketColumnarFromPropNameLSM("score"), "score", IndexTypePropColumnarValue},
		{helpers.BucketColumnarFromPropNameLSM("price_cents"), "price_cents", IndexTypePropColumnarValue},
		{helpers.BucketFromPropNameLSM("score"), "score", IndexTypePropValue},
	}
	for _, tc := range cases {
		t.Run(tc.bucketName, func(t *testing.T) {
			prop, indexType := GetPropNameAndIndexTypeFromBucketName(tc.bucketName)
			assert.Equal(t, tc.expectedProp, prop)
			assert.Equal(t, tc.expectedType, indexType)
		})
	}
}
