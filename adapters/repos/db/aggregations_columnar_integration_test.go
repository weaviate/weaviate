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

//go:build integrationTest

package db

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// Test_Aggregations_ColumnarFastPath verifies the filtered-aggregation
// columnar fast path (aggregator/filtered_columnar.go) end to end against
// the classic object-scan path. Two identical classes are created, one with
// IndexColumnar: true on its int/number/date properties, one without
// (control). The same deterministic data set — including objects missing
// the aggregated properties, updates, and deletes — is imported into both,
// and every filtered aggregation must yield IDENTICAL results on both
// classes. Both the point-lookup branch (< 2048 matching ids) and the
// block-scan branch (> 2048 matching ids) are exercised, before and after
// flushing memtables to columnar segments.
//
// The final phase ("value updates with preserved docIDs") is the
// regression guard for a bug this test originally pinned:
// DeltaSkipSearchable rebuilt Property structs without copying the
// HasColumnarIndex flag, so docID-preserving updates never reached the
// columnar bucket and the fast path served stale values (fixed in
// adapters/repos/db/inverted/delta_analyzer.go).
func Test_Aggregations_ColumnarFastPath(t *testing.T) {
	const (
		columnarClassName = "ColumnarAggColumnar"
		controlClassName  = "ColumnarAggControl"
		numObjects        = 5000
		// the docID count separating the point-lookup branch from the
		// block-scan branch in aggregateFromColumnar
		pointLookupThreshold = 2048
		// the one object that exercises float64 precision; its subset value
		// (99) is shared with no other object so a filter can isolate it
		precisionObjIdx    = 4242
		precisionObjSubset = 99
	)
	// 1 + 2^-24 collapses to 1.0 in float32; it must survive the columnar
	// store and aggregate exactly
	precisionValue := 1.0 + math.Pow(2, -24)

	vTrue := true

	makeClass := func(name string, columnar bool) *models.Class {
		var indexColumnar *bool
		if columnar {
			indexColumnar = &vTrue
		}
		return &models.Class{
			Class:               name,
			VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
			InvertedIndexConfig: invertedConfig(),
			Properties: []*models.Property{
				{Name: "subset", DataType: schema.DataTypeInt.PropString()},
				{Name: "intProp", DataType: schema.DataTypeInt.PropString(), IndexColumnar: indexColumnar},
				{Name: "numberProp", DataType: schema.DataTypeNumber.PropString(), IndexColumnar: indexColumnar},
				{Name: "dateProp", DataType: schema.DataTypeDate.PropString(), IndexColumnar: indexColumnar},
			},
		}
	}
	columnarClass := makeClass(columnarClassName, true)
	controlClass := makeClass(controlClassName, false)

	// --- deterministic data model -----------------------------------------

	type modelObj struct {
		idx      int
		subset   int64
		hasProps bool
		intVal   int64
		numVal   float64
		dateNano int64
		deleted  bool
	}

	const epochBase = int64(1600000000) * int64(time.Second)

	model := make([]*modelObj, numObjects)
	for i := 0; i < numObjects; i++ {
		model[i] = &modelObj{
			idx:      i,
			subset:   int64(i % 10),
			hasProps: i%13 != 0, // every 13th object misses the aggregated props
			intVal:   int64(i*3 - 7500),
			// all number values are multiples of 0.25 with bounded magnitude,
			// so every partial sum is exactly representable in float64 and
			// the sum is independent of visit order (the columnar scan path
			// visits docIDs in a different order than the object-scan path)
			numVal:   0.25*float64(i%2000) - 100,
			dateNano: epochBase + int64(i)*int64(time.Second) + int64(i%1000)*int64(time.Millisecond),
		}
	}
	model[precisionObjIdx].subset = precisionObjSubset
	model[precisionObjIdx].numVal = precisionValue
	require.True(t, model[precisionObjIdx].hasProps, "precision object must carry props")
	require.NotEqual(t, float64(float32(precisionValue)), precisionValue,
		"sanity: the precision value must actually exceed float32 precision")

	uuidFor := func(i int) strfmt.UUID {
		return strfmt.UUID(fmt.Sprintf("%08x-0000-4000-8000-%012x", i, i))
	}
	canonicalDate := func(nano int64) string {
		return time.Unix(0, nano).UTC().Format(time.RFC3339Nano)
	}
	propsFor := func(o *modelObj) map[string]interface{} {
		props := map[string]interface{}{"subset": float64(o.subset)}
		if o.hasProps {
			props["intProp"] = float64(o.intVal)
			props["numberProp"] = o.numVal
			props["dateProp"] = canonicalDate(o.dateNano)
		}
		return props
	}

	// --- repo setup (mirrors Test_Aggregations) ---------------------------

	dirName := t.TempDir()
	shardState := singleShardState()
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		class := &models.Class{Class: className}
		return readFunc(class, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()
	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	t.Cleanup(func() {
		require.Nil(t, repo.Shutdown(context.Background()))
	})
	migrator := NewMigrator(repo, logger, "node1")

	t.Run("create classes", func(t *testing.T) {
		schemaGetter.schema = schema.Schema{
			Objects: &models.Schema{
				Classes: []*models.Class{columnarClass, controlClass},
			},
		}
		require.Nil(t, migrator.AddClass(context.Background(), columnarClass))
		require.Nil(t, migrator.AddClass(context.Background(), controlClass))
	})

	// --- import helpers ----------------------------------------------------

	// putBatch (re-)imports the given objects into className. A non-nil
	// vector forces a docID change on update (compareObjsForInsertStatus
	// only preserves the docID when vectors are unchanged).
	putBatch := func(t *testing.T, className string, objs []*modelObj, vector models.C11yVector) {
		t.Helper()
		const chunk = 500
		for start := 0; start < len(objs); start += chunk {
			end := start + chunk
			if end > len(objs) {
				end = len(objs)
			}
			batch := make(objects.BatchObjects, 0, end-start)
			for j, o := range objs[start:end] {
				batch = append(batch, objects.BatchObject{
					OriginalIndex: j,
					Object: &models.Object{
						Class:      className,
						ID:         uuidFor(o.idx),
						Properties: propsFor(o),
						Vector:     vector,
					},
					UUID: uuidFor(o.idx),
				})
			}
			res, err := repo.BatchPutObjects(testCtx(), batch, nil, 0)
			require.Nil(t, err)
			for _, r := range res {
				require.Nil(t, r.Err)
			}
		}
	}

	flushAll := func(t *testing.T, className string) {
		t.Helper()
		idx := repo.GetIndex(schema.ClassName(className))
		require.NotNil(t, idx)
		require.Nil(t, idx.ForEachShard(func(_ string, shard ShardLike) error {
			return shard.Store().FlushMemtables(testCtx())
		}))
	}

	// --- expected results, computed from the model --------------------------

	type expectation struct {
		matching int // live objects matching the filter (incl. prop-less)
		// objects among matching that carry the aggregated props
		propCount        int
		intSum           int64
		intMin, intMax   int64
		numSum           float64
		numMin, numMax   float64
		dateMin, dateMax string
	}

	computeExpectation := func(match func(o *modelObj) bool) expectation {
		e := expectation{
			intMin: math.MaxInt64, intMax: math.MinInt64,
			numMin: math.MaxFloat64, numMax: -math.MaxFloat64,
		}
		var dateMin, dateMax int64 = math.MaxInt64, math.MinInt64
		for _, o := range model {
			if o.deleted || !match(o) {
				continue
			}
			e.matching++
			if !o.hasProps {
				continue
			}
			e.propCount++
			e.intSum += o.intVal
			if o.intVal < e.intMin {
				e.intMin = o.intVal
			}
			if o.intVal > e.intMax {
				e.intMax = o.intVal
			}
			e.numSum += o.numVal
			if o.numVal < e.numMin {
				e.numMin = o.numVal
			}
			if o.numVal > e.numMax {
				e.numMax = o.numVal
			}
			if o.dateNano < dateMin {
				dateMin = o.dateNano
			}
			if o.dateNano > dateMax {
				dateMax = o.dateNano
			}
		}
		e.dateMin = canonicalDate(dateMin)
		e.dateMax = canonicalDate(dateMax)
		return e
	}

	numericalAggregators := []aggregation.Aggregator{
		aggregation.CountAggregator,
		aggregation.SumAggregator,
		aggregation.MeanAggregator,
		aggregation.MaximumAggregator,
		aggregation.MinimumAggregator,
		aggregation.MedianAggregator,
		aggregation.ModeAggregator,
	}
	// no mode for dates: every date value in this data set is unique, so the
	// date mode would be an arbitrary map-iteration pick and not comparable
	dateAggregators := []aggregation.Aggregator{
		aggregation.CountAggregator,
		aggregation.MaximumAggregator,
		aggregation.MinimumAggregator,
		aggregation.MedianAggregator,
	}

	aggregate := func(t *testing.T, className string, filter *filters.LocalFilter) *aggregation.Result {
		t.Helper()
		res, err := repo.Aggregate(testCtx(), aggregation.Params{
			ClassName:        schema.ClassName(className),
			Filters:          filter,
			IncludeMetaCount: true,
			Properties: []aggregation.ParamProperty{
				{Name: "intProp", Aggregators: numericalAggregators},
				{Name: "numberProp", Aggregators: numericalAggregators},
				{Name: "dateProp", Aggregators: dateAggregators},
			},
		}, nil)
		require.Nil(t, err)
		require.Len(t, res.Groups, 1)
		return res
	}

	subsetFilter := func(operator filters.Operator, value int) *filters.LocalFilter {
		return &filters.LocalFilter{
			Root: &filters.Clause{
				Operator: operator,
				Value:    &filters.Value{Type: schema.DataTypeInt, Value: value},
				On:       &filters.Path{Property: "subset"},
			},
		}
	}

	assertAgainstModel := func(t *testing.T, res *aggregation.Result, e expectation) {
		t.Helper()
		group := res.Groups[0]
		assert.Equal(t, e.matching, group.Count, "meta count")

		intAggs := group.Properties["intProp"].NumericalAggregations
		require.NotNil(t, intAggs)
		assert.Equal(t, float64(e.propCount), intAggs["count"], "int count")
		assert.Equal(t, float64(e.intSum), intAggs["sum"], "int sum")
		assert.Equal(t, float64(e.intMin), intAggs["minimum"], "int min")
		assert.Equal(t, float64(e.intMax), intAggs["maximum"], "int max")
		assert.Equal(t, float64(e.intSum)/float64(e.propCount), intAggs["mean"], "int mean")

		numAggs := group.Properties["numberProp"].NumericalAggregations
		require.NotNil(t, numAggs)
		assert.Equal(t, float64(e.propCount), numAggs["count"], "number count")
		assert.Equal(t, e.numSum, numAggs["sum"], "number sum")
		assert.Equal(t, e.numMin, numAggs["minimum"], "number min")
		assert.Equal(t, e.numMax, numAggs["maximum"], "number max")
		assert.Equal(t, e.numSum/float64(e.propCount), numAggs["mean"], "number mean")

		dateAggs := group.Properties["dateProp"].DateAggregations
		require.NotNil(t, dateAggs)
		assert.Equal(t, int64(e.propCount), dateAggs["count"], "date count")
		assert.Equal(t, e.dateMin, dateAggs["minimum"], "date min")
		assert.Equal(t, e.dateMax, dateAggs["maximum"], "date max")
	}

	// runComparisons is the heart of the test: for every filter scenario the
	// columnar-backed class and the control class must return IDENTICAL
	// aggregation results, and both must match the model-derived expectation.
	runComparisons := func(t *testing.T) {
		t.Run("point lookup path (small filter result)", func(t *testing.T) {
			filter := subsetFilter(filters.OperatorEqual, 3)
			e := computeExpectation(func(o *modelObj) bool { return o.subset == 3 })
			require.Greater(t, e.matching, 0)
			require.Less(t, e.matching, pointLookupThreshold,
				"scenario must stay below the threshold to exercise point lookups")

			resColumnar := aggregate(t, columnarClassName, filter)
			resControl := aggregate(t, controlClassName, filter)

			assert.Equal(t, resControl.Groups, resColumnar.Groups,
				"columnar fast path must not change aggregation results")
			assertAgainstModel(t, resColumnar, e)
		})

		t.Run("scan path (large filter result)", func(t *testing.T) {
			filter := subsetFilter(filters.OperatorLessThanEqual, 7)
			e := computeExpectation(func(o *modelObj) bool { return o.subset <= 7 })
			require.Greater(t, e.matching, pointLookupThreshold,
				"scenario must exceed the threshold to exercise the block scan")

			resColumnar := aggregate(t, columnarClassName, filter)
			resControl := aggregate(t, controlClassName, filter)

			assert.Equal(t, resControl.Groups, resColumnar.Groups,
				"columnar fast path must not change aggregation results")
			assertAgainstModel(t, resColumnar, e)
		})

		t.Run("float64 precision survives the fast path", func(t *testing.T) {
			filter := subsetFilter(filters.OperatorEqual, precisionObjSubset)
			e := computeExpectation(func(o *modelObj) bool { return o.subset == precisionObjSubset })
			require.Equal(t, 1, e.matching, "filter must isolate the precision object")

			resColumnar := aggregate(t, columnarClassName, filter)
			resControl := aggregate(t, controlClassName, filter)

			assert.Equal(t, resControl.Groups, resColumnar.Groups)

			numAggs := resColumnar.Groups[0].Properties["numberProp"].NumericalAggregations
			require.NotNil(t, numAggs)
			assert.Equal(t, precisionValue, numAggs["sum"], "sum must be exact")
			assert.Equal(t, precisionValue, numAggs["mean"], "mean must be exact")
			assert.Equal(t, precisionValue, numAggs["minimum"], "min must be exact")
			assert.Equal(t, precisionValue, numAggs["maximum"], "max must be exact")
		})

		t.Run("filter matching nothing", func(t *testing.T) {
			filter := subsetFilter(filters.OperatorEqual, 12345)

			resColumnar := aggregate(t, columnarClassName, filter)
			resControl := aggregate(t, controlClassName, filter)

			assert.Equal(t, resControl.Groups, resColumnar.Groups)
			assert.Equal(t, 0, resColumnar.Groups[0].Count)
		})
	}

	// --- import + mutate -----------------------------------------------------

	t.Run("initial import", func(t *testing.T) {
		putBatch(t, columnarClassName, model, nil)
		putBatch(t, controlClassName, model, nil)
	})

	t.Run("flush initial import to segments", func(t *testing.T) {
		flushAll(t, columnarClassName)
		flushAll(t, controlClassName)
	})

	t.Run("apply prop drops and deletes", func(t *testing.T) {
		// re-put every 29th object WITHOUT the aggregated props: the object
		// stays, but the values must vanish from both classes' aggregations
		var propDrops []*modelObj
		for _, o := range model {
			if o.idx%29 == 0 && o.idx != precisionObjIdx && o.hasProps {
				o.hasProps = false
				propDrops = append(propDrops, o)
			}
		}
		require.NotEmpty(t, propDrops)
		putBatch(t, columnarClassName, propDrops, nil)
		putBatch(t, controlClassName, propDrops, nil)

		var deleted []*modelObj
		for _, o := range model {
			if o.idx%23 == 0 {
				o.deleted = true
				deleted = append(deleted, o)
			}
		}
		require.NotEmpty(t, deleted)
		for _, o := range deleted {
			for _, className := range []string{columnarClassName, controlClassName} {
				err := repo.DeleteObject(testCtx(), className, uuidFor(o.idx),
					time.Now(), nil, "", 0)
				require.Nil(t, err, "delete %d in %s", o.idx, className)
			}
		}
	})

	// --- preconditions: prove the fast path is actually reachable ------------

	t.Run("preconditions for the fast path", func(t *testing.T) {
		for _, propName := range []string{"intProp", "numberProp", "dateProp"} {
			prop, err := schema.GetPropertyByName(columnarClass, propName)
			require.Nil(t, err)
			require.True(t, inverted.HasColumnarIndex(prop),
				"columnar class prop %s must have the columnar index flag", propName)

			ctlProp, err := schema.GetPropertyByName(controlClass, propName)
			require.Nil(t, err)
			require.False(t, inverted.HasColumnarIndex(ctlProp))
		}

		idx := repo.GetIndex(schema.ClassName(columnarClassName))
		require.NotNil(t, idx)
		require.Nil(t, idx.ForEachShard(func(_ string, shard ShardLike) error {
			for _, propName := range []string{"intProp", "numberProp", "dateProp"} {
				bucket := shard.Store().Bucket(helpers.BucketColumnarFromPropNameLSM(propName))
				require.NotNil(t, bucket, "columnar bucket for %s must exist", propName)
				require.Equal(t, lsmkv.StrategyColumnar, bucket.Strategy())
			}

			// the columnar bucket must hold exactly one live row per live
			// object that (still) carries the property
			expectedLive := 0
			for _, o := range model {
				if !o.deleted && o.hasProps {
					expectedLive++
				}
			}
			liveRows := 0
			bucket := shard.Store().Bucket(helpers.BucketColumnarFromPropNameLSM("intProp"))
			require.Nil(t, bucket.ColumnarScan(0, nil, func(uint64, uint64) bool {
				liveRows++
				return true
			}))
			require.Equal(t, expectedLive, liveRows,
				"columnar bucket live-row count must match the model")
			return nil
		}))

		// the control class must NOT have columnar buckets, otherwise it
		// would not exercise the object-scan path
		ctlIdx := repo.GetIndex(schema.ClassName(controlClassName))
		require.NotNil(t, ctlIdx)
		require.Nil(t, ctlIdx.ForEachShard(func(_ string, shard ShardLike) error {
			for _, propName := range []string{"intProp", "numberProp", "dateProp"} {
				require.Nil(t, shard.Store().Bucket(helpers.BucketColumnarFromPropNameLSM(propName)),
					"control class must not have a columnar bucket for %s", propName)
			}
			return nil
		}))
	})

	// --- the comparisons ------------------------------------------------------

	t.Run("aggregations with mixed memtable and segment data", func(t *testing.T) {
		runComparisons(t)
	})

	t.Run("flush mutations to segments", func(t *testing.T) {
		flushAll(t, columnarClassName)
		flushAll(t, controlClassName)
	})

	t.Run("aggregations with all data in segments", func(t *testing.T) {
		runComparisons(t)
	})

	// value updates that CHANGE the docID (the vector changes alongside the
	// props, so compareObjsForInsertStatus does not preserve the docID): the
	// full property set is re-written under the new docID, including the
	// columnar buckets
	t.Run("value updates with changed docIDs", func(t *testing.T) {
		var updates []*modelObj
		for _, o := range model {
			if o.idx%19 == 0 && o.idx != precisionObjIdx && o.hasProps && !o.deleted {
				o.intVal += 1000003
				o.numVal += 0.5
				o.dateNano += int64(24 * time.Hour)
				updates = append(updates, o)
			}
		}
		require.NotEmpty(t, updates)
		vector := models.C11yVector{0.1, 0.2, 0.3, 0.4}
		putBatch(t, columnarClassName, updates, vector)
		putBatch(t, controlClassName, updates, vector)

		runComparisons(t)
	})

	// Regression guard: value updates that PRESERVE the docID (geo props,
	// vectors etc. unchanged → compareObjsForInsertStatus returns
	// preserve=true) go through inverted.DeltaSkipSearchable, which rebuilds
	// the ToAdd/ToDelete Property structs. It must copy HasColumnarIndex —
	// dropping it skips the columnar bucket on both delete and add, leaving
	// the fast path serving the PRE-update value.
	t.Run("value updates with preserved docIDs", func(t *testing.T) {
		var updates []*modelObj
		for _, o := range model {
			// skip %19 objects: they now carry a vector from the previous
			// phase; re-putting them without one would change the docID and
			// dodge the buggy delta path
			if o.idx%17 == 0 && o.idx%19 != 0 && o.idx != precisionObjIdx &&
				o.hasProps && !o.deleted {
				o.intVal += 2000033
				o.numVal += 0.75
				o.dateNano += int64(48 * time.Hour)
				updates = append(updates, o)
			}
		}
		require.NotEmpty(t, updates)
		putBatch(t, columnarClassName, updates, nil)
		putBatch(t, controlClassName, updates, nil)

		runComparisons(t)
	})
}
