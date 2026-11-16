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
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const enableFilterableReadsObjects = 20

func newEnableFilterableReadsClass(className string) *models.Class {
	class := newEnableFilterableTestClass(className)
	// An enable run does not create null and length buckets for a property that had no index.
	class.InvertedIndexConfig.IndexNullState = false
	class.InvertedIndexConfig.IndexPropertyLength = false
	for name, dt := range map[string]schema.DataType{
		"num": schema.DataTypeInt, "fl": schema.DataTypeNumber, "dt": schema.DataTypeDate, "ok": schema.DataTypeBoolean,
	} {
		class.Properties = append(class.Properties,
			&models.Property{Name: name, DataType: dt.PropString(), IndexFilterable: boolPtr(false)})
	}
	return class
}

func newEnableFilterableReadsObject(className string, i int) *storobj.Object {
	return &storobj.Object{MarshallerVersion: 1, Object: models.Object{
		ID: strfmt.UUID(uuid.NewString()), Class: className,
		Properties: map[string]any{
			"num": int64(i), "fl": float64(i) + 0.5, "ok": i%2 == 0,
			"dt": time.Date(2024, 1, 1, i, 0, 0, 0, time.UTC).Format(time.RFC3339),
		},
	}}
}

func enableFilterableSortedIDs(ctx context.Context, s *Shard, className string, filter *filters.LocalFilter,
	prop, order string, limit int,
) ([]strfmt.UUID, error) {
	sort := []filters.Sort{{Path: []string{prop}, Order: order}}
	var res []*storobj.Object
	var err error
	if filter == nil {
		res, err = s.ObjectList(ctx, limit, sort, nil, additional.Properties{}, schema.ClassName(className))
	} else {
		res, _, err = s.ObjectSearch(ctx, limit, filter, nil, sort, nil, additional.Properties{}, nil)
	}
	ids := make([]strfmt.UUID, len(res))
	for i, obj := range res {
		ids[i] = obj.ID()
	}
	return ids, err
}

func enableFilterableAggregate(ctx context.Context, s *Shard, className, prop string) string {
	res, err := s.Aggregate(ctx, aggregation.Params{
		ClassName: schema.ClassName(className),
		Properties: []aggregation.ParamProperty{{
			Name: schema.PropertyName(prop), Aggregators: []aggregation.Aggregator{aggregation.CountAggregator},
		}},
	}, nil)
	if err != nil {
		return "error: " + err.Error()
	}
	p := res.Groups[0].Properties[prop]
	return fmt.Sprintf("%v %+v", p.NumericalAggregations, p.BooleanAggregation)
}

// Sort and aggregate must answer as before the run until the swap.
func TestSortAndAggregateWhileAnEnableFilterableRunIsInFlight(t *testing.T) {
	sortProps := []string{"num", "fl", "dt"}
	aggProps := []string{"num", "fl", "dt", "ok"}
	ctx := testCtx()
	className := "EnableFilterableReads_" + uuid.NewString()[:8]
	shd, idx := testShardWithSettings(t, ctx, newEnableFilterableReadsClass(className),
		enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())
	for i := 0; i < enableFilterableReadsObjects; i++ {
		require.NoError(t, shard.PutObject(ctx, newEnableFilterableReadsObject(className, i)))
	}
	allObjects := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.OperatorNotEqual,
		On:       &filters.Path{Class: schema.ClassName(className), Property: filters.InternalPropID},
		Value:    &filters.Value{Value: uuid.NewString(), Type: schema.DataTypeText},
	}}

	// The planner reads the index only when limit is small against the object count.
	type query struct {
		filter      *filters.LocalFilter
		prop, order string
	}
	var queries []query
	for _, prop := range sortProps {
		queries = append(queries, query{nil, prop, "asc"}, query{nil, prop, "desc"})
	}
	queries = append(queries, query{allObjects, "num", "asc"})
	sorts := func() map[string]string {
		out := map[string]string{}
		for _, q := range queries {
			ids, err := enableFilterableSortedIDs(ctx, shard, className, q.filter, q.prop, q.order, 5)
			require.NoError(t, err)
			out[fmt.Sprintf("sort %s %s filtered=%v", q.prop, q.order, q.filter != nil)] = fmt.Sprint(ids)
		}
		return out
	}
	snapshot := func() map[string]string {
		out := sorts()
		for _, prop := range aggProps {
			out["aggregate "+prop] = enableFilterableAggregate(ctx, shard, className, prop)
		}
		return out
	}
	want, wantSorts := snapshot(), sorts()

	task, _ := newEnableFilterableTask(t, idx, className, shard.migrationUnit(), aggProps...)
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	assert.Equal(t, want, snapshot(), "after the backfill")
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	assert.Equal(t, want, snapshot(), "after prepare")
	require.NoError(t, task.RunSwapOnShard(ctx, shard))
	require.Equal(t, wantSorts, sorts(), "after the swap, served by the new index")
}

// A restart after the local swap, with the run finished but the flip not yet applied, keeps serving across the flip.
func TestRestartAfterAnEnableFilterableSwapServesAcrossTheFlip(t *testing.T) {
	ctx := testCtx()
	className := "EnableFilterableFlip_" + uuid.NewString()[:8]
	class := newEnableFilterableReadsClass(className)
	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	for i := 0; i < enableFilterableReadsObjects; i++ {
		require.NoError(t, shard.PutObject(ctx, newEnableFilterableReadsObject(className, i)))
	}
	task, _ := newEnableFilterableTask(t, idx, className, shard.migrationUnit(), "num")
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	name := shard.Name()
	require.NoError(t, shard.Shutdown(ctx))
	simulateProcessRestartBucketCleanup(t, shard.pathLSM())
	idx.db.indices[indexID(idx.Config.ClassName)] = idx
	installTestMigrationTaskSources(ctx, idx.db, nil, &distributedtask.Task{
		Namespace:      ReindexNamespace,
		TaskDescriptor: distributedtask.TaskDescriptor{ID: task.taskID, Version: task.taskVersion},
		Status:         distributedtask.TaskStatusFinished,
	})
	provider := NewReindexProvider(idx.db, nil, nil, idx.logger, idx.getSchema.NodeName(), nil, ctx)
	idx.shardReindexer = NewMigrationRecoveryReindexer(idx.logger, nil, provider)
	shard = openShardFromDisk(t, ctx, idx, class, name)
	defer func() { shard.Shutdown(context.Background()) }()
	require.Equal(t, []string{"num"}, shard.migrationRecords.DeferredPromotions(task.migrationRecordKey()),
		"the load holds the promotion back, so the deferred path serves the staged data")

	put := func(v int64) {
		obj := newEnableFilterableReadsObject(className, 0)
		obj.Object.Properties.(map[string]any)["num"] = v
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	equal := func(v int64) int {
		res, _, err := shard.ObjectSearch(ctx, 100, &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorEqual,
			On:       &filters.Path{Class: schema.ClassName(className), Property: "num"},
			Value:    &filters.Value{Value: int(v), Type: schema.DataTypeInt},
		}}, nil, nil, nil, additional.Properties{}, nil)
		require.NoError(t, err)
		return len(res)
	}
	sorted := func() []strfmt.UUID {
		ids, err := enableFilterableSortedIDs(ctx, shard, className, nil, "num", "asc", 5)
		require.NoError(t, err)
		return ids
	}
	count := func(n int) string { return fmt.Sprintf("map[count:%d] %+v", n, aggregation.Boolean{}) }

	require.Len(t, sorted(), 5)
	require.Equal(t, count(enableFilterableReadsObjects), enableFilterableAggregate(ctx, shard, className, "num"))
	put(777)

	for _, prop := range class.Properties {
		if prop.Name == "num" {
			prop.IndexFilterable = boolPtr(true)
			require.NoError(t, idx.updateProperty(ctx, prop))
		}
	}
	put(888)
	require.Len(t, sorted(), 5)
	require.Equal(t, 1, equal(3))
	require.Equal(t, 1, equal(777), "a write before the flip")
	require.Equal(t, 1, equal(888), "a write after the flip")
	require.Equal(t, count(enableFilterableReadsObjects+2), enableFilterableAggregate(ctx, shard, className, "num"))
}

// A migration that adds no index swaps out its live bucket, so an absent one is refused rather than installed.
func TestTheSwapOfAnExistingIndexRefusesAnAbsentLiveBucket(t *testing.T) {
	ctx := testCtx()
	className := "SwapRefuses_" + uuid.NewString()[:8]
	shd, idx := testShardWithSettings(t, ctx, newEnableFilterableReadsClass(className),
		enthnsw.UserConfig{Skip: true}, false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	task, _ := newEnableFilterableTask(t, idx, className, shard.migrationUnit(), "num")
	task.migrationType = ReindexTypeChangeTokenization
	live, staged := task.strategy.SourceBucketName("num"), task.ingestBucketName("num")
	require.NoError(t, shard.store.CreateOrLoadBucket(ctx, staged, shard.makeDefaultBucketOptions(lsmkv.StrategyRoaringSet)...))

	_, err := task.processOneSwapProp(ctx, shard.store, 0, "num")
	require.ErrorContains(t, err, "target bucket")
	require.Nil(t, shard.store.Bucket(live))
}
