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
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
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
	class.Properties = append(class.Properties,
		&models.Property{
			Name: "body", DataType: schema.DataTypeText.PropString(),
			Tokenization: models.PropertyTokenizationWord, IndexSearchable: boolPtr(false),
		},
		&models.Property{Name: "score", DataType: schema.DataTypeInt.PropString()})
	return class
}

func newEnableFilterableReadsObject(className string, i int) *storobj.Object {
	return &storobj.Object{MarshallerVersion: 1, Object: models.Object{
		ID: strfmt.UUID(fmt.Sprintf("00000000-0000-4000-8000-%012d", i)), Class: className,
		Properties: map[string]any{
			"num": int64(i), "fl": float64(i) + 0.5, "ok": i%2 == 0,
			"dt":    time.Date(2024, 1, 1, i, 0, 0, 0, time.UTC).Format(time.RFC3339),
			"body":  []string{"alpha", "beta", "gamma", "delta"}[i%4],
			"score": int64(i),
		},
	}}
}

// enableReadsNums lists, in ascending order, the "num" of each result, which its ID ends in.
func enableReadsNums(res []*storobj.Object, err error) string {
	if err != nil {
		return "error: " + err.Error()
	}
	nums := make([]int, len(res))
	for i, obj := range res {
		_, err := fmt.Sscanf(string(obj.ID())[24:], "%d", &nums[i])
		if err != nil {
			return "error: " + err.Error()
		}
	}
	slices.Sort(nums)
	return fmt.Sprint(nums)
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

// Until the swap, reads answer as before the run, and a cancelled run leaves no bucket behind.
func TestReadsWhileAnEnableRunIsInFlightOrCancelled(t *testing.T) {
	sortProps := []string{"num", "fl", "dt", "score"}
	aggProps := []string{"num", "fl", "dt", "ok"}
	rows := []struct {
		name, indexType string
		props           []string
		newTask         func(t *testing.T, idx *Index, className, unit string, gen int) *ShardReindexTaskGeneric
		query, want     string // the query the new index will serve, and its answer until the swap
	}{
		{
			name: "enable filterable", indexType: "filterable", props: aggProps,
			newTask: func(t *testing.T, idx *Index, className, unit string, gen int) *ShardReindexTaskGeneric {
				task, _ := newEnableFilterableTaskAtGeneration(t, idx, className, gen, unit, aggProps...)
				return task
			},
			query: "filter num = 7", want: "error: Filtering by property 'num' requires inverted index. " +
				"Is `indexFilterable` option of property 'num' enabled? Set it to `true` or leave empty",
		},
		{
			name: "enable searchable", indexType: "searchable", props: []string{"body"},
			newTask: func(t *testing.T, idx *Index, className, unit string, gen int) *ShardReindexTaskGeneric {
				task, strategy := newEnableSearchableTask(t, idx, className, "body", models.PropertyTokenizationWord, unit)
				task.taskVersion, strategy.generation = uint64(gen), gen
				return task
			},
			query: "bm25 body", want: "error: Searching by property 'body' requires inverted index. " +
				"Is `indexSearchable` option of property 'body' enabled? Set it to `true` or leave empty",
		},
		{
			name: "enable rangeable", indexType: "rangeable", props: []string{"score"},
			newTask: func(t *testing.T, idx *Index, className, unit string, gen int) *ShardReindexTaskGeneric {
				task, strategy := newFilterableToRangeableTask(t, idx, className, "score", unit)
				task.taskVersion, strategy.generation = uint64(gen), gen
				return task
			},
			query: "filter score > 12", want: "[13 14 15 16 17 18 19]",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			ctx := testCtx()
			className := "EnableReads_" + uuid.NewString()[:8]
			shd, idx := testShardWithSettings(t, ctx, newEnableFilterableReadsClass(className),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())
			for i := 0; i < enableFilterableReadsObjects; i++ {
				require.NoError(t, shard.PutObject(ctx, newEnableFilterableReadsObject(className, i)))
			}
			clause := func(prop string, op filters.Operator, v any, dt schema.DataType) *filters.LocalFilter {
				return &filters.LocalFilter{Root: &filters.Clause{
					Operator: op, Value: &filters.Value{Value: v, Type: dt},
					On: &filters.Path{Class: schema.ClassName(className), Property: schema.PropertyName(prop)},
				}}
			}
			search := func(f *filters.LocalFilter, kw *searchparams.KeywordRanking) string {
				res, _, err := shard.ObjectSearch(ctx, 100, f, kw, nil, nil, additional.Properties{}, nil)
				return enableReadsNums(res, err)
			}
			allObjects := clause(filters.InternalPropID, filters.OperatorNotEqual, uuid.NewString(), schema.DataTypeText)

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
				for _, prop := range append(aggProps, "score") {
					out["aggregate "+prop] = enableFilterableAggregate(ctx, shard, className, prop)
				}
				out["filter num = 7"] = search(clause("num", filters.OperatorEqual, 7, schema.DataTypeInt), nil)
				out["bm25 body"] = search(nil, &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"body"}, Query: "alpha"})
				out["filter score > 12"] = search(clause("score", filters.OperatorGreaterThan, 12, schema.DataTypeInt), nil)
				return out
			}
			want, wantSorts := snapshot(), sorts()
			require.Equal(t, row.want, want[row.query])

			// buckets lists what serves or stages the targeted properties, in the store and on disk.
			buckets := func(task *ShardReindexTaskGeneric, withStaged bool) []string {
				entries, err := os.ReadDir(shard.pathLSM())
				require.NoError(t, err)
				var out []string
				for _, prop := range row.props {
					live := task.strategy.SourceBucketName(prop)
					if shard.store.Bucket(live) != nil {
						out = append(out, "store: "+live)
					}
					if !withStaged {
						continue
					}
					for name := range shard.store.GetBucketsByName() {
						if strings.HasPrefix(name, live+"__") {
							out = append(out, "store: "+name)
						}
					}
					for _, e := range entries {
						if e.Name() == live || strings.HasPrefix(e.Name(), live+"__") {
							out = append(out, "disk: "+e.Name())
						}
					}
				}
				return out
			}

			task := row.newTask(t, idx, className, shard.migrationUnit(), 1)
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			assert.Equal(t, want, snapshot(), "after the backfill")
			assert.Empty(t, buckets(task, false), "no live bucket before the swap")
			extra := newEnableFilterableReadsObject(className, 100)
			require.NoError(t, shard.PutObject(ctx, extra), "a write in flight")
			require.NoError(t, shard.DeleteObject(ctx, extra.ID(), time.Now()), "a delete in flight")
			assert.Equal(t, want, snapshot(), "after a write and a delete in flight")

			// Cancel: the backfill has returned, so what is left is the cancel handler's sweep.
			for _, prop := range row.props {
				require.NoError(t, idx.cleanStalePartialReindexState(ctx, prop, row.indexType, nil, nil))
			}
			assert.Equal(t, want, snapshot(), "after the cancel")
			assert.Empty(t, buckets(task, true), "after the cancel")

			// A resubmitted run is a new task version.
			retry := row.newTask(t, idx, className, shard.migrationUnit(), 2)
			require.NoError(t, retry.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, retry.RunPrepareOnShard(ctx, shard))
			assert.Equal(t, want, snapshot(), "after prepare")
			assert.Empty(t, buckets(retry, false), "no live bucket before the swap")
			require.NoError(t, retry.RunSwapOnShard(ctx, shard))
			require.Equal(t, wantSorts, sorts(), "after the swap")
			require.Len(t, buckets(retry, false), len(row.props), "the swap installs a live bucket per property")
		})
	}
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
