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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

// propTask is a gen-1 task on prop whose unit runs on the fixture's shard.
func propTask(id, prop string, mt ReindexMigrationType, switched bool) distributedtask.ShardTaskState {
	st := viewTaskState(id, 1, mt, distributedtask.TaskStatusStarted, switched, true)
	h := st.Header.(ReindexTaskHeader)
	h.Properties = []string{prop}
	st.Header = h
	return st
}

func (f *viewFixture) putRow(title string, count, score int64) {
	obj := testObject(f.class.Class)
	obj.Object.Properties = map[string]any{"title": title, "count": count, "score": score}
	require.NoError(f.t, f.shard.PutObject(context.Background(), obj))
}

// newSwitchFixture stages three gen-1 builds and holds a view across their
// switch. Two objects are written before the staging and three after, so the
// builds hold only the last three.
func newSwitchFixture(t *testing.T) (f *viewFixture, held *shardView) {
	on, off := true, false
	f = newViewFixture(t, "ViewTest",
		&models.Property{Name: "title", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
		&models.Property{Name: "count", DataType: schema.DataTypeInt.PropString(), IndexRangeFilters: &off},
		&models.Property{Name: "score", DataType: schema.DataTypeInt.PropString(), IndexFilterable: &on})
	f.idx.Config.QueryBatchedContainsEnabled = runtime.NewDynamicValue(true)
	tasks := func(switched bool) []distributedtask.ShardTaskState {
		return []distributedtask.ShardTaskState{
			propTask("tok", "title", ReindexTypeChangeTokenization, switched),
			propTask("range", "count", ReindexTypeEnableRangeable, switched),
			propTask("rebuild", "score", ReindexTypeRepairFilterable, switched),
		}
	}
	f.putRow("hello world", 10, 1)
	f.putRow("hello there", 20, 2)
	f.tasks.set(tasks(false)...)
	require.NoError(t, f.shard.refreshView(context.Background()))
	f.putRow("hello world", 30, 3)
	f.putRow("hello world", 40, 4)
	f.putRow("other", 50, 5)
	// the sort planner counts objects on disk only
	require.NoError(t, f.shard.store.Bucket(helpers.ObjectsBucketLSM).FlushAndSwitch())

	held = f.shard.acquireView()
	t.Cleanup(func() {
		held.release()
		f.shard.closeViews() // the retire the release starts finishes before the temp dir goes
	})
	f.tasks.set(tasks(true)...)
	return f, held
}

func textFilter(op filters.Operator, value any, prop string, dt schema.DataType) *filters.LocalFilter {
	return &filters.LocalFilter{Root: &filters.Clause{
		Operator: op, On: &filters.Path{Class: "ViewTest", Property: schema.PropertyName(prop)},
		Value: &filters.Value{Value: value, Type: dt},
	}}
}

// Each query answers from the view it holds. The old view keeps its answer
// after the switch publishes, and a fresh query gets the new one.
func TestReindexViewReads(t *testing.T) {
	ctx := context.Background()
	matches := func(filter *filters.LocalFilter) func(s *Shard, v *shardView) string {
		return func(s *Shard, v *shardView) string {
			list, err := s.buildAllowList(ctx, v, filter, additional.Properties{})
			if err != nil {
				return "error: " + err.Error()
			}
			defer list.Close()
			return fmt.Sprint(list.Len())
		}
	}
	tests := []struct {
		name     string
		query    func(s *Shard, v *shardView) string
		old, new string
	}{
		{
			name:  "ContainsAny with 3 values",
			query: matches(textFilter(filters.ContainsAny, []string{"hello world", "nope", "zilch"}, "title", schema.DataTypeText)),
			old:   "3", new: "2",
		},
		{
			name:  "Equal on a property with both indexes",
			query: matches(textFilter(filters.OperatorEqual, "hello world", "title", schema.DataTypeText)),
			old:   "3", new: "2",
		},
		{
			name:  "Like on a property with both indexes",
			query: matches(textFilter(filters.OperatorLike, "hello w*", "title", schema.DataTypeText)),
			old:   "3", new: "2",
		},
		{
			name:  "GreaterThan across an enable-rangeable switch",
			query: matches(textFilter(filters.OperatorGreaterThan, 15, "count", schema.DataTypeInt)),
			old:   "4", new: "3",
		},
		{
			name: "sort on the property",
			query: func(s *Shard, v *shardView) string {
				qctx := helpers.InitSlowQueryDetails(ctx)
				// limit 1 makes the planner pick the filterable index. Its lowest
				// score is doc 0 in the old view and doc 2 in the new one.
				ids, err := s.sortedObjectList(qctx, v, 1, []filters.Sort{{Path: []string{"score"}, Order: "asc"}}, "ViewTest")
				if err != nil {
					return "error: " + err.Error()
				}
				if notes := fmt.Sprint(helpers.ExtractSlowQueryDetails(qctx)["sort_query_planner"]); strings.Contains(notes, "falling back") {
					return "the planner fell back: " + notes
				}
				return fmt.Sprint(ids)
			},
			old: "[0]", new: "[2]",
		},
		{
			name: "unfiltered aggregation of an int property",
			query: func(s *Shard, v *shardView) string {
				res, err := s.aggregate(ctx, v, aggregation.Params{ClassName: "ViewTest", Properties: []aggregation.ParamProperty{{
					Name: "score", Aggregators: []aggregation.Aggregator{aggregation.SumAggregator},
				}}}, nil)
				if err != nil {
					return "error: " + err.Error()
				}
				return fmt.Sprint(res.Groups[0].Properties["score"].NumericalAggregations["sum"])
			},
			old: "15", new: "12",
		},
		{
			name: "BM25",
			query: func(s *Shard, v *shardView) string {
				objs, _, err := inverted.NewBM25Searcher(s.index.GetInvertedIndexConfig().BM25, v, s.index.getSchema.ReadOnlyClass,
					s.index.classSearcher, s.index.getStopwordProvider(), s.GetPropertyLengthTracker(), s.index.logger,
					s.versioner.Version()).
					BM25F(ctx, nil, "ViewTest", 10, searchparams.KeywordRanking{Query: "hello world", Properties: []string{"title"}},
						additional.Properties{})
				if err != nil {
					return "error: " + err.Error()
				}
				return fmt.Sprint(len(objs))
			},
			old: "4", new: "2",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f, held := newSwitchFixture(t)
			published := make(chan error, 1)
			go func() { published <- f.shard.refreshView(ctx) }()
			require.Equal(t, tc.old, tc.query(f.shard, held), "a query holding the old view")
			require.NoError(t, <-published)
			require.Equal(t, tc.old, tc.query(f.shard, held), "the old view after the switch published")

			fresh := f.shard.acquireView()
			defer fresh.release()
			require.Equal(t, tc.new, tc.query(f.shard, fresh), "a query on the new view")
		})
	}
}
