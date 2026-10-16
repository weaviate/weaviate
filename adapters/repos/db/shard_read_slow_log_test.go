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
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config/runtime"
)

func TestShardSearchSlowLogRecord(t *testing.T) {
	filter := &filters.LocalFilter{Root: &filters.Clause{
		Operator: filters.ContainsAny,
		On:       &filters.Path{Class: "C", Property: "p"},
		Value:    &filters.Value{Value: make([]int, 1000), Type: schema.DataTypeInt},
	}}
	tests := map[string]func(ctx context.Context, s ShardLike){
		"ObjectSearch": func(ctx context.Context, s ShardLike) {
			_, _, _ = s.ObjectSearch(ctx, 10, filter, nil, nil, nil, additional.Properties{}, nil)
		},
		"ObjectVectorSearch": func(ctx context.Context, s ShardLike) {
			_, _, _ = s.ObjectVectorSearch(ctx, []models.Vector{[]float32{1, 2}}, []string{""}, 0, 10,
				filter, nil, nil, additional.Properties{}, nil, nil)
		},
	}
	for name, search := range tests {
		t.Run(name, func(t *testing.T) {
			logger, hook := test.NewNullLogger()
			shard, _ := testShard(t, context.Background(), "C", func(idx *Index) {
				idx.logger = logger
				idx.Config.QuerySlowLogEnabled = runtime.NewDynamicValue(true)
				idx.Config.QuerySlowLogThreshold = runtime.NewDynamicValue(time.Nanosecond)
			})

			ctx := helpers.InitSlowQueryDetails(context.Background())
			for _, key := range []string{"lsm_get_by_secondary", "lsm_get_by_secondary_with_view"} {
				for range 3 {
					helpers.AnnotateSlowQueryLogAppend(ctx, key, lsmkv.BucketSlowLogEntry{Total: time.Millisecond})
				}
			}
			search(ctx, shard)

			var record *logrus.Entry
			for _, e := range hook.AllEntries() {
				if e.Data["query"] == name {
					record = e
				}
			}
			require.NotNil(t, record, "no slow-query record logged")
			for _, key := range []string{"lsm_get_by_secondary", "lsm_get_by_secondary_with_view"} {
				assert.IsType(t, lsmkv.BucketSlowLogEntryStats{}, record.Data[key], key)
			}
			assert.IsType(t, filters.LogSummary{}, record.Data["filters"])
		})
	}
}
