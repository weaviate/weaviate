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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

const findUUIDsSlowLogProp = "rangeableInt"

// FindUUIDs bypasses buildAllowList, so it needs its own slow-log sink to
// record how the filter resolved. Asserting on the emitted record, not a
// test-installed one, is what catches that sink being removed.
func Test_FindUUIDs_RecordsHowTheFilterResolved(t *testing.T) {
	ctx := testCtx()
	shard, hook := newFindUUIDsSlowLogShard(t, ctx)

	uuids, err := shard.FindUUIDs(ctx, rangeableIntFilter(filters.OperatorLessThanEqual, 5), 0)
	require.NoError(t, err)
	require.Len(t, uuids, 5, "the filter matched nothing, so any assertion on its record is vacuous")

	entry := findSlowQueryEntry(t, hook, "FindUUIDs")
	require.Contains(t, entry.Data, roaringsetrange.DocBitmapAnnotation,
		"the range filter's annotation did not reach the record, so FindUUIDs resolved "+
			"the delete with no sink installed for it to land in")
	require.NotEmpty(t, entry.Data[roaringsetrange.DocBitmapAnnotation])
}

// The record is per FindUUIDs call, and a delete-heavy or replication-heavy
// workload makes one call per batch per shard. It has to sit behind the same
// bound as every other per-query record, not be emitted unconditionally.
func Test_FindUUIDs_RecordIsBoundedByTheSlowLogSwitch(t *testing.T) {
	ctx := testCtx()
	shard, hook := newFindUUIDsSlowLogShard(t, ctx, func(idx *Index) {
		idx.Config.QuerySlowLogEnabled = configRuntime.NewDynamicValue(false)
	})

	_, err := shard.FindUUIDs(ctx, rangeableIntFilter(filters.OperatorLessThanEqual, 5), 0)
	require.NoError(t, err)

	for _, entry := range hook.AllEntries() {
		require.NotEqualf(t, "FindUUIDs", entry.Data["query"],
			"a record was emitted with the slow log off: %s", entry.Message)
	}
}

// newFindUUIDsSlowLogShard builds a shard with ten objects at rangeableInt ==
// 1..10; the slow-log threshold is set below any achievable duration so every
// call reports and the assertion isn't a coin flip on sampling.
func newFindUUIDsSlowLogShard(t *testing.T, ctx context.Context, indexOpts ...func(*Index),
) (ShardLike, *test.Hook) {
	t.Helper()

	logger, hook := test.NewNullLogger()
	class := &models.Class{
		Class:               "FindUUIDsSlowLog",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{{
			Name:              findUUIDsSlowLogProp,
			DataType:          []string{string(schema.DataTypeInt)},
			IndexRangeFilters: boolPtr(true),
		}},
	}

	opts := append([]func(*Index){func(idx *Index) {
		idx.logger = logger
		idx.Config.IndexRangeableInMemory = true
		idx.Config.QuerySlowLogEnabled = configRuntime.NewDynamicValue(true)
		idx.Config.QuerySlowLogThreshold = configRuntime.NewDynamicValue(time.Nanosecond)
	}}, indexOpts...)

	shard, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false, opts...)

	for i := 1; i <= 10; i++ {
		require.NoError(t, shard.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)),
				Class:      class.Class,
				Properties: map[string]interface{}{findUUIDsSlowLogProp: float64(i)},
			},
		}))
	}
	hook.Reset()

	return shard, hook
}

func rangeableIntFilter(operator filters.Operator, value int) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: operator,
			On: &filters.Path{
				Class:    schema.ClassName("FindUUIDsSlowLog"),
				Property: schema.PropertyName(findUUIDsSlowLogProp),
			},
			Value: &filters.Value{Value: value, Type: schema.DataTypeInt},
		},
	}
}

func findSlowQueryEntry(t *testing.T, hook *test.Hook, query string) *logrus.Entry {
	t.Helper()

	for _, entry := range hook.AllEntries() {
		if entry.Data["query"] == query {
			return entry
		}
	}
	t.Fatalf("no slow-query record for %q; the shard emitted %d entries", query, len(hook.AllEntries()))
	return nil
}

// The slow-query record above only exists once QUERY_SLOW_LOG_ENABLED is on,
// which it is not by default, and is sampled after that. On a stock deployment
// the counter is the only evidence that a delete was served from the range
// cascade, so it has to move with the slow log off.
func Test_FindUUIDs_CountsCascadeRoutingWithTheSlowLogOff(t *testing.T) {
	ctx := testCtx()
	shard, hook := newFindUUIDsSlowLogShard(t, ctx, func(idx *Index) {
		idx.Config.QuerySlowLogEnabled = configRuntime.NewDynamicValue(false)
	})

	before := gatheredLabelValues(t, "weaviate_lsm_roaringsetrange_batch_delete_total")

	_, err := shard.FindUUIDs(ctx, rangeableIntFilter(filters.OperatorLessThanEqual, 5), 0)
	require.NoError(t, err)

	after := gatheredLabelValues(t, "weaviate_lsm_roaringsetrange_batch_delete_total")
	require.Equalf(t, before["cascade"]+1, after["cascade"],
		"a cascade-routed delete left no counter behind, so on stock settings nothing records it")
	require.Equal(t, before["other"], after["other"], "it was not routed through the cascade")

	for _, entry := range hook.AllEntries() {
		require.NotEqualf(t, "FindUUIDs", entry.Data["query"],
			"the slow log is off, so this reading has to come from the counter alone: %s", entry.Message)
	}
}
