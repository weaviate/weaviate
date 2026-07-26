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

// Delete resolves victims through Shard.FindUUIDs, which bypasses
// buildAllowList and so reaches the filter annotations with no slow-log sink
// installed. Without one the annotations land nowhere and the record says
// nothing about how the delete resolved. Asserting on the emitted record rather
// than on the ctx is what makes this fail if the sink stops being installed;
// asserting inside a searcher the test set up itself would pass either way.
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

// newFindUUIDsSlowLogShard builds a shard holding ten objects at
// rangeableInt == 1..10, with the slow log on and its threshold below any
// achievable duration so every call reports and the assertion is not a coin
// flip on sampling.
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
