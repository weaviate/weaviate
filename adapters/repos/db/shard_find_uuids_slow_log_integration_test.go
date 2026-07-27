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
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
)

const (
	findUUIDsSlowLogProp   = "rangeableInt"
	findUUIDsFilterableInt = "filterableInt"
	resolutionsSeries      = "weaviate_lsm_roaringsetrange_delete_filter_resolutions_total"
)

// FindUUIDs bypasses buildAllowList, so it needs its own slow-log sink;
// asserting on the real record catches that sink being removed. Source
// values are literals, not imported constants, since they're the
// operator-facing contract.
func Test_FindUUIDs_RecordsHowTheFilterResolved(t *testing.T) {
	tests := []struct {
		name              string
		rangeableInMemory bool
		wantSource        string
	}{
		{name: "the default path", rangeableInMemory: false, wantSource: "no_in_memory_segment"},
		{name: "in-memory range segment", rangeableInMemory: true, wantSource: "in_memory_segment"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			shard, hook := newFindUUIDsSlowLogShard(t, ctx, tt.rangeableInMemory)

			uuids, err := shard.FindUUIDs(ctx, rangeableIntFilter(filters.OperatorLessThanEqual, 5), 0)
			require.NoError(t, err)
			require.Len(t, uuids, 5, "the filter matched nothing, so any assertion on its record is vacuous")

			entry := findSlowQueryEntry(t, hook, "FindUUIDs")
			require.Contains(t, entry.Data, roaringsetrange.DocBitmapAnnotation,
				"the range filter's annotation did not reach the record, so FindUUIDs resolved "+
					"the delete with no sink installed for it to land in")

			reads, ok := entry.Data[roaringsetrange.DocBitmapAnnotation].([]map[string]any)
			require.Truef(t, ok, "the annotation is %T, so nothing can read the routing out of it",
				entry.Data[roaringsetrange.DocBitmapAnnotation])
			require.NotEmpty(t, reads)
			for _, read := range reads {
				require.Equal(t, tt.wantSource, read["source"],
					"the record names readers the read did not use")
			}
		})
	}
}

// The record must sit behind the same bound as every other per-query record,
// not fire unconditionally. That bound lives in the reporter, not the range
// index, so the default backing already exercises it.
func Test_FindUUIDs_RecordIsBoundedByTheSlowLogSwitch(t *testing.T) {
	ctx := testCtx()
	shard, hook := newFindUUIDsSlowLogShard(t, ctx, false, func(idx *Index) {
		idx.Config.QuerySlowLogEnabled = configRuntime.NewDynamicValue(false)
	})

	_, err := shard.FindUUIDs(ctx, rangeableIntFilter(filters.OperatorLessThanEqual, 5), 0)
	require.NoError(t, err)

	for _, entry := range hook.AllEntries() {
		require.NotEqualf(t, "FindUUIDs", entry.Data["query"],
			"a record was emitted with the slow log off: %s", entry.Message)
	}
}

// Pins the label to the readers that answered, not to the annotation's mere
// presence, which both reader sets write regardless of which one ran.
func Test_FindUUIDs_CountsWhichBackingResolvedTheFilter(t *testing.T) {
	tests := []struct {
		name              string
		rangeableInMemory bool
		wantRouted        string
	}{
		{name: "the default path", rangeableInMemory: false, wantRouted: "rangeable_no_in_memory_segment"},
		{name: "in-memory range segment", rangeableInMemory: true, wantRouted: "rangeable_in_memory"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			shard, hook := newFindUUIDsSlowLogShard(t, ctx, tt.rangeableInMemory, func(idx *Index) {
				idx.Config.QuerySlowLogEnabled = configRuntime.NewDynamicValue(false)
			})

			before := gatheredLabelValues(t, resolutionsSeries)
			require.NotEmptyf(t, before, "%s is not emitted, so nothing records the routing",
				resolutionsSeries)

			_, err := shard.FindUUIDs(ctx, rangeableIntFilter(filters.OperatorLessThanEqual, 5), 0)
			require.NoError(t, err)

			want := maps.Clone(before)
			want[tt.wantRouted]++
			require.Equalf(t, want, gatheredLabelValues(t, resolutionsSeries),
				"the resolution was not counted as %q", tt.wantRouted)

			for _, entry := range hook.AllEntries() {
				require.NotEqualf(t, "FindUUIDs", entry.Data["query"],
					"the slow log is off, so this reading has to come from the counter alone: %s",
					entry.Message)
			}
		})
	}
}

// Flushed-versus-unflushed is orthogonal to IndexRangeableInMemory, so it needs
// its own axis: a collection that has never flushed has no range segment on
// disk and is answered from the memtable alone, which is the state of every
// collection until its first flush. A label naming disk segments is false
// there, and this is what says so rather than leaving it to the help text.
func Test_FindUUIDs_CountsTheSameChildOnAnUnflushedCollection(t *testing.T) {
	tests := []struct {
		name              string
		rangeableInMemory bool
		flush             bool
		wantRouted        string
	}{
		{
			name:       "default path, nothing flushed",
			wantRouted: "rangeable_no_in_memory_segment",
		},
		{
			name:       "default path, flushed",
			flush:      true,
			wantRouted: "rangeable_no_in_memory_segment",
		},
		{
			name:              "in-memory range segment, nothing flushed",
			rangeableInMemory: true,
			wantRouted:        "rangeable_in_memory",
		},
		{
			name:              "in-memory range segment, flushed",
			rangeableInMemory: true,
			flush:             true,
			wantRouted:        "rangeable_in_memory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			shard, _ := newFindUUIDsSlowLogShard(t, ctx, tt.rangeableInMemory, func(idx *Index) {
				idx.Config.QuerySlowLogEnabled = configRuntime.NewDynamicValue(false)
			})

			if tt.flush {
				require.NoError(t, shard.Store().FlushMemtables(ctx))
			}

			segments := rangeableDiskSegments(t, shard)
			if tt.flush {
				require.NotZero(t, segments,
					"the flush left no range segment on disk, so this case reads the same backing as the unflushed one")
			} else {
				require.Zerof(t, segments,
					"%d range segments are on disk, so this case does not read an unflushed collection", segments)
				require.NotContainsf(t, tt.wantRouted, "disk",
					"%q names disk segments and none were read", tt.wantRouted)
			}

			before := gatheredLabelValues(t, resolutionsSeries)

			uuids, err := shard.FindUUIDs(ctx, rangeableIntFilter(filters.OperatorLessThanEqual, 5), 0)
			require.NoError(t, err)
			require.Len(t, uuids, 5, "the filter matched nothing, so any assertion on its routing is vacuous")

			want := maps.Clone(before)
			want[tt.wantRouted]++
			require.Equalf(t, want, gatheredLabelValues(t, resolutionsSeries),
				"the resolution was not counted as %q", tt.wantRouted)
		})
	}
}

// rangeableDiskSegments counts the range segments the property has on disk,
// which is what makes the flushed axis a measurement rather than an assumption.
func rangeableDiskSegments(t *testing.T, shard ShardLike) int {
	t.Helper()

	dir := filepath.Join(shard.pathLSM(),
		helpers.BucketRangeableFromPropNameLSM(findUUIDsSlowLogProp))
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	count := 0
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".db" {
			count++
		}
	}
	return count
}

// Pins that a resolution counts even when it matches nothing, since the
// series measures resolutions, not deletes.
func Test_FindUUIDs_CountsAResolutionThatMatchedNothing(t *testing.T) {
	tests := []struct {
		name       string
		filter     *filters.LocalFilter
		wantRouted string
	}{
		{
			name:       "rangeable property",
			filter:     rangeableIntFilter(filters.OperatorLessThan, 1),
			wantRouted: "rangeable_no_in_memory_segment",
		},
		{
			name:       "filterable property",
			filter:     filterableIntFilter(filters.OperatorEqual, 99),
			wantRouted: "non_rangeable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			shard, _ := newFindUUIDsSlowLogShard(t, ctx, false)

			before := gatheredLabelValues(t, resolutionsSeries)

			uuids, err := shard.FindUUIDs(ctx, tt.filter, 0)
			require.NoError(t, err)
			require.Empty(t, uuids, "the filter matched, so this proves nothing about an empty one")

			want := maps.Clone(before)
			want[tt.wantRouted]++
			require.Equalf(t, want, gatheredLabelValues(t, resolutionsSeries),
				"a resolution that matched nothing went uncounted, so the series counts "+
					"deletes rather than the resolutions it is named for")
		})
	}
}

// newFindUUIDsSlowLogShard builds a shard with ten objects at rangeableInt ==
// 1..10; the slow-log threshold is set below any achievable duration so every
// call reports and the assertion isn't a coin flip on sampling.
// rangeableInMemory is a parameter, not a default, so no test can silently
// inherit the wrong backing.
func newFindUUIDsSlowLogShard(t *testing.T, ctx context.Context, rangeableInMemory bool,
	indexOpts ...func(*Index),
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
		}, {
			Name:              findUUIDsFilterableInt,
			DataType:          []string{string(schema.DataTypeInt)},
			IndexFilterable:   boolPtr(true),
			IndexRangeFilters: boolPtr(false),
		}},
	}

	opts := append([]func(*Index){func(idx *Index) {
		idx.logger = logger
		idx.Config.IndexRangeableInMemory = rangeableInMemory
		idx.Config.QuerySlowLogEnabled = configRuntime.NewDynamicValue(true)
		idx.Config.QuerySlowLogThreshold = configRuntime.NewDynamicValue(time.Nanosecond)
	}}, indexOpts...)

	shard, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false, opts...)

	for i := 1; i <= 10; i++ {
		require.NoError(t, shard.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)),
				Class: class.Class,
				Properties: map[string]interface{}{
					findUUIDsSlowLogProp:   float64(i),
					findUUIDsFilterableInt: float64(i),
				},
			},
		}))
	}
	hook.Reset()

	return shard, hook
}

func rangeableIntFilter(operator filters.Operator, value int) *filters.LocalFilter {
	return findUUIDsIntFilter(findUUIDsSlowLogProp, operator, value)
}

func filterableIntFilter(operator filters.Operator, value int) *filters.LocalFilter {
	return findUUIDsIntFilter(findUUIDsFilterableInt, operator, value)
}

func findUUIDsIntFilter(prop string, operator filters.Operator, value int) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: operator,
			On: &filters.Path{
				Class:    schema.ClassName("FindUUIDsSlowLog"),
				Property: schema.PropertyName(prop),
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
