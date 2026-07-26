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
	"sort"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	sortLimitTotalObjects = 600
	sortLimitFilterFrom   = 100 // price >= 100 matches 500 of the 600 objects
	sortLimitQueryLimit   = 10
)

type sortLimitObj struct {
	id        strfmt.UUID
	price     int
	sortValue int
	code      string
}

// sortLimitFixture builds objects whose filter-traversal order and sort order
// are exact opposites: price ascends with i, sortValue descends with i. The
// inverted index is walked in ascending key order, so the first N rows a
// `price >= 100` walk yields are i=100..109, while the correct top-N for
// `sortValue asc` is i=599..590. The two sets are disjoint by construction,
// which is what makes a passing result meaningful.
func sortLimitFixture() []sortLimitObj {
	out := make([]sortLimitObj, sortLimitTotalObjects)
	for i := range out {
		out[i] = sortLimitObj{
			id:        strfmt.UUID(fmt.Sprintf("%08d-0000-0000-0000-%012d", i, i)),
			price:     i,
			sortValue: sortLimitTotalObjects - 1 - i,
			code:      fmt.Sprintf("obj-%04d", i),
		}
	}
	return out
}

func sortLimitClass(name string, rangeableOnPrice bool) *models.Class {
	return &models.Class{
		Class:               name,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{
				Name:              "price",
				DataType:          schema.DataTypeInt.PropString(),
				IndexFilterable:   boolRef(!rangeableOnPrice),
				IndexRangeFilters: boolRef(rangeableOnPrice),
			},
			{
				Name:              "sortValue",
				DataType:          schema.DataTypeInt.PropString(),
				IndexFilterable:   boolRef(true),
				IndexRangeFilters: boolRef(false),
			},
			{
				Name:            "code",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationField,
				IndexFilterable: boolRef(true),
				IndexSearchable: boolRef(false),
			},
		},
	}
}

func sortLimitFilter(className, propName string, value interface{},
	operator filters.Operator, dt schema.DataType,
) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: operator,
			On: &filters.Path{
				Class:    schema.ClassName(className),
				Property: schema.PropertyName(propName),
			},
			Value: &filters.Value{Value: value, Type: dt},
		},
	}
}

// expectedTopN computes ground truth without touching the query path: keep
// everything the predicate matches, order it by sortValue ascending, take n.
func expectedTopN(data []sortLimitObj, match func(sortLimitObj) bool, n int) []sortLimitObj {
	matched := make([]sortLimitObj, 0, len(data))
	for _, o := range data {
		if match(o) {
			matched = append(matched, o)
		}
	}
	sort.Slice(matched, func(a, b int) bool {
		return matched[a].sortValue < matched[b].sortValue
	})
	if n > len(matched) {
		n = len(matched)
	}
	return matched[:n]
}

func sortLimitDescribe(t *testing.T, res []search.Result, n int) string {
	t.Helper()
	if n > len(res) {
		n = len(res)
	}
	out := ""
	for i := 0; i < n; i++ {
		props, _ := res[i].Schema.(map[string]interface{})
		out += fmt.Sprintf("\n  [%2d] id=%s price=%v sortValue=%v",
			i, res[i].ID, props["price"], props["sortValue"])
	}
	return out
}

func sortLimitAssertTopN(t *testing.T, res []search.Result, want []sortLimitObj) {
	t.Helper()

	gotIDs := make([]strfmt.UUID, 0, len(want))
	gotSortValues := make([]interface{}, 0, len(want))
	for i := 0; i < len(want) && i < len(res); i++ {
		gotIDs = append(gotIDs, res[i].ID)
		props, _ := res[i].Schema.(map[string]interface{})
		gotSortValues = append(gotSortValues, props["sortValue"])
	}

	wantIDs := make([]strfmt.UUID, len(want))
	wantSortValues := make([]int, len(want))
	for i, o := range want {
		wantIDs[i] = o.id
		wantSortValues[i] = o.sortValue
	}

	require.GreaterOrEqualf(t, len(res), len(want),
		"want %d results, got %d", len(want), len(res))
	require.Equalf(t, wantIDs, gotIDs,
		"wrong top-%d selected.\nwant sortValues %v\ngot  sortValues %v\ngot results:%s",
		len(want), wantSortValues, gotSortValues, sortLimitDescribe(t, res, len(want)))
}

func setupSortLimitRepo(t *testing.T) *DB {
	t.Helper()

	dirName := t.TempDir()
	logger := logrus.New()
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(&models.Class{Class: className}, shardState)
		}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).
		Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).
		Return([]string{"node1"}, nil).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil,
		memwatch.NewDummyMonitor(), mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	filterableClass := sortLimitClass("SortLimitFilterable", false)
	rangeableClass := sortLimitClass("SortLimitRangeable", true)

	migrator := NewMigrator(repo, logger, "node1")
	require.Nil(t, migrator.AddClass(context.Background(), filterableClass))
	require.Nil(t, migrator.AddClass(context.Background(), rangeableClass))
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{
		Classes: []*models.Class{filterableClass, rangeableClass},
	}}

	for _, className := range []string{"SortLimitFilterable", "SortLimitRangeable"} {
		for _, o := range sortLimitFixture() {
			require.Nil(t, repo.PutObject(context.Background(), &models.Object{
				ID:    o.id,
				Class: className,
				Properties: map[string]interface{}{
					"price":     int64(o.price),
					"sortValue": int64(o.sortValue),
					"code":      o.code,
				},
			}, []float32{0.1, 0.2, 0.01, 0.2}, nil, nil, nil, 0))
		}
	}

	return repo
}

// Pins: a single-clause `where` on a filterable-only property truncates the
// allow list to `limit` docs in index-traversal order before the sort runs, so
// `where`+`sort`+`limit` returns the wrong N objects.
func TestKnownRedSortLimitTruncatesAllowListBeforeSorting(t *testing.T) {
	repo := setupSortLimitRepo(t)
	data := sortLimitFixture()

	priceMatch := func(o sortLimitObj) bool { return o.price >= sortLimitFilterFrom }
	codeMatch := func(o sortLimitObj) bool { return true } // code LIKE "obj-*"

	sortAsc := []filters.Sort{{Path: []string{"sortValue"}, Order: "asc"}}

	query := func(t *testing.T, className string, filter *filters.LocalFilter, limit int) []search.Result {
		t.Helper()
		res, err := repo.Search(context.Background(), dto.GetParams{
			ClassName:  className,
			Filters:    filter,
			Sort:       sortAsc,
			Pagination: &filters.Pagination{Limit: limit},
			Properties: search.SelectProperties{{Name: "price"}, {Name: "sortValue"}},
		})
		require.Nil(t, err)
		return res
	}

	// Positive control: the identical query shape with a limit larger than the
	// match set. If this fails the fixture is broken and nothing below means
	// anything.
	t.Run("control: filterable range filter, limit larger than match set", func(t *testing.T) {
		filter := sortLimitFilter("SortLimitFilterable", "price", sortLimitFilterFrom,
			filters.OperatorGreaterThanEqual, schema.DataTypeInt)
		res := query(t, "SortLimitFilterable", filter, sortLimitTotalObjects)
		require.Len(t, res, sortLimitTotalObjects-sortLimitFilterFrom)
		sortLimitAssertTopN(t, res, expectedTopN(data, priceMatch, sortLimitQueryLimit))
	})

	// Second control: the rangeable branch ignores the limit entirely, so the
	// same logical query must be correct there.
	t.Run("control: rangeable range filter, limit 10", func(t *testing.T) {
		filter := sortLimitFilter("SortLimitRangeable", "price", sortLimitFilterFrom,
			filters.OperatorGreaterThanEqual, schema.DataTypeInt)
		res := query(t, "SortLimitRangeable", filter, sortLimitQueryLimit)
		sortLimitAssertTopN(t, res, expectedTopN(data, priceMatch, sortLimitQueryLimit))
	})

	t.Run("red: filterable range filter, limit 10", func(t *testing.T) {
		filter := sortLimitFilter("SortLimitFilterable", "price", sortLimitFilterFrom,
			filters.OperatorGreaterThanEqual, schema.DataTypeInt)
		res := query(t, "SortLimitFilterable", filter, sortLimitQueryLimit)
		sortLimitAssertTopN(t, res, expectedTopN(data, priceMatch, sortLimitQueryLimit))
	})

	t.Run("red: filterable like filter, limit 10", func(t *testing.T) {
		filter := sortLimitFilter("SortLimitFilterable", "code", "obj-*",
			filters.OperatorLike, schema.DataTypeText)
		res := query(t, "SortLimitFilterable", filter, sortLimitQueryLimit)
		sortLimitAssertTopN(t, res, expectedTopN(data, codeMatch, sortLimitQueryLimit))
	})
}
