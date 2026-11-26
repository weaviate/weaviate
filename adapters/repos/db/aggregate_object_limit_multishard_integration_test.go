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
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/clients"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	objectLimitTestClass = "AggregateObjectLimitMultiShard"
	// hybridDefaultLimitForTest is the hybrid limit when objectLimit is omitted
	hybridDefaultLimitForTest = 15
)

// objectLimitTestObject is one fixture object: its vector, and a rank that
// identifies it in the aggregated sum.
type objectLimitTestObject struct {
	id     strfmt.UUID
	vector []float32
	rank   int
}

// Test_Aggregate_ObjectLimit_MultiShard pins that objectLimit bounds a vector
// or hybrid Aggregate globally: the aggregation must cover exactly the
// objectLimit best matches of the whole collection, not objectLimit per shard.
func Test_Aggregate_ObjectLimit_MultiShard(t *testing.T) {
	ctx := context.Background()
	repo, shardState := newMultiShardAggregateTestRepo(t)

	objects := objectLimitTestObjects(60)
	for _, obj := range objects {
		require.NoError(t, repo.PutObject(ctx, &models.Object{
			Class:      objectLimitTestClass,
			ID:         obj.id,
			Properties: map[string]interface{}{"rank": int64(obj.rank), "name": fmt.Sprintf("object %d", obj.rank)},
		}, obj.vector, nil, nil, nil, 0))
	}

	// the fixture must actually spread over every shard, otherwise this test
	// cannot observe a per-shard limit
	perShard := map[string]int{}
	for _, obj := range objects {
		perShard[shardState.PhysicalShard([]byte(obj.id))]++
	}
	require.Len(t, perShard, len(shardState.AllPhysicalShards()), "objects per shard: %v", perShard)

	query := []float32{1, 0, 0}

	for _, limit := range []int{1, 10, 25} {
		wantRanks := topRanksByCosine(objects, query, limit)

		t.Run(fmt.Sprintf("nearVector, objectLimit=%d", limit), func(t *testing.T) {
			res, err := repo.Aggregate(ctx, objectLimitAggregateParams(limit, query, nil), nil)
			require.NoError(t, err)
			assertAggregatedRanks(t, res, wantRanks)
		})

		t.Run(fmt.Sprintf("hybrid alpha=1, objectLimit=%d", limit), func(t *testing.T) {
			hybrid := &searchparams.HybridSearch{Query: "object", Alpha: 1, Vector: query}
			res, err := repo.Aggregate(ctx, objectLimitAggregateParams(limit, nil, hybrid), nil)
			require.NoError(t, err)
			assertAggregatedRanks(t, res, wantRanks)
		})
	}

	t.Run("nearVector, objectLimit larger than the collection", func(t *testing.T) {
		res, err := repo.Aggregate(ctx, objectLimitAggregateParams(1000, query, nil), nil)
		require.NoError(t, err)
		assertAggregatedRanks(t, res, topRanksByCosine(objects, query, len(objects)))
	})

	t.Run("nearVector, objectLimit with groupBy", func(t *testing.T) {
		limit := 10
		params := objectLimitAggregateParams(limit, query, nil)
		params.GroupBy = &filters.Path{Class: objectLimitTestClass, Property: "name"}
		res, err := repo.Aggregate(ctx, params, nil)
		require.NoError(t, err)

		total := 0
		for _, g := range res.Groups {
			total += g.Count
		}
		assert.Equal(t, limit, total, "objects counted over all groups")
	})
}

func objectLimitAggregateParams(limit int, vector []float32, hybrid *searchparams.HybridSearch) aggregation.Params {
	params := aggregation.Params{
		ClassName:        schema.ClassName(objectLimitTestClass),
		IncludeMetaCount: true,
		ObjectLimit:      &limit,
		Hybrid:           hybrid,
		Properties: []aggregation.ParamProperty{{
			Name:        "rank",
			Aggregators: []aggregation.Aggregator{aggregation.CountAggregator, aggregation.SumAggregator},
		}},
	}
	if vector != nil {
		params.SearchVector = vector
	}
	return params
}

func assertAggregatedRanks(t *testing.T, res *aggregation.Result, wantRanks []int) {
	t.Helper()
	require.NotNil(t, res)
	require.Len(t, res.Groups, 1)

	wantSum := 0
	for _, r := range wantRanks {
		wantSum += r
	}

	group := res.Groups[0]
	assert.Equal(t, len(wantRanks), group.Count, "meta count")
	rank := group.Properties["rank"].NumericalAggregations
	assert.Equal(t, float64(len(wantRanks)), rank["count"], "rank count")
	if len(wantRanks) == 0 {
		// an aggregation over no objects has no sum, on one shard as on many
		assert.Nil(t, rank["sum"], "sum of ranks over no objects")
		return
	}
	assert.Equal(t, float64(wantSum), rank["sum"], "sum of ranks: must be the global top %d", len(wantRanks))
}

// objectLimitTestObjects returns n objects on a quarter circle in the x/y
// plane, so every object has a distinct cosine distance to (1, 0, 0) and the
// expected top-k is unambiguous.
func objectLimitTestObjects(n int) []objectLimitTestObject {
	objects := make([]objectLimitTestObject, n)
	for i := range objects {
		angle := float64(i) * (math.Pi / 2) / float64(n)
		objects[i] = objectLimitTestObject{
			id:     strfmt.UUID(uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("object-limit-%d", i))).String()),
			vector: []float32{float32(math.Cos(angle)), float32(math.Sin(angle)), 0},
			rank:   i + 1,
		}
	}
	return objects
}

// topRanksByCosine is the brute-force reference: the ranks of the k objects
// closest to query by cosine distance.
func topRanksByCosine(objects []objectLimitTestObject, query []float32, k int) []int {
	type scored struct {
		rank int
		dist float64
	}
	all := make([]scored, len(objects))
	for i, obj := range objects {
		var dot, na, nb float64
		for d := range query {
			dot += float64(query[d]) * float64(obj.vector[d])
			na += float64(query[d]) * float64(query[d])
			nb += float64(obj.vector[d]) * float64(obj.vector[d])
		}
		all[i] = scored{rank: obj.rank, dist: 1 - dot/math.Sqrt(na*nb)}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].dist < all[j].dist })
	if k > len(all) {
		k = len(all)
	}
	ranks := make([]int, k)
	for i := range ranks {
		ranks[i] = all[i].rank
	}
	return ranks
}

// newMultiShardAggregateTestRepo starts a real single-node DB whose test class
// is split over the three shards of fixedMultiShardState.
func newMultiShardAggregateTestRepo(t *testing.T) (*DB, *sharding.State) {
	t.Helper()
	return newAggregateTestRepo(t, fixedMultiShardState())
}

// newAggregateTestRepo starts a real single-node DB whose test class uses
// shardState.
func newAggregateTestRepo(t *testing.T, shardState *sharding.State) (*DB, *sharding.State) {
	t.Helper()

	var repo *DB
	srv := httptest.NewServer(realShardCountHandler(func() *DB { return repo }))
	t.Cleanup(srv.Close)

	logger := logrus.New()
	class := &models.Class{
		Class:               objectLimitTestClass,
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		Properties: []*models.Property{
			{Name: "rank", DataType: schema.DataTypeInt.PropString()},
			{Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace},
		},
	}
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		return readFunc(&models.Class{Class: className}, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return(srv.URL[7:], true).Maybe()
	replicaClient, err := clients.NewReplicationClient(&http.Client{})
	require.NoError(t, err)

	repo, err = New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10000,
		QueryHybridMaximumResults: hybridDefaultLimitForTest,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, replicaClient, nil, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.NoError(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(testCtx()))
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	require.NoError(t, NewMigrator(repo, logger, "node1").AddClass(context.Background(), class))
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}
	return repo, shardState
}

// Test_Aggregate_RankedSelection_MultiShardMatchesSingleShard pins that every
// ranked aggregation (vector or hybrid search with a limit) over a collection
// split into several shards returns exactly what the same aggregation returns
// over a single shard holding the same data.
func Test_Aggregate_RankedSelection_MultiShardMatchesSingleShard(t *testing.T) {
	ctx := context.Background()
	single, _ := newAggregateTestRepo(t, singleShardState())
	multi, _ := newMultiShardAggregateTestRepo(t)

	objects := objectLimitTestObjects(60)
	for _, repo := range []*DB{single, multi} {
		for _, obj := range objects {
			// half of the objects also match the BM25 query "even", so the
			// sparse leg of a hybrid search ranks a different set than the
			// dense leg
			name := fmt.Sprintf("object %d", obj.rank)
			if obj.rank%2 == 0 {
				name += " even"
			}
			require.NoError(t, repo.PutObject(ctx, &models.Object{
				Class:      objectLimitTestClass,
				ID:         obj.id,
				Properties: map[string]interface{}{"rank": int64(obj.rank), "name": name},
			}, obj.vector, nil, nil, nil, 0))
		}
	}

	query := []float32{1, 0, 0}
	rankAbove := func(min int) *filters.LocalFilter {
		return &filters.LocalFilter{Root: &filters.Clause{
			Operator: filters.OperatorGreaterThan,
			On:       &filters.Path{Class: objectLimitTestClass, Property: "rank"},
			Value:    &filters.Value{Value: min, Type: schema.DataTypeInt},
		}}
	}
	limit := func(n int) *int { return &n }

	cases := []struct {
		name   string
		params func() aggregation.Params
	}{
		{"nearVector", func() aggregation.Params { return objectLimitAggregateParams(10, query, nil) }},
		{"nearVector with filter", func() aggregation.Params {
			p := objectLimitAggregateParams(10, query, nil)
			p.Filters = rankAbove(20)
			return p
		}},
		{"nearVector with certainty", func() aggregation.Params {
			p := objectLimitAggregateParams(30, query, nil)
			p.Certainty = 0.99
			return p
		}},
		{"nearVector matching nothing", func() aggregation.Params {
			p := objectLimitAggregateParams(10, query, nil)
			p.Filters = rankAbove(1000)
			return p
		}},
		{"nearVector with groupBy", func() aggregation.Params {
			p := objectLimitAggregateParams(10, query, nil)
			p.GroupBy = &filters.Path{Class: objectLimitTestClass, Property: "name"}
			return p
		}},
	}

	// vector distances do not depend on how the data is split, so a vector
	// aggregation over three shards must equal the one over a single shard
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want, err := single.Aggregate(ctx, tc.params(), nil)
			require.NoError(t, err)
			got, err := multi.Aggregate(ctx, tc.params(), nil)
			require.NoError(t, err)

			assert.ElementsMatch(t, aggregateGroupSummaries(want), aggregateGroupSummaries(got))
		})
	}

	// BM25 scores do depend on the split (each shard has its own IDF, as for
	// Get), so a hybrid aggregation is checked against a hybrid search over the
	// same collection instead: it must cover exactly the union of the BM25 top
	// n and the vector top n of the whole collection, which is what fusion
	// keeps.
	hybridCases := []struct {
		name   string
		alpha  float64
		limit  *int
		filter *filters.LocalFilter
	}{
		{name: "hybrid alpha=0", alpha: 0, limit: limit(10)},
		{name: "hybrid alpha=0.5", alpha: 0.5, limit: limit(10)},
		{name: "hybrid alpha=1", alpha: 1, limit: limit(10)},
		{name: "hybrid alpha=0.5 with filter", alpha: 0.5, limit: limit(10), filter: rankAbove(20)},
		{name: "hybrid alpha=0.5 matching nothing", alpha: 0.5, limit: limit(10), filter: rankAbove(1000)},
		{name: "hybrid without objectLimit", alpha: 0.5, limit: nil},
	}
	for _, repoCase := range []struct {
		name string
		repo *DB
	}{{"single shard", single}, {"three shards", multi}} {
		for _, tc := range hybridCases {
			t.Run(repoCase.name+", "+tc.name, func(t *testing.T) {
				n := hybridDefaultLimitForTest
				if tc.limit != nil {
					n = *tc.limit
				}
				wantRanks := hybridSearchRanks(t, repoCase.repo, "even", query, tc.alpha, n, tc.filter)

				p := objectLimitAggregateParams(10, nil, &searchparams.HybridSearch{Query: "even", Alpha: tc.alpha, Vector: query})
				p.ObjectLimit = tc.limit
				p.Filters = tc.filter
				res, err := repoCase.repo.Aggregate(ctx, p, nil)
				require.NoError(t, err)
				assertAggregatedRanks(t, res, wantRanks)
			})
		}
	}

	t.Run("three shards, hybrid with groupBy", func(t *testing.T) {
		wantRanks := hybridSearchRanks(t, multi, "even", query, 0.5, 10, nil)
		p := objectLimitAggregateParams(10, nil, &searchparams.HybridSearch{Query: "even", Alpha: 0.5, Vector: query})
		p.GroupBy = &filters.Path{Class: objectLimitTestClass, Property: "name"}
		p.Limit = limit(100)
		res, err := multi.Aggregate(ctx, p, nil)
		require.NoError(t, err)

		total := 0
		for _, g := range res.Groups {
			total += g.Count
		}
		assert.Equal(t, len(wantRanks), total, "objects counted over all groups")
	})
}

// hybridSearchRanks is the reference for a hybrid aggregation: the ranks of
// the union of the BM25 top n (alpha < 1) and the vector top n (alpha > 0) of
// the whole collection, found with the same searches as Get.
func hybridSearchRanks(t *testing.T, repo *DB, query string, vector []float32,
	alpha float64, n int, filter *filters.LocalFilter,
) []int {
	t.Helper()
	ctx := context.Background()
	ranks := map[int]struct{}{}

	if alpha < 1 {
		objs, _, err := repo.SparseObjectSearch(ctx, dto.GetParams{
			ClassName:      objectLimitTestClass,
			Filters:        filter,
			KeywordRanking: &searchparams.KeywordRanking{Type: "bm25", Query: query, Properties: []string{"name"}},
			Pagination:     &filters.Pagination{Limit: n},
		})
		require.NoError(t, err)
		for _, obj := range objs {
			ranks[rankOf(t, obj.Properties())] = struct{}{}
		}
	}
	if alpha > 0 {
		res, err := repo.VectorSearch(ctx, dto.GetParams{
			ClassName:  objectLimitTestClass,
			Filters:    filter,
			Pagination: &filters.Pagination{Limit: n},
			Properties: search.SelectProperties{{Name: "rank"}},
		}, []string{""}, []models.Vector{vector})
		require.NoError(t, err)
		for _, r := range res {
			ranks[rankOf(t, r.Schema)] = struct{}{}
		}
	}

	out := make([]int, 0, len(ranks))
	for r := range ranks {
		out = append(out, r)
	}
	sort.Ints(out)
	return out
}

func rankOf(t *testing.T, props interface{}) int {
	t.Helper()
	m, ok := props.(map[string]interface{})
	require.True(t, ok, "properties are %T", props)
	switch v := m["rank"].(type) {
	case float64:
		return int(v)
	case int64:
		return int(v)
	default:
		require.Failf(t, "unexpected rank type", "%T", v)
		return 0
	}
}

// aggregateGroupSummaries flattens a result into comparable per-group lines,
// independent of group order.
func aggregateGroupSummaries(res *aggregation.Result) []string {
	if res == nil {
		return nil
	}
	out := make([]string, len(res.Groups))
	for i, g := range res.Groups {
		var group interface{}
		if g.GroupedBy != nil {
			group = g.GroupedBy.Value
		}
		rank := g.Properties["rank"].NumericalAggregations
		out[i] = fmt.Sprintf("group=%v count=%d rankCount=%v rankSum=%v", group, g.Count, rank["count"], rank["sum"])
	}
	return out
}
