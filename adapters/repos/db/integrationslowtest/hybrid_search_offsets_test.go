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

package integrationslowtest

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/traverser"
)

const (
	paginationClassName       = "PaginationTest"
	paginationCollectionSize  = 1100
	paginationMaximumResults  = int64(1000)
	paginationHybridDimension = 5
)

var paginationHybridMaximumResults = []int64{10, 100, 1000}

func seedPaginationObjects(t *testing.T, repo *db.DB, seed *rand.Rand) {
	t.Helper()
	words := []string{"a", "b"}
	text := strings.Repeat(words[0]+" ", paginationCollectionSize) + strings.Repeat(words[1]+" ", paginationCollectionSize)
	for i := 0; i < paginationCollectionSize+1; i++ {
		data := map[string]interface{}{"title": fmt.Sprintf("%d", i), "text": text[i*2 : (i+paginationCollectionSize)*2]}
		obj := &models.Object{Class: paginationClassName, ID: intToUUID(i), Properties: data}
		require.NoError(t, repo.PutObject(context.Background(), obj, paginationVector(seed), nil, nil, nil, 0))
	}
}

func paginationVector(seed *rand.Rand) []float32 {
	vec := make([]float32, paginationHybridDimension)
	for i := range vec {
		vec[i] = seed.Float32()
	}
	return distancer.Normalize(vec)
}

func TestHybridOffsets(t *testing.T) {
	seed := rand.New(rand.NewSource(time.Now().UnixNano()))
	class := &models.Class{
		VectorIndexType:     "flat",
		VectorIndexConfig:   flat.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(1.2, 0.75, "none"),
		Class:               paginationClassName,
		Properties: []*models.Property{
			{
				Name:         "title",
				DataType:     []string{string(schema.DataTypeText)},
				Tokenization: "word",
			},
			{
				Name:         "text",
				DataType:     []string{string(schema.DataTypeText)},
				Tokenization: "word",
			},
		},
	}
	repo, schemaGetter := newRepo(t, repoParams{config: func(c *db.Config) {
		c.MemtablesFlushDirtyAfter = 99999
		c.MemtablesMaxActiveSeconds = 99999
		c.MemtablesMaxSizeMB = 1000
		c.QueryMaximumResults = paginationMaximumResults
		c.QueryHybridMaximumResults = paginationHybridMaximumResults[0]
		c.MaxImportGoroutinesFactor = 60
		c.EnableLazyLoadShards = nil
	}}, class)
	seedPaginationObjects(t, repo, seed)

	idx := repo.GetIndex(paginationClassName)
	require.NotNil(t, idx)

	queries := [][2]interface{}{
		{"a", paginationVector(seed)},
		{"a b", paginationVector(seed)},
	}

	for _, location := range []string{"memory", "disk"} {
		for _, queryHybridMaximumResult := range paginationHybridMaximumResults {
			myConfig := config.Config{
				QueryDefaults: config.QueryDefaults{
					Limit: paginationMaximumResults,
				},
				QueryMaximumResults:       paginationMaximumResults,
				QueryHybridMaximumResults: queryHybridMaximumResult,
			}
			pageSize := int(queryHybridMaximumResult / 10)
			paginations := []filters.Pagination{
				// base case, offset is 0 and limit is the maximum results
				{Offset: 0, Limit: int(queryHybridMaximumResult)},
				// normal pagination cases, offset is i*pageSize and limit is the page size
				{Offset: 0, Limit: pageSize},
				{Offset: pageSize, Limit: pageSize},
				{Offset: pageSize * 9, Limit: pageSize},
				// will fail, as the offset + limit exceeds the maximum results
				{Offset: pageSize * 10, Limit: pageSize},
				// "uneven" limit case
				{Offset: 1, Limit: 7},
				// same as Offset: 0, Limit: int(queryHybridMaximumResult)
				{Offset: 0, Limit: -1},
				// will NOT fail, as the offset is 0 and the limit is the maximum results.
				// This is a special case, where we override the maximum results with an offset of zero.
				// May return different results than the base case, as the offset is 0 and the limit is the maximum results.
				{Offset: 0, Limit: int(queryHybridMaximumResult) * 10},
				// will NOT fail, but may return results after queryHybridMaximumResult
				// May return different results than the base case, and will not be evaluated against the ground trut
				{Offset: 1, Limit: -1},
				// will fail with an error, as it exceeds the maximum results
				{Offset: pageSize, Limit: int(queryHybridMaximumResult)},
			}
			for _, queryAndVector := range queries {
				log, _ := test.NewNullLogger()
				explorer := traverser.NewExplorer(repo, log, nil, nil, myConfig)
				explorer.SetSchemaGetter(schemaGetter)
				for _, alpha := range []float64{0.0, 0.5, 1.0} {
					query := queryAndVector[0].(string)
					vector := queryAndVector[1].([]float32)

					gtResults := make(map[uint64]float32, 0)
					for p, pagination := range paginations {
						t.Run(fmt.Sprintf("hybrid search offset test (%s) (maximum hybrid %d) query '%s' alpha %.2f pagination %d:%d", location, queryHybridMaximumResult, query, alpha, pagination.Offset, pagination.Offset+pagination.Limit), func(t *testing.T) {
							params := dto.GetParams{
								ClassName: paginationClassName,
								HybridSearch: &searchparams.HybridSearch{
									Query:  query,
									Vector: vector,
									Alpha:  alpha,
								},
								Pagination: &pagination,
								Properties: search.SelectProperties{search.SelectProperty{Name: "title"}, search.SelectProperty{Name: "text"}},
								AdditionalProperties: additional.Properties{
									ExplainScore: true,
								},
							}

							hybridResults, err := explorer.Hybrid(context.TODO(), params)
							if pagination.Offset+pagination.Limit > int(queryHybridMaximumResult) {
								return
							}
							require.Nil(t, err)

							if p == 0 {
								for _, res := range hybridResults {
									gtResults[*res.DocID] = res.Score
								}
							} else {
								// Limit == -1 with an offset can include items beyond the ground-truth set.
								if pagination.Limit == -1 && pagination.Offset > 0 {
									return
								}
								if pagination.Limit != -1 && pagination.Offset+pagination.Limit > int(queryHybridMaximumResult) {
									// no need to check the results, as this is the exception where we override the maximum results with an offset of zero
									return
								}

								for rank, res := range hybridResults {
									innerRank := rank + pagination.Offset
									require.Equal(t, gtResults[*res.DocID], res.Score, "Score mismatch at rank %d", innerRank+1)
								}
							}
						})
					}
				}
			}
		}
		require.NoError(t, idx.ForEachShard(func(name string, shard db.ShardLike) error {
			return shard.Store().FlushMemtables(context.Background())
		}))
	}
}
