//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest

package db

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
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

var (
	collectionSize            = 11000
	queryMaximumResults       = int64(10000)
	queryHybridMaximumResults = []int64{100, 10, 200, 1000}
)

func SetupPaginationTestData(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger, k1, b float32) []string {
	class := &models.Class{
		VectorIndexType:     "flat",
		VectorIndexConfig:   flat.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(k1, b, "none"),
		Class:               "PaginationTest",
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
			{
				Name:         "random_text",
				DataType:     []string{string(schema.DataTypeText)},
				Tokenization: "word",
			},
		},
	}

	props := make([]string, len(class.Properties))
	for i, prop := range class.Properties {
		props[i] = prop.Name
	}
	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	schemaGetter.schema = schema

	migrator := NewMigrator(repo, logger)
	migrator.AddClass(context.Background(), class, schemaGetter.shardState)

	// generate different ratios

	words := []string{"a", "b"}

	text := strings.Repeat(words[0]+" ", collectionSize) + strings.Repeat(words[1]+" ", collectionSize)

	// n+1 is necessary to ensure that we have an object with all words[0] and an object with all words[1]
	for i := 0; i < collectionSize+1; i++ {
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())

		// text: words[0] * i and words[1] * (n - i)
		// random_text: random dist of words[0] and words[1]
		random_text := strings.Repeat(words[0]+" ", rand.IntN(5)) + strings.Repeat(words[1]+" ", rand.IntN(4))

		data := map[string]interface{}{"title": fmt.Sprintf("%d", i), "text": text[i*2 : (i+collectionSize)*2], "random_text": random_text}

		// create a random vector
		vector := generateVector()

		obj := &models.Object{Class: "PaginationTest", ID: id, Properties: data}
		err := repo.PutObject(context.Background(), obj, vector, nil, nil, nil, 0)
		require.Nil(t, err)
	}
	return props
}

func generateVector() []float32 {
	// floatValue := float32(n)
	// return distancer.Normalize([]float32{floatValue, floatValue, floatValue, floatValue, floatValue * floatValue})
	// random vector with 5 dimensions, ignore the n
	return distancer.Normalize(randomVector(getRandomSeed(), 5))
}

func TestHybridOffsets(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushDirtyAfter:  99999,
		MemtablesMaxActiveSeconds: 99999,
		MemtablesMaxSizeMB:        1000,
		RootPath:                  dirName,
		QueryMaximumResults:       queryMaximumResults,
		QueryHybridMaximumResults: queryHybridMaximumResults[0],
		MaxImportGoroutinesFactor: 60,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	SetupPaginationTestData(t, repo, schemaGetter, logger, 1.2, 0.75)

	idx := repo.GetIndex("PaginationTest")
	require.NotNil(t, idx)

	queries := [][2]interface{}{
		{"a", generateVector()},
		{"a b", generateVector()},
		{"b", generateVector()},
	}

	for _, queryHybridMaximumResult := range queryHybridMaximumResults {
		repo.config.QueryHybridMaximumResults = queryHybridMaximumResult
		repo.config.QueryMaximumResults = queryMaximumResults

		myConfig := config.Config{
			QueryDefaults: config.QueryDefaults{
				Limit: queryMaximumResults,
			},
			QueryMaximumResults:       queryMaximumResults,
			QueryHybridMaximumResults: queryHybridMaximumResult,
		}

		pageSize := int(queryHybridMaximumResult / 10)
		paginations := []filters.Pagination{
			// base case, no offset, limit is the maximum results. This is the ground truth for the other cases
			// must be at paginations index 0, as it is the base case and used to compare the other cases against
			{Offset: 0, Limit: int(queryHybridMaximumResult)},
			// normal pagination cases, offset is i*pageSize and limit is the page size
			{Offset: 0, Limit: pageSize},
			{Offset: pageSize, Limit: pageSize},
			{Offset: pageSize * 2, Limit: pageSize},
			{Offset: pageSize * 3, Limit: pageSize},
			{Offset: pageSize * 4, Limit: pageSize},
			{Offset: pageSize * 9, Limit: pageSize},
			// will fail, as the offset + limit exceeds the maximum results
			{Offset: pageSize * 10, Limit: pageSize},
			// "uneven" case
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

		for _, location := range []string{"memory", "disk"} {
			for _, queryAndVector := range queries {
				log, _ := test.NewNullLogger()
				explorer := traverser.NewExplorer(repo, log, nil, nil, myConfig)
				explorer.SetSchemaGetter(schemaGetter)
				for _, alpha := range []float64{0.0, 0.5, 1.0} {
					query := queryAndVector[0].(string)
					vector := queryAndVector[1].([]float32)

					gtResults := make([]uint64, 0)
					gtScores := make([]float32, 0)
					for p, pagination := range paginations {
						t.Run(fmt.Sprintf("hybrid search offset test (%s) (maximum hybrid %d) query '%s' alpha %.2f pagination %d-%d ", location, queryHybridMaximumResult, query, alpha, pagination.Offset, pagination.Offset+pagination.Limit), func(t *testing.T) {
							params := dto.GetParams{
								ClassName: "PaginationTest",
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
								t.Logf("Not validating the results for pagination as offset %d + limit %d > %d: results %d", pagination.Offset, pagination.Limit, int(queryHybridMaximumResult), len(hybridResults))
								return
							}
							require.Nil(t, err)

							if p == 0 {
								for _, res := range hybridResults {
									gtResults = append(gtResults, *res.DocID)
									gtScores = append(gtScores, res.Score)
								}
							} else {
								if pagination.Limit != -1 && pagination.Offset+pagination.Limit > int(queryHybridMaximumResult) {
									// no need to check the results, as this is the exception where we override the maximum results with an offset of zero
									t.Logf("Skipping result check for pagination offset %d + limit %d > %d", pagination.Offset, pagination.Limit, int(queryHybridMaximumResult))
									return
								}

								for rank, res := range hybridResults {
									innerRank := rank + pagination.Offset
									require.Equal(t, gtResults[innerRank], *res.DocID, "DocID mismatch at rank %d", innerRank+1)
									require.Equal(t, gtScores[innerRank], res.Score, "Score mismatch at rank %d", innerRank+1)
								}

							}
						})
					}
				}
			}
			idx.ForEachShard(func(name string, shard ShardLike) error {
				err := shard.Store().FlushMemtables(context.Background())
				require.Nil(t, err)
				return nil
			})
		}
	}
}
