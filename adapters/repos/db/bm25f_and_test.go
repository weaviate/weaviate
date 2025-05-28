//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest

package db

import (
	"context"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/local/common_filters"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestBM25FJourneyBlockAnd(t *testing.T) {
	config.DefaultUsingBlockMaxWAND = true
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil, memwatch.NewDummyMonitor())
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.Background()))
	defer repo.Shutdown(context.Background())

	props := SetupClass(t, repo, schemaGetter, logger, 1.2, 0.75)

	idx := repo.GetIndex("MyClass")

	require.NotNil(t, idx)

	// Check basic search
	addit := additional.Properties{}
	for _, location := range []string{"memory", "disk"} {
		t.Run("bm25f text with AND "+location, func(t *testing.T) {
			kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "This is how we get to BM25F", SearchOperator: common_filters.SearchOperatorAnd}
			res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
			// Print results
			t.Log("--- Start results for search with AND ---")
			for i, r := range res {
				t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
			}
			require.Nil(t, err)

			// Check results in correct order
			require.Equal(t, uint64(0), res[0].DocID)
		})

		t.Run("bm25f text with AND == minimum should match with len(queryTerms) "+location, func(t *testing.T) {
			q := "This is how we get to BM25F"
			kwr1 := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: q, SearchOperator: common_filters.SearchOperatorAnd}
			res1, scores1, err := idx.objectSearch(context.TODO(), 1000, nil, kwr1, nil, nil, addit, nil, "", 0, props)

			require.Nil(t, err)

			kwr2 := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: q, SearchOperator: common_filters.SearchOperatorOr, MinimumOrTokensMatch: len(strings.Split(q, " "))}
			res2, scores2, err := idx.objectSearch(context.TODO(), 1000, nil, kwr2, nil, nil, addit, nil, "", 0, props)

			require.Nil(t, err)
			// Print results
			t.Log("--- Start results for search with AND ---")
			for i, r := range res1 {
				t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores1[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
			}
			for i, r := range res2 {
				t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores2[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
			}

			require.Equal(t, len(res1), len(res2))

			for i := 0; i < len(res1); i++ {
				require.Equal(t, res1[i].DocID, res2[i].DocID)
				require.Equal(t, scores1[i], scores2[i])
			}
		})

		// depending on the minimum should match, we will have a different number of results showing up
		expectedSizes := []int{3, 3, 2, 2, 2, 2, 1, 1}
		for minimumOrTokensMatch, expectedSize := range expectedSizes {
			t.Run("bm25f text with minimum should match with 0...len(queryTerms) "+location, func(t *testing.T) {
				kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "This is how we get to BM25F", MinimumOrTokensMatch: minimumOrTokensMatch}
				res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
				// Print results
				t.Log("--- Start results for search with AND ---")
				for i, r := range res {
					t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
				}
				require.Nil(t, err)
				require.Equal(t, expectedSize, len(res))
				require.Equal(t, uint64(0), res[0].DocID)

				// if minimumOrTokensMatch < 3, title and description will both match, and thus the score will be higher
				if minimumOrTokensMatch < 3 {
					EqualFloats(t, scores[0], 5.470736, 3)
				} else {
					EqualFloats(t, scores[0], 4.0164075, 3)
				}
			})
		}

	}
}

func TestBM25FJourneyAnd(t *testing.T) {
	config.DefaultUsingBlockMaxWAND = false
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil, memwatch.NewDummyMonitor())
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.Background()))
	defer repo.Shutdown(context.Background())

	props := SetupClass(t, repo, schemaGetter, logger, 1.2, 0.75)

	idx := repo.GetIndex("MyClass")

	require.NotNil(t, idx)

	// Check basic search
	addit := additional.Properties{}
	for _, location := range []string{"memory", "disk"} {
		t.Run("bm25f text with AND "+location, func(t *testing.T) {
			kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "This is how we get to BM25F", SearchOperator: common_filters.SearchOperatorAnd}
			res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
			// Print results
			t.Log("--- Start results for search with AND ---")
			for i, r := range res {
				t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
			}
			require.Nil(t, err)

			// Check results in correct order
			require.Equal(t, uint64(0), res[0].DocID)
		})

		t.Run("bm25f text with AND == minimum should match with len(queryTerms) "+location, func(t *testing.T) {
			q := "This is how we get to BM25F"
			kwr1 := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: q, SearchOperator: common_filters.SearchOperatorAnd}
			res1, scores1, err := idx.objectSearch(context.TODO(), 1000, nil, kwr1, nil, nil, addit, nil, "", 0, props)

			require.Nil(t, err)

			kwr2 := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: q, SearchOperator: common_filters.SearchOperatorOr, MinimumOrTokensMatch: len(strings.Split(q, " "))}
			res2, scores2, err := idx.objectSearch(context.TODO(), 1000, nil, kwr2, nil, nil, addit, nil, "", 0, props)

			require.Nil(t, err)
			// Print results
			t.Log("--- Start results for search with AND ---")
			for i, r := range res1 {
				t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores1[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
			}
			for i, r := range res2 {
				t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores2[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
			}

			require.Equal(t, len(res1), len(res2))

			for i := 0; i < len(res1); i++ {
				require.Equal(t, res1[i].DocID, res2[i].DocID)
				require.Equal(t, scores1[i], scores2[i])
			}
		})

		// depending on the minimum should match, we will have a different number of results showing up
		expectedSizes := []int{3, 3, 2, 2, 2, 2, 1, 1}
		for minimumOrTokensMatch, expectedSize := range expectedSizes {
			t.Run("bm25f text with minimum should match with 0...len(queryTerms) "+location, func(t *testing.T) {
				kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "This is how we get to BM25F", MinimumOrTokensMatch: minimumOrTokensMatch}
				res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
				// Print results
				t.Log("--- Start results for search with AND ---")
				for i, r := range res {
					t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
				}
				require.Nil(t, err)
				require.Equal(t, expectedSize, len(res))
				require.Equal(t, uint64(0), res[0].DocID)

				EqualFloats(t, scores[0], 3.4539468, 3)
			})
		}

	}
}
