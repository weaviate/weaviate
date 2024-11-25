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
	"fmt"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func TestBM25FJourneyBlockMem(t *testing.T) {
	t.Setenv("USE_INVERTED_SEARCHABLE", "true")
	t.Setenv("USE_BLOCKMAX_WAND", "true")
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
	addit := additional.Properties{Vector: true}

	t.Run("bm25f journey", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description", "textField"}, Query: "journey"}
		res, scores, err := idx.objectSearch(context.Background(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)

		// Print results
		t.Log("--- Start results for basic search ---")
		for i, r := range res {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		// Check results in correct order
		require.Equal(t, uint64(4), res[0].DocID)
		require.Equal(t, uint64(5), res[1].DocID)
		require.Equal(t, uint64(6), res[2].DocID)
		require.Equal(t, uint64(2), res[3].DocID)
		require.Equal(t, uint64(3), res[4].DocID)
		require.Equal(t, uint64(0), res[5].DocID)

		// vectors should be returned
		require.NotNil(t, res[0].Vector)

		// Without additionalExplanations no explainScore entry should be present
		require.NotContains(t, res[0].Object.Additional, "explainScore")
	})

	// Check non-alpha search on string field

	// text/field are tokenized entirely, so we can search for non-alpha characters
	t.Run("bm25f textField non-alpha", func(t *testing.T) {
		kwrTextField := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description", "textField"}, Query: "*&^$@#$%^&*()(Offtopic!!!!"}
		addit = additional.Properties{}
		resTextField, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwrTextField, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)

		// Print results
		t.Log("--- Start results for textField search ---")
		for i, r := range resTextField {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		// Check results in correct order
		require.Equal(t, uint64(7), resTextField[0].DocID)
	})

	// text/field are not lower-cased before indexing, so upper case searches must be passed through unchanged.
	t.Run("bm25f textField caps", func(t *testing.T) {
		kwrTextField := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"textField"}, Query: "YELLING IS FUN"}
		addit := additional.Properties{}
		resTextField, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwrTextField, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)

		// Print results
		t.Log("--- Start results for textField caps search ---")
		for i, r := range resTextField {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		// Check results in correct order
		require.Equal(t, uint64(8), resTextField[0].DocID)
	})

	// Check basic text search WITH CAPS
	t.Run("bm25f text with caps", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "JOURNEY"}
		res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		// Print results
		t.Log("--- Start results for search with caps ---")
		for i, r := range res {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}
		require.Nil(t, err)

		// Check results in correct order
		require.Equal(t, uint64(4), res[0].DocID)
		require.Equal(t, uint64(5), res[1].DocID)
		require.Equal(t, uint64(6), res[2].DocID)
		require.Equal(t, uint64(2), res[3].DocID)
		require.Equal(t, uint64(3), res[4].DocID)
		require.Equal(t, uint64(0), res[5].DocID)
		require.Equal(t, uint64(1), res[6].DocID)
	})

	t.Run("bm25f journey boosted", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title^3", "description"}, Query: "journey"}
		res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)

		require.Nil(t, err)
		// Print results
		t.Log("--- Start results for boosted search ---")
		for i, r := range res {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		// Check results in correct order
		require.Equal(t, uint64(4), res[0].DocID)
		require.Equal(t, uint64(5), res[1].DocID)
		require.Equal(t, uint64(6), res[2].DocID)
		require.Equal(t, uint64(2), res[3].DocID)
		require.Equal(t, uint64(0), res[4].DocID)
		require.Equal(t, uint64(1), res[5].DocID)
		require.Equal(t, uint64(3), res[6].DocID)
	})

	t.Run("Check search with two terms", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "journey somewhere"}
		res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Equal(t, len(scores), len(res))
		// Check results in correct order
		require.Equal(t, uint64(4), res[0].DocID)
		require.Equal(t, uint64(5), res[1].DocID)
		require.Equal(t, uint64(1), res[2].DocID)
		require.Equal(t, uint64(6), res[3].DocID)
		require.Equal(t, uint64(2), res[4].DocID)
	})

	t.Run("bm25f journey somewhere no properties", func(t *testing.T) {
		// Check search with no properties (should include all properties)
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{}, Query: "journey somewhere"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)

		// Check results in correct order
		require.Equal(t, uint64(4), res[0].DocID)
		require.Equal(t, uint64(5), res[1].DocID)
		require.Equal(t, uint64(1), res[2].DocID)
		require.Equal(t, uint64(6), res[3].DocID)
	})

	t.Run("bm25f non alphanums", func(t *testing.T) {
		// Check search with no properties (should include all properties)
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{}, Query: "*&^$@#$%^&*()(Offtopic!!!!"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)
		require.Equal(t, uint64(7), res[0].DocID)
	})

	t.Run("First result has high score", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"description"}, Query: "about BM25F"}
		res, _, err := idx.objectSearch(context.TODO(), 5, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)

		require.Equal(t, uint64(0), res[0].DocID)
		require.Len(t, res, 4) // four results have one of the terms
	})

	t.Run("More results than limit", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"description"}, Query: "journey"}
		res, _, err := idx.objectSearch(context.TODO(), 5, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)

		require.Equal(t, uint64(4), res[0].DocID)
		require.Equal(t, uint64(5), res[1].DocID)
		require.Equal(t, uint64(6), res[2].DocID)
		require.Equal(t, uint64(3), res[3].DocID)
		require.Equal(t, uint64(2), res[4].DocID)
		require.Len(t, res, 5) // four results have one of the terms
	})

	t.Run("Results from three properties", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Query: "none"}
		res, _, err := idx.objectSearch(context.TODO(), 5, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)

		require.Equal(t, uint64(9), res[0].DocID)
		require.Equal(t, uint64(8), res[1].DocID)
		require.Equal(t, uint64(0), res[2].DocID)
		require.Len(t, res, 3)
	})

	t.Run("Include additional explanations", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"description"}, Query: "journey", AdditionalExplanations: true}
		res, _, err := idx.objectSearch(context.TODO(), 5, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)

		// With additionalExplanations explainScore entry should be present
		require.Contains(t, res[0].Object.Additional, "explainScore")
		require.Contains(t, res[0].Object.Additional["explainScore"], "BM25")
	})

	t.Run("Array fields text", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"multiTitles"}, Query: "dinner"}
		res, _, err := idx.objectSearch(context.TODO(), 5, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)

		require.Len(t, res, 2)
		require.Equal(t, uint64(0), res[0].DocID)
		require.Equal(t, uint64(1), res[1].DocID)
	})

	t.Run("Array fields string", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"multiTextWhitespace"}, Query: "MuuultiYell!"}
		res, _, err := idx.objectSearch(context.TODO(), 5, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)

		require.Len(t, res, 2)
		require.Equal(t, uint64(6), res[0].DocID)
		require.Equal(t, uint64(5), res[1].DocID)
	})

	t.Run("With autocut", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Query: "journey", Properties: []string{"description"}}
		resNoAutoCut, noautocutscores, err := idx.objectSearch(context.TODO(), 10, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)

		resAutoCut, autocutscores, err := idx.objectSearch(context.TODO(), 10, nil, kwr, nil, nil, addit, nil, "", 1, props)
		require.Nil(t, err)

		require.Less(t, len(resAutoCut), len(resNoAutoCut))

		EqualFloats(t, float32(0.51602507), noautocutscores[0], 5)
		EqualFloats(t, float32(0.4975062), noautocutscores[1], 5) // <= autocut last element
		EqualFloats(t, float32(0.34149727), noautocutscores[2], 5)
		EqualFloats(t, float32(0.3049518), noautocutscores[3], 5)
		EqualFloats(t, float32(0.27547202), noautocutscores[4], 5)

		require.Len(t, resAutoCut, 2)
		EqualFloats(t, float32(0.51602507), autocutscores[0], 5)
		EqualFloats(t, float32(0.4975062), autocutscores[1], 5)
	})
}

func TestBM25FSinglePropBlockMem(t *testing.T) {
	t.Setenv("USE_INVERTED_SEARCHABLE", "true")
	t.Setenv("USE_BLOCKMAX_WAND", "true")
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
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	props := SetupClass(t, repo, schemaGetter, logger, 0.5, 100)

	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	// Check boosted
	kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"description"}, Query: "journey"}
	addit := additional.Properties{}
	res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
	t.Log("--- Start results for singleprop search ---")
	for i, r := range res {
		t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
	}
	require.Nil(t, err)
	// Check results in correct order
	require.Equal(t, uint64(3), res[0].DocID)
	require.Equal(t, uint64(6), res[3].DocID)

	// Check scores
	EqualFloats(t, float32(0.1248), scores[0], 5)
	EqualFloats(t, float32(0.0363), scores[1], 5)
}

func TestBM25FWithFiltersBlockMem(t *testing.T) {
	t.Setenv("USE_INVERTED_SEARCHABLE", "true")
	t.Setenv("USE_BLOCKMAX_WAND", "true")
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
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	props := SetupClass(t, repo, schemaGetter, logger, 0.5, 100)

	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	filter := &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: filters.OperatorOr,
			Operands: []filters.Clause{
				{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    schema.ClassName("MyClass"),
						Property: schema.PropertyName("title"),
					},
					Value: &filters.Value{
						Value: "My",
						Type:  schema.DataType("text"),
					},
				},
				{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    schema.ClassName("MyClass"),
						Property: schema.PropertyName("title"),
					},
					Value: &filters.Value{
						Value: "journeys",
						Type:  schema.DataType("text"),
					},
				},
			},
		},
	}

	kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"description"}, Query: "journey"}
	addit := additional.Properties{}
	res, _, err := idx.objectSearch(context.TODO(), 1000, filter, kwr, nil, nil, addit, nil, "", 0, props)

	require.Nil(t, err)
	require.True(t, len(res) == 1)
	require.Equal(t, uint64(2), res[0].DocID)
}

func TestBM25FWithFilters_ScoreIsIdenticalWithOrWithoutFilterBlockMem(t *testing.T) {
	t.Setenv("USE_INVERTED_SEARCHABLE", "true")
	t.Setenv("USE_BLOCKMAX_WAND", "true")
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
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	props := SetupClassForFilterScoringTest(t, repo, schemaGetter, logger, 1.2, 0.75)

	idx := repo.GetIndex("FilterClass")
	require.NotNil(t, idx)

	filter := &filters.LocalFilter{
		Root: &filters.Clause{
			On: &filters.Path{
				Class:    schema.ClassName("FilterClass"),
				Property: schema.PropertyName("relatedToGolf"),
			},
			Operator: filters.OperatorEqual,
			Value: &filters.Value{
				Value: true,
				Type:  dtBool,
			},
		},
	}

	kwr := &searchparams.KeywordRanking{
		Type:       "bm25",
		Properties: []string{"description"},
		Query:      "koepka golf",
	}

	addit := additional.Properties{}
	filtered, filteredScores, err := idx.objectSearch(context.TODO(), 1000, filter, kwr, nil, nil, addit, nil, "", 0, props)
	require.Nil(t, err)
	unfiltered, unfilteredScores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)
	require.Nil(t, err)

	require.Len(t, filtered, 1)   // should match exactly one element
	require.Len(t, unfiltered, 2) // contains irrelevant result

	assert.Equal(t, uint64(0), filtered[0].DocID)   // brooks koepka result
	assert.Equal(t, uint64(0), unfiltered[0].DocID) // brooks koepka result

	assert.Equal(t, filteredScores[0], unfilteredScores[0])
}

func TestBM25FDifferentParamsJourneyBlockMem(t *testing.T) {
	t.Setenv("USE_INVERTED_SEARCHABLE", "true")
	t.Setenv("USE_BLOCKMAX_WAND", "true")
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
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	props := SetupClass(t, repo, schemaGetter, logger, 0.5, 100)

	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	// Check boosted
	kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title^2", "description"}, Query: "journey"}
	addit := additional.Properties{}
	res, scores, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0, props)

	// Print results
	t.Log("--- Start results for boosted search ---")
	for i, r := range res {
		t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
	}

	require.Nil(t, err)

	// Check results in correct order
	require.Equal(t, uint64(6), res[0].DocID)
	require.Equal(t, uint64(2), res[2].DocID)

	// Print results
	t.Log("--- Start results for boosted search ---")
	for i, r := range res {
		t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
	}

	// Check scores
	EqualFloats(t, float32(0.9860), scores[0], 5)
	EqualFloats(t, float32(0.6327), scores[1], 5)
}

// Compare with previous BM25 version to ensure the algorithm functions correctly
func TestBM25FCompareBlockMem(t *testing.T) {
	t.Setenv("USE_INVERTED_SEARCHABLE", "true")
	t.Setenv("USE_BLOCKMAX_WAND", "true")
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
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	props := SetupClass(t, repo, schemaGetter, logger, 0.5, 100)

	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	shardNames := idx.getSchema.CopyShardingState(idx.Config.ClassName.String()).AllPhysicalShards()

	for _, shardName := range shardNames {
		shard := idx.shards.Load(shardName)
		t.Logf("------ BM25F --------\n")
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "journey"}
		addit := additional.Properties{}

		withBM25Fobjs, withBM25Fscores, err := shard.ObjectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, props)
		require.Nil(t, err)

		for i, r := range withBM25Fobjs {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, withBM25Fscores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		t.Logf("------ BM25 --------\n")
		kwr.Type = ""

		objs, scores, err := shard.ObjectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, props)
		require.Nil(t, err)

		for i, r := range objs {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID, scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		require.Equal(t, len(withBM25Fobjs), len(objs))
		for i := range objs {
			t.Logf("%v: BM25F score: %v, BM25 score: %v", i, withBM25Fscores[i], scores[i])
			EqualFloats(t, withBM25Fscores[i], scores[i], 9)
		}

		// Not all the scores are unique and the search is not stable, so pick ones that don't move
		require.Equal(t, uint64(4), objs[0].DocID)
		require.Equal(t, uint64(5), objs[1].DocID)
		require.Equal(t, uint64(6), objs[2].DocID)
		require.Equal(t, uint64(1), objs[3].DocID)
		require.Equal(t, uint64(0), objs[4].DocID)
		require.Equal(t, uint64(2), objs[5].DocID)

		require.Equal(t, uint64(4), withBM25Fobjs[0].DocID)
		require.Equal(t, uint64(5), withBM25Fobjs[1].DocID)
		require.Equal(t, uint64(6), withBM25Fobjs[2].DocID)
		require.Equal(t, uint64(1), withBM25Fobjs[3].DocID)
		require.Equal(t, uint64(0), withBM25Fobjs[4].DocID)
		require.Equal(t, uint64(2), withBM25Fobjs[5].DocID)

	}
}

func TestBM25F_ComplexDocumentsBlockMem(t *testing.T) {
	t.Setenv("USE_INVERTED_SEARCHABLE", "true")
	t.Setenv("USE_BLOCKMAX_WAND", "true")
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{},
		},
	}

	repo, err := New(logger, Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil, memwatch.NewDummyMonitor())
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	classNone, props := SetupClassDocuments(t, repo, schemaGetter, logger, 0.5, 0.75, "none")
	idxNone := repo.GetIndex(schema.ClassName(classNone))
	require.NotNil(t, idxNone)

	addit := additional.Properties{}

	t.Run("single term", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Query: "considered a"}
		res, scores, err := idxNone.objectSearch(context.TODO(), 10, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)

		// Print results
		t.Log("--- Start results for boosted search ---")
		for i, r := range res {
			t.Logf("Result id: %v, score: %v, \n", r.DocID, scores[i])
		}

		// Check results in correct order
		require.Equal(t, uint64(3), res[0].DocID)
		require.Equal(t, uint64(0), res[1].DocID)
		require.Equal(t, uint64(2), res[2].DocID)
		require.Len(t, res, 3)

		// Check scores
		EqualFloats(t, float32(0.8550), scores[0], 5)
		EqualFloats(t, float32(0.5116), scores[1], 5)
		EqualFloats(t, float32(0.3325), scores[2], 5)
	})

	t.Run("Results without stopwords", func(t *testing.T) {
		kwrNoStopwords := &searchparams.KeywordRanking{Type: "bm25", Query: "example losing business"}
		resNoStopwords, resNoScores, err := idxNone.objectSearch(context.TODO(), 10, nil, kwrNoStopwords, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)

		classEn, properties := SetupClassDocuments(t, repo, schemaGetter, logger, 0.5, 0.75, "en")
		idxEn := repo.GetIndex(schema.ClassName(classEn))
		require.NotNil(t, idxEn)

		kwrStopwords := &searchparams.KeywordRanking{Type: "bm25", Query: "an example on losing the business"}
		resStopwords, resScores, err := idxEn.objectSearch(context.TODO(), 10, nil, kwrStopwords, nil, nil, addit, nil, "", 0, properties)
		require.Nil(t, err)

		require.Equal(t, len(resNoStopwords), len(resStopwords))
		for i, resNo := range resNoStopwords {
			resYes := resStopwords[i]
			require.Equal(t, resNo.DocID, resYes.DocID)
			require.Equal(t, resNoScores[i], resScores[i])
		}

		kwrStopwordsDuplicate := &searchparams.KeywordRanking{Type: "bm25", Query: "on an example on losing the business on"}
		resStopwordsDuplicate, duplicateScores, err := idxEn.objectSearch(context.TODO(), 10, nil, kwrStopwordsDuplicate, nil, nil, addit, nil, "", 0, properties)
		require.Nil(t, err)
		require.Equal(t, len(resNoStopwords), len(resStopwordsDuplicate))
		for i, resNo := range resNoStopwords {
			resYes := resStopwordsDuplicate[i]
			require.Equal(t, resNo.DocID, resYes.DocID)
			require.Equal(t, resNoScores[i], duplicateScores[i])
		}
	})
}

func TestBM25F_SortMultiPropBlockMem(t *testing.T) {
	t.Setenv("USE_INVERTED_SEARCHABLE", "true")
	t.Setenv("USE_BLOCKMAX_WAND", "true")
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{},
		},
	}
	repo, err := New(logger, Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil, memwatch.NewDummyMonitor())
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	className, props := MultiPropClass(t, repo, schemaGetter, logger, 0.5, 0.75)
	idx := repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, idx)

	addit := additional.Properties{}

	t.Run("single term", func(t *testing.T) {
		t.Skip("TODO")
		kwr := &searchparams.KeywordRanking{Type: "bm25", Query: "pepper banana"}
		res, scores, err := idx.objectSearch(context.TODO(), 2, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)

		// Print results
		t.Log("--- Start results for boosted search ---")
		for i, r := range res {
			t.Logf("Result id: %v, score: %v, \n", r.DocID, scores[i])
		}

		// Document 1 is a result for both terms
		require.Len(t, res, 2)
		require.Equal(t, uint64(1), res[0].DocID)
	})

	t.Run("two docs to test additional explanations", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Query: "pepper banana", AdditionalExplanations: true}
		res, _, err := idx.objectSearch(context.TODO(), 2, nil, kwr, nil, nil, addit, nil, "", 0, props)
		require.Nil(t, err)

		// Print results
		t.Log("--- Start results for boosted search ---")
		for _, r := range res {
			t.Logf("Result id: %v, score: %v, additional: %v\n", r.DocID, r.ExplainScore(), r.Object.Additional)
		}

		// We have two results, one that matches both terms and one that matches only one
		require.Len(t, res, 2)
		require.Equal(t, uint64(1), res[0].DocID)
		// these assertions failed if we didn't swap the positions of the additional explanations, as we would be getting the explanations from the second doc
		explanationString := fmt.Sprintf("%f", res[0].Object.Additional["explainScore"])
		require.True(t, strings.Contains(explanationString, "BM25F_pepper_frequency:1"))
		require.True(t, strings.Contains(explanationString, "BM25F_pepper_propLength:1"))
		require.True(t, strings.Contains(explanationString, "BM25F_banana_frequency:1"))
		require.True(t, strings.Contains(explanationString, "BM25F_banana_propLength:1"))
	})
}
