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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func BM25FinvertedConfig(k1, b float32, stopWordPreset string) *models.InvertedIndexConfig {
	return &models.InvertedIndexConfig{
		Bm25: &models.BM25Config{
			K1: k1,
			B:  b,
		},
		CleanupIntervalSeconds: 60,
		Stopwords: &models.StopwordConfig{
			Preset: stopWordPreset,
		},
		IndexNullState:      true,
		IndexPropertyLength: true,
	}
}

func SetupClass(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger, k1, b float32,
) {
	vFalse := false
	vTrue := true

	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(k1, b, "none"),
		Class:               "MyClass",

		Properties: []*models.Property{
			{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
			{
				Name:            "description",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
			{
				Name:            "review",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
			{
				Name:            "textField",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationField,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
			{
				Name:            "textWhitespace",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWhitespace,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
			{
				Name:            "relatedToGolf",
				DataType:        schema.DataTypeBoolean.PropString(),
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
			{
				Name:            "multiTitles",
				DataType:        schema.DataTypeTextArray.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
			{
				Name:            "multiTextWhitespace",
				DataType:        schema.DataTypeTextArray.PropString(),
				Tokenization:    models.PropertyTokenizationWhitespace,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
		},
	}

	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	schemaGetter.schema = schema

	migrator := NewMigrator(repo, logger)
	migrator.AddClass(context.Background(), class, schemaGetter.shardState)

	testData := []map[string]interface{}{}
	testData = append(testData, map[string]interface{}{"title": "Our journey to BM25F", "description": "This is how we get to BM25F", "review": "none none none", "multiTitles": []string{"breakfast", "dinner"}})
	testData = append(testData, map[string]interface{}{"title": "Why I dont like journey", "description": "This is about how we get somewhere", "multiTitles": []string{"going to a restaurant for dinner", "sandwiches and desert are a great lunch"}})
	testData = append(testData, map[string]interface{}{"title": "My journeys in Journey", "description": "A journey story about journeying"})
	testData = append(testData, map[string]interface{}{"title": "An unrelated title", "description": "Actually all about journey"})
	testData = append(testData, map[string]interface{}{"title": "journey journey", "description": "journey journey journey"})
	testData = append(testData, map[string]interface{}{"title": "journey", "description": "journey journey", "multiTextWhitespace": []string{"totally irrelevant:)", "we all MuuultiYell! together"}})
	testData = append(testData, map[string]interface{}{"title": "JOURNEY", "description": "A LOUD JOURNEY", "multiTextWhitespace": []string{"MuuultiYell!", "is fun"}})
	testData = append(testData, map[string]interface{}{"title": "An unrelated title", "description": "Absolutely nothing to do with the topic", "textField": "*&^$@#$%^&*()(Offtopic!!!!"})
	testData = append(testData, map[string]interface{}{"title": "none", "description": "other", "textField": "YELLING IS FUN"})
	testData = append(testData, map[string]interface{}{"title": "something", "description": "none none", "review": "none none none none none none"})

	for i, data := range testData {
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())

		obj := &models.Object{Class: "MyClass", ID: id, Properties: data, CreationTimeUnix: 1565612833955, LastUpdateTimeUnix: 10000020}
		vector := []float32{1, 3, 5, 0.4}
		//{title: "Our journey to BM25F", description: " This is how we get to BM25F"}}
		err := repo.PutObject(context.Background(), obj, vector, nil)
		require.Nil(t, err)
	}
}

// DuplicatedFrom SetupClass to make sure this new test does not alter the results of the existing one
func SetupClassForFilterScoringTest(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger, k1, b float32,
) {
	vFalse := false
	vTrue := true

	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(k1, b, "none"),
		Class:               "FilterClass",

		Properties: []*models.Property{
			{
				Name:            "description",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
			{
				Name:            "relatedToGolf",
				DataType:        schema.DataTypeBoolean.PropString(),
				IndexFilterable: &vTrue,
			},
		},
	}

	schema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	schemaGetter.schema = schema

	migrator := NewMigrator(repo, logger)
	migrator.AddClass(context.Background(), class, schemaGetter.shardState)

	testData := []map[string]interface{}{}
	testData = append(testData, map[string]interface{}{"description": "Brooks Koepka appeared a lot in the ms marco dataset. I was surprised to see golf content in there. I assume if the dataset was newer, we'd see a lot more Rory though.", "relatedToGolf": true})
	testData = append(testData, map[string]interface{}{"description": "While one would expect Koepka to be a somewhat rare name, it did appear in msmarco also outside the context of Brooks.", "relatedToGolf": false})

	for i, data := range testData {
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())

		obj := &models.Object{Class: "FilterClass", ID: id, Properties: data, CreationTimeUnix: 1565612833955, LastUpdateTimeUnix: 10000020}
		vector := []float32{1, 3, 5, 0.4}
		err := repo.PutObject(context.Background(), obj, vector, nil)
		require.Nil(t, err)
	}
}

func TestBM25FJourney(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	SetupClass(t, repo, schemaGetter, logger, 1.2, 0.75)

	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	// Check basic search
	addit := additional.Properties{}

	t.Run("bm25f journey", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description", "textField"}, Query: "journey"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)

		// Print results
		t.Log("--- Start results for basic search ---")
		for _, r := range res {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		// Check results in correct order
		require.Equal(t, uint64(4), res[0].DocID())
		require.Equal(t, uint64(5), res[1].DocID())
		require.Equal(t, uint64(6), res[2].DocID())
		require.Equal(t, uint64(3), res[3].DocID())
		require.Equal(t, uint64(0), res[4].DocID())
		require.Equal(t, uint64(2), res[5].DocID())

		// Without additionalExplanations no explainScore entry should be present
		require.Contains(t, res[0].Object.Additional, "score")
		require.NotContains(t, res[0].Object.Additional, "explainScore")
	})

	// Check non-alpha search on string field

	// text/field are tokenized entirely, so we can search for non-alpha characters
	t.Run("bm25f textField non-alpha", func(t *testing.T) {
		kwrTextField := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description", "textField"}, Query: "*&^$@#$%^&*()(Offtopic!!!!"}
		addit = additional.Properties{}
		resTextField, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwrTextField, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)

		// Print results
		t.Log("--- Start results for textField search ---")
		for _, r := range resTextField {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		// Check results in correct order
		require.Equal(t, uint64(7), resTextField[0].DocID())
	})

	// text/field are not lower-cased before indexing, so upper case searches must be passed through unchanged.
	t.Run("bm25f textField caps", func(t *testing.T) {
		kwrTextField := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"textField"}, Query: "YELLING IS FUN"}
		addit := additional.Properties{}
		resTextField, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwrTextField, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)

		// Print results
		t.Log("--- Start results for textField caps search ---")
		for _, r := range resTextField {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		// Check results in correct order
		require.Equal(t, uint64(8), resTextField[0].DocID())
	})

	// Check basic text search WITH CAPS
	t.Run("bm25f text with caps", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "JOURNEY"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0)
		// Print results
		t.Log("--- Start results for search with caps ---")
		for _, r := range res {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}
		require.Nil(t, err)

		// Check results in correct order
		require.Equal(t, uint64(4), res[0].DocID())
		require.Equal(t, uint64(5), res[1].DocID())
		require.Equal(t, uint64(6), res[2].DocID())
		require.Equal(t, uint64(2), res[3].DocID())
		require.Equal(t, uint64(3), res[4].DocID())
		require.Equal(t, uint64(0), res[5].DocID())
		require.Equal(t, uint64(1), res[6].DocID())
	})

	t.Run("bm25f journey boosted", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title^3", "description"}, Query: "journey"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0)

		require.Nil(t, err)
		// Print results
		t.Log("--- Start results for boosted search ---")
		for _, r := range res {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		// Check results in correct order
		require.Equal(t, uint64(4), res[0].DocID())
		require.Equal(t, uint64(5), res[1].DocID())
		require.Equal(t, uint64(6), res[2].DocID())
		require.Equal(t, uint64(0), res[3].DocID())
		require.Equal(t, uint64(1), res[4].DocID())
		require.Equal(t, uint64(2), res[5].DocID())
		require.Equal(t, uint64(3), res[6].DocID())
	})

	t.Run("Check search with two terms", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "journey somewhere"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)
		// Check results in correct order
		require.Equal(t, uint64(1), res[0].DocID())
		require.Equal(t, uint64(4), res[1].DocID())
		require.Equal(t, uint64(5), res[2].DocID())
		require.Equal(t, uint64(6), res[3].DocID())
		require.Equal(t, uint64(2), res[4].DocID())
	})

	t.Run("bm25f journey somewhere no properties", func(t *testing.T) {
		// Check search with no properties (should include all properties)
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{}, Query: "journey somewhere"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)

		// Check results in correct order
		require.Equal(t, uint64(1), res[0].DocID())
		require.Equal(t, uint64(4), res[1].DocID())
		require.Equal(t, uint64(5), res[2].DocID())
		require.Equal(t, uint64(6), res[3].DocID())
	})

	t.Run("bm25f non alphanums", func(t *testing.T) {
		// Check search with no properties (should include all properties)
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{}, Query: "*&^$@#$%^&*()(Offtopic!!!!"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)
		require.Equal(t, uint64(7), res[0].DocID())
	})

	t.Run("First result has high score", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"description"}, Query: "about BM25F"}
		res, _, err := idx.objectSearch(context.TODO(), 5, nil, kwr, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)

		require.Equal(t, uint64(0), res[0].DocID())
		require.Len(t, res, 4) // four results have one of the terms
	})

	t.Run("More results than limit", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"description"}, Query: "journey"}
		res, _, err := idx.objectSearch(context.TODO(), 5, nil, kwr, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)

		require.Equal(t, uint64(4), res[0].DocID())
		require.Equal(t, uint64(5), res[1].DocID())
		require.Equal(t, uint64(6), res[2].DocID())
		require.Equal(t, uint64(3), res[3].DocID())
		require.Equal(t, uint64(2), res[4].DocID())
		require.Len(t, res, 5) // four results have one of the terms
	})

	t.Run("Results from three properties", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Query: "none"}
		res, _, err := idx.objectSearch(context.TODO(), 5, nil, kwr, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)

		require.Equal(t, uint64(9), res[0].DocID())
		require.Equal(t, uint64(0), res[1].DocID())
		require.Equal(t, uint64(8), res[2].DocID())
		require.Len(t, res, 3)
	})

	t.Run("Include additional explanations", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"description"}, Query: "journey", AdditionalExplanations: true}
		res, _, err := idx.objectSearch(context.TODO(), 5, nil, kwr, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)

		// With additionalExplanations explainScore entry should be present
		require.Contains(t, res[0].Object.Additional, "score")
		require.Contains(t, res[0].Object.Additional, "explainScore")
		require.Contains(t, res[0].Object.Additional["explainScore"], "BM25")
	})

	t.Run("Array fields text", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"multiTitles"}, Query: "dinner"}
		res, _, err := idx.objectSearch(context.TODO(), 5, nil, kwr, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)

		require.Len(t, res, 2)
		require.Equal(t, uint64(0), res[0].DocID())
		require.Equal(t, uint64(1), res[1].DocID())
	})

	t.Run("Array fields string", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"multiTextWhitespace"}, Query: "MuuultiYell!"}
		res, _, err := idx.objectSearch(context.TODO(), 5, nil, kwr, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)

		require.Len(t, res, 2)
		require.Equal(t, uint64(6), res[0].DocID())
		require.Equal(t, uint64(5), res[1].DocID())
	})

	t.Run("With autocut", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Query: "journey", Properties: []string{"description"}}
		resNoAutoCut, _, err := idx.objectSearch(context.TODO(), 10, nil, kwr, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)

		resAutoCut, _, err := idx.objectSearch(context.TODO(), 10, nil, kwr, nil, nil, addit, nil, "", 1)
		require.Nil(t, err)

		require.Less(t, len(resAutoCut), len(resNoAutoCut))

		require.EqualValues(t, 0.5868752, resNoAutoCut[0].Score())
		require.EqualValues(t, 0.5450892, resNoAutoCut[1].Score()) // <= autocut last element
		require.EqualValues(t, 0.34149727, resNoAutoCut[2].Score())
		require.EqualValues(t, 0.3049518, resNoAutoCut[3].Score())
		require.EqualValues(t, 0.27547202, resNoAutoCut[4].Score())

		require.Len(t, resAutoCut, 2)
		require.EqualValues(t, 0.5868752, resAutoCut[0].Score())
		require.EqualValues(t, 0.5450892, resAutoCut[1].Score())
	})
}

func TestBM25FSingleProp(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	SetupClass(t, repo, schemaGetter, logger, 0.5, 100)

	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	// Check boosted
	kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"description"}, Query: "journey"}
	addit := additional.Properties{}
	res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0)
	t.Log("--- Start results for singleprop search ---")
	for _, r := range res {
		t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
	}
	require.Nil(t, err)
	// Check results in correct order
	require.Equal(t, uint64(3), res[0].DocID())
	require.Equal(t, uint64(4), res[3].DocID())

	// Check scores
	EqualFloats(t, float32(0.1248), res[0].Score(), 5)
	EqualFloats(t, float32(0.0363), res[1].Score(), 5)
}

func TestBM25FWithFilters(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	SetupClass(t, repo, schemaGetter, logger, 0.5, 100)

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
	res, _, err := idx.objectSearch(context.TODO(), 1000, filter, kwr, nil, nil, addit, nil, "", 0)

	require.Nil(t, err)
	require.True(t, len(res) == 1)
	require.Equal(t, uint64(2), res[0].DocID())
}

func TestBM25FWithFilters_ScoreIsIdenticalWithOrWithoutFilter(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	SetupClassForFilterScoringTest(t, repo, schemaGetter, logger, 1.2, 0.75)

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
	filtered, _, err := idx.objectSearch(context.TODO(), 1000, filter, kwr, nil, nil, addit, nil, "", 0)
	require.Nil(t, err)
	unfiltered, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0)
	require.Nil(t, err)

	require.Len(t, filtered, 1)   // should match exactly one element
	require.Len(t, unfiltered, 2) // contains irrelevant result

	assert.Equal(t, uint64(0), filtered[0].DocID())   // brooks koepka result
	assert.Equal(t, uint64(0), unfiltered[0].DocID()) // brooks koepka result

	assert.Equal(t, filtered[0].Score(), unfiltered[0].Score())
}

func TestBM25FDifferentParamsJourney(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	SetupClass(t, repo, schemaGetter, logger, 0.5, 100)

	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	// Check boosted
	kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title^2", "description"}, Query: "journey"}
	addit := additional.Properties{}
	res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0)

	// Print results
	t.Log("--- Start results for boosted search ---")
	for _, r := range res {
		t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
	}

	require.Nil(t, err)

	// Check results in correct order
	require.Equal(t, uint64(6), res[0].DocID())
	require.Equal(t, uint64(1), res[3].DocID())

	// Print results
	t.Log("--- Start results for boosted search ---")
	for _, r := range res {
		t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
	}

	// Check scores
	EqualFloats(t, float32(0.06023), res[0].Score(), 6)
	EqualFloats(t, float32(0.04238), res[1].Score(), 6)
}

func EqualFloats(t *testing.T, expected, actual float32, significantFigures int) {
	s1 := fmt.Sprintf("%v", expected)
	s2 := fmt.Sprintf("%v", actual)
	if len(s1) < 2 || len(s2) < 2 {
		t.Fail()
	}
	if len(s1) <= significantFigures {
		significantFigures = len(s1) - 1
	}
	if len(s2) <= significantFigures {
		significantFigures = len(s2) - 1
	}
	require.Equal(t, s1[:significantFigures+1], s2[:significantFigures+1])
}

// Compare with previous BM25 version to ensure the algorithm functions correctly
func TestBM25FCompare(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	SetupClass(t, repo, schemaGetter, logger, 0.5, 100)

	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	shardNames := idx.getSchema.CopyShardingState(idx.Config.ClassName.String()).AllPhysicalShards()

	for _, shardName := range shardNames {
		shard := idx.shards.Load(shardName)
		t.Logf("------ BM25F --------\n")
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "journey"}
		addit := additional.Properties{}

		withBM25Fobjs, withBM25Fscores, err := shard.ObjectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit)
		require.Nil(t, err)

		for i, r := range withBM25Fobjs {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), withBM25Fscores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		t.Logf("------ BM25 --------\n")
		kwr.Type = ""

		objs, scores, err := shard.ObjectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit)
		require.Nil(t, err)

		for i, r := range objs {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		require.Equal(t, len(withBM25Fobjs), len(objs))
		for i := range objs {
			t.Logf("%v: BM25F score: %v, BM25 score: %v", i, withBM25Fscores[i], scores[i])
			EqualFloats(t, withBM25Fscores[i], scores[i], 9)
		}

		// Not all the scores are unique and the search is not stable, so pick ones that don't move
		require.Equal(t, uint64(4), objs[0].DocID())
		require.Equal(t, uint64(5), objs[1].DocID())
		require.Equal(t, uint64(6), objs[2].DocID())
		require.Equal(t, uint64(1), objs[3].DocID())
		require.Equal(t, uint64(2), objs[4].DocID())
		require.Equal(t, uint64(0), objs[5].DocID())

		require.Equal(t, uint64(4), withBM25Fobjs[0].DocID())
		require.Equal(t, uint64(5), withBM25Fobjs[1].DocID())
		require.Equal(t, uint64(6), withBM25Fobjs[2].DocID())
		require.Equal(t, uint64(1), withBM25Fobjs[3].DocID())
		require.Equal(t, uint64(2), withBM25Fobjs[4].DocID())
		require.Equal(t, uint64(0), withBM25Fobjs[5].DocID())

	}
}

func Test_propertyHasSearchableIndex(t *testing.T) {
	vFalse := false
	vTrue := true

	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(1, 1, "none"),
		Class:               "MyClass",

		Properties: []*models.Property{
			{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: nil,
			},
			{
				Name:            "description",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
			{
				Name:            "textField",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationField,
				IndexFilterable: &vFalse,
				IndexSearchable: &vFalse,
			},
		},
	}

	ClassSchema := &models.Schema{
		Classes: []*models.Class{class},
	}
	t.Run("Property index", func(t *testing.T) {
		if got := inverted.PropertyHasSearchableIndex(ClassSchema, "MyClass", "description"); got != true {
			t.Errorf("PropertyHasSearchableIndex() = %v, want %v", got, true)
		}

		if got := inverted.PropertyHasSearchableIndex(ClassSchema, "MyClass", "description^2"); got != true {
			t.Errorf("PropertyHasSearchableIndex() = %v, want %v", got, true)
		}

		if got := inverted.PropertyHasSearchableIndex(ClassSchema, "MyClass", "textField"); got != false {
			t.Errorf("PropertyHasSearchableIndex() = %v, want %v", got, false)
		}

		if got := inverted.PropertyHasSearchableIndex(ClassSchema, "MyClass", "title"); got != true {
			t.Errorf("PropertyHasSearchableIndex() = %v, want %v", got, true)
		}
	})
}

func SetupClassDocuments(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger, k1, b float32, preset string,
) string {
	vFalse := false
	vTrue := true

	className := "DocumentsPreset_" + preset
	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(k1, b, preset),
		Class:               className,

		Properties: []*models.Property{
			{
				Name:            "document",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
		},
	}
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	migrator := NewMigrator(repo, logger)
	migrator.AddClass(context.Background(), class, schemaGetter.shardState)

	testData := []map[string]interface{}{}
	testData = append(testData, map[string]interface{}{"document": "No matter what you do, the question of \"\"what is income\"\" is *always* going to be an extremely complex question.   To use this particular example, is paying a royalty fee to an external party a legitimate business expense that is part of the cost of doing business and which subtracts from your \"\"income\"\"?"})
	testData = append(testData, map[string]interface{}{"document": "test"})
	testData = append(testData, map[string]interface{}{"document": "As long as the losing business is not considered \"\"passive activity\"\" or \"\"hobby\"\", then yes. Passive Activity is an activity where you do not have to actively do anything to generate income. For example - royalties or rentals. Hobby is an activity that doesn't generate profit. Generally, if your business doesn't consistently generate profit (the IRS looks at 3 out of the last 5 years), it may be characterized as hobby. For hobby, loss deduction is limited by the hobby income and the 2% AGI threshold."})
	testData = append(testData, map[string]interface{}{"document": "So you're basically saying that average market fluctuations have an affect on individual stocks, because individual stocks are often priced in relation to the growth of the market as a whole?  Also, what kinds of investments would be considered \"\"risk free\"\" in this nomenclature?"})

	for i, data := range testData {
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())

		obj := &models.Object{Class: className, ID: id, Properties: data, CreationTimeUnix: 1565612833955, LastUpdateTimeUnix: 10000020}
		vector := []float32{1, 3, 5, 0.4}
		//{title: "Our journey to BM25F", description: " This is how we get to BM25F"}}
		err := repo.PutObject(context.Background(), obj, vector, nil)
		require.Nil(t, err)
	}
	return className
}

func TestBM25F_ComplexDocuments(t *testing.T) {
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
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	classNone := SetupClassDocuments(t, repo, schemaGetter, logger, 0.5, 0.75, "none")
	idxNone := repo.GetIndex(schema.ClassName(classNone))
	require.NotNil(t, idxNone)

	addit := additional.Properties{}

	t.Run("single term", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Query: "considered a"}
		res, _, err := idxNone.objectSearch(context.TODO(), 10, nil, kwr, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)

		// Print results
		t.Log("--- Start results for boosted search ---")
		for _, r := range res {
			t.Logf("Result id: %v, score: %v, \n", r.DocID(), r.Score())
		}

		// Check results in correct order
		require.Equal(t, uint64(3), res[0].DocID())
		require.Equal(t, uint64(0), res[1].DocID())
		require.Equal(t, uint64(2), res[2].DocID())
		require.Len(t, res, 3)

		// Check scores
		EqualFloats(t, float32(0.8914), res[0].Score(), 5)
		EqualFloats(t, float32(0.5425), res[1].Score(), 5)
		EqualFloats(t, float32(0.3952), res[2].Score(), 5)
	})

	t.Run("Results without stopwords", func(t *testing.T) {
		kwrNoStopwords := &searchparams.KeywordRanking{Type: "bm25", Query: "example losing business"}
		resNoStopwords, _, err := idxNone.objectSearch(context.TODO(), 10, nil, kwrNoStopwords, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)

		classEn := SetupClassDocuments(t, repo, schemaGetter, logger, 0.5, 0.75, "en")
		idxEn := repo.GetIndex(schema.ClassName(classEn))
		require.NotNil(t, idxEn)
		kwrStopwords := &searchparams.KeywordRanking{Type: "bm25", Query: "an example on losing the business"}
		resStopwords, _, err := idxEn.objectSearch(context.TODO(), 10, nil, kwrStopwords, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)

		require.Equal(t, len(resNoStopwords), len(resStopwords))
		for i, resNo := range resNoStopwords {
			resYes := resStopwords[i]
			require.Equal(t, resNo.DocID(), resYes.DocID())
			require.Equal(t, resNo.Score(), resYes.Score())
		}

		kwrStopwordsDuplicate := &searchparams.KeywordRanking{Type: "bm25", Query: "on an example on losing the business on"}
		resStopwordsDuplicate, _, err := idxEn.objectSearch(context.TODO(), 10, nil, kwrStopwordsDuplicate, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)
		require.Equal(t, len(resNoStopwords), len(resStopwordsDuplicate))
		for i, resNo := range resNoStopwords {
			resYes := resStopwordsDuplicate[i]
			require.Equal(t, resNo.DocID(), resYes.DocID())
			require.Equal(t, resNo.Score(), resYes.Score())
		}
	})
}

func MultiPropClass(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger, k1, b float32) string {
	vFalse := false
	vTrue := true

	className := "MultiProps"
	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(k1, b, "none"),
		Class:               className,

		Properties: []*models.Property{
			{
				Name:            "document",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
			{
				Name:            "title",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: &vFalse,
				IndexSearchable: &vTrue,
			},
		},
	}
	schemaGetter.schema = schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	migrator := NewMigrator(repo, logger)
	migrator.AddClass(context.Background(), class, schemaGetter.shardState)

	testData := []map[string]interface{}{}
	testData = append(testData, map[string]interface{}{"document": "test", "title": "pepper"})
	testData = append(testData, map[string]interface{}{"document": "banana", "title": "pepper"})
	testData = append(testData, map[string]interface{}{"document": "apple", "title": "banana taste great"})
	testData = append(testData, map[string]interface{}{"document": "banana burger", "title": "test"})
	testData = append(testData, map[string]interface{}{"document": "carotte", "title": "great"})

	for i, data := range testData {
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())

		obj := &models.Object{Class: className, ID: id, Properties: data, CreationTimeUnix: 1565612833955, LastUpdateTimeUnix: 10000020}
		vector := []float32{1, 3, 5, 0.4}
		err := repo.PutObject(context.Background(), obj, vector, nil)
		require.Nil(t, err)
	}
	return className
}

func TestBM25F_SortMultiProp(t *testing.T) {
	t.Skip("Currently failing")
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
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	idx := repo.GetIndex(schema.ClassName(MultiPropClass(t, repo, schemaGetter, logger, 0.5, 0.75)))
	require.NotNil(t, idx)

	addit := additional.Properties{}

	t.Run("single term", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Query: "pepper banana"}
		res, _, err := idx.objectSearch(context.TODO(), 1, nil, kwr, nil, nil, addit, nil, "", 0)
		require.Nil(t, err)

		// Print results
		t.Log("--- Start results for boosted search ---")
		for _, r := range res {
			t.Logf("Result id: %v, score: %v, \n", r.DocID(), r.Score())
		}

		// Document 1 is a result for both terms
		require.Len(t, res, 1)
		require.Equal(t, uint64(1), res[0].DocID())
	})
}
