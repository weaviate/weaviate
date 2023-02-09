//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest

package db

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/searchparams"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func BM25FinvertedConfig(k1, b float32) *models.InvertedIndexConfig {
	return &models.InvertedIndexConfig{
		Bm25: &models.BM25Config{
			K1: k1,
			B:  b,
		},
		CleanupIntervalSeconds: 60,
		Stopwords: &models.StopwordConfig{
			Preset: "none",
		},
		IndexNullState:      true,
		IndexPropertyLength: true,
	}
}

func truePointer() *bool {
	t := true
	return &t
}

func falsePointer() *bool {
	t := false
	return &t
}

func SetupClass(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger, k1, b float32,
) {
	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(k1, b),
		Class:               "MyClass",

		Properties: []*models.Property{
			{
				Name:          "title",
				DataType:      []string{string(schema.DataTypeText)},
				Tokenization:  "word",
				IndexInverted: truePointer(),
			},
			{
				Name:          "description",
				DataType:      []string{string(schema.DataTypeText)},
				Tokenization:  "word",
				IndexInverted: truePointer(),
			},
			{
				Name:          "stringField",
				DataType:      []string{string(schema.DataTypeString)},
				Tokenization:  "field",
				IndexInverted: truePointer(),
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
	testData = append(testData, map[string]interface{}{"title": "Our journey to BM25F", "description": "This is how we get to BM25F"})
	testData = append(testData, map[string]interface{}{"title": "Why I dont like journey", "description": "This is about how we get somewhere"})
	testData = append(testData, map[string]interface{}{"title": "My journeys in Journey", "description": "A journey story about journeying"})
	testData = append(testData, map[string]interface{}{"title": "An unrelated title", "description": "Actually all about journey"})
	testData = append(testData, map[string]interface{}{"title": "journey journey", "description": "journey journey journey"})
	testData = append(testData, map[string]interface{}{"title": "journey", "description": "journey journey"})
	testData = append(testData, map[string]interface{}{"title": "JOURNEY", "description": "A LOUD JOURNEY"})
	testData = append(testData, map[string]interface{}{"title": "An unrelated title", "description": "Absolutely nothing to do with the topic", "stringField": "*&^$@#$%^&*()(Offtopic!!!!"})
	testData = append(testData, map[string]interface{}{"title": "none", "description": "none", "stringField": "YELLING IS FUN"})

	for i, data := range testData {
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())

		obj := &models.Object{Class: "MyClass", ID: id, Properties: data, CreationTimeUnix: 1565612833955, LastUpdateTimeUnix: 10000020}
		vector := []float32{1, 3, 5, 0.4}
		//{title: "Our journey to BM25F", description: " This is how we get to BM25F"}}
		err := repo.PutObject(context.Background(), obj, vector, nil)
		require.Nil(t, err)
	}
}

func TestBM25FJourney(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(context.TODO())
	require.Nil(t, err)
	defer repo.Shutdown(context.Background())

	SetupClass(t, repo, schemaGetter, logger, 1.2, 0.75)

	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	// Check basic search
	addit := additional.Properties{}

	t.Run("bm25f journey", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description", "stringField"}, Query: "journey"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)
		require.Nil(t, err)

		// Print results
		t.Log("--- Start results for basic search ---")
		for _, r := range res {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		// Check results in correct order
		require.Equal(t, uint64(4), res[0].DocID())
		require.Equal(t, uint64(5), res[1].DocID())

		// Check explainScore
		require.Contains(t, res[0].Object.Additional["explainScore"], "BM25F")
	})

	// Check non-alpha search on string field

	// String are by default not tokenized, so we can search for non-alpha characters
	t.Run("bm25f stringfield non-alpha", func(t *testing.T) {
		kwrStringField := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description", "stringField"}, Query: "*&^$@#$%^&*()(Offtopic!!!!"}
		addit = additional.Properties{}
		resStringField, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwrStringField, nil, addit)
		require.Nil(t, err)

		// Print results
		t.Log("--- Start results for stringField search ---")
		for _, r := range resStringField {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		// Check results in correct order
		require.Equal(t, uint64(7), resStringField[0].DocID())

		// Check explainScore
		require.Contains(t, resStringField[0].Object.Additional["explainScore"], "BM25F")
	})

	// String and text fields are indexed differently, so this checks the string indexing and searching.  In particular,
	// string fields are not lower-cased before indexing, so upper case searches must be passed through unchanged.
	t.Run("bm25f stringfield caps", func(t *testing.T) {
		kwrStringField := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"stringField"}, Query: "YELLING IS FUN"}
		addit := additional.Properties{}
		resStringField, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwrStringField, nil, addit)
		require.Nil(t, err)

		// Print results
		t.Log("--- Start results for stringField caps search ---")
		for _, r := range resStringField {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		// Check results in correct order
		require.Equal(t, uint64(8), resStringField[0].DocID())
	})

	// Check basic text search WITH CAPS
	t.Run("bm25f text with caps", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "JOURNEY"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)
		// Print results
		t.Log("--- Start results for search with caps ---")
		for _, r := range res {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}
		require.Nil(t, err)

		// Check results in correct order
		require.Equal(t, uint64(4), res[0].DocID())
		require.Equal(t, uint64(5), res[1].DocID())
	})

	t.Run("bm25f journey boosted", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title^3", "description"}, Query: "journey"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)

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
	})

	t.Run("Check search with two terms", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "journey somewhere"}
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)
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
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)
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
		res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)
		require.Nil(t, err)
		require.Equal(t, uint64(7), res[0].DocID())
	})

	t.Run("First result has high score", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"description"}, Query: "about BM25F"}
		res, _, err := idx.objectSearch(context.TODO(), 5, nil, kwr, nil, addit)
		require.Nil(t, err)

		require.Equal(t, uint64(0), res[0].DocID())
		require.Len(t, res, 4) // four results have one of the terms
	})

	t.Run("More results than limit", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"description"}, Query: "journey"}
		res, _, err := idx.objectSearch(context.TODO(), 5, nil, kwr, nil, addit)
		require.Nil(t, err)

		require.Equal(t, uint64(4), res[0].DocID())
		require.Equal(t, uint64(5), res[1].DocID())
		require.Equal(t, uint64(6), res[2].DocID())
		require.Equal(t, uint64(3), res[3].DocID())
		require.Equal(t, uint64(2), res[4].DocID())
		require.Len(t, res, 5) // four results have one of the terms
	})
}

func TestBM25FSingleProp(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(context.TODO())
	require.Nil(t, err)
	defer repo.Shutdown(context.Background())

	SetupClass(t, repo, schemaGetter, logger, 0.5, 100)

	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	// Check boosted
	kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"description"}, Query: "journey"}
	addit := additional.Properties{}
	res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)
	require.Nil(t, err)
	// Check results in correct order
	require.Equal(t, uint64(3), res[0].DocID())
	require.Equal(t, uint64(4), res[3].DocID())

	// Check scores
	EqualFloats(t, float32(0.38539), res[0].Score(), 5)
	EqualFloats(t, float32(0.04250), res[1].Score(), 5)
}

func TestBM25FDifferentParamsJourney(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(context.TODO())
	require.Nil(t, err)
	defer repo.Shutdown(context.Background())

	SetupClass(t, repo, schemaGetter, logger, 0.5, 100)

	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	// Check boosted
	kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title^2", "description"}, Query: "journey"}
	addit := additional.Properties{}
	res, _, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)

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
	EqualFloats(t, float32(0.05929), res[0].Score(), 6)
	EqualFloats(t, float32(0.04244), res[1].Score(), 6)
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
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(context.TODO())
	require.Nil(t, err)
	defer repo.Shutdown(context.Background())

	SetupClass(t, repo, schemaGetter, logger, 0.5, 100)

	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	shardNames := idx.getSchema.ShardingState(idx.Config.ClassName.String()).AllPhysicalShards()

	for _, shardName := range shardNames {
		shard := idx.Shards[shardName]
		t.Logf("------ BM25F --------\n")
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title"}, Query: "journey"}
		addit := additional.Properties{}

		withBM25Fobjs, withBM25Fscores, err := shard.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)

		for i, r := range withBM25Fobjs {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), withBM25Fscores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		t.Logf("------ BM25 --------\n")
		kwr.Type = ""

		objs, scores, err := shard.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)

		for i, r := range objs {
			t.Logf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), scores[i], r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
		}

		require.Nil(t, err)
		require.Equal(t, len(withBM25Fobjs), len(objs))
		for i := range objs {
			t.Logf("%v: BM25F score: %v, BM25 score: %v", i, withBM25Fscores[i], scores[i])
			EqualFloats(t, withBM25Fscores[i], scores[i], 9)
		}

		// Not all the scores are unique and the search is not stable, so pick ones that don't move
		require.Equal(t, withBM25Fobjs[2].DocID(), objs[2].DocID())
		require.Equal(t, withBM25Fobjs[5].DocID(), objs[5].DocID())
	}
}

func Test_propertyIsIndexed(t *testing.T) {
	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(1, 1),
		Class:               "MyClass",

		Properties: []*models.Property{
			{
				Name:          "title",
				DataType:      []string{string(schema.DataTypeText)},
				Tokenization:  "word",
				IndexInverted: nil,
			},
			{
				Name:          "description",
				DataType:      []string{string(schema.DataTypeText)},
				Tokenization:  "word",
				IndexInverted: truePointer(),
			},
			{
				Name:          "stringField",
				DataType:      []string{string(schema.DataTypeString)},
				Tokenization:  "field",
				IndexInverted: falsePointer(),
			},
		},
	}

	ClassSchema := &models.Schema{
		Classes: []*models.Class{class},
	}
	t.Run("Property index", func(t *testing.T) {
		if got := schema.PropertyIsIndexed(ClassSchema, "MyClass", "description"); got != true {
			t.Errorf("propertyIsIndexed() = %v, want %v", got, true)
		}

		if got := schema.PropertyIsIndexed(ClassSchema, "MyClass", "description^2"); got != true {
			t.Errorf("propertyIsIndexed() = %v, want %v", got, true)
		}

		if got := schema.PropertyIsIndexed(ClassSchema, "MyClass", "stringField"); got != false {
			t.Errorf("propertyIsIndexed() = %v, want %v", got, false)
		}

		if got := schema.PropertyIsIndexed(ClassSchema, "MyClass", "title"); got != true {
			t.Errorf("propertyIsIndexed() = %v, want %v", got, true)
		}
	})
}

func SetupClassDocuments(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger, k1, b float32,
) {
	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(k1, b),
		Class:               "Documents",

		Properties: []*models.Property{
			{
				Name:          "document",
				DataType:      []string{string(schema.DataTypeText)},
				Tokenization:  "word",
				IndexInverted: truePointer(),
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
	testData = append(testData, map[string]interface{}{"document": "No matter what you do, the question of \"\"what is income\"\" is *always* going to be an extremely complex question.   To use this particular example, is paying a royalty fee to an external party a legitimate business expense that is part of the cost of doing business and which subtracts from your \"\"income\"\"?"})
	testData = append(testData, map[string]interface{}{"document": "test"})
	testData = append(testData, map[string]interface{}{"document": "As long as the losing business is not considered \"\"passive activity\"\" or \"\"hobby\"\", then yes. Passive Activity is an activity where you do not have to actively do anything to generate income. For example - royalties or rentals. Hobby is an activity that doesn't generate profit. Generally, if your business doesn't consistently generate profit (the IRS looks at 3 out of the last 5 years), it may be characterized as hobby. For hobby, loss deduction is limited by the hobby income and the 2% AGI threshold."})
	testData = append(testData, map[string]interface{}{"document": "So you're basically saying that average market fluctuations have an affect on individual stocks, because individual stocks are often priced in relation to the growth of the market as a whole?  Also, what kinds of investments would be considered \"\"risk free\"\" in this nomenclature?"})

	for i, data := range testData {
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())

		obj := &models.Object{Class: "Documents", ID: id, Properties: data, CreationTimeUnix: 1565612833955, LastUpdateTimeUnix: 10000020}
		vector := []float32{1, 3, 5, 0.4}
		//{title: "Our journey to BM25F", description: " This is how we get to BM25F"}}
		err := repo.PutObject(context.Background(), obj, vector, nil)
		require.Nil(t, err)
	}
}

func TestBM25F_ComplexDocuments(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		MemtablesFlushIdleAfter:   60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(context.TODO())
	require.Nil(t, err)
	defer repo.Shutdown(context.Background())

	SetupClassDocuments(t, repo, schemaGetter, logger, 0.5, 0.75)

	idx := repo.GetIndex("Documents")
	require.NotNil(t, idx)

	t.Run("single term", func(t *testing.T) {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Query: "considered a"}
		addit := additional.Properties{}
		res, _, err := idx.objectSearch(context.TODO(), 10, nil, kwr, nil, addit)
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
		EqualFloats(t, float32(0.89207935), res[0].Score(), 5)
		EqualFloats(t, float32(0.5427927), res[1].Score(), 5)
		EqualFloats(t, float32(0.39563084), res[2].Score(), 5)
	})
}
