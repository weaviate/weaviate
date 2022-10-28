//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest
// +build integrationTest

package db

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/searchparams"
	enthnsw "github.com/semi-technologies/weaviate/entities/vectorindex/hnsw"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
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

func SetupClass(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger, k1, b float32) {
	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(k1, b),
		Class:               "MyClass",
		Properties: []*models.Property{
			{
				Name:         "title",
				DataType:     []string{string(schema.DataTypeText)},
				Tokenization: "word",
			},
			{
				Name:         "description",
				DataType:     []string{string(schema.DataTypeText)},
				Tokenization: "word",
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

	for i, data := range testData {
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())

		obj := &models.Object{Class: "MyClass", ID: id, Properties: data, CreationTimeUnix: 1565612833955, LastUpdateTimeUnix: 10000020}
		vector := []float32{1, 3, 5, 0.4}
		//{title: "Our journey to BM25F", description: " This is how we get to BM25F"}}
		err := repo.PutObject(context.Background(), obj, vector)
		require.Nil(t, err)
	}
}

func TestBM25FJourney(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		FlushIdleAfter:            60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(context.TODO())
	require.Nil(t, err)
	defer repo.Shutdown(context.Background())

	SetupClass(t, repo, schemaGetter, logger, 1.2, 0.75)

	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	// Check basic search with one property
	kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "journey"}
	addit := additional.Properties{}
	res, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)
	require.Nil(t, err)

	// Check results in correct order
	require.Equal(t, uint64(4), res[0].DocID())
	require.Equal(t, uint64(5), res[1].DocID())

	// Check scoreExplain
	require.Contains(t, res[0].Object.Additional["scoreExplain"], "BM25F")

	// Check basic search WITH CAPS
	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "JOURNEY"}
	addit = additional.Properties{}
	res, err = idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)
	// Print results
	fmt.Println("--- Start results for search with caps ---")
	for _, r := range res {
		fmt.Printf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
	}
	require.Nil(t, err)

	// Check results in correct order
	require.Equal(t, uint64(4), res[0].DocID())
	require.Equal(t, uint64(5), res[1].DocID())

	// Check boosted
	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "journey^3"}
	addit = additional.Properties{}
	res, err = idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)

	require.Nil(t, err)

	// Check results in correct order
	require.Equal(t, uint64(4), res[0].DocID())
	require.Equal(t, uint64(5), res[1].DocID())
	require.Equal(t, uint64(6), res[2].DocID())
	require.Equal(t, uint64(2), res[3].DocID())
	// Print results
	fmt.Println("--- Start results for boosted search ---")
	for _, r := range res {
		fmt.Printf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
	}

	// Check scores
	require.Equal(t, float32(0.061129723), res[0].Score())
	require.Equal(t, float32(0.059050433), res[1].Score())
	require.Equal(t, float32(0.05252086), res[2].Score())
	require.Equal(t, float32(0.044437673), res[3].Score())

	// Check search with two terms
	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "journey somewhere"}
	res, err = idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)
	require.Nil(t, err)

	// Check results in correct order
	require.Equal(t, uint64(1), res[0].DocID())
	require.Equal(t, uint64(4), res[1].DocID())
	require.Equal(t, uint64(5), res[2].DocID())
	require.Equal(t, uint64(6), res[3].DocID())
	require.Equal(t, uint64(2), res[4].DocID())

	fmt.Println("Search with no properties")
	// Check search with no properties (should include all properties)
	kwr = &searchparams.KeywordRanking{Type: "bm25", Properties: []string{}, Query: "journey somewhere"}
	res, err = idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)
	require.Nil(t, err)

	// Check results in correct order
	require.Equal(t, uint64(1), res[0].DocID())
	require.Equal(t, uint64(4), res[1].DocID())
	require.Equal(t, uint64(5), res[2].DocID())
	require.Equal(t, uint64(6), res[3].DocID())
	require.Equal(t, uint64(2), res[4].DocID())
}

func TestBM25FDifferentParamsJourney(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{shardState: singleShardState()}
	repo := New(logger, Config{
		FlushIdleAfter:            60,
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(context.TODO())
	require.Nil(t, err)
	defer repo.Shutdown(context.Background())

	SetupClass(t, repo, schemaGetter, logger, 0.5, 100)

	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	// Check scores change when k1 and b are changed
	// Check boosted
	kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: "journey^3"}
	addit := additional.Properties{}
	res, err := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)

	// Print results
	fmt.Println("--- Start results for search with caps ---")
	for _, r := range res {
		fmt.Printf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
	}

	require.Nil(t, err)

	// Check results in correct order
	require.Equal(t, uint64(6), res[0].DocID())
	require.Equal(t, uint64(3), res[1].DocID())
	require.Equal(t, uint64(0), res[2].DocID())
	require.Equal(t, uint64(1), res[3].DocID())
	// Print results
	fmt.Println("--- Start results for boosted search ---")
	for _, r := range res {
		fmt.Printf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
	}

	// Check scores
	require.Equal(t, float32(0.02404321), res[0].Score())
	require.Equal(t, float32(0.014773461), res[1].Score())
	require.Equal(t, float32(0.014773461), res[2].Score())
	require.Equal(t, float32(0.006913103), res[3].Score())
}
