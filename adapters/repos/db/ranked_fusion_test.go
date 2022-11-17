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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/searchparams"
	"github.com/semi-technologies/weaviate/entities/storobj"
	enthnsw "github.com/semi-technologies/weaviate/entities/vectorindex/hnsw"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest"
)

/*
  {
        "document": "BACKGROUND: Common warts (verruca vulgaris) are benign epithelial proliferations associated with human papillomavirus (HPV) infection. Salicylic acid and cryotherapy are the most frequent treatments for common warts, but can be painful and cause scarring, and have high failure and recrudescence rates. Topical vitamin A has been shown to be a successful treatment of common warts in prior informal studies. CASE: The subject is a healthy, physically-active 30 old female with a 9 year history of common warts on the back of the right hand. The warts resisted treatment with salicylic acid, apple cider vinegar and an over-the-counter blend of essential oils marketed for the treatment of warts. Daily topical application of natural vitamin A derived from fish liver oil (25,000 IU) led to replacement of all the warts with normal skin. Most of the smaller warts had been replaced by 70 days. A large wart on the middle knuckle required 6 months of vitamin A treatment to resolve completely. CONCLUSION: Retinoids should be further investigated in controlled studies to determine their effectiveness in treating common warts and the broad range of other benign and cancerous lesions induced by HPVs.",
        "DocID": "MED-941"
    },

*/

type TestDoc struct {
	DocID    string
	Document string
}

/*
	{
	    "queryID": "PLAIN-4",
	    "query": "Using Diet to Treat Asthma and Eczema",
	    "matchingDocIDs": [
	        "MED-2441",
	        "MED-2472",
	        "MED-2444"
	    ]
	},
*/
type TestQuery struct {
	QueryID        string
	Query          string
	MatchingDocIDs []string
}

func SetupStandardTestData(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger, k1, b float32) {
	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(k1, b),
		Class:               "StandardTest",
		Properties: []*models.Property{
			{
				Name:         "document",
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

	// Load text from file standard_test_data.json
	// This is a list of 1000 documents from the MEDLINE database
	// Each document is a medical abstract

	data, _ := ioutil.ReadFile("NFCorpus-Corpus.json")
	var docs []TestDoc
	json.Unmarshal(data, &docs)

	

	for i, doc := range docs {
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())

		data := map[string]interface{}{"document": doc.Document, "code": doc.DocID}
		obj := &models.Object{Class: "StandardTest", ID: id, Properties: data, CreationTimeUnix: 1565612833955, LastUpdateTimeUnix: 10000020}
		//vector := []float32{1, 3, 5, 0.4} //FIXME, make correct vectors?
		err := repo.PutObject(context.Background(), obj, nil)
		require.Nil(t, err)
	}



}


func TestHybrid(t *testing.T) {
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

	SetupStandardTestData(t, repo, schemaGetter, logger, 1.2, 0.75)

	idx := repo.GetIndex("StandardTest")
	require.NotNil(t, idx)

	//Load queries from file standard_test_queries.json
	// This is a list of 100 queries from the MEDLINE database

	data, _ := ioutil.ReadFile("NFCorpus-Query.json")
	var queries []TestQuery
	json.Unmarshal(data, &queries)
	for _, query := range queries {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{}, Query: query.Query}
		addit := additional.Properties{}
		res, _ := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)
		idx.HybridSearch

		fmt.Printf("query for %s returned %d results\n", query.Query, len(res))

	}
}

func TestBIER(t *testing.T) {
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

	SetupStandardTestData(t, repo, schemaGetter, logger, 1.2, 0.75)

	idx := repo.GetIndex("StandardTest")
	require.NotNil(t, idx)

	//Load queries from file standard_test_queries.json
	// This is a list of 100 queries from the MEDLINE database

	data, _ := ioutil.ReadFile("NFCorpus-Query.json")
	var queries []TestQuery
	json.Unmarshal(data, &queries)
	for _, query := range queries {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{}, Query: query.Query}
		addit := additional.Properties{}
		res, _ := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)

		fmt.Printf("query for %s returned %d results\n", query.Query, len(res))
		//fmt.Printf("Results: %v\n", res)

		//for j, doc := range res {
		//	fmt.Printf("res %v, %v\n", j, doc.Object.GetAdditionalProperty("code"))
		//}

		//Check the docIDs are the same
		for j, doc := range res[0:10] {
			fmt.Printf("Result: rank %v, docID %v, score %v (%v)\n", j, doc.Object.GetAdditionalProperty("code"), doc.Score(), doc.Object.GetAdditionalProperty("document"))
			fmt.Printf("Expected: rank %v, docID %v\n", j, query.MatchingDocIDs[j].Object.GetAdditionalProperty("code"))
			require.Equal(t, query.MatchingDocIDs[j], doc.Object.GetAdditionalProperty("code").(string))
		}

	}

}

func FusionConfig(k1, b float32) *models.InvertedIndexConfig {
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

func SetupFusionClass(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger, k1, b float32) {
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

	testData = append(testData, map[string]interface{}{"title": "Why I dont like peanuts", "description": "This is about how we get somewhere"})
	testData = append(testData, map[string]interface{}{"title": "My peanuts in Journey", "description": "A peanut story about journeying"})
	testData = append(testData, map[string]interface{}{"title": "An unrelated title", "description": "Actually all about peanuts"})
	testData = append(testData, map[string]interface{}{"title": "peanuts peanuts", "description": "peanuts peanuts peanuts"})
	testData = append(testData, map[string]interface{}{"title": "peanuts", "description": "peanuts peanuts"})
	testData = append(testData, map[string]interface{}{"title": "PEANUTS", "description": "A LOUD JOURNEY"})

	for i, data := range testData {
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())

		obj := &models.Object{Class: "MyClass", ID: id, Properties: data, CreationTimeUnix: 1565612833955, LastUpdateTimeUnix: 10000020}
		vector := []float32{1, 3, 5, 0.4}
		//{title: "Our journey to BM25F", description: " This is how we get to BM25F"}}
		err := repo.PutObject(context.Background(), obj, vector)
		require.Nil(t, err)
	}
}

func QuickSearch(idx *Index, searchTerms string) []*storobj.Object {
	kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{"title", "description"}, Query: searchTerms}
	addit := additional.Properties{}
	res, _ := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)
	return res
}

// Do a reciprocal rank fusion on the results of two queries
func FusionConcatenate(results [][]*storobj.Object) []*storobj.Object {
	//Concatenate the results
	concatenatedResults := []*storobj.Object{}
	for _, result := range results {
		concatenatedResults = append(concatenatedResults, result...)
	}
	return concatenatedResults
}



func FusionScoreMerge(results [][]*storobj.Object) []*storobj.Object {
	//Concatenate the results
	concatenatedResults := []*storobj.Object{}
	for _, result := range results {
		concatenatedResults = append(concatenatedResults, result...)
	}

	sort.Slice(concatenatedResults, func(i, j int) bool {
		a := concatenatedResults[i].Object.Additional["score"]
		if a == nil {
			return true
		}
		b := concatenatedResults[j].Object.Additional["score"]
		if b == nil {
			return true
		}

		return a.(float32) > b.(float32)
	})
	return concatenatedResults
}

func FusionScoreCombSUM(results [][]*storobj.Object) []*storobj.Object {

	allDocs := map[int64]*storobj.Object{}
	//Loop over each array of results and add the score of each document to the totals
	totals := map[int64]float32{}
	for _, resultSet := range results {
		for _, doc := range resultSet {
			allDocs[int64(doc.DocID())] = doc
			score := doc.Object.Additional["score"].(float32)

			if _, ok := totals[int64(doc.DocID())]; ok {
				totals[int64(doc.DocID())] = totals[int64(doc.DocID())] + score
			} else {
				totals[int64(doc.DocID())] = score
			}

		}
	}

	out := []*storobj.Object{}
	for docID, score := range totals {
		doc := allDocs[docID]
		doc.Object.Additional["score"] = score
		out = append(out, doc)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Object.Additional["score"].(float32) > out[j].Object.Additional["score"].(float32)
	})

	return out
}

func TestRFJourney(t *testing.T) {
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

	SetupFusionClass(t, repo, schemaGetter, logger, 1.2, 0.75)

	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	// Check basic search with one property
	results_set_1 := QuickSearch(idx, "peanuts")
	results_set_2 := QuickSearch(idx, "journey")

	res := FusionScoreMerge([][]*storobj.Object{results_set_1, results_set_2})
	fmt.Println("--- Start results for fusion ---")
	for _, r := range res {
		fmt.Printf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
	}

	results_set_1 = QuickSearch(idx, "peanuts")
	results_set_2 = QuickSearch(idx, "journey")
	res = FusionScoreCombSUM([][]*storobj.Object{results_set_1, results_set_2})
	fmt.Println("--- Start results for CombSUM ---")
	for _, r := range res {
		fmt.Printf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.DocID(), r.Score(), r.Object.Properties.(map[string]interface{})["title"], r.Object.Properties.(map[string]interface{})["description"], r.Object.Additional)
	}

	t.Fail()
}
