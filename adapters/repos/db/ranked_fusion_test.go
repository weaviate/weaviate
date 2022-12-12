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
	
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/sirupsen/logrus/hooks/test"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/searchparams"
	"github.com/semi-technologies/weaviate/entities/storobj"
	enthnsw "github.com/semi-technologies/weaviate/entities/vectorindex/hnsw"
	"github.com/semi-technologies/weaviate/usecases/modules"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
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
		// vector := []float32{1, 3, 5, 0.4} //FIXME, make correct vectors?
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
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(context.TODO())
	require.Nil(t, err)
	defer repo.Shutdown(context.Background())

	SetupStandardTestData(t, repo, schemaGetter, logger, 1.2, 0.75)

	idx := repo.GetIndex("StandardTest")
	require.NotNil(t, idx)

	// Load queries from file standard_test_queries.json
	// This is a list of 100 queries from the MEDLINE database

	data, _ := ioutil.ReadFile("NFCorpus-Query.json")
	var queries []TestQuery
	json.Unmarshal(data, &queries)
	for _, query := range queries {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{}, Query: query.Query}
		addit := additional.Properties{}
		res, _ := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)

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
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(context.TODO())
	require.Nil(t, err)
	defer repo.Shutdown(context.Background())

	SetupStandardTestData(t, repo, schemaGetter, logger, 1.2, 0.75)

	idx := repo.GetIndex("StandardTest")
	require.NotNil(t, idx)

	// Load queries from file standard_test_queries.json
	// This is a list of 100 queries from the MEDLINE database

	data, _ := ioutil.ReadFile("NFCorpus-Query.json")
	var queries []TestQuery
	json.Unmarshal(data, &queries)
	for _, query := range queries {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{}, Query: query.Query}
		addit := additional.Properties{}
		res, _ := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, addit)

		fmt.Printf("query for %s returned %d results\n", query.Query, len(res))
		// fmt.Printf("Results: %v\n", res)

		//for j, doc := range res {
		//	fmt.Printf("res %v, %v\n", j, doc.Object.GetAdditionalProperty("code"))
		//}

		//Check the docIDs are the same
		//for j, doc := range res[0:10] {
		//	fmt.Printf("Result: rank %v, docID %v, score %v (%v)\n", j, doc.Object.GetAdditionalProperty("code"), doc.Score(), doc.Object.GetAdditionalProperty("document"))
		//	fmt.Printf("Expected: rank %v, docID %v\n", j, query.MatchingDocIDs[j].Object.GetAdditionalProperty("code"))
		//	require.Equal(t, query.MatchingDocIDs[j], doc.Object.GetAdditionalProperty("code").(string))
		//}

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


func makeObj(repo *DB, i int, props map[string]interface{}, vec []float32) error {
	id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())

		obj := &models.Object{Class: "MyClass", ID: id, Properties: props, CreationTimeUnix: 1565612833955, LastUpdateTimeUnix: 10000020}
		vector := vec
		err := repo.PutObject(context.Background(), obj, vector)
		return err
}

func SetupFusionClass(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger, k1, b float32) *models.Class{
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
	makeObj(repo, 0,  map[string]interface{}{"title": "Our journey to BM25F", "description": "This is how we get to BM25F"}, []float32{-0.29170582, -0.1946087, 0.036431823, 0.26190448, 0.1457185, -0.14958556, -0.18537146, 0.07159941, 0.028719425, 0.63250583, 0.38365078, 0.14427806, 0.08599936, -0.22769132, 0.012356845, 0.1745498, -0.05338763, -0.18834473, -0.0138510205, -0.21322891, 0.41655016, 0.36946756, -0.102047764, -0.13615902, 0.0023411182, 0.0052980483, 0.2901254, 0.027369997, -0.4641809, -0.1864577, -0.16460614, 0.5820531, 0.73910016, 0.49295703, -0.16398112, -0.22996113, -0.2959687, 0.23074625, 0.102437995, 0.9463584, -0.19165684, 0.1549793, -0.14856845, -0.26328856, -0.14780469, 0.29380506, -0.032966714, -0.18360177, -0.4507165, 0.122722775, -0.5373467, 0.24550001, -0.15466066, 0.08185806, -0.27158406, 0.38852823, 0.36281735, 0.33427292, 0.11524541, 0.32807767, 0.17401297, -1.3853226, -0.27978614, -0.08156872, 0.44477567, 0.025277905, 0.041836135, -0.13199031, -0.41666776, 0.27898142, -0.3336197, -0.24264707, -0.1975279, -0.26044354, 0.49063462, 0.13728416, 0.14936821, 0.22115646, -0.17020147, 0.16663271, 0.085312955, -0.3771666, -0.1715253, 0.029143725, 1.2090286, 0.08519085, 0.0943012, 0.09288853, 0.13448654, -0.10038949, 0.23678489, -0.053011682, 0.4862355, 0.13452885, -0.4574392, 0.1733113, -0.2459824, -0.296974, 0.23954737, 0.20237334, -0.289904, -0.3268171, 1.0279706, 0.057567768, -0.6174384, -0.24310505, 0.04238193, -0.6489579, -0.4174467, 0.29001248, -0.09460957, 0.3890354, -0.5039936, 0.13017255, -0.044932034, -0.46513474, 0.48295417, -0.41836813, -0.34934738, -0.11688752, 0.1291535, 0.2659835, 0.11663312, -0.008710672, -0.09679036, -0.19841288, 1.4258541, 0.30154166, -0.19053933, -0.2675264, 0.5689585, -0.03839084, -0.1382742, -0.59870595, -0.2983454, 0.29235086, 0.3866961, 0.1503561, -0.0004716046, 0.31118277, -0.1502577, 0.02452082, 0.11508085, -0.019777212, -0.05323995, 0.26706985, 0.30423498, -0.27874094, -0.07524122, -0.18219128, 0.01101992, 0.114810504, -0.8511009, -0.4132696, 0.025746733, 0.25158665, -0.25635052, 0.2888975, -0.664737, -0.25225267, -0.733587, 0.01786557, -0.056306545, -0.25526676, 0.0037638196, 0.25607088, 0.20650004, -0.22988722, 0.0051780567, -0.08996672, -0.2434907, 0.21344475, 0.16807927, 0.22440813, -0.48684725, -0.83381325, -0.38593814, -0.28743052, 0.25769538, -0.11687578, -0.011640976, -1.1901944, -0.14203803, -0.1918306, 0.35678816, -0.11999867, -0.09413086, 0.32493654, -0.11633605, 0.38255847, 0.13848558, 0.14571144, 0.41798294, 0.17062645, -0.2323147, 0.20100908, 0.12705114, 0.29455423, 0.154282, 0.003132331, -0.6443605, -0.107634686, 0.6672533, 0.2224891, 0.09176391, 0.25671208, 0.31121197, -0.42522871, 0.10567059, -0.12112835, 0.012253742, 0.33349872, -0.287634, 0.14589946, 0.12491401, -0.002573505, 0.21295182, 0.31351516, 0.4021269, -0.122544184, -0.07630599, 0.030675285, 0.07460723, -0.070872836, 0.10217365, -0.023208441, -0.6565414, -0.01579284, -0.17936167, -0.28660992, -0.53966, -0.23021139, 0.46134365, 0.19351104, 0.09691361, 0.124294795, 0.2826092, 0.34959507, -0.02733651, 0.26470944, 0.20114984, -0.35994378, 0.33248606, 0.44509178, 0.13575919, 0.06968486, -0.5628688, 0.4261216, 0.30926275, 0.043423116, 0.3979026, 0.4127355, 0.3737104, 0.14433874, -0.0840666, -0.07744212, -0.58105636, 0.16744706, -0.47435883, 0.00010625898, 0.11343549, 0.26976803, -0.19726484, -0.47937715, 0.32771236, 0.1456814, -0.16883603, -0.37237936, -0.47352853, -0.35689202, -0.19449078, -0.05345933, -0.055309724, -0.40180895, -0.33430198, 0.32843456, -0.37935862, 0.07344113, 0.27799, 0.14406933, -0.23281038, -0.32313085, 0.13489413, 0.1729742, 0.28639472, 0.08728724, 0.16991487, 0.58374715, 0.046064503, 0.052310575, 0.035705972, 0.081370145, 0.1791731, -0.033532497, 0.09987714, 0.43302703, -0.13957712, 0.19043393, -0.016490348, -0.05788552})
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
	return class
}



// Do a reciprocal rank fusion on the results of two queries
func FusionConcatenate(results [][]*storobj.Object) []*storobj.Object {
	// Concatenate the results
	concatenatedResults := []*storobj.Object{}
	for _, result := range results {
		concatenatedResults = append(concatenatedResults, result...)
	}
	return concatenatedResults
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
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	repo.SetSchemaGetter(schemaGetter)
	err := repo.WaitForStartup(context.TODO())
	require.Nil(t, err)
	defer repo.Shutdown(context.Background())





	class := SetupFusionClass(t, repo, schemaGetter, logger, 1.2, 0.75)

	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)


	// Check basic search with one property
	results_set_1 ,err:= repo.VectorClassSearch(context.TODO(), traverser.GetParams{
		ClassName: "MyClass",
		SearchVector: peanutsVector(),
		Pagination: &filters.Pagination{
			Limit: 100,
		},
	})
	require.Nil(t, err)
	results_set_2,err :=repo.VectorClassSearch(context.TODO(), traverser.GetParams{
		ClassName: "MyClass",
		SearchVector: journeyVector(),
		Pagination: &filters.Pagination{
			Limit: 100,
		},
	})
	require.Nil(t, err)

	res := traverser.FusionScoreCombSUM([][]search.Result{results_set_1, results_set_2})
	fmt.Println("--- Start results for CombSUM ---")
	for _, r := range res {
		fmt.Printf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.ID, r.Score, r.AdditionalProperties["title"], r.AdditionalProperties["description"], r.AdditionalProperties)
	}


	//Create fake explorer for testing
	params := traverser.GetParams{
		ClassName: "MyClass",
		HybridSearch:&searchparams.HybridSearch{
			Query:"peanuts",
			Limit:100,
			Alpha: 0.5,
		},
		Pagination: &filters.Pagination{
			Offset: 0,
			Limit:  100,
		},

	}

	prov := modules.NewProvider()
	prov.SetClassDefaults(class) 


	metrics := &fakeMetrics{}
	log, _ := test.NewNullLogger()
	explorer := traverser.NewExplorer(repo, log, prov, metrics)
	res1, err:=explorer.Hybrid(context.TODO(), params)
	require.Nil(t, err)
	fmt.Println(res1)
}


func journeyVector() []float32 {
	return []float32{-0.523002,0.14169,0.016461,-0.069062,0.487908,-0.024193,-0.282436,0.004778,-0.378135,0.396011,0.094045,-0.06584,0.061162,-0.600018,-0.110189,0.244562,0.433501,0.303775,-0.451004,-0.453709,0.350324,0.2047,-0.091615,-0.282805,-0.232953,-0.215143,0.333113,-0.126952,-0.639225,0.101498,0.232343,0.58831,0.971,0.494446,-0.483305,-0.873438,-0.483694,0.406465,0.342816,1.253387,-0.24718,-0.046063,-0.660406,0.103386,-0.06063,0.3422,0.322542,0.026074,-0.623612,0.489793,-0.632363,0.448922,-0.370049,0.212377,-0.315855,0.364525,0.056798,0.805679,0.145633,0.850648,0.432728,-1.431841,-0.226569,-0.315194,0.560742,0.261859,-0.001653,-0.068738,-0.662729,-0.049259,-0.380322,-0.374194,0.363328,0.341796,-0.077566,0.503337,0.353664,-0.045754,-0.499081,0.198603,0.038837,-0.460198,0.00735,-0.270993,0.950923,-0.085815,-0.52167,-0.10439,0.31398,-0.560229,0.411738,-0.129033,-0.009998,0.443882,-0.045643,-0.078445,-0.259311,-0.08337,0.232652,-0.015912,-0.229458,-0.474973,1.265934,-0.204483,-0.293586,-0.619023,0.158895,-0.730671,-0.163626,0.411716,-0.000132,0.069014,-0.682714,0.303234,0.299097,-0.484469,0.608172,-0.163785,-0.419754,-0.160745,0.278904,0.550542,-0.008052,0.160397,-0.211354,-0.19755,1.182627,0.705073,-0.461941,-0.235292,0.534275,-0.096419,-0.405812,-0.157745,-0.335469,0.200545,0.406497,-0.05341,-0.009234,-0.029925,-0.394101,-0.060133,0.182601,0.615583,0.212157,0.363921,0.41868,-0.652791,0.657173,-0.131662,0.269305,0.381748,-0.827964,-0.452596,0.201918,0.0673,-0.020293,0.486942,-0.72454,-0.435051,-0.615452,-0.218852,0.090703,-0.471036,0.032373,0.569953,0.098359,-0.570767,-0.21015,-0.53019,-0.227117,0.327978,0.087079,-0.115037,0.09193,-0.922884,-0.165566,-0.353596,0.535904,-0.328579,0.029465,-1.508702,-0.320394,-0.596324,0.290277,-0.272515,0.104348,0.062855,-0.236447,0.388958,-0.186552,-0.156253,0.355678,0.53834,-0.321627,0.486004,0.301326,0.786779,0.430292,-0.012458,-0.164964,-0.072951,0.746564,0.19136,0.003213,0.53479,0.511118,-0.559153,-0.088731,-0.436206,0.421004,0.193043,-0.656222,0.133223,0.00107,0.037087,0.263503,0.378593,0.158718,-0.401664,-0.10563,-0.111221,0.018598,-0.036396,0.189584,-0.347721,-0.544111,-0.018158,0.134147,-0.362431,-0.702383,-0.375221,0.365745,0.118082,-0.19102,-0.150732,0.638995,0.070662,-0.054605,0.221755,0.23726,-0.274418,0.294639,0.221177,-0.012947,0.08444,-0.486605,-0.225034,0.774728,0.167609,0.766647,0.381622,0.241907,-0.196452,0.245138,-0.203225,-0.701671,0.236662,-0.627221,0.143006,0.055671,0.564561,-0.114897,-0.542244,0.464601,0.201577,-0.177196,-0.795015,-0.580793,-0.134996,-0.579672,-0.399042,0.008118,-0.458077,-0.43296,0.074138,0.328092,0.02934,0.406294,0.330677,-0.138583,-0.676608,-0.099983,-0.137182,0.713108,0.248643,0.153462,0.56039,-0.109877,0.260655,-0.529779,-0.13416,0.067448,-0.139468,-0.179535,0.372629,0.287185,0.100582,0.093573,-0.208796}
}

func peanutsVector() []float32 {
	return []float32{0.563772,-0.779601,-0.18491,0.509093,0.080691,-0.621506,-0.127855,-0.165435,0.57496,0.006945,0.452967,-0.285534,-0.129205,0.193883,0.092732,0.083284,0.714696,0.107078,-0.398886,-0.117344,-0.387671,0.026748,-0.562581,-0.007178,-0.354846,-0.431299,-0.788228,0.175199,0.914486,0.441425,0.089804,0.284472,0.106916,-0.133174,0.399299,0.002177,0.551474,0.389343,-0.016404,0.770212,-0.219833,0.303322,0.127598,-0.378037,-0.172971,0.394854,-0.424415,-0.71173,0.080323,-0.406372,0.398395,-0.594257,-0.418287,0.055755,-0.352343,-0.393373,-0.732443,0.333113,0.420378,-0.50231,0.261863,-0.061356,-0.180985,0.311916,-0.180207,-0.154169,0.371969,0.454717,0.320499,-0.182448,0.087347,0.585272,0.136098,0.288909,-0.229571,-0.140278,0.229644,-0.557327,-0.110147,0.034364,-0.021627,-0.598707,0.221168,-0.059591,-0.203555,-0.434876,0.209634,-0.460895,-0.345391,-0.18248,-0.24853,0.730295,-0.295402,-0.562237,0.255922,0.076661,-0.713794,-0.354747,-1.109888,-0.066694,-0.195747,-0.282781,0.459869,-0.309599,-0.002211,-0.274471,-0.003621,0.008228,0.011961,-0.258772,-0.210687,-0.664148,-0.257968,0.231335,0.530392,-0.205764,-0.621055,-0.440582,0.080335,0.017367,0.880771,0.656272,-0.713248,-0.208629,0.095346,0.336802,0.888765,0.251927,0.066473,0.182678,-0.220494,0.288927,-0.602036,0.057106,-0.594172,0.848978,0.751973,0.090758,-0.732184,0.683475,-0.075085,0.381326,-0.076531,-0.253831,0.10311,-0.02988,-0.043583,0.005746,-0.460183,-0.189048,0.25792,0.477565,0.391953,0.08469,-0.10022,0.454383,0.170811,0.196819,-0.760276,0.045886,-0.743934,0.190072,-0.216326,-0.624262,-0.22944,0.066233,1.024283,0.044009,-0.373543,-0.243663,0.204444,0.402183,0.043356,0.31716,0.302178,0.369374,0.36901,0.02886,-0.26132,-0.234714,-0.791308,-0.433528,-0.098797,-0.447567,-0.124892,-0.119958,0.31019,-0.096092,-0.259021,-0.078099,-0.178679,0.14879,0.106432,-0.450003,-0.294972,0.044257,0.402832,0.263266,-0.309787,-0.17766,-0.399104,0.577422,0.30102,0.05326,-0.271873,0.204839,-0.019002,-0.743543,0.739314,-0.115868,-0.504568,-0.115713,0.042769,-0.123561,-0.057097,0.407096,0.770627,0.372981,-0.321945,0.349865,0.437571,-0.77394,-0.090017,-0.011273,-0.468664,-0.735247,-0.745655,0.018983,-0.248165,0.215342,-0.136942,-0.458205,0.4572,-0.032293,0.654409,-0.024184,-0.392144,0.634579,0.222185,0.471951,-0.063678,-0.473611,0.796793,-0.295494,-0.157621,-0.103365,-0.564606,-0.092231,-0.517754,-0.369358,0.137479,-0.214837,0.11057,-0.095227,0.726768,-0.079352,-0.065927,-0.846602,-0.317556,-0.344271,0.201353,-0.367633,-0.004477,0.157801,-0.249114,-0.549599,-0.147123,0.308084,-0.175564,0.306867,-0.071157,-0.588356,0.450987,-0.184879,-0.096782,-0.006346,-0.017689,0.005998,0.200963,0.225338,0.189993,-1.105824,0.520005,0.129679,0.198194,-0.254813,-0.127583,0.326054,0.009956,-0.016008,-0.483044,0.801135,-0.517766,0.067179,-0.372756,-0.511781,0.058562,-0.082906,-0.28168,-0.285859}
}