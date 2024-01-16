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
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modules"
	"github.com/weaviate/weaviate/usecases/traverser"
	"github.com/weaviate/weaviate/usecases/traverser/hybrid"
)

type TestDoc struct {
	DocID    string
	Document string
}

type TestQuery struct {
	QueryID        string
	Query          string
	MatchingDocIDs []string
}

var defaultConfig = config.Config{
	QueryDefaults: config.QueryDefaults{
		Limit: 100,
	},
	QueryMaximumResults: 100,
}

func SetupStandardTestData(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger, k1, b float32) {
	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(k1, b, "none"),
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

	data, _ := os.ReadFile("NFCorpus-Corpus.json")
	var docs []TestDoc
	json.Unmarshal(data, &docs)

	for i, doc := range docs {
		id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())

		data := map[string]interface{}{"document": doc.Document, "code": doc.DocID}
		obj := &models.Object{Class: "StandardTest", ID: id, Properties: data, CreationTimeUnix: 1565612833955, LastUpdateTimeUnix: 10000020}
		err := repo.PutObject(context.Background(), obj, nil, nil)
		require.Nil(t, err)
	}
}

func TestHybrid(t *testing.T) {
	dirName := t.TempDir()
	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	SetupStandardTestData(t, repo, schemaGetter, logger, 1.2, 0.75)

	idx := repo.GetIndex("StandardTest")
	require.NotNil(t, idx)

	// Load queries from file standard_test_queries.json
	// This is a list of 100 queries from the MEDLINE database

	data, _ := os.ReadFile("NFCorpus-Query.json")
	var queries []TestQuery
	json.Unmarshal(data, &queries)
	for _, query := range queries {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{}, Query: query.Query}
		addit := additional.Properties{}
		res, _, _ := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0)

		fmt.Printf("query for %s returned %d results\n", query.Query, len(res))

	}
}

func TestBIER(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	SetupStandardTestData(t, repo, schemaGetter, logger, 1.2, 0.75)

	idx := repo.GetIndex("StandardTest")
	require.NotNil(t, idx)

	// Load queries from file standard_test_queries.json
	// This is a list of 100 queries from the MEDLINE database

	data, _ := os.ReadFile("NFCorpus-Query.json")
	var queries []TestQuery
	json.Unmarshal(data, &queries)
	for _, query := range queries {
		kwr := &searchparams.KeywordRanking{Type: "bm25", Properties: []string{}, Query: query.Query}
		addit := additional.Properties{}
		res, _, _ := idx.objectSearch(context.TODO(), 1000, nil, kwr, nil, nil, addit, nil, "", 0)

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

func addObj(repo *DB, i int, props map[string]interface{}, vec []float32) error {
	id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())

	obj := &models.Object{Class: "MyClass", ID: id, Properties: props, CreationTimeUnix: 1565612833955, LastUpdateTimeUnix: 10000020}
	vector := vec
	err := repo.PutObject(context.Background(), obj, vector, nil)
	return err
}

func SetupFusionClass(t require.TestingT, repo *DB, schemaGetter *fakeSchemaGetter, logger logrus.FieldLogger, k1, b float32) *models.Class {
	class := &models.Class{
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: BM25FinvertedConfig(k1, b, "none"),
		Class:               "MyClass",
		Properties: []*models.Property{
			{
				Name:         "title",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord,
			},
			{
				Name:         "description",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWord,
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

	addObj(repo, 0, map[string]interface{}{"title": "Our journey to BM25F", "description": "This is how we get to BM25F"}, []float32{
		-0.04488207, -0.32971063, 0.23568298, 0.004971265, -0.12588727, 0.0036464946, -0.6209942, -0.23247992, -0.19338948, 0.3544481, 0.00050193403, 0.36604947, -0.13813384, -0.126298, 0.05640272, -0.36604303, -0.10976448, 0.196644, -0.02206351, -0.27649152, -0.0050981278, 0.020616557, 0.14605781, 0.093781, -0.25838777, -0.5038474, -0.46846977, 0.13360861, -0.15851927, 0.55075127, 0.34870508, 0.085248806, -0.02763206, -0.07068178, -0.26878145, 0.2814703, -0.33317414, 0.48958343, -0.39648432, 0.606744, 0.12882654, 0.07246548, 0.54059577, 0.19526751, 0.85892624, 0.1485534, 0.19790995, -0.34280643, 0.27512825, 0.105886005, 0.030610563, 0.3811836, 0.18384686, 0.29538825, -0.020791993, -0.31372088, -0.08811446, -0.12979206, 0.30209363, -0.14561261, 0.22077207, -0.40219122, -0.40567216, -0.08740993, -0.31625694, -0.18109407, 0.5411316, -0.09015073, 0.22272661, 0.13575949, -0.36186692, -0.02766613, -0.22463024, 0.15271285, 0.304631, -0.57313913, 0.31346974, -0.118818894, -0.36198893, 0.24609627, 0.47406524, -0.55662453, 0.37812573, -0.2959746, 0.6146945, -0.3934654, 0.30840993, -0.24944904, -0.2063059, -0.48078862, -0.08967737, -0.1273727, 0.1587198, -0.44776592, 0.0048942068, 0.18738478, -0.4544592, -0.4755225, 0.2156486, 0.39935833, 0.25160903, -0.35463294, 0.60699236, -0.12445828, -0.029569108, -0.4983043, 0.44752246, -0.2340386, -0.27559096, 0.67984164, 0.51470226, -0.5285723, 0.0024457057, -0.20425095, -0.4915065, -0.3221788, -0.15558766, 0.20102327, 0.23525643, 0.28365672, 0.58687097, 0.3190138, -0.31130886, 0.053733524, -0.10361888, -0.2598206, 1.5977107, 0.60224503, 0.0074084206, 0.17191416, 0.5619663, 0.41178456, 0.27006987, -0.5354418, -0.054428957, 0.6849038, -0.024342017, 0.43103293, -0.22892271, 0.036829337, -0.103084944, -0.2021301, -0.11352237, -0.17110321, 0.76075333, -0.1755375, 0.029183118, -0.34927735, 0.22040148, -0.18136469, 0.16048056, 0.34151044, -0.048658744, 0.03941434, 0.45190382, 0.103645615, 0.10437423, -0.086864054, 0.523172, -0.59672165, -0.1225319, -0.5800122, 0.2197229, 0.49325037, 0.30607533, 0.012414166, 0.1539727, -0.60095996, 0.05142522, 0.021675617, -0.54661363, -0.0050268047, -0.507448, -0.04522115, -0.77988946, 0.10536073, 0.099219516, 0.40711993, 0.27353838, 0.1728696, -0.4171313, -1.3076599, -0.19778727, -0.23201689, 0.40729725, -0.28640944, 0.06354561, -0.3877251, -0.7938625, 0.29908186, -0.24450836, -0.22622268, 0.32792783, 0.28376722, -0.3685573, 0.031423382, -0.012464195, 0.2254249, 0.26994115, -0.19821979, -0.24086252, 0.24454598, 0.30043048, -0.627896, -0.3355214, -0.14054148, 0.50488055, -0.073988594, -0.31053177, 0.36260405, -0.56093204, 0.12066587, -0.47301888, 0.88418764, 0.09010807, -0.10899238, 0.62317103, 0.27237964, -0.604178, 0.0067386073, 0.1370205, -0.094664395, 0.3479645, 0.25092986, 0.16948108, -0.20874223, -0.54980844, -0.100548536, 0.47177002, -0.4981452, 0.1815202, -0.80878633, -0.076736815, 0.43152434, -0.43210435, -0.28010413, 0.1249095, 0.385616, -0.2984289, -0.006841246, 0.3496464, -0.33298343, 0.06344994, 0.37393335, 0.18608452, -0.10631552, -0.40111285, -0.146849, -0.04161288, -0.31621853, -0.06889858, 0.13343252, -0.11599523, 0.5377954, 0.25938663, -0.43172404, -0.7476662, -0.54316807, 0.0029651164, 0.09958581, 0.0730254, 0.22785394, 0.3276773, 0.01816153, 0.094938636, 0.71604383, -0.09648144, -0.0035640472, -0.5383972, 0.28588042, 0.7625968, -0.22359839, 0.17167832, -0.06235203, -0.32480234, -0.18599075, 0.1570872, -0.06470149, -0.029198159, 0.23251827, 0.100047514, -0.06314679, 0.6390605, -0.06232509, 0.76272035, 0.2975126, 0.15871438, 0.18222457, -0.548036, 0.23633306, -0.17981203, 0.023965335, 0.24478278, -0.21601695, -0.108217336, 0.05834005, 0.3718355, 0.0970174, 0.04476983, -0.118143275,
	})

	addObj(repo, 1, map[string]interface{}{"title": "Our peanuts to BM25F", "description": "This is how we get to BM25F"}, []float32{0.11676752, -0.4837953, -0.06559026, 0.3242706, 0.08680799, -0.30777612, -0.22926088, 0.01667141, 0.31844103, 0.4666344, 0.417305, 0.06108997, -0.0740552, 0.14234918, 0.06823654, 0.16182217, -0.012199775, -0.17269811, -0.16104576, -0.09208117, 0.063624315, 0.3113634, -0.3830663, 0.05831715, -0.14125349, -0.26962206, -0.0696671, -0.013111545, 0.20097807, 0.033809602, -0.048573885, 0.46815604, 0.32582077, 0.32308698, 0.20355524, -0.08757271, 0.17099291, 0.31500003, -0.05445185, 0.7712824, -0.2096038, 0.28787872, 0.10871067, -0.3266944, -0.1633618, 0.34630018, -0.15387866, -0.45506623, -0.21508889, -0.19249445, -0.28801772, -0.2694916, -0.18476918, -0.12890251, -0.29947013, 0.0008435306, -0.06490287, -0.006560939, 0.24637267, -0.111215346, 0.3775517, -0.82433224, -0.3179537, 0.022306278, 0.19248968, -0.1701471, 0.052865, -0.044782564, -0.10222186, 0.09571932, -0.19251339, 0.241193, -0.13216764, -0.19301765, 0.46628228, -0.29973802, 0.0030274116, 0.01664786, 0.1216316, 0.12837356, -0.048461247, -0.56439394, 0.06110007, 0.102808535, 0.63137263, -0.13134736, 0.41365498, -0.113528065, -0.06924132, 0.1076709, -0.06833764, 0.31522226, 0.13445137, -0.16227263, -0.15102008, 0.23768687, -0.41108298, -0.473573, -0.35702798, 0.21465969, -0.30590045, -0.26616427, 0.7287231, -0.036261655, -0.34903425, -0.1396425, -0.022058574, -0.33956096, -0.3359471, -0.035496157, -0.1786069, -0.0857123, -0.0845917, 0.13232024, -0.02890402, -0.45281035, -0.026353035, -0.39124215, -0.15753527, -0.075793914, 0.35795033, 0.35925874, -0.1423145, -0.0969307, 0.08920737, 0.15772092, 1.3536518, 0.29779792, -0.05407743, -0.048793554, 0.12263066, -0.06248072, -0.49598575, -0.46484944, -0.31050035, 0.6283043, 0.5242193, 0.25987545, -0.2584134, 0.32898954, 0.014580286, 0.14016634, -0.010093123, -0.22610027, -0.029830063, 0.18112054, 0.020298548, -0.025797658, -0.40394786, -0.17097965, 0.11640611, 0.29304397, -0.27026933, -0.14832975, -0.099585906, 0.4554175, -0.0018298444, 0.23190805, -0.65866566, -0.09366216, -0.7000203, 0.004698127, -0.17523476, -0.34830904, -0.16284281, 0.15495956, 0.5772887, 0.048939474, -0.12923703, -0.236143, -0.03874896, 0.2960667, 0.029154046, 0.42814374, -0.4332385, -0.31293675, -0.10682973, -0.12069777, 0.071893886, 0.06644212, -0.46342105, -0.8599067, 0.017380634, -0.38347453, 0.14165273, -0.08906643, -0.06801824, 0.19660597, -0.06807183, 0.33882818, 0.044932134, 0.27550527, 0.2308957, -0.101730466, -0.19064885, -0.015364495, 0.0149245, 0.24177131, -0.15636654, -0.002376896, -0.6399841, 0.14845476, 0.46339074, 0.036926877, -0.067630276, 0.289784, 0.15529989, -0.5235124, 0.50196457, -0.004536148, -0.3716798, 0.047304608, -0.027990041, 0.15901157, 0.021176483, 0.35387334, 0.4457043, 0.094738215, 0.08722517, 0.0450516, 0.1739127, -0.2606226, 0.035999063, -0.12919275, -0.11809982, -0.20865, -0.6917279, 0.093973815, -0.38069052, -0.114874505, -0.3051481, -0.357749, 0.48254266, 0.31795567, 0.37491056, 0.0047062743, -0.1265727, 0.51655954, 0.1622121, 0.39811996, -0.002116253, -0.375531, 0.6347343, 0.14833164, 0.032251768, 0.021101426, -0.34346518, 0.22451165, 0.028649824, -0.04794777, 0.056036226, 0.14179966, 0.32724753, 0.17185552, 0.2504634, -0.05013007, -0.31430584, -0.22200464, -0.508279, -0.10017326, 0.16302426, -0.09568865, 0.05985463, -0.22916546, -0.084666654, -0.15271503, -0.24385636, -0.028514259, -0.33194387, -0.17132543, -0.1474212, -0.18526097, 0.2198915, -0.1689729, -0.19907063, 0.19941927, -0.47478884, 0.0695081, 0.3741401, 0.19423902, 0.085894205, -0.53214043, 0.33309302, 0.18701339, 0.23461546, -0.14038202, 0.07201847, 0.3462437, 0.1640635, 0.07200127, -0.09130982, 0.3868172, -0.09754013, 0.040958565, -0.18743117, 0.14117524, -0.18739408, 0.13669269, -0.09902989, -0.16762646})

	addObj(repo, 2, map[string]interface{}{"title": "Elephant Parade", "description": "Elephants elephants elephant"}, []float32{-0.04488207, -0.32971063, 0.23568298, 0.004971265, -0.12588727, 0.0036464946, -0.6209942, -0.23247992, -0.19338948, 0.3544481, 0.00050193403, 0.36604947, -0.13813384, -0.126298, 0.05640272, -0.36604303, -0.10976448, 0.196644, -0.02206351, -0.27649152, -0.0050981278, 0.020616557, 0.14605781, 0.093781, -0.25838777, -0.5038474, -0.46846977, 0.13360861, -0.15851927, 0.55075127, 0.34870508, 0.085248806, -0.02763206, -0.07068178, -0.26878145, 0.2814703, -0.33317414, 0.48958343, -0.39648432, 0.606744, 0.12882654, 0.07246548, 0.54059577, 0.19526751, 0.85892624, 0.1485534, 0.19790995, -0.34280643, 0.27512825, 0.105886005, 0.030610563, 0.3811836, 0.18384686, 0.29538825, -0.020791993, -0.31372088, -0.08811446, -0.12979206, 0.30209363, -0.14561261, 0.22077207, -0.40219122, -0.40567216, -0.08740993, -0.31625694, -0.18109407, 0.5411316, -0.09015073, 0.22272661, 0.13575949, -0.36186692, -0.02766613, -0.22463024, 0.15271285, 0.304631, -0.57313913, 0.31346974, -0.118818894, -0.36198893, 0.24609627, 0.47406524, -0.55662453, 0.37812573, -0.2959746, 0.6146945, -0.3934654, 0.30840993, -0.24944904, -0.2063059, -0.48078862, -0.08967737, -0.1273727, 0.1587198, -0.44776592, 0.0048942068, 0.18738478, -0.4544592, -0.4755225, 0.2156486, 0.39935833, 0.25160903, -0.35463294, 0.60699236, -0.12445828, -0.029569108, -0.4983043, 0.44752246, -0.2340386, -0.27559096, 0.67984164, 0.51470226, -0.5285723, 0.0024457057, -0.20425095, -0.4915065, -0.3221788, -0.15558766, 0.20102327, 0.23525643, 0.28365672, 0.58687097, 0.3190138, -0.31130886, 0.053733524, -0.10361888, -0.2598206, 1.5977107, 0.60224503, 0.0074084206, 0.17191416, 0.5619663, 0.41178456, 0.27006987, -0.5354418, -0.054428957, 0.6849038, -0.024342017, 0.43103293, -0.22892271, 0.036829337, -0.103084944, -0.2021301, -0.11352237, -0.17110321, 0.76075333, -0.1755375, 0.029183118, -0.34927735, 0.22040148, -0.18136469, 0.16048056, 0.34151044, -0.048658744, 0.03941434, 0.45190382, 0.103645615, 0.10437423, -0.086864054, 0.523172, -0.59672165, -0.1225319, -0.5800122, 0.2197229, 0.49325037, 0.30607533, 0.012414166, 0.1539727, -0.60095996, 0.05142522, 0.021675617, -0.54661363, -0.0050268047, -0.507448, -0.04522115, -0.77988946, 0.10536073, 0.099219516, 0.40711993, 0.27353838, 0.1728696, -0.4171313, -1.3076599, -0.19778727, -0.23201689, 0.40729725, -0.28640944, 0.06354561, -0.3877251, -0.7938625, 0.29908186, -0.24450836, -0.22622268, 0.32792783, 0.28376722, -0.3685573, 0.031423382, -0.012464195, 0.2254249, 0.26994115, -0.19821979, -0.24086252, 0.24454598, 0.30043048, -0.627896, -0.3355214, -0.14054148, 0.50488055, -0.073988594, -0.31053177, 0.36260405, -0.56093204, 0.12066587, -0.47301888, 0.88418764, 0.09010807, -0.10899238, 0.62317103, 0.27237964, -0.604178, 0.0067386073, 0.1370205, -0.094664395, 0.3479645, 0.25092986, 0.16948108, -0.20874223, -0.54980844, -0.100548536, 0.47177002, -0.4981452, 0.1815202, -0.80878633, -0.076736815, 0.43152434, -0.43210435, -0.28010413, 0.1249095, 0.385616, -0.2984289, -0.006841246, 0.3496464, -0.33298343, 0.06344994, 0.37393335, 0.18608452, -0.10631552, -0.40111285, -0.146849, -0.04161288, -0.31621853, -0.06889858, 0.13343252, -0.11599523, 0.5377954, 0.25938663, -0.43172404, -0.7476662, -0.54316807, 0.0029651164, 0.09958581, 0.0730254, 0.22785394, 0.3276773, 0.01816153, 0.094938636, 0.71604383, -0.09648144, -0.0035640472, -0.5383972, 0.28588042, 0.7625968, -0.22359839, 0.17167832, -0.06235203, -0.32480234, -0.18599075, 0.1570872, -0.06470149, -0.029198159, 0.23251827, 0.100047514, -0.06314679, 0.6390605, -0.06232509, 0.76272035, 0.2975126, 0.15871438, 0.18222457, -0.548036, 0.23633306, -0.17981203, 0.023965335, 0.24478278, -0.21601695, -0.108217336, 0.05834005, 0.3718355, 0.0970174, 0.04476983, -0.118143275})

	return class
}

func TestRFJourney(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		QueryLimit:                20,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	class := SetupFusionClass(t, repo, schemaGetter, logger, 1.2, 0.75)
	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	doc1 := &hybrid.Result{
		Result: &search.Result{
			ID: strfmt.UUID("e6f7e8b1-ac53-48eb-b6e4-cbe67396bcfa"),
			Schema: map[string]interface{}{
				"title": "peanuts",
			},
			Vector: []float32{0.1, 0.2, 0.3, 0.4, 0.5},
			Score:  0.1,
		},
	}

	doc2 := &hybrid.Result{
		Result: &search.Result{
			ID: strfmt.UUID("2b7a8bc9-29d9-4cc8-b145-a0baf5fc231d"),
			Schema: map[string]interface{}{
				"title": "journey",
			},
			Vector: []float32{0.5, 0.4, 0.3, 0.3, 0.1},
			Score:  0.2,
		},
	}

	doc3 := &hybrid.Result{
		Result: &search.Result{
			ID: strfmt.UUID("dddddddd-29d9-4cc8-b145-a0baf5fc231d"),
			Schema: map[string]interface{}{
				"title": "alalala",
			},
			Vector: []float32{0.5, 0.4, 0.3, 0.3, 0.1},
			Score:  0.2,
		},
	}

	resultSet1 := []*hybrid.Result{doc1, doc2, doc3}
	resultSet2 := []*hybrid.Result{doc2, doc1, doc3}

	t.Run("Fusion Reciprocal", func(t *testing.T) {
		results := hybrid.FusionRanked([]float64{0.4, 0.6},
			[][]*hybrid.Result{resultSet1, resultSet2}, []string{"set1", "set2"})
		fmt.Println("--- Start results for Fusion Reciprocal ---")
		for _, result := range results {
			schema := result.Schema.(map[string]interface{})
			fmt.Println(schema["title"], result.ID, result.Score)
		}
		require.Equal(t, 3, len(results))
		require.Equal(t, resultSet2[0].ID, results[0].ID)
		require.Equal(t, resultSet2[1].ID, results[1].ID)
		require.Equal(t, resultSet2[2].ID, results[2].ID)
		require.Equal(t, float32(0.016287679), results[0].Score)
		require.Equal(t, float32(0.016234796), results[1].Score)
	})

	t.Run("Fusion Reciprocal 2", func(t *testing.T) {
		results := hybrid.FusionRanked([]float64{0.8, 0.2},
			[][]*hybrid.Result{resultSet1, resultSet2}, []string{"set1", "set2"})
		fmt.Println("--- Start results for Fusion Reciprocal ---")
		for _, result := range results {
			schema := result.Schema.(map[string]interface{})
			fmt.Println(schema["title"], result.ID, result.Score)
		}
		require.Equal(t, 3, len(results))
		require.Equal(t, resultSet2[0].ID, results[1].ID)
		require.Equal(t, resultSet2[1].ID, results[0].ID)
		require.Equal(t, resultSet2[2].ID, results[2].ID)
		require.Equal(t, float32(0.016340561), results[0].Score)
		require.Equal(t, float32(0.016181914), results[1].Score)
	})

	t.Run("Vector Only", func(t *testing.T) {
		results := hybrid.FusionRanked([]float64{0.0, 1.0},
			[][]*hybrid.Result{resultSet1, resultSet2}, []string{"set1", "set2"})
		fmt.Println("--- Start results for Fusion Reciprocal ---")
		for _, result := range results {
			schema := result.Schema.(map[string]interface{})
			fmt.Println(schema["title"], result.ID, result.Score)
		}
		require.Equal(t, 3, len(results))
		require.Equal(t, resultSet2[0].ID, results[0].ID)
		require.Equal(t, resultSet2[1].ID, results[1].ID)
		require.Equal(t, resultSet2[2].ID, results[2].ID)
		require.Equal(t, float32(0.016393442), results[0].Score)
		require.Equal(t, float32(0.016129032), results[1].Score)
	})

	t.Run("BM25 only", func(t *testing.T) {
		results := hybrid.FusionRanked([]float64{1.0, 0.0},
			[][]*hybrid.Result{resultSet1, resultSet2}, []string{"set1", "set2"})
		fmt.Println("--- Start results for Fusion Reciprocal ---")
		for _, result := range results {
			schema := result.Schema.(map[string]interface{})
			fmt.Println(schema["title"], result.ID, result.Score)
		}
		require.Equal(t, 3, len(results))
		require.Equal(t, resultSet1[0].ID, results[0].ID)
		require.Equal(t, resultSet1[1].ID, results[1].ID)
		require.Equal(t, resultSet1[2].ID, results[2].ID)
		require.Equal(t, float32(0.016393442), results[0].Score)
		require.Equal(t, float32(0.016129032), results[1].Score)
	})

	// Check basic search with one property
	results_set_1, err := repo.VectorSearch(context.TODO(), dto.GetParams{
		ClassName:    "MyClass",
		SearchVector: peanutsVector(),
		Pagination: &filters.Pagination{
			Offset: 0,
			Limit:  6,
		},
	})

	require.Nil(t, err)
	results_set_2, err := repo.VectorSearch(context.TODO(), dto.GetParams{
		ClassName:    "MyClass",
		SearchVector: journeyVector(),
		Pagination: &filters.Pagination{
			Offset: 0,
			Limit:  6,
		},
	})
	require.Nil(t, err)

	// convert search.Result to hybrid.Result
	var results_set_1_hybrid []*hybrid.Result
	for _, r := range results_set_1 {
		// parse the last 12 digits of the id to get the uint64
		id, err := strconv.Atoi(string(r.ID)[len(r.ID)-12:])
		if err != nil {
			fmt.Println(err)
		}

		results_set_1_hybrid = append(results_set_1_hybrid, &hybrid.Result{
			DocID:  uint64(id),
			Result: &r,
		})
	}

	var results_set_2_hybrid []*hybrid.Result
	for _, r := range results_set_2 {
		// parse the last 12 digits of the id to get the uint64
		id, err := strconv.Atoi(string(r.ID)[len(r.ID)-12:])
		if err != nil {
			fmt.Println(err)
		}

		results_set_2_hybrid = append(results_set_2_hybrid, &hybrid.Result{
			DocID:  uint64(id),
			Result: &r,
		})
	}

	res := hybrid.FusionRanked([]float64{0.2, 0.8}, [][]*hybrid.Result{results_set_1_hybrid, results_set_2_hybrid}, []string{"set1", "set2"})
	fmt.Println("--- Start results for Fusion Reciprocal (", len(res), ")---")
	for _, r := range res {

		schema := r.Schema.(map[string]interface{})
		title := schema["title"].(string)
		description := schema["description"].(string)
		fmt.Printf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.ID, r.Score, title, description, r.AdditionalProperties)
	}

	require.Equal(t, "00000000-0000-0000-0000-000000000000", string(res[0].ID))

	t.Run("Hybrid", func(t *testing.T) {
		params := dto.GetParams{
			ClassName: "MyClass",
			HybridSearch: &searchparams.HybridSearch{
				Query:  "elephant",
				Vector: elephantVector(),
				Alpha:  0.5,
			},
			Pagination: &filters.Pagination{
				Offset: 0,
				Limit:  6,
			},
		}

		prov := modules.NewProvider()
		prov.SetClassDefaults(class)

		metrics := &fakeMetrics{}
		log, _ := test.NewNullLogger()
		explorer := traverser.NewExplorer(repo, log, prov, metrics, defaultConfig)
		hybridResults, err := explorer.Hybrid(context.TODO(), params)
		require.Nil(t, err)

		fmt.Println("--- Start results for hybrid ---")
		for _, r := range hybridResults {
			schema := r.Schema.(map[string]interface{})
			title := schema["title"].(string)
			description := schema["description"].(string)
			fmt.Printf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.ID, r.Score, title, description, r.AdditionalProperties)
		}
	})

	t.Run("Hybrid with negative limit", func(t *testing.T) {
		params := dto.GetParams{
			ClassName: "MyClass",
			HybridSearch: &searchparams.HybridSearch{
				Query:  "Elephant Parade",
				Vector: elephantVector(),
				Alpha:  0.5,
			},
			Pagination: &filters.Pagination{
				Offset: 0,
				Limit:  -1,
			},
		}

		prov := modules.NewProvider()
		prov.SetClassDefaults(class)

		metrics := &fakeMetrics{}
		log, _ := test.NewNullLogger()
		explorer := traverser.NewExplorer(repo, log, prov, metrics, defaultConfig)
		hybridResults, err := explorer.Hybrid(context.TODO(), params)

		fmt.Println("--- Start results for hybrid with negative limit ---")
		for _, r := range hybridResults {
			schema := r.Schema.(map[string]interface{})
			title := schema["title"].(string)
			description := schema["description"].(string)
			fmt.Printf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.ID, r.Score, title, description, r.AdditionalProperties)
		}
		require.Nil(t, err)
		require.True(t, len(hybridResults) > 0)
	})

	t.Run("Hybrid with offset", func(t *testing.T) {
		params := dto.GetParams{
			ClassName: "MyClass",
			HybridSearch: &searchparams.HybridSearch{
				Query:  "Elephant Parade",
				Vector: elephantVector(),
				Alpha:  0.5,
			},
			Pagination: &filters.Pagination{
				Offset: 2,
				Limit:  1,
			},
		}

		prov := modules.NewProvider()
		prov.SetClassDefaults(class)

		metrics := &fakeMetrics{}
		log, _ := test.NewNullLogger()
		explorer := traverser.NewExplorer(repo, log, prov, metrics, defaultConfig)
		hybridResults, err := explorer.Hybrid(context.TODO(), params)

		fmt.Println("--- Start results for hybrid with offset ---")
		for _, r := range hybridResults {
			schema := r.Schema.(map[string]interface{})
			title := schema["title"].(string)
			description := schema["description"].(string)
			fmt.Printf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.ID, r.Score, title, description, r.AdditionalProperties)
		}

		require.Nil(t, err)
		require.True(t, len(hybridResults) == 1)
		require.True(t, hybridResults[0].ID == "00000000-0000-0000-0000-000000000001")
	})

	t.Run("Hybrid with offset", func(t *testing.T) {
		params := dto.GetParams{
			ClassName: "MyClass",
			HybridSearch: &searchparams.HybridSearch{
				Query:  "Elephant Parade",
				Vector: elephantVector(),
				Alpha:  0.5,
			},
			Pagination: &filters.Pagination{
				Offset: 4,
				Limit:  1,
			},
		}

		prov := modules.NewProvider()
		prov.SetClassDefaults(class)

		metrics := &fakeMetrics{}
		log, _ := test.NewNullLogger()
		explorer := traverser.NewExplorer(repo, log, prov, metrics, defaultConfig)
		hybridResults, err := explorer.Hybrid(context.TODO(), params)

		fmt.Println("--- Start results for hybrid with offset ---")
		for _, r := range hybridResults {
			schema := r.Schema.(map[string]interface{})
			title := schema["title"].(string)
			description := schema["description"].(string)
			fmt.Printf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.ID, r.Score, title, description, r.AdditionalProperties)
		}

		require.Nil(t, err)
		require.True(t, len(hybridResults) == 0)
	})
}

func TestRFJourneyWithFilters(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		QueryLimit:                20,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	class := SetupFusionClass(t, repo, schemaGetter, logger, 1.2, 0.75)
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
						Value: "elephant",
						Type:  schema.DataTypeText,
					},
				},
				{
					Operator: filters.OperatorEqual,
					On: &filters.Path{
						Class:    schema.ClassName("MyClass"),
						Property: schema.PropertyName("title"),
					},
					Value: &filters.Value{
						Value: "elephant",
						Type:  schema.DataTypeText,
					},
				},
			},
		},
	}

	filter1 := &filters.LocalFilter{
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
						Type:  schema.DataTypeText,
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
						Type:  schema.DataTypeText,
					},
				},
			},
		},
	}

	t.Run("Hybrid with filter - no results expected", func(t *testing.T) {
		params := dto.GetParams{
			ClassName: "MyClass",
			HybridSearch: &searchparams.HybridSearch{
				Query:  "elephant",
				Vector: elephantVector(),
				Alpha:  0.5,
			},
			Pagination: &filters.Pagination{
				Offset: 0,
				Limit:  100,
			},
			Filters: filter1,
		}

		prov := modules.NewProvider()
		prov.SetClassDefaults(class)

		metrics := &fakeMetrics{}
		log, _ := test.NewNullLogger()
		explorer := traverser.NewExplorer(repo, log, prov, metrics, defaultConfig)
		hybridResults, err := explorer.Hybrid(context.TODO(), params)
		require.Nil(t, err)
		require.Equal(t, 0, len(hybridResults))
	})

	t.Run("Hybrid", func(t *testing.T) {
		params := dto.GetParams{
			ClassName: "MyClass",
			HybridSearch: &searchparams.HybridSearch{
				Query:  "elephant",
				Vector: elephantVector(),
				Alpha:  0.5,
			},
			Pagination: &filters.Pagination{
				Offset: 0,
				Limit:  -1,
			},
		}

		prov := modules.NewProvider()
		prov.SetClassDefaults(class)

		metrics := &fakeMetrics{}
		log, _ := test.NewNullLogger()
		explorer := traverser.NewExplorer(repo, log, prov, metrics, defaultConfig)
		hybridResults, err := explorer.Hybrid(context.TODO(), params)
		require.Nil(t, err)
		require.Equal(t, 3, len(hybridResults))

		fmt.Println("--- Start results for hybrid ---")
		for _, r := range hybridResults {
			schema := r.Schema.(map[string]interface{})
			title := schema["title"].(string)
			description := schema["description"].(string)
			fmt.Printf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.ID, r.Score, title, description, r.AdditionalProperties)
		}
		require.Equal(t, strfmt.UUID("00000000-0000-0000-0000-000000000002"), hybridResults[0].ID)
	})

	t.Run("Hybrid with filter", func(t *testing.T) {
		params := dto.GetParams{
			ClassName: "MyClass",
			HybridSearch: &searchparams.HybridSearch{
				Query:  "elephant",
				Vector: elephantVector(),
				Alpha:  0.5,
			},
			Pagination: &filters.Pagination{
				Offset: 0,
				Limit:  -1,
			},
			Filters: filter,
		}

		prov := modules.NewProvider()
		prov.SetClassDefaults(class)

		metrics := &fakeMetrics{}
		log, _ := test.NewNullLogger()
		explorer := traverser.NewExplorer(repo, log, prov, metrics, defaultConfig)
		hybridResults, err := explorer.Hybrid(context.TODO(), params)
		require.Nil(t, err)
		require.Equal(t, 1, len(hybridResults))

		fmt.Println("--- Start results for hybrid ---")
		for _, r := range hybridResults {
			schema := r.Schema.(map[string]interface{})
			title := schema["title"].(string)
			description := schema["description"].(string)
			fmt.Printf("Result id: %v, score: %v, title: %v, description: %v, additional %+v\n", r.ID, r.Score, title, description, r.AdditionalProperties)
		}
		require.Equal(t, strfmt.UUID("00000000-0000-0000-0000-000000000002"), hybridResults[0].ID)
	})
}

func TestStability(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		QueryLimit:                20,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	SetupFusionClass(t, repo, schemaGetter, logger, 1.2, 0.75)
	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	doc1 := &hybrid.Result{
		Result: &search.Result{
			ID: strfmt.UUID("e6f7e8b1-ac53-48eb-b6e4-cbe67396bcfa"),
			Schema: map[string]interface{}{
				"title": "peanuts",
			},
			Vector: []float32{0.1, 0.2, 0.3, 0.4, 0.5},
			Score:  0.1,
		},
	}

	doc2 := &hybrid.Result{
		Result: &search.Result{
			ID: strfmt.UUID("e6f7e8b1-ac53-48eb-b6e4-cbe67396bcfb"),
			Schema: map[string]interface{}{
				"title": "peanuts",
			},
			Vector: []float32{0.1, 0.2, 0.3, 0.4, 0.5},
			Score:  0.1,
		},
	}

	doc3 := &hybrid.Result{
		Result: &search.Result{
			ID: strfmt.UUID("e6f7e8b1-ac53-48eb-b6e4-cbe67396bcfc"),
			Schema: map[string]interface{}{
				"title": "peanuts",
			},
			Vector: []float32{0.1, 0.2, 0.3, 0.4, 0.5},
			Score:  0.1,
		},
	}

	resultSet1 := []*hybrid.Result{doc1, doc2, doc3}
	resultSet2 := []*hybrid.Result{doc2, doc1, doc3}

	t.Run("Fusion Reciprocal", func(t *testing.T) {
		results := hybrid.FusionRanked([]float64{0.4, 0.6},
			[][]*hybrid.Result{resultSet1, resultSet2}, []string{"set1", "set2"})
		fmt.Println("--- Start results for Fusion Reciprocal ---")
		for _, result := range results {
			schema := result.Schema.(map[string]interface{})
			fmt.Println(schema["title"], result.ID, result.Score)
		}
		require.Equal(t, 3, len(results))
		require.Equal(t, resultSet2[0].ID, results[0].ID)
		require.Equal(t, resultSet2[1].ID, results[1].ID)
		require.Equal(t, resultSet2[2].ID, results[2].ID)
	})
}

func elephantVector() []float32 {
	return []float32{
		-0.106136, -0.021716, 0.632442, 0.195315, -0.038854, -0.260533, -0.728847, -0.313725, -0.161967, 0.179243, -0.124185, 0.158839, 0.09563, -0.071267, 0.073928, -0.096735, 0.27266, -0.204127, -0.387028, -0.361406, -0.278027, 0.298766, 0.265405, 0.037477, -0.079904, -0.778953, -0.525643, -0.052346, -0.2174, 0.095746, 0.610937, 0.315672, -0.125526, 0.013475, -0.075578, -0.053183, -0.381475, 0.620278, -0.093857, 0.802608, -0.105773, -0.007902, 0.663528, 0.407708, 0.753832, 0.420718, 0.139289, -0.126864, 0.36345, -0.039222, 0.089002, 0.092151, 0.138025, 0.18881, 0.51416, -0.391045, -0.169528, -0.044023, 0.437196, -0.23917, 0.081247, -0.440846, -0.484764, 0.090495, 0.001852, -0.03441, 0.18548, -0.440182, 0.286827, -0.081451, 0.030155, -0.072746, -0.366531, 0.354118, 0.418432, -0.305682, 0.515893, -0.424999, -0.495273, 0.731375, 0.358407, -0.415989, 0.441337, -0.022167, 0.318837, -0.473018, 0.342046, -0.499794, -0.303161, -0.379234, -0.279082, -0.325648, 0.200613, -0.457396, 0.116745, 0.225836, -0.322175, -0.151425, 0.322014, 0.077097, 0.049998, -0.01005, 0.489028, -0.273297, 0.218896, -0.507729, 0.488891, -0.207774, -0.499136, 0.992803, 0.379556, -0.572352, -0.295821, -0.071392, -0.625823, -0.425159, 0.024593, 0.307965, 0.311686, 0.287844, 0.435028, 0.454474, -0.208158, -0.111947, -0.380334, -0.392014, 1.747561, 0.360315, 0.472088, 0.273835, 0.635424, 0.390057, -0.021349, -0.746944, 0.265353, 0.60709, -0.171053, 0.408823, -0.059646, 0.058306, -0.062817, -0.41064, -0.342016, -0.048077, 0.862758, -0.217101, -0.048961, -0.314094, 0.228395, -0.339353, 0.558551, 0.370054, -0.319855, 0.543137, 0.71334, 0.166296, 0.040412, -0.160482, 0.432088, -0.491292, 0.072819, -0.409627, 0.300197, 0.169077, 0.44379, 0.117131, 0.142459, -0.482226, -0.100245, 0.058273, -0.590567, -0.061971, -0.415718, -0.018105, -0.693528, -0.047609, -0.041873, 0.606186, 0.19767, -0.091001, -0.315381, -1.234111, 0.228805, -0.636861, 0.208757, -0.270024, -0.259684, -0.351592, -0.978549, 0.683986, -0.331669, -0.078729, 0.385676, 0.390955, -0.901898, -0.071451, -0.103991, 0.206379, 0.469656, 0.071528, -0.152589, 0.282268, 0.539651, -0.856463, -0.344053, -0.40572, 0.771483, -0.065611, -0.408832, 0.303948, -0.565157, 0.153293, -0.699892, 1.112725, 0.259508, 0.135771, 0.484552, 0.151274, -0.743235, 0.069811, 0.137583, 0.212661, 0.376839, 0.136164, 0.145626, -0.466645, -0.474334, -0.365033, 0.251158, -0.313904, 0.210487, -1.016155, 0.262768, 0.432895, -0.291339, -0.221825, 0.513278, 0.659038, -0.401398, -0.164522, 0.395279, -0.449811, 0.076142, 0.389243, 0.076184, 0.05539, -0.597094, -0.149824, 0.206724, -0.477001, -0.315719, 0.166689, -0.357187, 0.34429, 0.256624, -0.236781, -0.713059, -0.440255, 0.27353, -0.032257, 0.06925, 0.359134, -0.088975, 0.112507, -0.071103, 0.880417, 0.528587, 0.155656, -0.720531, 0.3068, 0.754715, 0.009366, 0.067487, -0.11898, -0.471064, -0.396507, 0.298669, 0.038283, 0.057218, -0.075818, -0.01513, -0.319236, 0.692123, -0.122985, 0.875938, 0.378184, 0.427029, 0.315545, -0.549573, 0.389602, -0.017071, 0.160122, 0.368208, 0.060474, -0.199651, 0.087829, 0.447339, 0.012265, -0.095388, -0.07034,
	}
}

// "journey"
func journeyVector() []float32 {
	return []float32{
		-0.523002, 0.14169, 0.016461, -0.069062, 0.487908, -0.024193, -0.282436, 0.004778, -0.378135, 0.396011, 0.094045, -0.06584, 0.061162, -0.600018, -0.110189, 0.244562, 0.433501, 0.303775, -0.451004, -0.453709, 0.350324, 0.2047, -0.091615, -0.282805, -0.232953, -0.215143, 0.333113, -0.126952, -0.639225, 0.101498, 0.232343, 0.58831, 0.971, 0.494446, -0.483305, -0.873438, -0.483694, 0.406465, 0.342816, 1.253387, -0.24718, -0.046063, -0.660406, 0.103386, -0.06063, 0.3422, 0.322542, 0.026074, -0.623612, 0.489793, -0.632363, 0.448922, -0.370049, 0.212377, -0.315855, 0.364525, 0.056798, 0.805679, 0.145633, 0.850648, 0.432728, -1.431841, -0.226569, -0.315194, 0.560742, 0.261859, -0.001653, -0.068738, -0.662729, -0.049259, -0.380322, -0.374194, 0.363328, 0.341796, -0.077566, 0.503337, 0.353664, -0.045754, -0.499081, 0.198603, 0.038837, -0.460198, 0.00735, -0.270993, 0.950923, -0.085815, -0.52167, -0.10439, 0.31398, -0.560229, 0.411738, -0.129033, -0.009998, 0.443882, -0.045643, -0.078445, -0.259311, -0.08337, 0.232652, -0.015912, -0.229458, -0.474973, 1.265934, -0.204483, -0.293586, -0.619023, 0.158895, -0.730671, -0.163626, 0.411716, -0.000132, 0.069014, -0.682714, 0.303234, 0.299097, -0.484469, 0.608172, -0.163785, -0.419754, -0.160745, 0.278904, 0.550542, -0.008052, 0.160397, -0.211354, -0.19755, 1.182627, 0.705073, -0.461941, -0.235292, 0.534275, -0.096419, -0.405812, -0.157745, -0.335469, 0.200545, 0.406497, -0.05341, -0.009234, -0.029925, -0.394101, -0.060133, 0.182601, 0.615583, 0.212157, 0.363921, 0.41868, -0.652791, 0.657173, -0.131662, 0.269305, 0.381748, -0.827964, -0.452596, 0.201918, 0.0673, -0.020293, 0.486942, -0.72454, -0.435051, -0.615452, -0.218852, 0.090703, -0.471036, 0.032373, 0.569953, 0.098359, -0.570767, -0.21015, -0.53019, -0.227117, 0.327978, 0.087079, -0.115037, 0.09193, -0.922884, -0.165566, -0.353596, 0.535904, -0.328579, 0.029465, -1.508702, -0.320394, -0.596324, 0.290277, -0.272515, 0.104348, 0.062855, -0.236447, 0.388958, -0.186552, -0.156253, 0.355678, 0.53834, -0.321627, 0.486004, 0.301326, 0.786779, 0.430292, -0.012458, -0.164964, -0.072951, 0.746564, 0.19136, 0.003213, 0.53479, 0.511118, -0.559153, -0.088731, -0.436206, 0.421004, 0.193043, -0.656222, 0.133223, 0.00107, 0.037087, 0.263503, 0.378593, 0.158718, -0.401664, -0.10563, -0.111221, 0.018598, -0.036396, 0.189584, -0.347721, -0.544111, -0.018158, 0.134147, -0.362431, -0.702383, -0.375221, 0.365745, 0.118082, -0.19102, -0.150732, 0.638995, 0.070662, -0.054605, 0.221755, 0.23726, -0.274418, 0.294639, 0.221177, -0.012947, 0.08444, -0.486605, -0.225034, 0.774728, 0.167609, 0.766647, 0.381622, 0.241907, -0.196452, 0.245138, -0.203225, -0.701671, 0.236662, -0.627221, 0.143006, 0.055671, 0.564561, -0.114897, -0.542244, 0.464601, 0.201577, -0.177196, -0.795015, -0.580793, -0.134996, -0.579672, -0.399042, 0.008118, -0.458077, -0.43296, 0.074138, 0.328092, 0.02934, 0.406294, 0.330677, -0.138583, -0.676608, -0.099983, -0.137182, 0.713108, 0.248643, 0.153462, 0.56039, -0.109877, 0.260655, -0.529779, -0.13416, 0.067448, -0.139468, -0.179535, 0.372629, 0.287185, 0.100582, 0.093573, -0.208796,
	}
}

// "peanuts"
func peanutsVector() []float32 {
	return []float32{0.563772, -0.779601, -0.18491, 0.509093, 0.080691, -0.621506, -0.127855, -0.165435, 0.57496, 0.006945, 0.452967, -0.285534, -0.129205, 0.193883, 0.092732, 0.083284, 0.714696, 0.107078, -0.398886, -0.117344, -0.387671, 0.026748, -0.562581, -0.007178, -0.354846, -0.431299, -0.788228, 0.175199, 0.914486, 0.441425, 0.089804, 0.284472, 0.106916, -0.133174, 0.399299, 0.002177, 0.551474, 0.389343, -0.016404, 0.770212, -0.219833, 0.303322, 0.127598, -0.378037, -0.172971, 0.394854, -0.424415, -0.71173, 0.080323, -0.406372, 0.398395, -0.594257, -0.418287, 0.055755, -0.352343, -0.393373, -0.732443, 0.333113, 0.420378, -0.50231, 0.261863, -0.061356, -0.180985, 0.311916, -0.180207, -0.154169, 0.371969, 0.454717, 0.320499, -0.182448, 0.087347, 0.585272, 0.136098, 0.288909, -0.229571, -0.140278, 0.229644, -0.557327, -0.110147, 0.034364, -0.021627, -0.598707, 0.221168, -0.059591, -0.203555, -0.434876, 0.209634, -0.460895, -0.345391, -0.18248, -0.24853, 0.730295, -0.295402, -0.562237, 0.255922, 0.076661, -0.713794, -0.354747, -1.109888, -0.066694, -0.195747, -0.282781, 0.459869, -0.309599, -0.002211, -0.274471, -0.003621, 0.008228, 0.011961, -0.258772, -0.210687, -0.664148, -0.257968, 0.231335, 0.530392, -0.205764, -0.621055, -0.440582, 0.080335, 0.017367, 0.880771, 0.656272, -0.713248, -0.208629, 0.095346, 0.336802, 0.888765, 0.251927, 0.066473, 0.182678, -0.220494, 0.288927, -0.602036, 0.057106, -0.594172, 0.848978, 0.751973, 0.090758, -0.732184, 0.683475, -0.075085, 0.381326, -0.076531, -0.253831, 0.10311, -0.02988, -0.043583, 0.005746, -0.460183, -0.189048, 0.25792, 0.477565, 0.391953, 0.08469, -0.10022, 0.454383, 0.170811, 0.196819, -0.760276, 0.045886, -0.743934, 0.190072, -0.216326, -0.624262, -0.22944, 0.066233, 1.024283, 0.044009, -0.373543, -0.243663, 0.204444, 0.402183, 0.043356, 0.31716, 0.302178, 0.369374, 0.36901, 0.02886, -0.26132, -0.234714, -0.791308, -0.433528, -0.098797, -0.447567, -0.124892, -0.119958, 0.31019, -0.096092, -0.259021, -0.078099, -0.178679, 0.14879, 0.106432, -0.450003, -0.294972, 0.044257, 0.402832, 0.263266, -0.309787, -0.17766, -0.399104, 0.577422, 0.30102, 0.05326, -0.271873, 0.204839, -0.019002, -0.743543, 0.739314, -0.115868, -0.504568, -0.115713, 0.042769, -0.123561, -0.057097, 0.407096, 0.770627, 0.372981, -0.321945, 0.349865, 0.437571, -0.77394, -0.090017, -0.011273, -0.468664, -0.735247, -0.745655, 0.018983, -0.248165, 0.215342, -0.136942, -0.458205, 0.4572, -0.032293, 0.654409, -0.024184, -0.392144, 0.634579, 0.222185, 0.471951, -0.063678, -0.473611, 0.796793, -0.295494, -0.157621, -0.103365, -0.564606, -0.092231, -0.517754, -0.369358, 0.137479, -0.214837, 0.11057, -0.095227, 0.726768, -0.079352, -0.065927, -0.846602, -0.317556, -0.344271, 0.201353, -0.367633, -0.004477, 0.157801, -0.249114, -0.549599, -0.147123, 0.308084, -0.175564, 0.306867, -0.071157, -0.588356, 0.450987, -0.184879, -0.096782, -0.006346, -0.017689, 0.005998, 0.200963, 0.225338, 0.189993, -1.105824, 0.520005, 0.129679, 0.198194, -0.254813, -0.127583, 0.326054, 0.009956, -0.016008, -0.483044, 0.801135, -0.517766, 0.067179, -0.372756, -0.511781, 0.058562, -0.082906, -0.28168, -0.285859}
}

type fakeMetrics struct {
	mock.Mock
}

func (m *fakeMetrics) AddUsageDimensions(class, query, op string, dims int) {
	m.Called(class, query, op, dims)
}

type fakeObjectSearcher struct{}

func (f *fakeObjectSearcher) Search(context.Context, dto.GetParams) ([]search.Result, error) {
	return nil, nil
}

func (f *fakeObjectSearcher) VectorSearch(context.Context, dto.GetParams) ([]search.Result, error) {
	return nil, nil
}

func (f *fakeObjectSearcher) CrossClassVectorSearch(context.Context, []float32, int, int, *filters.LocalFilter) ([]search.Result, error) {
	return nil, nil
}

func (f *fakeObjectSearcher) Object(ctx context.Context, className string, id strfmt.UUID, props search.SelectProperties, additional additional.Properties, properties *additional.ReplicationProperties, tenant string) (*search.Result, error) {
	return nil, nil
}

func (f *fakeObjectSearcher) ObjectsByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties, additional additional.Properties, tenant string) (search.Results, error) {
	return nil, nil
}

func (f *fakeObjectSearcher) SparseObjectSearch(ctx context.Context, params dto.GetParams) ([]*storobj.Object, []float32, error) {
	out := []*storobj.Object{
		{
			Object: models.Object{
				ID: "9889a225-3b28-477d-b8fc-5f6071bb4731",
			},

			Vector: []float32{1, 2, 3},
		},
		{
			Object: models.Object{
				ID: "0bcdef12-3314-442e-a4d1-e94d7c0afc3a",
			},
			Vector: []float32{4, 5, 6},
		},
	}
	lim := params.Pagination.Offset + params.Pagination.Limit
	if lim > len(out) {
		lim = len(out)
	}

	return out[:lim], []float32{0.008, 0.001}[:lim], nil
}

func (f *fakeObjectSearcher) DenseObjectSearch(ctx context.Context, class string, vector []float32, offset int, limit int, filters *filters.LocalFilter, additinal additional.Properties, tenant string) ([]*storobj.Object, []float32, error) {
	out := []*storobj.Object{
		{
			Object: models.Object{
				ID: "79a636c2-3314-442e-a4d1-e94d7c0afc3a",
			},

			Vector: []float32{4, 5, 6},
		},
		{
			Object: models.Object{
				ID: "9889a225-3b28-477d-b8fc-5f6071bb4731",
			},

			Vector: []float32{1, 2, 3},
		},
	}
	lim := offset + limit
	if lim > len(out) {
		lim = len(out)
	}

	return out[:lim], []float32{0.009, 0.008}[:lim], nil
}

func CopyElems[T any](list1, list2 []T, pos int) bool {
	if len(list1) != len(list2) {
		return false
	}
	if pos < 0 || pos >= len(list1) {
		return true
	}
	list1[pos] = list2[pos]
	return CopyElems(list1, list2, pos+1)
}

func (f *fakeObjectSearcher) ResolveReferences(ctx context.Context, objs search.Results, props search.SelectProperties, groupBy *searchparams.GroupBy, additional additional.Properties, tenant string) (search.Results, error) {
	// Convert res1 to search.Results
	out := make(search.Results, len(objs))
	CopyElems(out, objs, 0)

	return out, nil
}

func TestHybridOverSearch(t *testing.T) {
	dirName := t.TempDir()

	logger := logrus.New()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: singleShardState(),
	}
	repo, err := New(logger, Config{
		RootPath:                  dirName,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		QueryLimit:                20,
	}, &fakeRemoteClient{}, &fakeNodeResolver{}, &fakeRemoteNodeClient{}, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))
	defer repo.Shutdown(context.Background())

	fos := &fakeObjectSearcher{}

	class := SetupFusionClass(t, repo, schemaGetter, logger, 1.2, 0.75)
	idx := repo.GetIndex("MyClass")
	require.NotNil(t, idx)

	t.Run("Hybrid", func(t *testing.T) {
		params := dto.GetParams{
			ClassName: "MyClass",
			HybridSearch: &searchparams.HybridSearch{
				Query:  "elephant",
				Vector: elephantVector(),
				Alpha:  0.5,
			},
			Pagination: &filters.Pagination{
				Offset: 0,
				Limit:  1,
			},
		}

		prov := modules.NewProvider()
		prov.SetClassDefaults(class)

		metrics := &fakeMetrics{}
		log, _ := test.NewNullLogger()
		explorer := traverser.NewExplorer(fos, log, prov, metrics, defaultConfig)
		hybridResults, err := explorer.Hybrid(context.TODO(), params)
		require.Nil(t, err)
		require.Equal(t, 1, len(hybridResults))
		require.Equal(t, strfmt.UUID("9889a225-3b28-477d-b8fc-5f6071bb4731"), hybridResults[0].ID)
		// require.Equal(t, "79a636c2-3314-442e-a4d1-e94d7c0afc3a", hybridResults[1].ID)
	})
}
