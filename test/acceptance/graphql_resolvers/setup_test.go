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

package test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/multishard"
)

func TestGraphQL_AsyncIndexing(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithText2VecContextionary().
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	defer helper.SetupClient(fmt.Sprintf("%s:%s", helper.ServerHost, helper.ServerPort))
	helper.SetupClient(compose.GetWeaviate().URI())

	testGraphQL(t)
}

func TestGraphQL_SyncIndexing(t *testing.T) {
	testGraphQL(t)
}

func testGraphQL(t *testing.T) {
	// tests with classes that have objects with same uuids
	t.Run("import test data (near object search class)", addTestDataNearObjectSearch)

	t.Run("running Get nearObject against shadowed objects", runningGetNearObjectWithShadowedObjects)
	t.Run("running Aggregate nearObject against shadowed objects", runningAggregateNearObjectWithShadowedObjects)
	t.Run("running Explore nearObject against shadowed objects", runningExploreNearObjectWithShadowedObjects)

	deleteObjectClass(t, "NearObjectSearch")
	deleteObjectClass(t, "NearObjectSearchShadow")

	// setup tests
	t.Run("setup test schema", addTestSchema)
	t.Run("import test data (city, country, airport)", addTestDataCityAirport)
	t.Run("import test data (companies)", addTestDataCompanies)
	t.Run("import test data (person)", addTestDataPersons)
	t.Run("import test data (pizzas)", addTestDataPizzas)
	t.Run("import test data (array class)", addTestDataArrayClass)
	t.Run("import test data (duplicates class)", addTestDataDuplicatesClass)
	t.Run("import test data (500 random strings)", addTestDataRansomNotes)
	t.Run("import test data (multi shard)", addTestDataMultiShard)
	t.Run("import test data (date field class)", addDateFieldClass)
	t.Run("import test data (custom vector class)", addTestDataCVC)
	t.Run("import test data (class without properties)", addTestDataNoProperties)
	t.Run("import test data (cursor api)", addTestDataCursorSearch)

	// explore tests
	t.Run("expected explore failures with invalid conditions", exploreWithExpectedFailures)

	// get tests
	t.Run("getting objects", gettingObjects)
	t.Run("getting objects with filters", gettingObjectsWithFilters)
	t.Run("getting objects with geo filters", gettingObjectsWithGeoFilters)
	t.Run("getting objects with grouping", gettingObjectsWithGrouping)
	t.Run("getting objects with additional props", gettingObjectsWithAdditionalProps)
	t.Run("getting objects with near fields", gettingObjectsWithNearFields)
	t.Run("getting objects with near fields with multi shard setup", gettingObjectsWithNearFieldsMultiShard)
	t.Run("getting objects with sort", gettingObjectsWithSort)
	t.Run("getting objects with hybrid search", getWithHybridSearch)
	t.Run("expected get failures with invalid conditions", getsWithExpectedFailures)
	t.Run("cursor through results", getWithCursorSearch)
	t.Run("groupBy objects", groupByObjects)

	// aggregate tests
	t.Run("aggregates noPropsClass without grouping", aggregateNoPropsClassWithoutGroupByTest)
	t.Run("aggregates arrayClass without grouping", aggregateArrayClassWithoutGroupByTest)
	t.Run("aggregates arrayClass with grouping", aggregateArrayClassWithGroupByTest)
	t.Run("aggregates duplicatesClass without grouping", aggregateDuplicatesClassWithoutGroupByTest)
	t.Run("aggregates duplicatesClass with grouping", aggregateDuplicatesClassWithGroupByTest)
	t.Run("aggregates city without grouping", aggregateCityClassWithoutGroupByTest)
	t.Run("aggregates city with grouping", aggregateCityClassWithGroupByTest)

	t.Run("aggregates local meta string props not set everywhere", localMeta_StringPropsNotSetEverywhere)
	t.Run("aggregates local meta with where and nearText filters", localMetaWithWhereAndNearTextFilters)
	t.Run("aggregates local meta with where and nearObject filters", localMetaWithWhereAndNearObjectFilters)
	t.Run("aggregates local meta with nearVector filters", localMetaWithNearVectorFilter)
	t.Run("aggregates local meta with where and nearVector nearMedia", localMetaWithWhereAndNearVectorFilters)
	t.Run("aggregates local meta with where groupBy and nearMedia filters", localMetaWithWhereGroupByNearMediaFilters)
	t.Run("aggregates local meta with objectLimit and nearMedia filters", localMetaWithObjectLimit)
	t.Run("aggregates on date fields", aggregatesOnDateFields)
	t.Run("aggregates with hybrid search", aggregationWithHybridSearch)
	t.Run("expected aggregate failures with invalid conditions", aggregatesWithExpectedFailures)

	t.Run("metrics count is stable when more classes are added", metricsCount)

	// tear down
	deleteObjectClass(t, "Person")
	deleteObjectClass(t, "Pizza")
	deleteObjectClass(t, "Country")
	deleteObjectClass(t, "City")
	deleteObjectClass(t, "Airport")
	deleteObjectClass(t, "Company")
	deleteObjectClass(t, "RansomNote")
	deleteObjectClass(t, "MultiShard")
	deleteObjectClass(t, "HasDateField")
	deleteObjectClass(t, arrayClassName)
	deleteObjectClass(t, duplicatesClassName)
	deleteObjectClass(t, noPropsClassName)
	deleteObjectClass(t, "CursorClass")

	// only run after everything else is deleted, this way, we can also run an
	// all-class Explore since all vectors which are now left have the same
	// dimensions.

	t.Run("getting objects with custom vectors", gettingObjectsWithCustomVectors)
	t.Run("explore objects with custom vectors", exploreObjectsWithCustomVectors)

	deleteObjectClass(t, "CustomVectorClass")
}

func addTestSchema(t *testing.T) {
	createObjectClass(t, &models.Class{
		Class: "Country",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	})

	// City class has only one vectorizable field: "name"
	// the rest of the fields are explicitly set to skip vectorization
	// to not to downgrade the distance/certainty result on which the
	// aggregate tests are based on.
	createObjectClass(t, &models.Class{
		Class: "City",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{IndexNullState: true, IndexPropertyLength: true, IndexTimestamps: true},
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": false,
					},
				},
			},
			{
				Name:     "inCountry",
				DataType: []string{"Country"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:     "population",
				DataType: []string{"int"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:     "location",
				DataType: []string{"geoCoordinates"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:     "isCapital",
				DataType: []string{"boolean"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:     "cityArea",
				DataType: []string{"number"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:     "cityRights",
				DataType: []string{"date"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:         "timezones",
				DataType:     schema.DataTypeTextArray.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:     "museums",
				DataType: []string{"text[]"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:     "history",
				DataType: []string{"text"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
			{
				Name:     "phoneNumber",
				DataType: []string{"phoneNumber"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"skip": true,
					},
				},
			},
		},
	})

	createObjectClass(t, &models.Class{
		Class: "Airport",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords: &models.StopwordConfig{
				Preset: "en",
			},
			IndexTimestamps: true,
		},
		Properties: []*models.Property{
			{
				Name:         "code",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:     "phone",
				DataType: []string{"phoneNumber"},
			},
			{
				Name:     "inCity",
				DataType: []string{"City"},
			},
			{
				Name:     "airportId",
				DataType: []string{"uuid"},
			},
		},
	})

	createObjectClass(t, &models.Class{
		Class: "Company",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": false,
			},
		},
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizePropertyName": false,
					},
				},
			},
			{
				Name:     "inCity",
				DataType: []string{"City"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizePropertyName": false,
					},
				},
			},
		},
	})

	createObjectClass(t, &models.Class{
		Class: "Person",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": false,
			},
		},
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizePropertyName": false,
					},
				},
			},
			{
				Name:     "livesIn",
				DataType: []string{"City"},
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizePropertyName": false,
					},
				},
			},
			{
				Name:         "profession",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationField,
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizePropertyName": false,
					},
				},
			},
			{
				Name:         "about",
				DataType:     schema.DataTypeTextArray.PropString(),
				Tokenization: models.PropertyTokenizationField,
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizePropertyName": false,
					},
				},
			},
		},
	})

	createObjectClass(t, &models.Class{
		Class: "Pizza",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": false,
			},
		},
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationField,
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizePropertyName": false,
					},
				},
			},
			{
				Name:         "description",
				DataType:     []string{string(schema.DataTypeText)},
				Tokenization: models.PropertyTokenizationWord,
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizePropertyName": false,
					},
				},
			},
		},
	})

	createObjectClass(t, &models.Class{
		Class: "RansomNote",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		Properties: []*models.Property{
			{
				Name:         "contents",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	})

	createObjectClass(t, multishard.ClassContextionaryVectorizer())

	createObjectClass(t, &models.Class{
		Class: "HasDateField",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		Properties: []*models.Property{
			{
				Name:         "unique",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:     "timestamp",
				DataType: []string{"date"},
			},
			{
				Name:         "identical",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	})

	createObjectClass(t, &models.Class{
		Class:      "CustomVectorClass",
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	})

	createObjectClass(t, noPropsClassSchema())
	createObjectClass(t, arrayClassSchema())
	createObjectClass(t, duplicatesClassSchema())
}

const (
	netherlands   strfmt.UUID = "67b79643-cf8b-4b22-b206-6e63dbb4e57a"
	germany       strfmt.UUID = "561eea29-b733-4079-b50b-cfabd78190b7"
	amsterdam     strfmt.UUID = "8f5f8e44-d348-459c-88b1-c1a44bb8f8be"
	rotterdam     strfmt.UUID = "660db307-a163-41d2-8182-560782cd018f"
	berlin        strfmt.UUID = "9b9cbea5-e87e-4cd0-89af-e2f424fd52d6"
	dusseldorf    strfmt.UUID = "6ffb03f8-a853-4ec5-a5d8-302e45aaaf13"
	missingisland strfmt.UUID = "823abeca-eef3-41c7-b587-7a6977b08003"
	nullisland    strfmt.UUID = "823abeca-eef3-41c7-b587-7a6977b08067"
	airport1      strfmt.UUID = "4770bb19-20fd-406e-ac64-9dac54c27a0f"
	airport2      strfmt.UUID = "cad6ab9b-5bb9-4388-a933-a5bdfd23db37"
	airport3      strfmt.UUID = "55a4dbbb-e2af-4b2a-901d-98146d1eeca7"
	airport4      strfmt.UUID = "62d15920-b546-4844-bc87-3ae33268fab5"
	cvc1          strfmt.UUID = "1ffeb3e1-1258-4c2a-afc3-55543f6c44b8"
	cvc2          strfmt.UUID = "df22e5c4-5d17-49f9-a71d-f392a82bc086"
	cvc3          strfmt.UUID = "c28a039a-d509-4c2e-940a-8b109e5bebf4"

	quattroFormaggi strfmt.UUID = "152500c6-4a8a-4732-aede-9fcab7e43532"
	fruttiDiMare    strfmt.UUID = "a828e9aa-d1b6-4644-8569-30d404e31a0d"
	hawaii          strfmt.UUID = "ed75037b-0748-4970-811e-9fe835ed41d1"
	doener          strfmt.UUID = "a655292d-1b93-44a1-9a47-57b6922bb455"
)

var (
	historyAmsterdam  = "Due to its geographical location in what used to be wet peatland, the founding of Amsterdam is of a younger age than the founding of other urban centers in the Low Countries. However, in and around the area of what later became Amsterdam, local farmers settled as early as three millennia ago. They lived along the prehistoric IJ river and upstream of its tributary Amstel. The prehistoric IJ was a shallow and quiet stream in peatland behind beach ridges. This secluded area could grow there into an important local settlement center, especially in the late Bronze Age, the Iron Age and the Roman Age. Neolithic and Roman artefacts have also been found downstream of this area, in the prehistoric Amstel bedding under Amsterdam's Damrak and Rokin, such as shards of Bell Beaker culture pottery (2200-2000 BC) and a granite grinding stone (2700-2750 BC).[27][28] But the location of these artefacts around the river banks of the Amstel probably point to a presence of a modest semi-permanent or seasonal settlement of the previous mentioned local farmers. A permanent settlement would not have been possible, since the river mouth and the banks of the Amstel in this period in time were too wet for permanent habitation"
	historyRotterdam  = "On 7 July 1340, Count Willem IV of Holland granted city rights to Rotterdam, whose population then was only a few thousand.[14] Around the year 1350, a shipping canal (the Rotterdamse Schie) was completed, which provided Rotterdam access to the larger towns in the north, allowing it to become a local trans-shipment centre between the Netherlands, England and Germany, and to urbanize"
	historyBerlin     = "The earliest evidence of settlements in the area of today's Berlin are remnants of a house foundation dated to 1174, found in excavations in Berlin Mitte,[27] and a wooden beam dated from approximately 1192.[28] The first written records of towns in the area of present-day Berlin date from the late 12th century. Spandau is first mentioned in 1197 and Köpenick in 1209, although these areas did not join Berlin until 1920.[29] The central part of Berlin can be traced back to two towns. Cölln on the Fischerinsel is first mentioned in a 1237 document, and Berlin, across the Spree in what is now called the Nikolaiviertel, is referenced in a document from 1244.[28] 1237 is considered the founding date of the city.[30] The two towns over time formed close economic and social ties, and profited from the staple right on the two important trade routes Via Imperii and from Bruges to Novgorod.[12] In 1307, they formed an alliance with a common external policy, their internal administrations still being separated"
	historyDusseldorf = "The first written mention of Düsseldorf (then called Dusseldorp in the local Low Rhenish dialect) dates back to 1135. Under Emperor Friedrich Barbarossa the small town of Kaiserswerth to the north of Düsseldorf became a well-fortified outpost, where soldiers kept a watchful eye on every movement on the Rhine. Kaiserswerth eventually became a suburb of Düsseldorf in 1929. In 1186, Düsseldorf came under the rule of the Counts of Berg. 14 August 1288 is one of the most important dates in the history of Düsseldorf. On this day the sovereign Count Adolf VIII of Berg granted the village on the banks of the Düssel town privileges. Before this, a bloody struggle for power had taken place between the Archbishop of Cologne and the count of Berg, culminating in the Battle of Worringen"
)

func addTestDataCityAirport(t *testing.T) {
	// countries
	createObject(t, &models.Object{
		Class: "Country",
		ID:    netherlands,
		Properties: map[string]interface{}{
			"name": "Netherlands",
		},
	})
	createObject(t, &models.Object{
		Class: "Country",
		ID:    germany,
		Properties: map[string]interface{}{
			"name": "Germany",
		},
	})

	// cities
	createObject(t, &models.Object{
		Class: "City",
		ID:    amsterdam,
		Properties: map[string]interface{}{
			"name":       "Amsterdam",
			"population": 1800000,
			"location": map[string]interface{}{
				"latitude":  52.366667,
				"longitude": 4.9,
			},
			"inCountry": []interface{}{
				map[string]interface{}{
					"beacon": crossref.NewLocalhost("Country", netherlands).String(),
				},
			},
			"isCapital":  true,
			"cityArea":   float64(891.95),
			"cityRights": mustParseYear("1400"),
			"timezones":  []string{"CET", "CEST"},
			"museums":    []string{"Stedelijk Museum", "Rijksmuseum"},
			"history":    historyAmsterdam,
			"phoneNumber": map[string]interface{}{
				"input": "+311000004",
			},
		},
	})
	createObject(t, &models.Object{
		Class: "City",
		ID:    rotterdam,
		Properties: map[string]interface{}{
			"name":       "Rotterdam",
			"population": 600000,
			"inCountry": []interface{}{
				map[string]interface{}{
					"beacon": crossref.NewLocalhost("Country", netherlands).String(),
				},
			},
			"isCapital":  false,
			"cityArea":   float64(319.35),
			"cityRights": mustParseYear("1283"),
			"timezones":  []string{"CET", "CEST"},
			"museums":    []string{"Museum Boijmans Van Beuningen", "Wereldmuseum", "Witte de With Center for Contemporary Art"},
			"history":    historyRotterdam,
			"phoneNumber": map[string]interface{}{
				"input": "+311000000",
			},
		},
	})
	createObject(t, &models.Object{
		Class: "City",
		ID:    berlin,
		Properties: map[string]interface{}{
			"name":       "Berlin",
			"population": 3470000,
			"inCountry": []interface{}{
				map[string]interface{}{
					"beacon": crossref.NewLocalhost("Country", germany).String(),
				},
			},
			"isCapital":  true,
			"cityArea":   float64(891.96),
			"cityRights": mustParseYear("1400"),
			"timezones":  []string{"CET", "CEST"},
			"museums":    []string{"German Historical Museum"},
			"history":    historyBerlin,
			"phoneNumber": map[string]interface{}{
				"input": "+311000002",
			},
		},
	})
	createObject(t, &models.Object{
		Class: "City",
		ID:    dusseldorf,
		Properties: map[string]interface{}{
			"name":       "Dusseldorf",
			"population": 600000,
			"inCountry": []interface{}{
				map[string]interface{}{
					"beacon": crossref.NewLocalhost("Country", germany).String(),
				},
			},
			"location": map[string]interface{}{
				"latitude":  51.225556,
				"longitude": 6.782778,
			},
			"isCapital":  false,
			"cityArea":   float64(217.22),
			"cityRights": mustParseYear("1135"),
			"timezones":  []string{"CET", "CEST"},
			"museums":    []string{"Schlossturm", "Schiffahrt Museum", "Onomato"},
			"history":    historyDusseldorf,
			"phoneNumber": map[string]interface{}{
				"input": "+311000001",
			},
		},
	})

	createObject(t, &models.Object{
		Class: "City",
		ID:    missingisland,
		Properties: map[string]interface{}{
			"name":       "Missing Island",
			"population": 0,
			"location": map[string]interface{}{
				"latitude":  0,
				"longitude": 0,
			},
			"isCapital": false,
		},
	})

	createObject(t, &models.Object{
		Class: "City",
		ID:    nullisland,
		Properties: map[string]interface{}{
			"name":        nil,
			"population":  nil,
			"inCountry":   nil,
			"location":    nil,
			"isCapital":   nil,
			"cityArea":    nil,
			"cityRights":  nil,
			"timezones":   nil,
			"museums":     nil,
			"history":     nil,
			"phoneNumber": nil,
		},
	})

	// airports
	createObject(t, &models.Object{
		Class: "Airport",
		ID:    airport1,
		Properties: map[string]interface{}{
			"code":      "10000",
			"airportId": uuid.MustParse("00000000-0000-0000-0000-000000010000").String(),
			"phone": map[string]interface{}{
				"input": "+311234567",
			},
			"inCity": []interface{}{
				map[string]interface{}{
					"beacon": crossref.NewLocalhost("City", amsterdam).String(),
				},
			},
		},
	})
	createObject(t, &models.Object{
		Class: "Airport",
		ID:    airport2,
		Properties: map[string]interface{}{
			"code":      "20000",
			"airportId": uuid.MustParse("00000000-0000-0000-0000-000000020000").String(),
			"inCity": []interface{}{
				map[string]interface{}{
					"beacon": crossref.NewLocalhost("City", rotterdam).String(),
				},
			},
		},
	})
	createObject(t, &models.Object{
		Class: "Airport",
		ID:    airport3,
		Properties: map[string]interface{}{
			"code":      "30000",
			"airportId": uuid.MustParse("00000000-0000-0000-0000-000000030000").String(),
			"inCity": []interface{}{
				map[string]interface{}{
					"beacon": crossref.NewLocalhost("City", dusseldorf).String(),
				},
			},
		},
	})
	createObject(t, &models.Object{
		Class: "Airport",
		ID:    airport4,
		Properties: map[string]interface{}{
			"code":      "40000",
			"airportId": uuid.MustParse("00000000-0000-0000-0000-000000040000").String(),
			"inCity": []interface{}{
				map[string]interface{}{
					"beacon": crossref.NewLocalhost("City", berlin).String(),
				},
			},
		},
	})
}

func addTestDataCompanies(t *testing.T) {
	var (
		microsoft1 strfmt.UUID = "cfa3b21e-ca4f-4db7-a432-7fc6a23c534d"
		microsoft2 strfmt.UUID = "8f75ed97-39dd-4294-bff7-ecabd7923062"
		microsoft3 strfmt.UUID = "f343f51d-7e05-4084-bd66-d504db3b6bec"
		apple1     strfmt.UUID = "477fec91-1292-4928-8f53-f0ff49c76900"
		apple2     strfmt.UUID = "bb2cfdba-d4ba-4cf8-abda-e719ef35ac33"
		apple3     strfmt.UUID = "b71d2b4c-3da1-4684-9c5e-aabd2a4f2998"
		google1    strfmt.UUID = "8c2e21fc-46fe-4999-b41c-a800595129af"
		google2    strfmt.UUID = "62b969c6-f184-4be0-8c40-7470af417cfc"
		google3    strfmt.UUID = "c7829929-2037-4420-acbc-a433269feb93"
	)

	type companyTemplate struct {
		id     strfmt.UUID
		name   string
		inCity []strfmt.UUID
	}

	companies := []companyTemplate{
		{id: microsoft1, name: "Microsoft Inc.", inCity: []strfmt.UUID{dusseldorf}},
		{id: microsoft2, name: "Microsoft Incorporated", inCity: []strfmt.UUID{dusseldorf, amsterdam}},
		{id: microsoft3, name: "Microsoft", inCity: []strfmt.UUID{berlin}},
		{id: apple1, name: "Apple Inc."},
		{id: apple2, name: "Apple Incorporated"},
		{id: apple3, name: "Apple"},
		{id: google1, name: "Google Inc."},
		{id: google2, name: "Google Incorporated"},
		{id: google3, name: "Google"},
	}

	// companies
	for _, company := range companies {
		inCity := []interface{}{}
		for _, c := range company.inCity {
			inCity = append(inCity,
				map[string]interface{}{
					"beacon": crossref.NewLocalhost("City", c).String(),
				})
		}

		createObject(t, &models.Object{
			Class: "Company",
			ID:    company.id,
			Properties: map[string]interface{}{
				"inCity": inCity,
				"name":   company.name,
			},
		})
	}

	assertGetObjectEventually(t, companies[len(companies)-1].id)
}

func addTestDataPersons(t *testing.T) {
	var (
		alice strfmt.UUID = "5d0fa6ee-21c4-4b46-a735-f0208717837d"
		bob   strfmt.UUID = "8615585a-2960-482d-b19d-8bee98ade52c"
		john  strfmt.UUID = "3ef44474-b5e5-455d-91dc-d917b5b76165"
		petra strfmt.UUID = "15d222c9-8c36-464b-bedb-113faa1c1e4c"
	)

	type personTemplate struct {
		id         strfmt.UUID
		name       string
		livesIn    []strfmt.UUID
		profession string
		about      []string
	}

	persons := []personTemplate{
		{
			id: alice, name: "Alice", livesIn: []strfmt.UUID{}, profession: "Quality Control Analyst",
			about: []string{"loves travelling very much"},
		},
		{
			id: bob, name: "Bob", livesIn: []strfmt.UUID{amsterdam}, profession: "Mechanical Engineer",
			about: []string{"loves travelling", "hates cooking"},
		},
		{
			id: john, name: "John", livesIn: []strfmt.UUID{amsterdam, berlin}, profession: "Senior Mechanical Engineer",
			about: []string{"hates swimming", "likes cooking", "loves travelling"},
		},
		{
			id: petra, name: "Petra", livesIn: []strfmt.UUID{amsterdam, berlin, dusseldorf}, profession: "Quality Assurance Manager",
			about: []string{"likes swimming", "likes cooking for family"},
		},
	}

	// persons
	for _, person := range persons {
		livesIn := []interface{}{}
		for _, c := range person.livesIn {
			livesIn = append(livesIn,
				map[string]interface{}{
					"beacon": crossref.NewLocalhost("City", c).String(),
				})
		}

		createObject(t, &models.Object{
			Class: "Person",
			ID:    person.id,
			Properties: map[string]interface{}{
				"livesIn":    livesIn,
				"name":       person.name,
				"profession": person.profession,
				"about":      person.about,
			},
		})
	}

	assertGetObjectEventually(t, persons[len(persons)-1].id)
}

func addTestDataPizzas(t *testing.T) {
	createObject(t, &models.Object{
		Class: "Pizza",
		ID:    quattroFormaggi,
		Properties: map[string]interface{}{
			"name":        "Quattro Formaggi",
			"description": "Pizza quattro formaggi Italian: [ˈkwattro forˈmaddʒi] (four cheese pizza) is a variety of pizza in Italian cuisine that is topped with a combination of four kinds of cheese, usually melted together, with (rossa, red) or without (bianca, white) tomato sauce. It is popular worldwide, including in Italy,[1] and is one of the iconic items from pizzerias's menus.",
		},
	})
	createObject(t, &models.Object{
		Class: "Pizza",
		ID:    fruttiDiMare,
		Properties: map[string]interface{}{
			"name":        "Frutti di Mare",
			"description": "Frutti di Mare is an Italian type of pizza that may be served with scampi, mussels or squid. It typically lacks cheese, with the seafood being served atop a tomato sauce.",
		},
	})
	createObject(t, &models.Object{
		Class: "Pizza",
		ID:    hawaii,
		Properties: map[string]interface{}{
			"name":        "Hawaii",
			"description": "Universally accepted to be the best pizza ever created.",
		},
	})
	createObject(t, &models.Object{
		Class: "Pizza",
		ID:    doener,
		Properties: map[string]interface{}{
			"name":        "Doener",
			"description": "A innovation, some say revolution, in the pizza industry.",
		},
	})

	assertGetObjectEventually(t, quattroFormaggi)
	assertGetObjectEventually(t, fruttiDiMare)
	assertGetObjectEventually(t, hawaii)
	assertGetObjectEventually(t, doener)
}

func addTestDataCVC(t *testing.T) {
	// add one object individually
	createObject(t, &models.Object{
		Class:  "CustomVectorClass",
		ID:     cvc1,
		Vector: []float32{1.1, 1.1, 1.1},
		Properties: map[string]interface{}{
			"name": "Ford",
		},
	})

	assertGetObjectEventually(t, cvc1)

	createObjectsBatch(t, []*models.Object{
		{
			Class:  "CustomVectorClass",
			ID:     cvc2,
			Vector: []float32{1.1, 1.1, 0.1},
			Properties: map[string]interface{}{
				"name": "Tesla",
			},
		},
		{
			Class:  "CustomVectorClass",
			ID:     cvc3,
			Vector: []float32{1.1, 0, 0},
			Properties: map[string]interface{}{
				"name": "Mercedes",
			},
		},
	})
	assertGetObjectEventually(t, cvc3)
}

func addTestDataNoProperties(t *testing.T) {
	for _, object := range noPropsClassObjects() {
		createObject(t, object)
		assertGetObjectEventually(t, object.ID)
	}
}

func addTestDataArrayClass(t *testing.T) {
	for _, object := range arrayClassObjects() {
		createObject(t, object)
		assertGetObjectEventually(t, object.ID)
	}
}

func addTestDataDuplicatesClass(t *testing.T) {
	for _, object := range duplicatesClassObjects() {
		createObject(t, object)
		assertGetObjectEventually(t, object.ID)
	}
}

func addTestDataRansomNotes(t *testing.T) {
	const (
		noteLengthMin = 4
		noteLengthMax = 1024

		batchSize  = 10
		numBatches = 50
	)

	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < numBatches; i++ {
		batch := make([]*models.Object, batchSize)
		for j := 0; j < batchSize; j++ {
			noteLength := noteLengthMin + seededRand.Intn(noteLengthMax-noteLengthMin+1)
			note := helper.GetRandomString(noteLength)

			batch[j] = &models.Object{
				Class:      "RansomNote",
				Properties: map[string]interface{}{"contents": note},
			}
		}

		createObjectsBatch(t, batch)
	}
}

func addTestDataMultiShard(t *testing.T) {
	for _, multiShard := range multishard.Objects() {
		helper.CreateObject(t, multiShard)
		helper.AssertGetObjectEventually(t, multiShard.Class, multiShard.ID)
	}
}

func addTestDataNearObjectSearch(t *testing.T) {
	classNames := []string{"NearObjectSearch", "NearObjectSearchShadow"}
	ids := []strfmt.UUID{
		"aa44bbee-ca5f-4db7-a412-5fc6a2300001",
		"aa44bbee-ca5f-4db7-a412-5fc6a2300002",
		"aa44bbee-ca5f-4db7-a412-5fc6a2300003",
		"aa44bbee-ca5f-4db7-a412-5fc6a2300004",
		"aa44bbee-ca5f-4db7-a412-5fc6a2300005",
	}
	names := []string{
		"Mount Everest",
		"Amsterdam is a cool city",
		"Football is a game where people run after ball",
		"Berlin is Germany's capital city",
		"London is a cool city",
	}

	for _, className := range classNames {
		createObjectClass(t, &models.Class{
			Class: className,
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Properties: []*models.Property{
				{
					Name:         "name",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
			},
		})
	}

	for i, id := range ids {
		createObject(t, &models.Object{
			Class: classNames[0],
			ID:    id,
			Properties: map[string]interface{}{
				"name": names[i],
			},
		})
		assertGetObjectEventually(t, id)
		createObject(t, &models.Object{
			Class: classNames[1],
			ID:    id,
			Properties: map[string]interface{}{
				"name": fmt.Sprintf("altered contents of: %v", names[i]),
			},
		})
		assertGetObjectEventually(t, id)
	}

	createObject(t, &models.Object{
		Class: classNames[0],
		ID:    "aa44bbee-ca5f-4db7-a412-5fc6a2300011",
		Properties: map[string]interface{}{
			"name": "the same content goes here just for explore tests",
		},
	})
	assertGetObjectEventually(t, "aa44bbee-ca5f-4db7-a412-5fc6a2300011")
	createObject(t, &models.Object{
		Class: classNames[1],
		ID:    "aa44bbee-ca5f-4db7-a412-5fc6a2300011",
		Properties: map[string]interface{}{
			"name": "the same content goes here just for explore tests",
		},
	})
	assertGetObjectEventually(t, "aa44bbee-ca5f-4db7-a412-5fc6a2300011")
}

const (
	cursorClassID1 = strfmt.UUID("00000000-0000-0000-0000-000000000001")
	cursorClassID2 = strfmt.UUID("00000000-0000-0000-0000-000000000002")
	cursorClassID3 = strfmt.UUID("00000000-0000-0000-0000-000000000003")
	cursorClassID4 = strfmt.UUID("00000000-0000-0000-0000-000000000004")
	cursorClassID5 = strfmt.UUID("00000000-0000-0000-0000-000000000005")
	cursorClassID6 = strfmt.UUID("00000000-0000-0000-0000-000000000006")
	cursorClassID7 = strfmt.UUID("00000000-0000-0000-0000-000000000007")
)

func addTestDataCursorSearch(t *testing.T) {
	className := "CursorClass"
	ids := []strfmt.UUID{
		cursorClassID1,
		cursorClassID2,
		cursorClassID3,
		cursorClassID4,
		cursorClassID5,
		cursorClassID6,
		cursorClassID7,
	}
	names := []string{
		"Mount Everest",
		"Amsterdam is a cool city",
		"Football is a game where people run after ball",
		"Berlin is Germany's capital city",
		"London is a cool city",
		"Wroclaw is a really cool city",
		"Brisbane is a city in Australia",
	}

	createObjectClass(t, &models.Class{
		Class: className,
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
	})

	for i, id := range ids {
		createObject(t, &models.Object{
			Class: className,
			ID:    id,
			Properties: map[string]interface{}{
				"name": names[i],
			},
		})
		assertGetObjectEventually(t, id)
	}
}

func addDateFieldClass(t *testing.T) {
	timestamps := []string{
		"2022-06-16T22:18:59.640162Z",
		"2022-06-16T22:19:01.495967Z",
		"2022-06-16T22:19:03.495596Z",
		"2022-06-16T22:19:04.3828349Z",
		"2022-06-16T22:19:05.894857Z",
		"2022-06-16T22:19:06.394958Z",
		"2022-06-16T22:19:07.589828Z",
		"2022-06-16T22:19:08.112395Z",
		"2022-06-16T22:19:10.339493Z",
		"2022-06-16T22:19:11.837473Z",
	}

	for i := 0; i < len(timestamps); i++ {
		createObject(t, &models.Object{
			Class: "HasDateField",
			Properties: map[string]interface{}{
				"unique":    fmt.Sprintf("#%d", i+1),
				"timestamp": timestamps[i],
				"identical": "hello!",
			},
		})
	}
}

func mustParseYear(year string) time.Time {
	date := fmt.Sprintf("%s-01-01T00:00:00+02:00", year)
	asTime, err := time.Parse(time.RFC3339, date)
	if err != nil {
		panic(err)
	}
	return asTime
}
