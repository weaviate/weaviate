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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/cities"
	"github.com/weaviate/weaviate/test/helper/sample-schema/multishard"
)

func TestGraphQL_AsyncIndexing(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithText2VecContextionary().
		WithBackendFilesystem().
		WithWeaviateEnv("ASYNC_INDEXING", "true").
		WithWeaviateEnv("ASYNC_INDEXING_STALE_TIMEOUT", "100ms").
		WithWeaviateEnv("QUEUE_SCHEDULER_INTERVAL", "100ms").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	testGraphQL(t, compose.GetWeaviate().URI())
}

func TestGraphQL_SyncIndexing(t *testing.T) {
	testGraphQL(t, "localhost:8080")
}

func testGraphQL(t *testing.T, host string) {
	helper.SetupClient(host)
	// tests with classes that have objects with same uuids
	t.Run("import test data (near object search class)", addTestDataNearObjectSearch)

	t.Run("running Get nearObject against shadowed objects", runningGetNearObjectWithShadowedObjects)
	t.Run("running Aggregate nearObject against shadowed objects", runningAggregateNearObjectWithShadowedObjects)
	t.Run("running Explore nearObject against shadowed objects", runningExploreNearObjectWithShadowedObjects)

	deleteObjectClass(t, "NearObjectSearch")
	deleteObjectClass(t, "NearObjectSearchShadow")

	// setup tests
	t.Run("setup test schema", func(t *testing.T) { addTestSchema(t, host) })
	t.Run("import test data (city, country, airport)", func(t *testing.T) { addTestDataCityAirport(t, host) })
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

	t.Run("aggregates with hybrid search", aggregationWithHybridSearch)

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
	deleteObjectClass(t, "CompanyGroup")

	// only run after everything else is deleted, this way, we can also run an
	// all-class Explore since all vectors which are now left have the same
	// dimensions.

	t.Run("getting objects with custom vectors", gettingObjectsWithCustomVectors)
	t.Run("explore objects with custom vectors", exploreObjectsWithCustomVectors)

	deleteObjectClass(t, "CustomVectorClass")
}

func TestAggregateHybrid(t *testing.T) {
	host := "localhost:8080"
	t.Run("setup test schema", func(t *testing.T) { addTestSchema(t, host) })

	t.Run("import test data (company groups)", addTestDataCompanyGroups)
	t.Run("import test data (500 random strings)", addTestDataRansomNotes)

	t.Run("aggregate hybrid groupby", aggregateHybridGroupBy)

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
	deleteObjectClass(t, "CompanyGroup")
	deleteObjectClass(t, "CustomVectorClass")
}

func TestGroupBy(t *testing.T) {
	host := "localhost:8080"
	t.Run("setup test schema", func(t *testing.T) { addTestSchema(t, host) })

	t.Run("import test data (company groups)", addTestDataCompanyGroups)
	t.Run("import test data (500 random strings)", addTestDataRansomNotes)

	t.Run("groupBy objects with bm25", groupByBm25)
	t.Run("groupBy objects with hybrid nearvector", groupByHybridNearVector)

	t.Run("conflicting subsearches", conflictingSubSearches)
	t.Run("vector and nearText", vectorNearText)

	t.Run("0 alpha no query", twoVector)

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
	deleteObjectClass(t, "CompanyGroup")
	deleteObjectClass(t, "CustomVectorClass")
}

func boolRef(a bool) *bool {
	return &a
}

func addTestSchema(t *testing.T, host string) {
	// Country, City, Airport schema
	cities.CreateCountryCityAirportSchema(t, host)

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
		Class: "CompanyGroup",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": false,
			},
		},
		Properties: []*models.Property{
			{
				Name:            "name",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationWord,
				IndexFilterable: boolRef(true),
				IndexSearchable: boolRef(true),
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizePropertyName": false,
					},
				},
			},
			{
				Name:            "city",
				DataType:        schema.DataTypeText.PropString(),
				Tokenization:    models.PropertyTokenizationField,
				IndexFilterable: boolRef(true),
				IndexSearchable: boolRef(true),
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

	hnswConfig := enthnsw.NewDefaultUserConfig()
	hnswConfig.MaxConnections = 64 // RansomNote tests require higher default max connections (reduced in 1.26)
	createObjectClass(t, &models.Class{
		Class: "RansomNote",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		VectorIndexConfig: hnswConfig,
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
	netherlands   strfmt.UUID = cities.Netherlands
	germany       strfmt.UUID = cities.Germany
	amsterdam     strfmt.UUID = cities.Amsterdam
	rotterdam     strfmt.UUID = cities.Rotterdam
	berlin        strfmt.UUID = cities.Berlin
	dusseldorf    strfmt.UUID = cities.Dusseldorf
	missingisland strfmt.UUID = cities.Missingisland
	nullisland    strfmt.UUID = cities.Nullisland
	airport1      strfmt.UUID = cities.Airport1
	airport2      strfmt.UUID = cities.Airport2
	airport3      strfmt.UUID = cities.Airport3
	airport4      strfmt.UUID = cities.Airport4
	cvc1          strfmt.UUID = "1ffeb3e1-1258-4c2a-afc3-55543f6c44b8"
	cvc2          strfmt.UUID = "df22e5c4-5d17-49f9-a71d-f392a82bc086"
	cvc3          strfmt.UUID = "c28a039a-d509-4c2e-940a-8b109e5bebf4"

	quattroFormaggi strfmt.UUID = "152500c6-4a8a-4732-aede-9fcab7e43532"
	fruttiDiMare    strfmt.UUID = "a828e9aa-d1b6-4644-8569-30d404e31a0d"
	hawaii          strfmt.UUID = "ed75037b-0748-4970-811e-9fe835ed41d1"
	doener          strfmt.UUID = "a655292d-1b93-44a1-9a47-57b6922bb455"
)

var (
	historyAmsterdam  = cities.HistoryAmsterdam
	historyRotterdam  = cities.HistoryRotterdam
	historyBerlin     = cities.HistoryBerlin
	historyDusseldorf = cities.HistoryDusseldorf
)

func addTestDataCityAirport(t *testing.T, host string) {
	cities.InsertCountryCityAirportObjects(t, host)
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

func addTestDataCompanyGroups(t *testing.T) {
	var (
		microsoft1 strfmt.UUID = "1fa3b21e-ca4f-4db7-a432-7fc6a23c534d"
		microsoft2 strfmt.UUID = "1f75ed97-39dd-4294-bff7-ecabd7923062"
		microsoft3 strfmt.UUID = "1343f51d-7e05-4084-bd66-d504db3b6bec"
		apple1     strfmt.UUID = "177fec91-1292-4928-8f53-f0ff49c76900"
		apple2     strfmt.UUID = "1b2cfdba-d4ba-4cf8-abda-e719ef35ac33"
		apple3     strfmt.UUID = "171d2b4c-3da1-4684-9c5e-aabd2a4f2998"
		google1    strfmt.UUID = "1c2e21fc-46fe-4999-b41c-a800595129af"
		google2    strfmt.UUID = "12b969c6-f184-4be0-8c40-7470af417cfc"
		google3    strfmt.UUID = "17829929-2037-4420-acbc-a433269feb93"
	)

	type companyTemplate struct {
		id     strfmt.UUID
		name   string
		inCity string
	}

	companies := []companyTemplate{
		{id: microsoft1, name: "Microsoft Inc.", inCity: "dusseldorf"},
		{id: microsoft2, name: "Microsoft Incorporated", inCity: "amsterdam"},
		{id: microsoft3, name: "Microsoft", inCity: "berlin"},
		{id: apple1, name: "Apple Inc.", inCity: "berlin"},
		{id: apple2, name: "Apple Incorporated", inCity: "dusseldorf"},
		{id: apple3, name: "Apple", inCity: "amsterdam"},
		{id: google1, name: "Google Inc.", inCity: "amsterdam"},
		{id: google2, name: "Google Incorporated", inCity: "berlin"},
		{id: google3, name: "Google", inCity: "dusseldorf"},
	}

	// companies
	for _, company := range companies {

		createObject(t, &models.Object{
			Class: "CompanyGroup",
			ID:    company.id,
			Properties: map[string]interface{}{
				"city": company.inCity,
				"name": company.name,
			},
		})
		fmt.Printf("created company  %s\n", company.name)
	}

	assertGetObjectEventually(t, companies[len(companies)-1].id)
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

	className := "RansomNote"
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < numBatches; i++ {
		batch := make([]*models.Object, batchSize)
		for j := 0; j < batchSize; j++ {
			noteLength := noteLengthMin + seededRand.Intn(noteLengthMax-noteLengthMin+1)
			note := helper.GetRandomString(noteLength)

			batch[j] = &models.Object{
				Class:      className,
				Properties: map[string]interface{}{"contents": note},
			}
		}

		createObjectsBatch(t, batch)
	}

	t.Run("wait for all objects to be indexed", func(t *testing.T) {
		// wait for all of the objects to get indexed
		waitForIndexing(t, className)
	})
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

	waitForIndexing(t, classNames[0])
	waitForIndexing(t, classNames[1])
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

	waitForIndexing(t, className)
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

func waitForIndexing(t *testing.T, className string) {
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		verbose := verbosity.OutputVerbose
		params := nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(className)
		body, clientErr := helper.Client(t).Nodes.NodesGetClass(params, nil)
		resp, err := body.Payload, clientErr
		require.NoError(ct, err)
		require.NotEmpty(ct, resp.Nodes)
		for _, n := range resp.Nodes {
			require.NotEmpty(ct, n.Shards)
			for _, s := range n.Shards {
				assert.Equal(ct, "READY", s.VectorIndexingStatus)
			}
		}
	}, 15*time.Second, 500*time.Millisecond)
}
