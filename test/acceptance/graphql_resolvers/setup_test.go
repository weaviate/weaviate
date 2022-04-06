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

package test

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/batch"
	"github.com/semi-technologies/weaviate/client/objects"
	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	sch "github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

func Test_GraphQL(t *testing.T) {
	t.Run("setup test schema", addTestSchema)
	t.Run("import test data (city, country, airport)", addTestDataCityAirport)
	t.Run("import test data (companies)", addTestDataCompanies)
	t.Run("import test data (person)", addTestDataPersons)
	t.Run("import test data (pizzas)", addTestDataPizzas)
	t.Run("import test data (custom vector class)", addTestDataCVC)
	t.Run("import test data (array class)", addTestDataArrayClasses)

	// tests
	t.Run("getting objects", gettingObjects)
	t.Run("getting objects with filters", gettingObjectsWithFilters)
	t.Run("getting objects with geo filters", gettingObjectsWithGeoFilters)
	t.Run("getting objects with grouping", gettingObjectsWithGrouping)
	t.Run("getting objects with additional props", gettingObjectsWithAdditionalProps)
	t.Run("aggregates without grouping or filters", aggregatesWithoutGroupingOrFilters)
	t.Run("aggregates local meta with filters", localMetaWithFilters)
	t.Run("aggregates local meta string props not set everywhere", localMeta_StringPropsNotSetEverywhere)
	t.Run("aggregates array class without grouping or filters", aggregatesArrayClassWithoutGroupingOrFilters)
	t.Run("aggregates array class with grouping", aggregatesArrayClassWithGrouping)

	// tear down
	deleteObjectClass(t, "Person")
	deleteObjectClass(t, "Pizza")
	deleteObjectClass(t, "Country")
	deleteObjectClass(t, "City")
	deleteObjectClass(t, "Airport")
	deleteObjectClass(t, "Company")
	deleteObjectClass(t, "ArrayClass")

	// only run after everything else is deleted, this way, we can also run an
	// all-class Explore since all vectors which are now left have the same
	// dimensions.
	t.Run("getting objects with custom vectors", gettingObjectsWithCustomVectors)
	deleteObjectClass(t, "CustomVectorClass")
}

func createObjectClass(t *testing.T, class *models.Class) {
	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func createObject(t *testing.T, object *models.Object) {
	params := objects.NewObjectsCreateParams().WithBody(object)
	resp, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func createObjectsBatch(t *testing.T, objects []*models.Object) {
	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{
			Objects: objects,
		})
	resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
	for _, elem := range resp.Payload {
		assert.Nil(t, elem.Result.Errors)
	}
}

func deleteObjectClass(t *testing.T, class string) {
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(class)
	delRes, err := helper.Client(t).Schema.SchemaObjectsDelete(delParams, nil)
	helper.AssertRequestOk(t, delRes, err, nil)
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
				Name:     "name",
				DataType: []string{"string"},
			},
		},
	})

	createObjectClass(t, &models.Class{
		Class: "City",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: []string{"string"},
			},
			{
				Name:     "inCountry",
				DataType: []string{"Country"},
			},
			{
				Name:     "population",
				DataType: []string{"int"},
			},
			{
				Name:     "location",
				DataType: []string{"geoCoordinates"},
			},
			{
				Name:     "isCapital",
				DataType: []string{"boolean"},
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
		Properties: []*models.Property{
			{
				Name:     "code",
				DataType: []string{"string"},
			},
			{
				Name:     "phone",
				DataType: []string{"phoneNumber"},
			},
			{
				Name:     "inCity",
				DataType: []string{"City"},
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
				Name:     "name",
				DataType: []string{"string"},
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
				Name:     "name",
				DataType: []string{"string"},
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
				DataType:     []string{string(sch.DataTypeString)},
				Tokenization: models.PropertyTokenizationField,
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizePropertyName": false,
					},
				},
			},
			{
				Name:         "about",
				DataType:     []string{string(sch.DataTypeStringArray)},
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
				DataType:     []string{string(sch.DataTypeString)},
				Tokenization: models.PropertyTokenizationField,
				ModuleConfig: map[string]interface{}{
					"text2vec-contextionary": map[string]interface{}{
						"vectorizePropertyName": false,
					},
				},
			},
			{
				Name:         "description",
				DataType:     []string{string(sch.DataTypeText)},
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
		Class:      "CustomVectorClass",
		Vectorizer: "none",
		Properties: []*models.Property{},
	})

	createObjectClass(t, &models.Class{
		Class: "ArrayClass",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{
				"vectorizeClassName": true,
			},
		},
		Properties: []*models.Property{
			{
				Name:     "strings",
				DataType: []string{"string[]"},
			},
			{
				Name:     "numbers",
				DataType: []string{"number[]"},
			},
			{
				Name:     "booleans",
				DataType: []string{"boolean[]"},
			},
		},
	})
}

const (
	netherlands strfmt.UUID = "67b79643-cf8b-4b22-b206-6e63dbb4e57a"
	germany     strfmt.UUID = "561eea29-b733-4079-b50b-cfabd78190b7"
	amsterdam   strfmt.UUID = "8f5f8e44-d348-459c-88b1-c1a44bb8f8be"
	rotterdam   strfmt.UUID = "660db307-a163-41d2-8182-560782cd018f"
	berlin      strfmt.UUID = "9b9cbea5-e87e-4cd0-89af-e2f424fd52d6"
	dusseldorf  strfmt.UUID = "6ffb03f8-a853-4ec5-a5d8-302e45aaaf13"
	nullisland  strfmt.UUID = "823abeca-eef3-41c7-b587-7a6977b08003"
	airport1    strfmt.UUID = "4770bb19-20fd-406e-ac64-9dac54c27a0f"
	airport2    strfmt.UUID = "cad6ab9b-5bb9-4388-a933-a5bdfd23db37"
	airport3    strfmt.UUID = "55a4dbbb-e2af-4b2a-901d-98146d1eeca7"
	airport4    strfmt.UUID = "62d15920-b546-4844-bc87-3ae33268fab5"
	cvc1        strfmt.UUID = "1ffeb3e1-1258-4c2a-afc3-55543f6c44b8"
	cvc2        strfmt.UUID = "df22e5c4-5d17-49f9-a71d-f392a82bc086"
	cvc3        strfmt.UUID = "c28a039a-d509-4c2e-940a-8b109e5bebf4"

	quattroFormaggi strfmt.UUID = "152500c6-4a8a-4732-aede-9fcab7e43532"
	fruttiDiMare    strfmt.UUID = "a828e9aa-d1b6-4644-8569-30d404e31a0d"
	hawaii          strfmt.UUID = "ed75037b-0748-4970-811e-9fe835ed41d1"
	doener          strfmt.UUID = "a655292d-1b93-44a1-9a47-57b6922bb455"
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
					"beacon": crossref.New("localhost", netherlands).String(),
				},
			},
			"isCapital": true,
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
					"beacon": crossref.New("localhost", netherlands).String(),
				},
			},
			"isCapital": false,
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
					"beacon": crossref.New("localhost", germany).String(),
				},
			},
			"isCapital": true,
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
					"beacon": crossref.New("localhost", germany).String(),
				},
			},
			"location": map[string]interface{}{
				"latitude":  51.225556,
				"longitude": 6.782778,
			},
			"isCapital": false,
		},
	})

	createObject(t, &models.Object{
		Class: "City",
		ID:    nullisland,
		Properties: map[string]interface{}{
			"name":       "Null Island",
			"population": 0,
			"location": map[string]interface{}{
				"latitude":  0,
				"longitude": 0,
			},
			"isCapital": false,
		},
	})

	// airports
	createObject(t, &models.Object{
		Class: "Airport",
		ID:    airport1,
		Properties: map[string]interface{}{
			"code": "10000",
			"phone": map[string]interface{}{
				"input": "+311234567",
			},
			"inCity": []interface{}{
				map[string]interface{}{
					"beacon": crossref.New("localhost", amsterdam).String(),
				},
			},
		},
	})
	createObject(t, &models.Object{
		Class: "Airport",
		ID:    airport2,
		Properties: map[string]interface{}{
			"code": "20000",
			"inCity": []interface{}{
				map[string]interface{}{
					"beacon": crossref.New("localhost", rotterdam).String(),
				},
			},
		},
	})
	createObject(t, &models.Object{
		Class: "Airport",
		ID:    airport3,
		Properties: map[string]interface{}{
			"code": "30000",
			"inCity": []interface{}{
				map[string]interface{}{
					"beacon": crossref.New("localhost", dusseldorf).String(),
				},
			},
		},
	})
	createObject(t, &models.Object{
		Class: "Airport",
		ID:    airport4,
		Properties: map[string]interface{}{
			"code": "40000",
			"inCity": []interface{}{
				map[string]interface{}{
					"beacon": crossref.New("localhost", berlin).String(),
				},
			},
		},
	})

	// wait for consistency
	assertGetObjectEventually(t, airport1)
	assertGetObjectEventually(t, airport2)
	assertGetObjectEventually(t, airport3)
	assertGetObjectEventually(t, airport4)

	// give cache some time to become hot
	time.Sleep(2 * time.Second)
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
					"beacon": crossref.New("localhost", c).String(),
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
					"beacon": crossref.New("localhost", c).String(),
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
	// add one object indivdually
	createObject(t, &models.Object{
		Class:  "CustomVectorClass",
		ID:     cvc1,
		Vector: []float32{1.1, 1.1, 1.1},
	})

	assertGetObjectEventually(t, cvc1)

	createObjectsBatch(t, []*models.Object{
		{
			Class:  "CustomVectorClass",
			ID:     cvc2,
			Vector: []float32{1.1, 1.1, 0.1},
		},
		{
			Class:  "CustomVectorClass",
			ID:     cvc3,
			Vector: []float32{1.1, 0, 0},
		},
	})
	assertGetObjectEventually(t, cvc3)
}

func addTestDataArrayClasses(t *testing.T) {
	var (
		arrayClassID1 strfmt.UUID = "cfa3b21e-ca5f-4db7-a412-5fc6a23c534a"
		arrayClassID2 strfmt.UUID = "cfa3b21e-ca5f-4db7-a412-5fc6a23c534b"
		arrayClassID3 strfmt.UUID = "cfa3b21e-ca5f-4db7-a412-5fc6a23c534c"
	)
	createObject(t, &models.Object{
		Class: "ArrayClass",
		ID:    arrayClassID1,
		Properties: map[string]interface{}{
			"strings":  []string{"a", "b", "c"},
			"numbers":  []float64{1.0, 2.0, 3.0},
			"booleans": []bool{true, true},
		},
	})
	assertGetObjectEventually(t, arrayClassID1)

	createObject(t, &models.Object{
		Class: "ArrayClass",
		ID:    arrayClassID2,
		Properties: map[string]interface{}{
			"strings":  []string{"a", "b"},
			"numbers":  []float64{1.0, 2.0},
			"booleans": []bool{false, false},
		},
	})
	assertGetObjectEventually(t, arrayClassID2)

	createObject(t, &models.Object{
		Class: "ArrayClass",
		ID:    arrayClassID3,
		Properties: map[string]interface{}{
			"strings":  []string{"a"},
			"numbers":  []float64{1.0},
			"booleans": []bool{true, false},
		},
	})
	assertGetObjectEventually(t, arrayClassID3)
}
