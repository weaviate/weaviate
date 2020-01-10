//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package test

import (
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/client/things"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
)

func Test_GraphQL(t *testing.T) {
	t.Run("setup test schema", addTestSchema)
	t.Run("import test data", addTestData)

	// tests
	t.Run("getting objects", gettingObjects)
	t.Run("getting objects with filters", gettingObjectsWithFilters)
	t.Run("getting objects with geo filters", gettingObjectsWithGeoFilters)

	// tear down
	deleteThingClass(t, "Country")
	deleteThingClass(t, "City")
	deleteThingClass(t, "Airport")
}

func createThingClass(t *testing.T, class *models.Class) {
	params := schema.NewSchemaThingsCreateParams().WithThingClass(class)
	resp, err := helper.Client(t).Schema.SchemaThingsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func createThing(t *testing.T, thing *models.Thing) {
	params := things.NewThingsCreateParams().WithBody(thing)
	resp, err := helper.Client(t).Things.ThingsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func deleteThingClass(t *testing.T, class string) {
	delParams := schema.NewSchemaThingsDeleteParams().WithClassName(class)
	delRes, err := helper.Client(t).Schema.SchemaThingsDelete(delParams, nil)
	helper.AssertRequestOk(t, delRes, err, nil)
}

func addTestSchema(t *testing.T) {
	createThingClass(t, &models.Class{
		Class:              "Country",
		VectorizeClassName: true,
		Properties: []*models.Property{
			&models.Property{
				Name:     "name",
				DataType: []string{"string"},
			},
		},
	})

	createThingClass(t, &models.Class{
		Class:              "City",
		VectorizeClassName: true,
		Properties: []*models.Property{
			&models.Property{
				Name:     "name",
				DataType: []string{"string"},
			},
			&models.Property{
				Name:     "inCountry",
				DataType: []string{"Country"},
			},
			&models.Property{
				Name:     "population",
				DataType: []string{"int"},
			},
			&models.Property{
				Name:     "location",
				DataType: []string{"geoCoordinates"},
			},
		},
	})

	createThingClass(t, &models.Class{
		Class:              "Airport",
		VectorizeClassName: true,
		Properties: []*models.Property{
			&models.Property{
				Name:     "code",
				DataType: []string{"string"},
			},
			&models.Property{
				Name:     "inCity",
				DataType: []string{"City"},
			},
		},
	})
}

func addTestData(t *testing.T) {

	var (
		netherlands strfmt.UUID = "67b79643-cf8b-4b22-b206-6e63dbb4e57a"
		germany     strfmt.UUID = "561eea29-b733-4079-b50b-cfabd78190b7"
		amsterdam   strfmt.UUID = "8f5f8e44-d348-459c-88b1-c1a44bb8f8be"
		rotterdam   strfmt.UUID = "660db307-a163-41d2-8182-560782cd018f"
		berlin      strfmt.UUID = "9b9cbea5-e87e-4cd0-89af-e2f424fd52d6"
		dusseldorf  strfmt.UUID = "6ffb03f8-a853-4ec5-a5d8-302e45aaaf13"
		airport1    strfmt.UUID = "4770bb19-20fd-406e-ac64-9dac54c27a0f"
		airport2    strfmt.UUID = "cad6ab9b-5bb9-4388-a933-a5bdfd23db37"
		airport3    strfmt.UUID = "55a4dbbb-e2af-4b2a-901d-98146d1eeca7"
		airport4    strfmt.UUID = "62d15920-b546-4844-bc87-3ae33268fab5"
	)

	// countries
	createThing(t, &models.Thing{
		Class: "Country",
		ID:    netherlands,
		Schema: map[string]interface{}{
			"name": "Netherlands",
		},
	})
	createThing(t, &models.Thing{
		Class: "Country",
		ID:    germany,
		Schema: map[string]interface{}{
			"name": "Germany",
		},
	})

	// cities
	createThing(t, &models.Thing{
		Class: "City",
		ID:    amsterdam,
		Schema: map[string]interface{}{
			"name":       "Amsterdam",
			"population": 1800000,
			"location": map[string]interface{}{
				"latitude":  52.366667,
				"longitude": 4.9,
			},
			"inCountry": []interface{}{
				map[string]interface{}{
					"beacon": crossref.New("localhost", netherlands, kind.Thing).String(),
				},
			},
		},
	})
	createThing(t, &models.Thing{
		Class: "City",
		ID:    rotterdam,
		Schema: map[string]interface{}{
			"name":       "Rotterdam",
			"population": 600000,
			"inCountry": []interface{}{
				map[string]interface{}{
					"beacon": crossref.New("localhost", netherlands, kind.Thing).String(),
				},
			},
		},
	})
	createThing(t, &models.Thing{
		Class: "City",
		ID:    berlin,
		Schema: map[string]interface{}{
			"name":       "Berlin",
			"population": 3470000,
			"inCountry": []interface{}{
				map[string]interface{}{
					"beacon": crossref.New("localhost", germany, kind.Thing).String(),
				},
			},
		},
	})
	createThing(t, &models.Thing{
		Class: "City",
		ID:    dusseldorf,
		Schema: map[string]interface{}{
			"name":       "Dusseldorf",
			"population": 600000,
			"inCountry": []interface{}{
				map[string]interface{}{
					"beacon": crossref.New("localhost", germany, kind.Thing).String(),
				},
			},
			"location": map[string]interface{}{
				"latitude":  51.225556,
				"longitude": 6.782778,
			},
		},
	})

	// airports
	createThing(t, &models.Thing{
		Class: "Airport",
		ID:    airport1,
		Schema: map[string]interface{}{
			"code": "10000",
			"inCity": []interface{}{
				map[string]interface{}{
					"beacon": crossref.New("localhost", amsterdam, kind.Thing).String(),
				},
			},
		},
	})
	createThing(t, &models.Thing{
		Class: "Airport",
		ID:    airport2,
		Schema: map[string]interface{}{
			"code": "20000",
			"inCity": []interface{}{
				map[string]interface{}{
					"beacon": crossref.New("localhost", rotterdam, kind.Thing).String(),
				},
			},
		},
	})
	createThing(t, &models.Thing{
		Class: "Airport",
		ID:    airport3,
		Schema: map[string]interface{}{
			"code": "30000",
			"inCity": []interface{}{
				map[string]interface{}{
					"beacon": crossref.New("localhost", dusseldorf, kind.Thing).String(),
				},
			},
		},
	})
	createThing(t, &models.Thing{
		Class: "Airport",
		ID:    airport4,
		Schema: map[string]interface{}{
			"code": "40000",
			"inCity": []interface{}{
				map[string]interface{}{
					"beacon": crossref.New("localhost", berlin, kind.Thing).String(),
				},
			},
		},
	})

	// wait for consistency
	assertGetThingEventually(t, airport1)
	assertGetThingEventually(t, airport2)
	assertGetThingEventually(t, airport3)
	assertGetThingEventually(t, airport4)

	// give cache some time to become hot
	time.Sleep(2 * time.Second)
}
