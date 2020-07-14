//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/client/things"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
)

// This test suite is meant to prevent a regression on
// https://github.com/semi-technologies/weaviate/issues/868, hence it tries to
// reprodcue the steps outlined in there as closely as possible
func Test_CREFWithCardinalityMany_UsingPatch(t *testing.T) {
	defer func() {
		// clean up so we can run this test multiple times in a row
		delCityParams := schema.NewSchemaThingsDeleteParams().WithClassName("ReferenceTestCity")
		dresp, err := helper.Client(t).Schema.SchemaThingsDelete(delCityParams, nil)
		t.Logf("clean up - delete city \n%v\n %v", dresp, err)

		delPlaceParams := schema.NewSchemaThingsDeleteParams().WithClassName("ReferenceTestPlace")
		dresp, err = helper.Client(t).Schema.SchemaThingsDelete(delPlaceParams, nil)
		t.Logf("clean up - delete place \n%v\n %v", dresp, err)
	}()

	t.Log("1. create ReferenceTestPlace class")
	placeClass := &models.Class{
		Class: "ReferenceTestPlace",
		Properties: []*models.Property{
			&models.Property{
				DataType: []string{"string"},
				Name:     "name",
			},
		},
	}
	params := schema.NewSchemaThingsCreateParams().WithThingClass(placeClass)
	resp, err := helper.Client(t).Schema.SchemaThingsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("2. create ReferenceTestCity class with HasPlaces (many) cross-ref")
	cardinalityMany := "many"
	cityClass := &models.Class{
		Class: "ReferenceTestCity",
		Properties: []*models.Property{
			&models.Property{
				DataType: []string{"string"},
				Name:     "name",
			},
			&models.Property{
				DataType:    []string{"ReferenceTestPlace"},
				Name:        "HasPlaces",
				Cardinality: &cardinalityMany,
			},
		},
	}
	params = schema.NewSchemaThingsCreateParams().WithThingClass(cityClass)
	resp, err = helper.Client(t).Schema.SchemaThingsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("3. add two places and save their IDs")
	place1ID := assertCreateThing(t, "ReferenceTestPlace", map[string]interface{}{
		"name": "Place 1",
	})
	place2ID := assertCreateThing(t, "ReferenceTestPlace", map[string]interface{}{
		"name": "Place 2",
	})
	assertGetThingEventually(t, place1ID)
	assertGetThingEventually(t, place2ID)

	t.Log("4. add one city")
	cityID := assertCreateThing(t, "ReferenceTestCity", map[string]interface{}{
		"name": "My City",
	})
	assertGetThingEventually(t, cityID)

	t.Log("5. patch city to point to the first place")
	patchParams := things.NewThingsPatchParams().
		WithID(cityID).
		WithBody(&models.Thing{
			Class: "ReferenceTestCity",
			Schema: map[string]interface{}{
				"hasPlaces": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/things/%s", place1ID.String()),
					},
				},
			},
		})
	patchResp, err := helper.Client(t).Things.ThingsPatch(patchParams, nil)
	helper.AssertRequestOk(t, patchResp, err, nil)

	t.Log("6. verify first cross ref was added")

	actualThunk := func() interface{} {
		cityAfterFirstPatch := assertGetThing(t, cityID)
		return cityAfterFirstPatch.Schema
	}

	testhelper.AssertEventuallyEqual(t, map[string]interface{}{
		"name": "My City",
		"hasPlaces": []interface{}{
			map[string]interface{}{
				"beacon": fmt.Sprintf("weaviate://localhost/things/%s", place1ID.String()),
				"href":   fmt.Sprintf("/v1/things/%s", place1ID.String()),
			},
		},
	}, actualThunk)

	t.Log("7. patch city to point to the second place")
	patchParams = things.NewThingsPatchParams().
		WithID(cityID).
		WithBody(&models.Thing{
			Class: "ReferenceTestCity",
			Schema: map[string]interface{}{
				"hasPlaces": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/things/%s", place2ID.String()),
					},
				},
			},
		})
	patchResp, err = helper.Client(t).Things.ThingsPatch(patchParams, nil)
	helper.AssertRequestOk(t, patchResp, err, nil)

	actualThunk = func() interface{} {
		city := assertGetThing(t, cityID)
		return city.Schema.(map[string]interface{})["hasPlaces"].([]interface{})
	}

	t.Log("9. verify both cross refs are present")
	expectedRefs := []interface{}{
		map[string]interface{}{
			"beacon": fmt.Sprintf("weaviate://localhost/things/%s", place1ID.String()),
			"href":   fmt.Sprintf("/v1/things/%s", place1ID.String()),
		},
		map[string]interface{}{
			"beacon": fmt.Sprintf("weaviate://localhost/things/%s", place2ID.String()),
			"href":   fmt.Sprintf("/v1/things/%s", place2ID.String()),
		},
	}

	testhelper.AssertEventuallyEqual(t, expectedRefs, actualThunk)
}

// This test suite is meant to prevent a regression on
// https://github.com/semi-technologies/weaviate/issues/868, hence it tries to
// reprodcue the steps outlined in there as closely as possible
func Test_CREFWithCardinalityMany_UsingPostReference(t *testing.T) {
	defer func() {
		// clean up so we can run this test multiple times in a row
		delCityParams := schema.NewSchemaThingsDeleteParams().WithClassName("ReferenceTestCity")
		dresp, err := helper.Client(t).Schema.SchemaThingsDelete(delCityParams, nil)
		t.Logf("clean up - delete city \n%v\n %v", dresp, err)

		delPlaceParams := schema.NewSchemaThingsDeleteParams().WithClassName("ReferenceTestPlace")
		dresp, err = helper.Client(t).Schema.SchemaThingsDelete(delPlaceParams, nil)
		t.Logf("clean up - delete place \n%v\n %v", dresp, err)
	}()

	t.Log("1. create ReferenceTestPlace class")
	placeClass := &models.Class{
		Class: "ReferenceTestPlace",
		Properties: []*models.Property{
			&models.Property{
				DataType: []string{"string"},
				Name:     "name",
			},
		},
	}
	params := schema.NewSchemaThingsCreateParams().WithThingClass(placeClass)
	resp, err := helper.Client(t).Schema.SchemaThingsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("2. create ReferenceTestCity class with HasPlaces (many) cross-ref")
	cardinalityMany := "many"
	cityClass := &models.Class{
		Class: "ReferenceTestCity",
		Properties: []*models.Property{
			&models.Property{
				DataType: []string{"string"},
				Name:     "name",
			},
			&models.Property{
				DataType:    []string{"ReferenceTestPlace"},
				Name:        "HasPlaces",
				Cardinality: &cardinalityMany,
			},
		},
	}
	params = schema.NewSchemaThingsCreateParams().WithThingClass(cityClass)
	resp, err = helper.Client(t).Schema.SchemaThingsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("3. add two places and save their IDs")
	place1ID := assertCreateThing(t, "ReferenceTestPlace", map[string]interface{}{
		"name": "Place 1",
	})
	place2ID := assertCreateThing(t, "ReferenceTestPlace", map[string]interface{}{
		"name": "Place 2",
	})
	assertGetThingEventually(t, place1ID)
	assertGetThingEventually(t, place2ID)
	t.Logf("Place 1 ID: %s", place1ID)
	t.Logf("Place 2 ID: %s", place2ID)

	t.Log("4. add one city")
	cityID := assertCreateThing(t, "ReferenceTestCity", map[string]interface{}{
		"name": "My City",
	})
	assertGetThingEventually(t, cityID)

	t.Log("5. POST /references/ for place 1")
	postRefParams := things.NewThingsReferencesCreateParams().
		WithID(cityID).
		WithPropertyName("hasPlaces").
		WithBody(&models.SingleRef{
			Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", place1ID.String())),
		})
	postRefResponse, err := helper.Client(t).Things.ThingsReferencesCreate(postRefParams, nil)
	helper.AssertRequestOk(t, postRefResponse, err, nil)

	actualThunk := func() interface{} {
		city := assertGetThing(t, cityID)
		return city.Schema
	}
	t.Log("7. verify first cross ref was added")
	testhelper.AssertEventuallyEqual(t, map[string]interface{}{
		"name": "My City",
		"hasPlaces": []interface{}{
			map[string]interface{}{
				"beacon": fmt.Sprintf("weaviate://localhost/things/%s", place1ID.String()),
				"href":   fmt.Sprintf("/v1/things/%s", place1ID.String()),
			},
		},
	}, actualThunk)

	t.Log("8. POST /references/ for place 2")
	postRefParams = things.NewThingsReferencesCreateParams().
		WithID(cityID).
		WithPropertyName("hasPlaces").
		WithBody(&models.SingleRef{
			Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", place2ID.String())),
		})
	postRefResponse, err = helper.Client(t).Things.ThingsReferencesCreate(postRefParams, nil)
	helper.AssertRequestOk(t, postRefResponse, err, nil)

	t.Log("9. verify both cross refs are present")
	actualThunk = func() interface{} {
		city := assertGetThing(t, cityID)
		return city.Schema.(map[string]interface{})["hasPlaces"].([]interface{})
	}

	expectedRefs := []interface{}{
		map[string]interface{}{
			"beacon": fmt.Sprintf("weaviate://localhost/things/%s", place1ID.String()),
			"href":   fmt.Sprintf("/v1/things/%s", place1ID.String()),
		},
		map[string]interface{}{
			"beacon": fmt.Sprintf("weaviate://localhost/things/%s", place2ID.String()),
			"href":   fmt.Sprintf("/v1/things/%s", place2ID.String()),
		},
	}

	testhelper.AssertEventuallyEqual(t, expectedRefs, actualThunk)
}
