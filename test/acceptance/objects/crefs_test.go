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

package test

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	testhelper "github.com/weaviate/weaviate/test/helper"
)

func Test_refs_without_to_class(t *testing.T) {
	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{Class: "ReferenceTo"})
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	refFromClass := &models.Class{
		Class: "ReferenceFrom",
		Properties: []*models.Property{
			{
				DataType: []string{"ReferenceTo"},
				Name:     "ref",
			},
		},
	}
	params2 := clschema.NewSchemaObjectsCreateParams().WithObjectClass(refFromClass)
	resp2, err := helper.Client(t).Schema.SchemaObjectsCreate(params2, nil)
	helper.AssertRequestOk(t, resp2, err, nil)

	defer deleteObjectClass(t, "ReferenceTo")
	defer deleteObjectClass(t, "ReferenceFrom")

	refToId := assertCreateObject(t, "ReferenceTo", map[string]interface{}{})
	assertGetObjectEventually(t, refToId)
	refFromId := assertCreateObject(t, "ReferenceFrom", map[string]interface{}{})
	assertGetObjectEventually(t, refFromId)

	postRefParams := objects.NewObjectsClassReferencesCreateParams().
		WithID(refFromId).
		WithPropertyName("ref").WithClassName(refFromClass.Class).
		WithBody(&models.SingleRef{
			Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", refToId.String())),
		})
	postRefResponse, err := helper.Client(t).Objects.ObjectsClassReferencesCreate(postRefParams, nil)
	helper.AssertRequestOk(t, postRefResponse, err, nil)

	// validate that ref was create for the correct class
	objWithRef := func() interface{} {
		obj := assertGetObjectWithClass(t, refFromId, "ReferenceFrom")
		return obj.Properties
	}
	testhelper.AssertEventuallyEqual(t, map[string]interface{}{
		"ref": []interface{}{
			map[string]interface{}{
				"beacon": fmt.Sprintf("weaviate://localhost/%s/%s", "ReferenceTo", refToId.String()),
				"href":   fmt.Sprintf("/v1/objects/%s/%s", "ReferenceTo", refToId.String()),
			},
		},
	}, objWithRef)

	// delete reference without class
	deleteRefParams := objects.NewObjectsClassReferencesDeleteParams().
		WithID(refFromId).
		WithPropertyName("ref").WithClassName(refFromClass.Class).
		WithBody(&models.SingleRef{
			Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", refToId.String())),
		})
	deleteRefResponse, err := helper.Client(t).Objects.ObjectsClassReferencesDelete(deleteRefParams, nil)
	helper.AssertRequestOk(t, deleteRefResponse, err, nil)
	objWithoutRef := func() interface{} {
		obj := assertGetObjectWithClass(t, refFromId, "ReferenceFrom")
		return obj.Properties
	}
	testhelper.AssertEventuallyEqual(t, map[string]interface{}{
		"ref": []interface{}{},
	}, objWithoutRef)
}

// This test suite is meant to prevent a regression on
// https://github.com/weaviate/weaviate/issues/868, hence it tries to
// reprodcue the steps outlined in there as closely as possible
func Test_CREFWithCardinalityMany_UsingPatch(t *testing.T) {
	defer func() {
		// clean up so we can run this test multiple times in a row
		delCityParams := clschema.NewSchemaObjectsDeleteParams().WithClassName("ReferenceTestCity")
		dresp, err := helper.Client(t).Schema.SchemaObjectsDelete(delCityParams, nil)
		t.Logf("clean up - delete city \n%v\n %v", dresp, err)

		delPlaceParams := clschema.NewSchemaObjectsDeleteParams().WithClassName("ReferenceTestPlace")
		dresp, err = helper.Client(t).Schema.SchemaObjectsDelete(delPlaceParams, nil)
		t.Logf("clean up - delete place \n%v\n %v", dresp, err)
	}()

	t.Log("1. create ReferenceTestPlace class")
	placeClass := &models.Class{
		Class: "ReferenceTestPlace",
		Properties: []*models.Property{
			{
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				Name:         "name",
			},
		},
	}
	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(placeClass)
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("2. create ReferenceTestCity class with HasPlaces (many) cross-ref")
	cityClass := &models.Class{
		Class: "ReferenceTestCity",
		Properties: []*models.Property{
			{
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				Name:         "name",
			},
			{
				DataType: []string{"ReferenceTestPlace"},
				Name:     "HasPlaces",
			},
		},
	}
	params = clschema.NewSchemaObjectsCreateParams().WithObjectClass(cityClass)
	resp, err = helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("3. add two places and save their IDs")
	place1ID := assertCreateObject(t, "ReferenceTestPlace", map[string]interface{}{
		"name": "Place 1",
	})
	place2ID := assertCreateObject(t, "ReferenceTestPlace", map[string]interface{}{
		"name": "Place 2",
	})
	assertGetObjectEventually(t, place1ID)
	assertGetObjectEventually(t, place2ID)

	t.Log("4. add one city")
	cityID := assertCreateObject(t, "ReferenceTestCity", map[string]interface{}{
		"name": "My City",
	})
	assertGetObjectEventually(t, cityID)

	t.Log("5. patch city to point to the first place")
	patchParams := objects.NewObjectsPatchParams().
		WithID(cityID).
		WithBody(&models.Object{
			Class: "ReferenceTestCity",
			Properties: map[string]interface{}{
				"hasPlaces": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/%s", place1ID.String()),
					},
				},
			},
		})
	patchResp, err := helper.Client(t).Objects.ObjectsPatch(patchParams, nil)
	helper.AssertRequestOk(t, patchResp, err, nil)

	t.Log("6. verify first cross ref was added")

	actualThunk := func() interface{} {
		cityAfterFirstPatch := assertGetObject(t, cityID)
		return cityAfterFirstPatch.Properties
	}

	testhelper.AssertEventuallyEqual(t, map[string]interface{}{
		"name": "My City",
		"hasPlaces": []interface{}{
			map[string]interface{}{
				"beacon": fmt.Sprintf("weaviate://localhost/%s", place1ID.String()),
				"href":   fmt.Sprintf("/v1/objects/%s", place1ID.String()),
			},
		},
	}, actualThunk)

	t.Log("7. patch city to point to the second place")
	patchParams = objects.NewObjectsPatchParams().
		WithID(cityID).
		WithBody(&models.Object{
			Class: "ReferenceTestCity",
			Properties: map[string]interface{}{
				"hasPlaces": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/%s", place2ID.String()),
					},
				},
			},
		})
	patchResp, err = helper.Client(t).Objects.ObjectsPatch(patchParams, nil)
	helper.AssertRequestOk(t, patchResp, err, nil)

	actualThunk = func() interface{} {
		city := assertGetObject(t, cityID)
		return city.Properties.(map[string]interface{})["hasPlaces"].([]interface{})
	}

	t.Log("9. verify both cross refs are present")
	expectedRefs := []interface{}{
		map[string]interface{}{
			"beacon": fmt.Sprintf("weaviate://localhost/%s", place1ID.String()),
			"href":   fmt.Sprintf("/v1/objects/%s", place1ID.String()),
		},
		map[string]interface{}{
			"beacon": fmt.Sprintf("weaviate://localhost/%s", place2ID.String()),
			"href":   fmt.Sprintf("/v1/objects/%s", place2ID.String()),
		},
	}

	testhelper.AssertEventuallyEqual(t, expectedRefs, actualThunk)
}

// This test suite is meant to prevent a regression on
// https://github.com/weaviate/weaviate/issues/868, hence it tries to
// reprodcue the steps outlined in there as closely as possible
func Test_CREFWithCardinalityMany_UsingPostReference(t *testing.T) {
	defer func() {
		// clean up so we can run this test multiple times in a row
		delCityParams := clschema.NewSchemaObjectsDeleteParams().WithClassName("ReferenceTestCity")
		dresp, err := helper.Client(t).Schema.SchemaObjectsDelete(delCityParams, nil)
		t.Logf("clean up - delete city \n%v\n %v", dresp, err)

		delPlaceParams := clschema.NewSchemaObjectsDeleteParams().WithClassName("ReferenceTestPlace")
		dresp, err = helper.Client(t).Schema.SchemaObjectsDelete(delPlaceParams, nil)
		t.Logf("clean up - delete place \n%v\n %v", dresp, err)
	}()

	t.Log("1. create ReferenceTestPlace class")
	placeClass := &models.Class{
		Class: "ReferenceTestPlace",
		Properties: []*models.Property{
			{
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				Name:         "name",
			},
		},
	}
	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(placeClass)
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("2. create ReferenceTestCity class with HasPlaces (many) cross-ref")
	cityClass := &models.Class{
		Class: "ReferenceTestCity",
		Properties: []*models.Property{
			{
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				Name:         "name",
			},
			{
				DataType: []string{"ReferenceTestPlace"},
				Name:     "HasPlaces",
			},
		},
	}
	params = clschema.NewSchemaObjectsCreateParams().WithObjectClass(cityClass)
	resp, err = helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("3. add two places and save their IDs")
	place1ID := assertCreateObject(t, "ReferenceTestPlace", map[string]interface{}{
		"name": "Place 1",
	})
	place2ID := assertCreateObject(t, "ReferenceTestPlace", map[string]interface{}{
		"name": "Place 2",
	})
	assertGetObjectEventually(t, place1ID)
	assertGetObjectEventually(t, place2ID)
	t.Logf("Place 1 ID: %s", place1ID)
	t.Logf("Place 2 ID: %s", place2ID)

	t.Log("4. add one city")
	cityID := assertCreateObject(t, "ReferenceTestCity", map[string]interface{}{
		"name": "My City",
	})
	assertGetObjectEventually(t, cityID)

	t.Log("5. POST /references/ for place 1")
	postRefParams := objects.NewObjectsReferencesCreateParams().
		WithID(cityID).
		WithPropertyName("hasPlaces").
		WithBody(&models.SingleRef{
			Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", place1ID.String())),
		})
	postRefResponse, err := helper.Client(t).Objects.ObjectsReferencesCreate(postRefParams, nil)
	helper.AssertRequestOk(t, postRefResponse, err, nil)

	actualThunk := func() interface{} {
		city := assertGetObject(t, cityID)
		return city.Properties
	}
	t.Log("7. verify first cross ref was added")
	testhelper.AssertEventuallyEqual(t, map[string]interface{}{
		"name": "My City",
		"hasPlaces": []interface{}{
			map[string]interface{}{
				"beacon": fmt.Sprintf("weaviate://localhost/%s", place1ID.String()),
				"href":   fmt.Sprintf("/v1/objects/%s", place1ID.String()),
			},
		},
	}, actualThunk)

	t.Log("8. POST /references/ for place 2")
	postRefParams = objects.NewObjectsReferencesCreateParams().
		WithID(cityID).
		WithPropertyName("hasPlaces").
		WithBody(&models.SingleRef{
			Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", place2ID.String())),
		})
	postRefResponse, err = helper.Client(t).Objects.ObjectsReferencesCreate(postRefParams, nil)
	helper.AssertRequestOk(t, postRefResponse, err, nil)

	t.Log("9. verify both cross refs are present")
	actualThunk = func() interface{} {
		city := assertGetObject(t, cityID)
		return city.Properties.(map[string]interface{})["hasPlaces"].([]interface{})
	}

	expectedRefs := []interface{}{
		map[string]interface{}{
			"beacon": fmt.Sprintf("weaviate://localhost/%s", place1ID.String()),
			"href":   fmt.Sprintf("/v1/objects/%s", place1ID.String()),
		},
		map[string]interface{}{
			"beacon": fmt.Sprintf("weaviate://localhost/%s", place2ID.String()),
			"href":   fmt.Sprintf("/v1/objects/%s", place2ID.String()),
		},
	}

	testhelper.AssertEventuallyEqual(t, expectedRefs, actualThunk)
}
