package test

import (
	"fmt"
	"testing"

	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/client/things"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

// This test suite is meant to prevent a regression on
// https://github.com/semi-technologies/weaviate/issues/868, hence it tries to
// reprodcue the steps outlined in there as closely as possible

func Test_CREFWithCardinalityMany(t *testing.T) {
	defer func() {
		// clean up so we can run this test multiple times in a row
		delCityParams := schema.NewWeaviateSchemaThingsDeleteParams().WithClassName("ReferenceTestCity")
		dresp, err := helper.Client(t).Schema.WeaviateSchemaThingsDelete(delCityParams, nil)
		t.Logf("clean up - delete city \n%v\n %v", dresp, err)

		delPlaceParams := schema.NewWeaviateSchemaThingsDeleteParams().WithClassName("ReferenceTestPlace")
		dresp, err = helper.Client(t).Schema.WeaviateSchemaThingsDelete(delPlaceParams, nil)
		t.Logf("clean up - delete place \n%v\n %v", dresp, err)
	}()

	t.Log("1. create ReferenceTestPlace class")
	placeClass := &models.SemanticSchemaClass{
		Class: "ReferenceTestPlace",
		Properties: []*models.SemanticSchemaClassProperty{
			&models.SemanticSchemaClassProperty{
				DataType: []string{"string"},
				Name:     "name",
			},
		},
	}
	params := schema.NewWeaviateSchemaThingsCreateParams().WithThingClass(placeClass)
	resp, err := helper.Client(t).Schema.WeaviateSchemaThingsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("2. create ReferenceTestCity class with HasPlaces (many) cross-ref")
	cardinalityMany := "many"
	cityClass := &models.SemanticSchemaClass{
		Class: "ReferenceTestCity",
		Properties: []*models.SemanticSchemaClassProperty{
			&models.SemanticSchemaClassProperty{
				DataType: []string{"string"},
				Name:     "name",
			},
			&models.SemanticSchemaClassProperty{
				DataType:    []string{"ReferenceTestPlace"},
				Name:        "HasPlaces",
				Cardinality: &cardinalityMany,
			},
		},
	}
	params = schema.NewWeaviateSchemaThingsCreateParams().WithThingClass(cityClass)
	resp, err = helper.Client(t).Schema.WeaviateSchemaThingsCreate(params, nil)
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
	add := "add"
	path := "/schema/hasPlaces"
	patchParams := things.NewWeaviateThingsPatchParams().
		WithID(cityID).
		WithBody([]*models.PatchDocument{
			&models.PatchDocument{
				Op:   &add,
				Path: &path,
				Value: []interface{}{
					map[string]interface{}{
						"$cref": fmt.Sprintf("weaviate://localhost/things/%s", place1ID.String()),
					},
				},
			},
		})
	patchResp, err := helper.Client(t).Things.WeaviateThingsPatch(patchParams, nil)
	helper.AssertRequestOk(t, patchResp, err, nil)

	t.Log("5. get city again")
	cityAfterFirstPatch := assertGetThing(t, cityID)

	t.Log("6. verify first cross ref was added")
	assert.Equal(t, map[string]interface{}{
		"name": "My City",
		"hasPlaces": []interface{}{
			map[string]interface{}{
				"$cref": fmt.Sprintf("weaviate://localhost/things/%s", place1ID.String()),
			},
		},
	}, cityAfterFirstPatch.Schema)

	t.Log("7. patch city to point to the second place")
	add = "add"
	path = "/schema/hasPlaces/-"
	patchParams = things.NewWeaviateThingsPatchParams().
		WithID(cityID).
		WithBody([]*models.PatchDocument{
			&models.PatchDocument{
				Op:   &add,
				Path: &path,
				Value: map[string]interface{}{
					"$cref": fmt.Sprintf("weaviate://localhost/things/%s", place2ID.String()),
				},
			},
		})
	patchResp, err = helper.Client(t).Things.WeaviateThingsPatch(patchParams, nil)
	helper.AssertRequestOk(t, patchResp, err, nil)

	t.Log("8. get city again")
	cityAfterSecondPatch := assertGetThing(t, cityID)

	t.Log("9. verify both cross refs are present")
	refs := cityAfterSecondPatch.Schema.(map[string]interface{})["hasPlaces"].([]interface{})
	expectedRefs := []interface{}{
		map[string]interface{}{
			"$cref": fmt.Sprintf("weaviate://localhost/things/%s", place1ID.String()),
		},
		map[string]interface{}{
			"$cref": fmt.Sprintf("weaviate://localhost/things/%s", place2ID.String()),
		},
	}

	assert.ElementsMatch(t, expectedRefs, refs)
}
