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
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	testhelper "github.com/semi-technologies/weaviate/test/helper"
)

// See https://github.com/semi-technologies/weaviate/issues/980
func Test_AddingReferenceWithoutWaiting_UsingPostThings(t *testing.T) {
	defer func() {
		// clean up so we can run this test multiple times in a row
		delCityParams := schema.NewSchemaThingsDeleteParams().WithClassName("ReferenceWaitingTestCity")
		dresp, err := helper.Client(t).Schema.SchemaThingsDelete(delCityParams, nil)
		t.Logf("clean up - delete city \n%v\n %v", dresp, err)

		delPlaceParams := schema.NewSchemaThingsDeleteParams().WithClassName("ReferenceWaitingTestPlace")
		dresp, err = helper.Client(t).Schema.SchemaThingsDelete(delPlaceParams, nil)
		t.Logf("clean up - delete place \n%v\n %v", dresp, err)
	}()

	t.Log("1. create ReferenceTestPlace class")
	placeClass := &models.Class{
		Class: "ReferenceWaitingTestPlace",
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

	t.Log("2. create ReferenceTestCity class with HasPlace cross-ref")
	cityClass := &models.Class{
		Class: "ReferenceWaitingTestCity",
		Properties: []*models.Property{
			&models.Property{
				DataType: []string{"string"},
				Name:     "name",
			},
			&models.Property{
				DataType: []string{"ReferenceWaitingTestPlace"},
				Name:     "HasPlace",
			},
		},
	}
	params = schema.NewSchemaThingsCreateParams().WithThingClass(cityClass)
	resp, err = helper.Client(t).Schema.SchemaThingsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("3. add a places and save the ID")
	placeID := assertCreateThing(t, "ReferenceWaitingTestPlace", map[string]interface{}{
		"name": "Place 1",
	})

	t.Log("4. add one city with ref to the place")
	cityID := assertCreateThing(t, "ReferenceWaitingTestCity", map[string]interface{}{
		"name": "My City",
		"hasPlace": models.MultipleRef{
			&models.SingleRef{
				Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/things/%s", placeID.String())),
			},
		},
	})

	assertGetThingEventually(t, cityID)

	actualThunk := func() interface{} {
		city := assertGetThing(t, cityID)
		return city.Schema
	}
	t.Log("7. verify first cross ref was added")
	testhelper.AssertEventuallyEqual(t, map[string]interface{}{
		"name": "My City",
		"hasPlace": []interface{}{
			map[string]interface{}{
				"beacon": fmt.Sprintf("weaviate://localhost/things/%s", placeID.String()),
				"href":   fmt.Sprintf("/v1/things/%s", placeID.String()),
			},
		},
	}, actualThunk)
}
