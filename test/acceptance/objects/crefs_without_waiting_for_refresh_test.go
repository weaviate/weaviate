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
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	testhelper "github.com/weaviate/weaviate/test/helper"
)

// See https://github.com/weaviate/weaviate/issues/980
func Test_AddingReferenceWithoutWaiting_UsingPostObjects(t *testing.T) {
	defer func() {
		// clean up so we can run this test multiple times in a row
		delCityParams := clschema.NewSchemaObjectsDeleteParams().WithClassName("ReferenceWaitingTestCity")
		dresp, err := helper.Client(t).Schema.SchemaObjectsDelete(delCityParams, nil)
		t.Logf("clean up - delete city \n%v\n %v", dresp, err)

		delPlaceParams := clschema.NewSchemaObjectsDeleteParams().WithClassName("ReferenceWaitingTestPlace")
		dresp, err = helper.Client(t).Schema.SchemaObjectsDelete(delPlaceParams, nil)
		t.Logf("clean up - delete place \n%v\n %v", dresp, err)
	}()

	t.Log("1. create ReferenceTestPlace class")
	placeClass := &models.Class{
		Class: "ReferenceWaitingTestPlace",
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

	t.Log("2. create ReferenceTestCity class with HasPlace cross-ref")
	cityClass := &models.Class{
		Class: "ReferenceWaitingTestCity",
		Properties: []*models.Property{
			{
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				Name:         "name",
			},
			{
				DataType: []string{"ReferenceWaitingTestPlace"},
				Name:     "HasPlace",
			},
		},
	}
	params = clschema.NewSchemaObjectsCreateParams().WithObjectClass(cityClass)
	resp, err = helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)

	t.Log("3. add a places and save the ID")
	placeID := assertCreateObject(t, "ReferenceWaitingTestPlace", map[string]interface{}{
		"name": "Place 1",
	})

	t.Log("4. add one city with ref to the place")
	cityID := assertCreateObject(t, "ReferenceWaitingTestCity", map[string]interface{}{
		"name": "My City",
		"hasPlace": models.MultipleRef{
			&models.SingleRef{
				Beacon: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", placeID.String())),
			},
		},
	})

	assertGetObjectEventually(t, cityID)

	actualThunk := func() interface{} {
		city := assertGetObject(t, cityID)
		return city.Properties
	}
	t.Log("7. verify first cross ref was added")
	testhelper.AssertEventuallyEqual(t, map[string]interface{}{
		"name": "My City",
		"hasPlace": []interface{}{
			map[string]interface{}{
				"beacon": fmt.Sprintf("weaviate://localhost/%s/%s", "ReferenceWaitingTestPlace", placeID.String()),
				"href":   fmt.Sprintf("/v1/objects/%s/%s", "ReferenceWaitingTestPlace", placeID.String()),
			},
		},
	}, actualThunk)
}
