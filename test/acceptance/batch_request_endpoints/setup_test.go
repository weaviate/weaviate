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

package batch_request_endpoints

import (
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/helper"
)

func Test_Batch(t *testing.T) {
	// there is no gql provider if there is no schema, so we need some sort of a schema

	t.Run("setup", func(t *testing.T) {
		createObjectClass(t, &models.Class{
			Class: "BulkTest",
			Properties: []*models.Property{
				{
					Name:     "name",
					DataType: []string{"string"},
				},
			},
		})
		createObjectClass(t, &models.Class{
			Class: "BulkTestSource",
			Properties: []*models.Property{
				{
					Name:     "name",
					DataType: []string{"string"},
				},
				{
					Name:     "ref",
					DataType: []string{"BulkTest"},
				},
			},
		})
		createObjectClass(t, &models.Class{
			Class: "BulkTestTarget",
			Properties: []*models.Property{
				{
					Name:     "intProp",
					DataType: []string{"int"},
				},
				{
					Name:     "fromSource",
					DataType: []string{"BulkTestSource"},
				},
			},
		})
	})

	time.Sleep(2000 * time.Millisecond)

	t.Run("gql results order", batchJourney)
	t.Run("gql results order", gqlResultsOrder)
	t.Run("batch delete", batchDeleteJourney)

	deleteObjectClass(t, "BulkTest")
	deleteObjectClass(t, "BulkTestSource")
	deleteObjectClass(t, "BulkTestTarget")
}

func createObjectClass(t *testing.T, class *models.Class) {
	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func deleteObjectClass(t *testing.T, class string) {
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(class)
	delRes, err := helper.Client(t).Schema.SchemaObjectsDelete(delParams, nil)
	helper.AssertRequestOk(t, delRes, err, nil)
}
