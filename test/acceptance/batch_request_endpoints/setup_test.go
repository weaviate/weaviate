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

package batch_request_endpoints

import (
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
)

func Test_Batch(t *testing.T) {
	// there is no gql provider if there is no schema, so we need some sort of a schema

	t.Run("setup", func(t *testing.T) {
		createThingClass(t, &models.Class{
			Class: "BulkTest",
			Properties: []*models.Property{
				&models.Property{
					Name:     "name",
					DataType: []string{"string"},
				},
			},
		})
	})

	time.Sleep(2000 * time.Millisecond)

	t.Run("gql results order", gqlResultsOrder)

	deleteThingClass(t, "BulkTest")
}

func createThingClass(t *testing.T, class *models.Class) {
	params := schema.NewSchemaThingsCreateParams().WithThingClass(class)
	resp, err := helper.Client(t).Schema.SchemaThingsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func deleteThingClass(t *testing.T, class string) {
	delParams := schema.NewSchemaThingsDeleteParams().WithClassName(class)
	delRes, err := helper.Client(t).Schema.SchemaThingsDelete(delParams, nil)
	helper.AssertRequestOk(t, delRes, err, nil)
}
