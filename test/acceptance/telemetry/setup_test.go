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

// TODO gh-1232: remove
// import (
// 	"testing"

// 	"github.com/weaviate/weaviate/client/schema"
// 	"github.com/weaviate/weaviate/entities/models"
// 	"github.com/weaviate/weaviate/test/helper"
// )

// func Test_Actions(t *testing.T) {
// 	t.Run("setup", func(t *testing.T) {
// 		createActionClass(t, &models.Class{
// 			Class: "MonitoringTestAction",
// 			Properties: []*models.Property{
// 				&models.Property{
// 					Name:     "testString",
// DataType:     schema.DataTypeText.PropString(),
// Tokenization: models.PropertyTokenizationWhitespace,
// 				},
// 				&models.Property{
// 					Name:     "testWholeNumber",
// 					DataType: []string{"int"},
// 				},
// 				&models.Property{
// 					Name:     "testNumber",
// 					DataType: []string{"number"},
// 				},
// 				&models.Property{
// 					Name:     "testDateTime",
// 					DataType: []string{"date"},
// 				},
// 				&models.Property{
// 					Name:     "testTrueFalse",
// 					DataType: []string{"boolean"},
// 				},
// 			},
// 		})
// 	})

// 	// tests
// 	t.Run("create action logging", createActionLogging)

// 	// tear down
// 	deleteActionClass(t, "MonitoringTestAction")
// }

// func createActionClass(t *testing.T, class *models.Class) {
// 	params := schema.NewSchemaActionsCreateParams().WithActionClass(class)
// 	resp, err := helper.Client(t).Schema.SchemaActionsCreate(params, nil)
// 	helper.AssertRequestOk(t, resp, err, nil)
// }

// func deleteActionClass(t *testing.T, class string) {
// 	delParams := schema.NewSchemaActionsDeleteParams().WithClassName(class)
// 	delRes, err := helper.Client(t).Schema.SchemaActionsDelete(delParams, nil)
// 	helper.AssertRequestOk(t, delRes, err, nil)
// }
