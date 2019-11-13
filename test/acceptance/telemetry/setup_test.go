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

	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
)

func Test_Actions(t *testing.T) {
	t.Run("setup", func(t *testing.T) {
		createActionClass(t, &models.Class{
			Class: "MonitoringTestAction",
			Properties: []*models.Property{
				&models.Property{
					Name:     "testString",
					DataType: []string{"string"},
				},
				&models.Property{
					Name:     "testWholeNumber",
					DataType: []string{"int"},
				},
				&models.Property{
					Name:     "testNumber",
					DataType: []string{"number"},
				},
				&models.Property{
					Name:     "testDateTime",
					DataType: []string{"date"},
				},
				&models.Property{
					Name:     "testTrueFalse",
					DataType: []string{"boolean"},
				},
			},
		})
	})

	// tests
	t.Run("create action logging", createActionLogging)

	// tear down
	deleteActionClass(t, "MonitoringTestAction")
}

func createActionClass(t *testing.T, class *models.Class) {
	params := schema.NewSchemaActionsCreateParams().WithActionClass(class)
	resp, err := helper.Client(t).Schema.SchemaActionsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func deleteActionClass(t *testing.T, class string) {
	delParams := schema.NewSchemaActionsDeleteParams().WithClassName(class)
	delRes, err := helper.Client(t).Schema.SchemaActionsDelete(delParams, nil)
	helper.AssertRequestOk(t, delRes, err, nil)
}
