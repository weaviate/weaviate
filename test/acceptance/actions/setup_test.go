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

package test

import (
	"testing"

	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
)

func Test_Objects(t *testing.T) {
	t.Run("setup", func(t *testing.T) {
		assertCreateObjectClass(t, &models.Class{
			Class: "ObjectTestThing",
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Properties: []*models.Property{
				{
					Name:     "testString",
					DataType: []string{"string"},
				},
			},
		})
		assertCreateObjectClass(t, &models.Class{
			Class: "TestObject",
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Properties: []*models.Property{
				{
					Name:     "testString",
					DataType: []string{"string"},
				},
				{
					Name:     "testWholeNumber",
					DataType: []string{"int"},
				},
				{
					Name:     "testNumber",
					DataType: []string{"number"},
				},
				{
					Name:     "testDateTime",
					DataType: []string{"date"},
				},
				{
					Name:     "testTrueFalse",
					DataType: []string{"boolean"},
				},
				{
					Name:     "testReference",
					DataType: []string{"ObjectTestThing"},
				},
			},
		})
		assertCreateObjectClass(t, &models.Class{
			Class: "TestObjectTwo",
			ModuleConfig: map[string]interface{}{
				"text2vec-contextionary": map[string]interface{}{
					"vectorizeClassName": true,
				},
			},
			Properties: []*models.Property{
				{
					Name:     "testReference",
					DataType: []string{"TestObject"},
				},
				{
					Name:     "testReferences",
					DataType: []string{"TestObject"},
				},
				{
					Name:     "testString",
					DataType: []string{"string"},
				},
			},
		})
	})

	// tests
	t.Run("adding objects", addingObjects)
	t.Run("removing objects", removingObjects)
	t.Run("object references", objectReferences)
	t.Run("updating objects deprecated", updateObjectsDeprecated)
	t.Run("updating object", updateObjects)
	t.Run("patch object", patchObjects)
	t.Run("head object", headObject)

	// tear down
	deleteObjectClass(t, "ObjectTestThing")
	deleteObjectClass(t, "TestObject")
	deleteObjectClass(t, "TestObjectTwo")
}

func assertCreateObjectClass(t *testing.T, class *models.Class) {
	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func deleteObjectClass(t *testing.T, class string) {
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(class)
	delRes, err := helper.Client(t).Schema.SchemaObjectsDelete(delParams, nil)
	helper.AssertRequestOk(t, delRes, err, nil)
}

func deleteClassObject(t *testing.T, class string) (*schema.SchemaObjectsDeleteOK, error) {
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(class)
	return helper.Client(t).Schema.SchemaObjectsDelete(delParams, nil)
}
