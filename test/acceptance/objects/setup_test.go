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
	// t.Run("setup", func(t *testing.T) {
	createObjectClass(t, &models.Class{
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
				Name:     "testPhoneNumber",
				DataType: []string{"phoneNumber"},
			},
		},
	})
	createObjectClass(t, &models.Class{
		Class:      "TestObjectCustomVector",
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:     "description",
				DataType: []string{"text"},
			},
		},
	})
	createObjectClass(t, &models.Class{
		Class:      "TestDeleteClassOne",
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:     "text",
				DataType: []string{"text"},
			},
		},
	})
	createObjectClass(t, &models.Class{
		Class:      "TestDeleteClassTwo",
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:     "text",
				DataType: []string{"text"},
			},
		},
	})
	//	})

	// tests
	t.Run("listing objects", listingObjects)
	t.Run("searching for neighbors", searchNeighbors)
	t.Run("running a feature projection", featureProjection)
	t.Run("creating objects", creatingObjects)

	t.Run("custom vector journey", customVectors)
	t.Run("auto schema", autoSchemaObjects)
	t.Run("checking object's existence", checkObjects)
	t.Run("delete request deletes all objects with a given ID", deleteAllObjectsFromAllClasses)
	t.Run("delete one class object", deleteClassObject)

	// tear down
	deleteObjectClass(t, "TestObject")
	deleteObjectClass(t, "TestObjectCustomVector")
	deleteObjectClass(t, "NonExistingClass")
	deleteObjectClass(t, "TestDeleteClassOne")
	deleteObjectClass(t, "TestDeleteClassTwo")
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
