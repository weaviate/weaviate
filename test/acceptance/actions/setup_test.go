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

	"github.com/semi-technologies/weaviate/entities/models"
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

	// tear down
	assertDeleteObjectClass(t, "ObjectTestThing")
	assertDeleteObjectClass(t, "TestObject")
	assertDeleteObjectClass(t, "TestObjectTwo")
}
