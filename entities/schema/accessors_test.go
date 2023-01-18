//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)

func Test_Accessors(t *testing.T) {
	car := &models.Class{
		Class: "Car",
		Properties: []*models.Property{
			{Name: "modelName", DataType: []string{"string"}},
			{Name: "manufacturerName", DataType: []string{"string"}},
			{Name: "horsepower", DataType: []string{"int"}},
		},
	}

	train := &models.Class{
		Class: "Train",
		Properties: []*models.Property{
			{Name: "capacity", DataType: []string{"int"}},
			{Name: "trainCompany", DataType: []string{"string"}},
		},
	}

	action := &models.Class{
		Class:      "SomeAction",
		Properties: []*models.Property{},
	}

	schema := Empty()
	schema.Objects.Classes = []*models.Class{car, train, action}

	t.Run("GetClass by kind and name", func(t *testing.T) {
		class := schema.GetClass("Car")
		assert.Equal(t, car, class)

		class = schema.GetClass("Invalid")
		assert.Equal(t, (*models.Class)(nil), class)
	})

	t.Run("FindClass by name (without providing the kind)", func(t *testing.T) {
		class := schema.FindClassByName("Car")
		assert.Equal(t, car, class)

		class = schema.FindClassByName("SomeAction")
		assert.Equal(t, action, class)

		class = schema.FindClassByName("Invalid")
		assert.Equal(t, (*models.Class)(nil), class)
	})

	t.Run("GetPropsOfType", func(t *testing.T) {
		props := schema.GetPropsOfType("string")

		expectedProps := []ClassAndProperty{
			{
				ClassName:    "Car",
				PropertyName: "modelName",
			},
			{
				ClassName:    "Car",
				PropertyName: "manufacturerName",
			},
			{
				ClassName:    "Train",
				PropertyName: "trainCompany",
			},
		}

		assert.ElementsMatch(t, expectedProps, props)
	})

	t.Run("GetProperty by kind, classname, name", func(t *testing.T) {
		prop, err := schema.GetProperty("Car", "modelName")
		assert.Nil(t, err)

		expectedProp := &models.Property{
			Name:     "modelName",
			DataType: []string{"string"},
		}

		assert.Equal(t, expectedProp, prop)
	})

	t.Run("GetProperty for invalid class", func(t *testing.T) {
		_, err := schema.GetProperty("WrongClass", "modelName")
		assert.Equal(t, errors.New("no such class with name 'WrongClass' found in the schema. Check your schema files for which classes are available"), err)
	})

	t.Run("GetProperty for invalid prop", func(t *testing.T) {
		_, err := schema.GetProperty("Car", "wrongProperty")
		assert.Equal(t, errors.New("no such prop with name 'wrongProperty' found in class 'Car' in the schema. Check your schema files for which properties in this class are available"), err)
	})
}
