/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package schema

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/stretchr/testify/assert"
)

func Test_GetAllPropsOfType(t *testing.T) {

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

	schema := Empty()
	schema.Things.Classes = []*models.Class{car, train}
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
}
