/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package schema

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/stretchr/testify/assert"
)

func Test_GetAllPropsOfType(t *testing.T) {

	car := &models.SemanticSchemaClass{
		Class: "Car",
		Properties: []*models.SemanticSchemaClassProperty{
			{Name: "modelName", AtDataType: []string{"string"}},
			{Name: "manufacturerName", AtDataType: []string{"string"}},
			{Name: "horsepower", AtDataType: []string{"int"}},
		},
	}

	train := &models.SemanticSchemaClass{
		Class: "Train",
		Properties: []*models.SemanticSchemaClassProperty{
			{Name: "capacity", AtDataType: []string{"int"}},
			{Name: "trainCompany", AtDataType: []string{"string"}},
		},
	}

	schema := Empty()
	schema.Things.Classes = []*models.SemanticSchemaClass{car, train}
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
