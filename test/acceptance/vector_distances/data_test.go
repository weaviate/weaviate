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

import (
	"testing"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func addTestSchemaCosine(t *testing.T) {
	createObjectClass(t, &models.Class{
		Class:      "Cosine_Class",
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
		VectorIndexConfig: map[string]interface{}{
			"distance": "cosine",
		},
	})
}

func addTestSchemaOther(t *testing.T) {
	createObjectClass(t, &models.Class{
		Class:      "Dot_Class",
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
		VectorIndexConfig: map[string]interface{}{
			"distance": "dot",
		},
	})

	createObjectClass(t, &models.Class{
		Class:      "L2Squared_Class",
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
		VectorIndexConfig: map[string]interface{}{
			"distance": "l2-squared",
		},
	})

	createObjectClass(t, &models.Class{
		Class:      "Manhattan_Class",
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
		VectorIndexConfig: map[string]interface{}{
			"distance": "manhattan",
		},
	})

	createObjectClass(t, &models.Class{
		Class:      "Hamming_Class",
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
		VectorIndexConfig: map[string]interface{}{
			"distance": "hamming",
		},
	})
}
