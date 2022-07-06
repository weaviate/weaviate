package test

import (
	"testing"

	"github.com/semi-technologies/weaviate/entities/models"
)

func addTestSchemaCosine(t *testing.T) {
	createObjectClass(t, &models.Class{
		Class:      "Cosine_Class",
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: []string{"string"},
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
				Name:     "name",
				DataType: []string{"string"},
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
				Name:     "name",
				DataType: []string{"string"},
			},
		},
		VectorIndexConfig: map[string]interface{}{
			"distance": "l2-squared",
		},
	})
}
