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

package fixtures

import (
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

var (
	BringYourOwnColBERTClassName       = "BringYourOwnColBERT"
	BringYourOwnColBERTPropertyName    = "name"
	BringYourOwnColBERTNamedVectorName = "byoc"
)

var BringYourOwnColBERTClass = func(className string) *models.Class {
	return &models.Class{
		Class: BringYourOwnColBERTClassName,
		Properties: []*models.Property{
			{
				Name: BringYourOwnColBERTPropertyName, DataType: []string{schema.DataTypeText.String()},
			},
		},
		VectorConfig: map[string]models.VectorConfig{
			BringYourOwnColBERTNamedVectorName: {
				Vectorizer: map[string]interface{}{
					"none": map[string]interface{}{},
				},
				VectorIndexConfig: map[string]interface{}{
					"multivector": map[string]interface{}{
						"enabled": true,
					},
				},
				VectorIndexType: "hnsw",
			},
		},
	}
}

var BringYourOwnColBERTObjects = []struct {
	ID     string
	Name   string
	Vector [][]float32
}{
	{
		ID: "00000000-0000-0000-0000-000000000001", Name: "a", Vector: [][]float32{{0.11, 0.12}, {0.13, 0.14}, {0.15, 0.16}},
	},
	{
		ID: "00000000-0000-0000-0000-000000000002", Name: "b", Vector: [][]float32{{0.21, 0.22}, {0.23, 0.24}, {0.25, 0.26}},
	},
	{
		ID: "00000000-0000-0000-0000-000000000003", Name: "c", Vector: [][]float32{{0.31, 0.32}, {0.33, 0.34}, {0.35, 0.36}},
	},
	{
		ID: "00000000-0000-0000-0000-000000000004", Name: "d", Vector: [][]float32{{0.41, 0.42}, {0.43, 0.44}, {0.45, 0.46}},
	},
	{
		ID: "00000000-0000-0000-0000-000000000005", Name: "e", Vector: [][]float32{{0.51, 0.52}, {0.53, 0.54}, {0.55, 0.56}},
	},
	{
		ID: "00000000-0000-0000-0000-000000000006", Name: "f", Vector: [][]float32{{0.61, 0.62}, {0.63, 0.64}, {0.65, 0.66}},
	},
	{
		ID: "00000000-0000-0000-0000-000000000007", Name: "g", Vector: [][]float32{{0.71, 0.72}, {0.73, 0.74}, {0.75, 0.76}},
	},
	{
		ID: "00000000-0000-0000-0000-000000000008", Name: "h", Vector: [][]float32{{0.81, 0.82}, {0.83, 0.84}, {0.85, 0.86}},
	},
	{
		ID: "00000000-0000-0000-0000-000000000009", Name: "h", Vector: [][]float32{{0.91, 0.92}, {0.93, 0.94}, {0.95, 0.96}},
	},
}
