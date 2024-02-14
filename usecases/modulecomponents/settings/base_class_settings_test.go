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

package settings

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/modules"
)

func Test_BaseClassSettings(t *testing.T) {
	targetVector := "targetVector"
	propertyToIndex := "someProp"
	class := &models.Class{
		Class: "MyClass",
		VectorConfig: map[string]models.VectorConfig{
			targetVector: {
				Vectorizer: map[string]interface{}{
					"my-module": map[string]interface{}{
						"vectorizeClassName": false,
						"properties":         []interface{}{propertyToIndex},
					},
				},
				VectorIndexType: "hnsw",
			},
		},
		Properties: []*models.Property{
			{
				Name: propertyToIndex,
				ModuleConfig: map[string]interface{}{
					"my-module": map[string]interface{}{
						"skip":                  true,
						"vectorizePropertyName": true,
					},
				},
			},
			{
				Name: "otherProp",
			},
		},
	}

	cfg := modules.NewClassBasedModuleConfig(class, "my-module", "tenant", targetVector)
	ic := NewBaseClassSettings(cfg)

	assert.True(t, ic.PropertyIndexed(propertyToIndex))
	assert.True(t, ic.VectorizePropertyName(propertyToIndex))
	assert.False(t, ic.PropertyIndexed("otherProp"))
	assert.False(t, ic.VectorizePropertyName("otherProp"))
	assert.False(t, ic.VectorizeClassName())
}
