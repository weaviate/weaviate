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

package modelsext

import "github.com/weaviate/weaviate/entities/models"

// DefaultNamedVectorName is a default vector named used to create a named vector or to allow access
// to legacy vector through named vector API.
const DefaultNamedVectorName = "default"

// ClassHasLegacyVectorIndex checks whether there is a legacy index configured on a class.
func ClassHasLegacyVectorIndex(class *models.Class) bool {
	return class.Vectorizer != "" || class.VectorIndexConfig != nil || class.VectorIndexType != ""
}

// ClassGetVectorConfig returns the vector config for a given class and target vector.
// There is a special case for the default vector name, which is used to access the legacy vector.
func ClassGetVectorConfig(class *models.Class, targetVector string) (models.VectorConfig, bool) {
	if cfg, ok := class.VectorConfig[targetVector]; ok {
		return cfg, ok
	}

	if (ClassHasLegacyVectorIndex(class) && targetVector == DefaultNamedVectorName) || targetVector == "" {
		return models.VectorConfig{
			VectorIndexConfig: class.VectorIndexConfig,
			VectorIndexType:   class.VectorIndexType,
			Vectorizer:        class.Vectorizer,
		}, true
	}

	return models.VectorConfig{}, false
}
