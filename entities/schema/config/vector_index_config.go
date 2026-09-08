//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
)

type VectorIndexConfig interface {
	IndexType() string
	DistanceName() string
	IsMultiVector() bool
}

// ExtractVectorConfigs returns every vector config of the class keyed by target vector,
// with a legacy class-level vector index under the empty key. A class can carry both —
// named vectors added to a class created with a legacy vector — so the two sources are
// merged rather than treated as mutually exclusive.
func ExtractVectorConfigs(class *models.Class) (map[string]models.VectorConfig, error) {
	if !modelsext.ClassHasLegacyVectorIndex(class) {
		return class.VectorConfig, nil
	}

	vectorIndexConfig, ok := class.VectorIndexConfig.(VectorIndexConfig)
	if !ok {
		return nil, fmt.Errorf("class '%s' vector index: config is not schema.VectorIndexConfig: %T",
			class.Class, class.VectorIndexConfig)
	}

	configs := make(map[string]models.VectorConfig, len(class.VectorConfig)+1)
	for targetVector, cfg := range class.VectorConfig {
		configs[targetVector] = cfg
	}
	configs[""] = models.VectorConfig{Vectorizer: class.Vectorizer, VectorIndexConfig: vectorIndexConfig, VectorIndexType: class.VectorIndexType}
	return configs, nil
}

func TypeAssertVectorIndex(class *models.Class, targetVectors []string) ([]VectorIndexConfig, error) {
	if len(class.VectorConfig) == 0 || (modelsext.ClassHasLegacyVectorIndex(class) && len(targetVectors) == 0) {
		vectorIndexConfig, ok := class.VectorIndexConfig.(VectorIndexConfig)
		if !ok {
			return nil, fmt.Errorf("class '%s' vector index: config is not schema.VectorIndexConfig: %T",
				class.Class, class.VectorIndexConfig)
		}
		return []VectorIndexConfig{vectorIndexConfig}, nil
	}

	if len(class.VectorConfig) == 1 {
		var vectorConfig models.VectorConfig
		for _, v := range class.VectorConfig {
			vectorConfig = v
			break
		}
		vectorIndexConfig, ok := vectorConfig.VectorIndexConfig.(VectorIndexConfig)
		if !ok {
			return nil, fmt.Errorf("class '%s' vector index: config is not schema.VectorIndexConfig: %T",
				class.Class, class.VectorIndexConfig)
		}
		return []VectorIndexConfig{vectorIndexConfig}, nil
	}

	if len(targetVectors) == 0 {
		return nil, errors.Errorf("multiple vector configs found for class '%s', but no target vector specified", class.Class)
	}

	configs := make([]VectorIndexConfig, 0, len(targetVectors))
	for _, targetVector := range targetVectors {
		vectorConfig, ok := modelsext.ClassGetVectorConfig(class, targetVector)
		if !ok {
			return nil, errors.Errorf("vector config not found for target vector: %s", targetVector)
		}
		vectorIndexConfig, ok := vectorConfig.VectorIndexConfig.(VectorIndexConfig)
		if !ok {
			return nil, fmt.Errorf("targetVector '%s' vector index: config is not schema.VectorIndexConfig: %T",
				targetVector, class.VectorIndexConfig)
		}
		configs = append(configs, vectorIndexConfig)
	}
	return configs, nil
}
