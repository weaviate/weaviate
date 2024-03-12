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

package schema

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
)

type VectorIndexConfig interface {
	IndexType() string
	DistanceName() string
}

func TypeAssertVectorIndex(class *models.Class, targetVectors []string) (VectorIndexConfig, error) {
	if len(class.VectorConfig) == 0 {
		vectorIndexConfig, ok := class.VectorIndexConfig.(VectorIndexConfig)
		if !ok {
			return nil, fmt.Errorf("class '%s' vector index: config is not schema.VectorIndexConfig: %T",
				class.Class, class.VectorIndexConfig)
		}
		return vectorIndexConfig, nil
	} else if len(class.VectorConfig) == 1 {
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
		return vectorIndexConfig, nil
	} else {
		if len(targetVectors) != 1 {
			return nil, errors.Errorf("multiple vector configs found for class '%s', but no target vector specified", class.Class)
		}
		vectorConfig, ok := class.VectorConfig[targetVectors[0]]
		if !ok {
			return nil, errors.Errorf("vector config not found for target vector: %s", targetVectors[0])
		}
		vectorIndexConfig, ok := vectorConfig.VectorIndexConfig.(VectorIndexConfig)
		if !ok {
			return nil, fmt.Errorf("targetVector '%s' vector index: config is not schema.VectorIndexConfig: %T",
				targetVectors[0], class.VectorIndexConfig)
		}
		return vectorIndexConfig, nil
	}
}
