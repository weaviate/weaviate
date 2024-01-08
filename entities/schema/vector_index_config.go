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

	"github.com/weaviate/weaviate/entities/models"
)

type VectorIndexConfig interface {
	IndexType() string
	DistanceName() string
}

func TypeAssertVectorIndex(class *models.Class) (VectorIndexConfig, error) {
	if config, ok := class.VectorIndexConfig.(VectorIndexConfig); ok {
		return config, nil
	}

	return nil, fmt.Errorf("class '%s' vector index: config is not schema.VectorIndexConfig: %T",
		class.Class, class.VectorIndexConfig)
}
