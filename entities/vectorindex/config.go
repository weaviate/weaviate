//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package vectorindex

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// ParseAndValidateConfig from an unknown input value, as this is not further
// specified in the API to allow of exchanging the index type
func ParseAndValidateConfig(input interface{}, vectorIndexType string) (schema.VectorIndexConfig, error) {
	if input == nil {
		return nil, nil
	}

	switch vectorIndexType {
	case "hnsw":
		return hnsw.ParseAndValidateConfig(input)
	case "flat":
		return flat.ParseAndValidateConfig(input)
	default:
		return nil, fmt.Errorf("Invalid vectorIndexType (­%s). Supported types are hnsw and flat", vectorIndexType)
	}
}
