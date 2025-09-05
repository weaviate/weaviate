//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package vectorindex

import (
	"errors"
	"fmt"
	"os"

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/vectorindex/spfresh"
)

const (
	DefaultVectorIndexType = VectorIndexTypeHNSW
	VectorIndexTypeHNSW    = "hnsw"
	VectorIndexTypeFLAT    = "flat"
	VectorIndexTypeDYNAMIC = "dynamic"
	VectorIndexTypeSPFRESH = "spfresh"
)

// ParseAndValidateConfig from an unknown input value, as this is not further
// specified in the API to allow of exchanging the index type
func ParseAndValidateConfig(input interface{}, vectorIndexType string, isMultiVector bool) (schemaConfig.VectorIndexConfig, error) {
	if len(vectorIndexType) == 0 {
		vectorIndexType = DefaultVectorIndexType
	}

	if os.Getenv("SPFRESH_ENABLED") == "true" && vectorIndexType == VectorIndexTypeSPFRESH {
		return nil, errors.New("spfresh is not enabled via SPFRESH_ENABLED=true")
	}

	switch vectorIndexType {
	case VectorIndexTypeHNSW:
		return hnsw.ParseAndValidateConfig(input, isMultiVector)
	case VectorIndexTypeFLAT:
		return flat.ParseAndValidateConfig(input)
	case VectorIndexTypeDYNAMIC:
		return dynamic.ParseAndValidateConfig(input, isMultiVector)
	case VectorIndexTypeSPFRESH:
		return spfresh.ParseAndValidateConfig(input)
	default:
		return nil, fmt.Errorf("invalid vector index %q. Supported types are hnsw and flat", vectorIndexType)
	}
}
