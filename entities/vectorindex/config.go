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

package vectorindex

import (
	"encoding/json"
	"fmt"

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	hfresh "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const (
	DefaultVectorIndexType = VectorIndexTypeHNSW
	VectorIndexTypeHNSW    = "hnsw"
	VectorIndexTypeFLAT    = "flat"
	VectorIndexTypeDYNAMIC = "dynamic"
	VectorIndexTypeHFresh  = "hfresh"
	// VectorIndexTypeNone is a sentinel value, NOT a real index type.
	// It marks a named vector whose search index has been dropped via the
	// DeleteClassVectorIndex API. It must never be accepted as a user-supplied
	// index type in AddClass or UpdateClass requests.
	VectorIndexTypeNone = "none"
)

// ParseAndValidateConfig from an unknown input value, as this is not further
// specified in the API to allow of exchanging the index type
func ParseAndValidateConfig(input interface{}, vectorIndexType string, isMultiVector bool) (schemaConfig.VectorIndexConfig, error) {
	if len(vectorIndexType) == 0 {
		vectorIndexType = DefaultVectorIndexType
	}

	switch vectorIndexType {
	case VectorIndexTypeHNSW:
		return hnsw.ParseAndValidateConfig(input, isMultiVector)
	case VectorIndexTypeFLAT:
		return flat.ParseAndValidateConfig(input)
	case VectorIndexTypeDYNAMIC:
		return dynamic.ParseAndValidateConfig(input, isMultiVector)
	case VectorIndexTypeHFresh:
		return hfresh.ParseAndValidateConfig(input, isMultiVector)
	default:
		return nil, fmt.Errorf("invalid vector index %q. Supported types are hnsw and flat", vectorIndexType)
	}
}

// UnmarshalConfig decodes a config that was written by marshalling the parsed
// config of indexType, as a shard's vector index mapping stores it. It fills
// in no defaults and validates nothing: what was recorded is what comes back.
func UnmarshalConfig(indexType string, data []byte) (schemaConfig.VectorIndexConfig, error) {
	switch indexType {
	case VectorIndexTypeHNSW:
		var cfg hnsw.UserConfig
		return cfg, json.Unmarshal(data, &cfg)
	case VectorIndexTypeFLAT:
		var cfg flat.UserConfig
		return cfg, json.Unmarshal(data, &cfg)
	case VectorIndexTypeDYNAMIC:
		var cfg dynamic.UserConfig
		return cfg, json.Unmarshal(data, &cfg)
	case VectorIndexTypeHFresh:
		var cfg hfresh.UserConfig
		return cfg, json.Unmarshal(data, &cfg)
	default:
		return nil, fmt.Errorf("unknown vector index type %q", indexType)
	}
}
