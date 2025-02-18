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

package dynamic

import (
	"fmt"

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const (
	DefaultThreshold = 10_000
)

type UserConfig struct {
	Distance  string          `json:"distance"`
	Threshold uint64          `json:"threshold"`
	HnswUC    hnsw.UserConfig `json:"hnsw"`
	FlatUC    flat.UserConfig `json:"flat"`
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
	return "dynamic"
}

func (u UserConfig) DistanceName() string {
	return u.Distance
}

func (u UserConfig) IsMultiVector() bool {
	return false
}

// SetDefaults in the user-specifyable part of the config
func (u *UserConfig) SetDefaults() {
	u.Threshold = DefaultThreshold
	u.Distance = common.DefaultDistanceMetric
	u.HnswUC = hnsw.NewDefaultUserConfig()
	u.FlatUC = flat.NewDefaultUserConfig()
}

func NewDefaultUserConfig() UserConfig {
	uc := UserConfig{}
	uc.SetDefaults()
	return uc
}

// ParseAndValidateConfig from an unknown input value, as this is not further
// specified in the API to allow of exchanging the index type
func ParseAndValidateConfig(input interface{}, isMultiVector bool) (schemaConfig.VectorIndexConfig, error) {
	uc := UserConfig{}
	uc.SetDefaults()

	if input == nil {
		return uc, nil
	}

	asMap, ok := input.(map[string]interface{})
	if !ok || asMap == nil {
		return uc, fmt.Errorf("input must be a non-nil map")
	}

	if err := common.OptionalStringFromMap(asMap, "distance", func(v string) {
		uc.Distance = v
	}); err != nil {
		return uc, err
	}

	if err := common.OptionalIntFromMap(asMap, "threshold", func(v int) {
		uc.Threshold = uint64(v)
	}); err != nil {
		return uc, err
	}

	hnswConfig, ok := asMap["hnsw"]
	if ok && hnswConfig != nil {
		hnswUC, err := hnsw.ParseAndValidateConfig(hnswConfig, isMultiVector)
		if err != nil {
			return uc, err
		}

		castedHnswUC, ok := hnswUC.(hnsw.UserConfig)
		if !ok {
			return uc, fmt.Errorf("invalid hnsw configuration")
		}
		uc.HnswUC = castedHnswUC
		if uc.HnswUC.Multivector.Enabled {
			return uc, fmt.Errorf("multi vector index is not supported for dynamic index")
		}

	}

	flatConfig, ok := asMap["flat"]
	if !ok || flatConfig == nil {
		return uc, nil
	}

	flatUC, err := flat.ParseAndValidateConfig(flatConfig)
	if err != nil {
		return uc, err
	}

	castedFlatUC, ok := flatUC.(flat.UserConfig)
	if !ok {
		return uc, fmt.Errorf("invalid flat configuration")
	}
	uc.FlatUC = castedFlatUC

	return uc, nil
}
