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

package composer

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	"github.com/weaviate/weaviate/entities/vectorindex/flat"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const (
	DefaultThreshold = 10_000
)

type UserConfig struct {
	Distance   string          `json:"distance"`
	Threeshold uint64          `json:"threshold"`
	HnswUC     hnsw.UserConfig `json:"hnswuc"`
	FlatUC     flat.UserConfig `json:"flatuc"`
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
	return "composer"
}

func (u UserConfig) DistanceName() string {
	return u.Distance
}

// SetDefaults in the user-specifyable part of the config
func (u *UserConfig) SetDefaults() {
	u.Threeshold = DefaultThreshold
	u.Distance = common.DefaultDistanceMetric
	u.HnswUC = hnsw.NewDefaultUserConfig()
	u.FlatUC = flat.NewDefaultUserConfig()
}

// ParseAndValidateConfig from an unknown input value, as this is not further
// specified in the API to allow of exchanging the index type
func ParseAndValidateConfig(input interface{}) (schema.VectorIndexConfig, error) {
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

	if err := common.OptionalIntFromMap(asMap, "vectorCacheMaxObjects", func(v int) {
		uc.Threeshold = uint64(v)
	}); err != nil {
		return uc, err
	}

	hnswConfig, ok := asMap["hnswuc"]
	if !ok || hnswConfig == nil {
		return uc, nil
	}

	hnswUC, err := hnsw.ParseAndValidateConfig(hnswConfig)
	if err != nil {
		return uc, err
	}

	castedHnswUC, ok := hnswUC.(hnsw.UserConfig)
	if !ok {
		return uc, fmt.Errorf("invalid hnswUC configuration")
	}
	uc.HnswUC = castedHnswUC

	flatConfig, ok := asMap["flatuc"]
	if !ok || flatConfig == nil {
		return uc, nil
	}

	flatUC, err := flat.ParseAndValidateConfig(flatConfig)
	if err != nil {
		return uc, err
	}

	castedFlatUC, ok := flatUC.(flat.UserConfig)
	if !ok {
		return uc, fmt.Errorf("invalid flatUC configuration")
	}
	uc.FlatUC = castedFlatUC

	return uc, nil
}

func NewDefaultUserConfig() UserConfig {
	uc := UserConfig{}
	uc.SetDefaults()
	return uc
}
