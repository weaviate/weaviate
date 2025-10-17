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

package spfresh

import (
	"fmt"

	"fmt"

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	vectorIndexCommon "github.com/weaviate/weaviate/entities/vectorindex/common"
)

const (
	DefaultMaxPostingSize     = 0 // it means that it will be computed dynamically by the index
	DefaultMinPostingSize     = 10
	DefaultReplicas           = 8
	DefaultRNGFactor          = 10.0
	DefaultSearchProbe        = 64
	DefaultCentroidsIndexType = "hnsw"
)

// UserConfig defines the configuration options for the SPFresh index.
// Will be populated once we decide what should be exposed.
type UserConfig struct {
	MaxPostingSize     uint32  `json:"maxPostingSize"`
	MinPostingSize     uint32  `json:"minPostingSize"`
	Replicas           uint32  `json:"replicas"`
	RNGFactor          float32 `json:"rngFactor"`
	SearchProbe        uint32  `json:"searchProbe"`
	CentroidsIndexType string  `json:"centroidsIndexType"`
	Distance           string  `json:"distance"`
	// TODO: add quantization config
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
	return "spfresh"
}

func (u UserConfig) DistanceName() string {
	return u.Distance
}

func (u UserConfig) IsMultiVector() bool {
	return false
}

// SetDefaults in the user-specifyable part of the config
func (u *UserConfig) SetDefaults() {
	u.MaxPostingSize = DefaultMaxPostingSize
	u.MinPostingSize = DefaultMinPostingSize
	u.Replicas = DefaultReplicas
	u.RNGFactor = DefaultRNGFactor
	u.SearchProbe = DefaultSearchProbe
	u.CentroidsIndexType = DefaultCentroidsIndexType
	u.Distance = vectorIndexCommon.DefaultDistanceMetric

	// TODO: add quantization config
}

func NewDefaultUserConfig() UserConfig {
	var uc UserConfig
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

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "maxPostingSize", func(v int) {
		uc.MaxPostingSize = uint32(v)
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "minPostingSize", func(v int) {
		uc.MinPostingSize = uint32(v)
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "replicas", func(v int) {
		uc.Replicas = uint32(v)
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "rngFactor", func(v int) {
		uc.RNGFactor = float32(v)
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalIntFromMap(asMap, "searchProbe", func(v int) {
		uc.SearchProbe = uint32(v)
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalStringFromMap(asMap, "centroidsIndexType", func(v string) {
		uc.CentroidsIndexType = v
	}); err != nil {
		return uc, err
	}

	if err := vectorIndexCommon.OptionalStringFromMap(asMap, "distance", func(v string) {
		uc.Distance = v
	}); err != nil {
		return uc, err
	}

	// TODO: add quantization config

	return uc, nil
}
