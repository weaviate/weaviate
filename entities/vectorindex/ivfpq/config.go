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

package ivfpq

import (
	"fmt"

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	vectorindexcommon "github.com/weaviate/weaviate/entities/vectorindex/common"
)

const (
	DefaultCompressionRescore = -1 // indicates "let Weaviate pick"
	DefaultProbingSize        = 50_000
	DefaultDistanceCutOff     = 0.1
)

type UserConfig struct {
	Distance       string  `json:"distance"`
	ProbingSize    int     `json:"probingSize"`
	DistanceCutOff float32 `json:"distCutOff"`
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
	return "ivfpq"
}

func (u UserConfig) DistanceName() string {
	return u.Distance
}

func (u UserConfig) IsMultiVector() bool {
	return false
}

// SetDefaults in the user-specifyable part of the config
func (u *UserConfig) SetDefaults() {
	u.Distance = vectorindexcommon.DefaultDistanceMetric
	u.ProbingSize = DefaultProbingSize
	u.DistanceCutOff = DefaultDistanceCutOff
}

// ParseAndValidateConfig from an unknown input value, as this is not further
// specified in the API to allow of exchanging the index type
func ParseAndValidateConfig(input interface{}) (schemaConfig.VectorIndexConfig, error) {
	uc := UserConfig{}
	uc.SetDefaults()

	if input == nil {
		return uc, nil
	}

	asMap, ok := input.(map[string]interface{})
	if !ok || asMap == nil {
		return uc, fmt.Errorf("input must be a non-nil map")
	}

	if err := vectorindexcommon.OptionalStringFromMap(asMap, "distance", func(v string) {
		uc.Distance = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalIntFromMap(asMap, "probingSize", func(v int) {
		uc.ProbingSize = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalFloatFromMap(asMap, "distanceCutOff", func(v float32) {
		uc.DistanceCutOff = v
	}); err != nil {
		return uc, err
	}

	return uc, nil
}

func NewDefaultUserConfig() UserConfig {
	uc := UserConfig{}
	uc.SetDefaults()
	return uc
}
