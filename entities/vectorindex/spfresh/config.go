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

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
	vectorIndexCommon "github.com/weaviate/weaviate/entities/vectorindex/common"
)

// UserConfig defines the configuration options for the SPFresh index.
// Will be populated once we decide what should be exposed.
type UserConfig struct {
	Distance string `json:"distance"`
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
	u.Distance = common.DistanceL2Squared
}

func NewDefaultUserConfig() UserConfig {
	var uc UserConfig
	uc.SetDefaults()
	return uc
}

// ParseAndValidateConfig from an unknown input value, as this is not further
// specified in the API to allow of exchanging the index type
func ParseAndValidateConfig(input any, isMultiVector bool) (schemaConfig.VectorIndexConfig, error) {
	var uc UserConfig
	uc.SetDefaults()

	if input == nil {
		return uc, nil
	}

	asMap, ok := input.(map[string]any)
	if !ok || asMap == nil {
		return uc, fmt.Errorf("input must be a non-nil map")
	}

	err := vectorIndexCommon.OptionalStringFromMap(asMap, "distance", func(v string) {
		uc.Distance = v
	})
	if err != nil {
		return uc, err
	}

	return uc, nil
}
