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

package flat

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	vectorindexcommon "github.com/weaviate/weaviate/entities/vectorindex/common"
)

const (
	DefaultEF              = -1 // indicates "let Weaviate pick"
	DefaultFullyOnDisk     = false
	DefaultPartiallyOnDisk = false
	DefaultCompression     = CompressionBQ
	DefaultUseIvf          = false

	CompressionNone = "none"
	CompressionBQ   = "binary"
	CompressionPQ   = "product-quantisation"
)

type UserConfig struct {
	EF              int    `json:"ef"`
	Distance        string `json:"distance"`
	FullyOnDisk     bool   `json:"fullyOnDisk"`
	PartiallyOnDisk bool   `json:"partiallyOnDisk"`
	Compression     string `json:"compression"`
	UseIvf          bool   `json:"useIvf"`
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
	return "flat"
}

func (u UserConfig) DistanceName() string {
	return u.Distance
}

// SetDefaults in the user-specifyable part of the config
func (u *UserConfig) SetDefaults() {
	u.EF = DefaultEF
	u.FullyOnDisk = DefaultFullyOnDisk
	u.PartiallyOnDisk = DefaultPartiallyOnDisk
	u.Compression = DefaultCompression
	u.FullyOnDisk = DefaultFullyOnDisk
	u.Distance = vectorindexcommon.DefaultDistanceMetric
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

	if err := vectorindexcommon.OptionalIntFromMap(asMap, "ef", func(v int) {
		uc.EF = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalStringFromMap(asMap, "distance", func(v string) {
		uc.Distance = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalBoolFromMap(asMap, "fullyOnDisk", func(v bool) {
		uc.FullyOnDisk = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalBoolFromMap(asMap, "partiallyOnDisk", func(v bool) {
		uc.PartiallyOnDisk = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalBoolFromMap(asMap, "useIvf", func(v bool) {
		uc.UseIvf = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalStringFromMap(asMap, "compression", func(v string) {
		uc.Compression = v
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

func TypeAssertVectorIndex(class *models.Class) (UserConfig, error) {
	flatConfig, ok := class.VectorIndexConfig.(UserConfig)
	if !ok {
		return UserConfig{}, fmt.Errorf("class '%s' vector index: config is not flat.UserConfig: %T",
			class.Class, class.VectorIndexConfig)
	}

	return flatConfig, nil
}
