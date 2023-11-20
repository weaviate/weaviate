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
	"errors"
	"fmt"

	"github.com/weaviate/weaviate/entities/schema"
	vectorindexcommon "github.com/weaviate/weaviate/entities/vectorindex/common"
)

const (
	DefaultVectorCache           = false
	DefaultVectorCacheMaxObjects = 1e12
	DefaultCompressionEnabled    = false
	DefaultCompressionRescore    = -1 // indicates "let Weaviate pick"
)

type CompressionUserConfig struct {
	Enabled bool `json:"enabled"`
	Rescore int  `json:"rescore"`
}

type UserConfig struct {
	Distance              string                `json:"distance"`
	VectorCache           bool                  `json:"vectorCache"`
	VectorCacheMaxObjects int                   `json:"vectorCacheMaxObjects"`
	PQ                    CompressionUserConfig `json:"pq"`
	BQ                    CompressionUserConfig `json:"bq"`
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
	u.VectorCache = DefaultVectorCache
	u.VectorCacheMaxObjects = DefaultVectorCacheMaxObjects
	u.Distance = vectorindexcommon.DefaultDistanceMetric
	u.PQ.Enabled = DefaultCompressionEnabled
	u.PQ.Rescore = DefaultCompressionRescore
	u.BQ.Enabled = DefaultCompressionEnabled
	u.BQ.Rescore = DefaultCompressionRescore
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

	if err := vectorindexcommon.OptionalStringFromMap(asMap, "distance", func(v string) {
		uc.Distance = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalBoolFromMap(asMap, "vectorCache", func(v bool) {
		uc.VectorCache = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalIntFromMap(asMap, "vectorCacheMaxObjects", func(v int) {
		uc.VectorCacheMaxObjects = v
	}); err != nil {
		return uc, err
	}

	if err := parseCompressionMap(asMap, &uc); err != nil {
		return uc, err
	}

	return uc, nil
}

func parseCompressionMap(in map[string]interface{}, uc *UserConfig) error {
	pqConfigValue, pqOk := in["pq"]
	bqConfigValue, bqOk := in["bq"]
	if !pqOk && !bqOk {
		return nil
	}

	if pqOk {
		pqConfigMap, ok := pqConfigValue.(map[string]interface{})
		if !ok {
			return nil
		}

		if err := vectorindexcommon.OptionalBoolFromMap(pqConfigMap, "enabled", func(v bool) {
			uc.PQ.Enabled = v
		}); err != nil {
			return err
		}

		if err := vectorindexcommon.OptionalIntFromMap(pqConfigMap, "rescore", func(v int) {
			uc.PQ.Rescore = v
		}); err != nil {
			return err
		}
	}

	if bqOk {
		bqConfigMap, ok := bqConfigValue.(map[string]interface{})
		if !ok {
			return nil
		}

		if err := vectorindexcommon.OptionalBoolFromMap(bqConfigMap, "enabled", func(v bool) {
			uc.BQ.Enabled = v
		}); err != nil {
			return err
		}

		if err := vectorindexcommon.OptionalIntFromMap(bqConfigMap, "rescore", func(v int) {
			uc.BQ.Rescore = v
		}); err != nil {
			return err
		}
	}
	if uc.PQ.Enabled && uc.BQ.Enabled {
		return errors.New("cannot activate dual compression. Select either PQ or BQ please")
	}
	return nil
}

func NewDefaultUserConfig() UserConfig {
	uc := UserConfig{}
	uc.SetDefaults()
	return uc
}
