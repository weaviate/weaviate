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

package flat

import (
	"errors"
	"fmt"

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	vectorindexcommon "github.com/weaviate/weaviate/entities/vectorindex/common"
)

const (
	DefaultVectorCache           = false
	DefaultVectorCacheMaxObjects = 1e12
	DefaultCompressionEnabled    = false
	DefaultCompressionRescore    = -1 // indicates "let Weaviate pick"
	DefaultRQBits                = 8
)

type CompressionUserConfig struct {
	Enabled      bool `json:"enabled"`
	RescoreLimit int  `json:"rescoreLimit"`
	Cache        bool `json:"cache"`
}

type RQUserConfig struct {
	Enabled      bool `json:"enabled"`
	RescoreLimit int  `json:"rescoreLimit"`
	Cache        bool `json:"cache"`
	Bits         int  `json:"bits,omitempty"`
}

type UserConfig struct {
	Distance                 string                `json:"distance"`
	VectorCacheMaxObjects    int                   `json:"vectorCacheMaxObjects"`
	PQ                       CompressionUserConfig `json:"pq"`
	BQ                       CompressionUserConfig `json:"bq"`
	SQ                       CompressionUserConfig `json:"sq"`
	RQ                       RQUserConfig          `json:"rq"`
	SkipDefaultQuantization  bool                  `json:"skipDefaultQuantization"`
	TrackDefaultQuantization bool                  `json:"trackDefaultQuantization"`
}

// IndexType returns the type of the underlying vector index, thus making sure
// the schema.VectorIndexConfig interface is implemented
func (u UserConfig) IndexType() string {
	return "flat"
}

func (u UserConfig) DistanceName() string {
	return u.Distance
}

func (u UserConfig) IsMultiVector() bool {
	return false
}

// SetDefaults in the user-specifyable part of the config
func (u *UserConfig) SetDefaults() {
	u.PQ.Cache = DefaultVectorCache
	u.BQ.Cache = DefaultVectorCache
	u.RQ.Cache = DefaultVectorCache
	u.VectorCacheMaxObjects = DefaultVectorCacheMaxObjects
	u.Distance = vectorindexcommon.DefaultDistanceMetric
	u.PQ.Enabled = DefaultCompressionEnabled
	u.PQ.RescoreLimit = DefaultCompressionRescore
	u.BQ.Enabled = DefaultCompressionEnabled
	u.BQ.RescoreLimit = DefaultCompressionRescore
	u.SQ.Enabled = DefaultCompressionEnabled
	u.SQ.RescoreLimit = DefaultCompressionRescore
	u.RQ.Enabled = DefaultCompressionEnabled
	u.RQ.RescoreLimit = DefaultCompressionRescore
	u.RQ.Bits = DefaultRQBits
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

	if err := vectorindexcommon.OptionalIntFromMap(asMap, "vectorCacheMaxObjects", func(v int) {
		uc.VectorCacheMaxObjects = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalBoolFromMap(asMap, "skipDefaultQuantization", func(v bool) {
		uc.SkipDefaultQuantization = v
	}); err != nil {
		return uc, err
	}

	if err := vectorindexcommon.OptionalBoolFromMap(asMap, "trackDefaultQuantization", func(v bool) {
		uc.TrackDefaultQuantization = v
	}); err != nil {
		return uc, err
	}

	if err := parseCompression(asMap, &uc); err != nil {
		return uc, err
	}

	return uc, nil
}

func parseCompressionMap(in interface{}, cuc *CompressionUserConfig) error {
	configMap, ok := in.(map[string]interface{})
	if ok {
		if err := vectorindexcommon.OptionalBoolFromMap(configMap, "enabled", func(v bool) {
			cuc.Enabled = v
		}); err != nil {
			return err
		}

		if err := vectorindexcommon.OptionalBoolFromMap(configMap, "cache", func(v bool) {
			cuc.Cache = v
		}); err != nil {
			return err
		}

		if err := vectorindexcommon.OptionalIntFromMap(configMap, "rescoreLimit", func(v int) {
			cuc.RescoreLimit = v
		}); err != nil {
			return err
		}
	}
	return nil
}

func parseCompression(in map[string]interface{}, uc *UserConfig) error {
	pqConfigValue, pqOk := in["pq"]
	bqConfigValue, bqOk := in["bq"]
	sqConfigValue, sqOk := in["sq"]
	rqConfigValue, rqOk := in["rq"]

	if !pqOk && !bqOk && !sqOk && !rqOk {
		return nil
	}

	if pqOk {
		err := parseCompressionMap(pqConfigValue, &uc.PQ)
		if err != nil {
			return err
		}
	}

	if bqOk {
		err := parseCompressionMap(bqConfigValue, &uc.BQ)
		if err != nil {
			return err
		}
	}

	if sqOk {
		err := parseCompressionMap(sqConfigValue, &uc.SQ)
		if err != nil {
			return err
		}
	}

	if rqOk {
		err := parseRQConfig(rqConfigValue, &uc.RQ)
		if err != nil {
			return err
		}
	}

	compressionConfigs := []CompressionUserConfig{uc.PQ, uc.BQ, uc.SQ}
	totalEnabled := 0

	for _, compressionConfig := range compressionConfigs {
		if compressionConfig.Cache && !compressionConfig.Enabled {
			return errors.New("not possible to use the cache without compression")
		}
		if compressionConfig.Enabled {
			totalEnabled++
		}
	}

	// Handle RQ separately since it has a different type
	if uc.RQ.Cache && !uc.RQ.Enabled {
		return errors.New("not possible to use the cache without compression")
	}
	if uc.RQ.Enabled {
		totalEnabled++
		// Validate RQ bits
		if uc.RQ.Bits != 1 && uc.RQ.Bits != 8 {
			return errors.New("RQ bits must be either 1 or 8")
		}
	}

	if totalEnabled > 1 {
		return errors.New("cannot enable multiple quantization methods at the same time")
	}

	// TODO: remove once PQ and SQ are supported
	if uc.PQ.Enabled {
		return errors.New("PQ is not currently supported for flat indices")
	}
	if uc.SQ.Enabled {
		return errors.New("SQ is not currently supported for flat indices")
	}

	return nil
}

func parseRQConfig(in interface{}, rq *RQUserConfig) error {
	configMap, ok := in.(map[string]interface{})
	if ok {
		if err := vectorindexcommon.OptionalBoolFromMap(configMap, "enabled", func(v bool) {
			rq.Enabled = v
		}); err != nil {
			return err
		}

		if err := vectorindexcommon.OptionalBoolFromMap(configMap, "cache", func(v bool) {
			rq.Cache = v
		}); err != nil {
			return err
		}

		if err := vectorindexcommon.OptionalIntFromMap(configMap, "rescoreLimit", func(v int) {
			rq.RescoreLimit = v
		}); err != nil {
			return err
		}

		if err := vectorindexcommon.OptionalIntFromMap(configMap, "bits", func(v int) {
			rq.Bits = v
		}); err != nil {
			return err
		}
	}
	return nil
}

func NewDefaultUserConfig() UserConfig {
	uc := UserConfig{}
	uc.SetDefaults()
	return uc
}

func ParseDefaultQuantization(vectorIndexConfig schemaConfig.VectorIndexConfig, compression string) (schemaConfig.VectorIndexConfig, error) {
	flatConfig := vectorIndexConfig.(UserConfig)
	rqEnabled := flatConfig.RQ.Enabled
	bqEnabled := flatConfig.BQ.Enabled
	skipDefaultQuantization := flatConfig.SkipDefaultQuantization
	flatConfig.TrackDefaultQuantization = false
	if rqEnabled || bqEnabled || skipDefaultQuantization {
		return flatConfig, nil
	}
	switch compression {
	case "rq-1":
		flatConfig.RQ.Enabled = true
		flatConfig.RQ.Bits = 1
		flatConfig.RQ.RescoreLimit = DefaultCompressionRescore
		flatConfig.RQ.Cache = DefaultVectorCache
	case "rq-8":
		flatConfig.RQ.Enabled = true
		flatConfig.RQ.Bits = 8
		flatConfig.RQ.RescoreLimit = DefaultCompressionRescore
		flatConfig.RQ.Cache = DefaultVectorCache
	case "bq":
		flatConfig.BQ.Enabled = true
		flatConfig.BQ.RescoreLimit = DefaultCompressionRescore
		flatConfig.BQ.Cache = DefaultVectorCache
	default:
		return flatConfig, errors.New("invalid default quantization for flat index: " + compression)
	}
	flatConfig.TrackDefaultQuantization = true
	return flatConfig, nil
}
