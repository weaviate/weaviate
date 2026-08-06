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

package hnsw

import (
	"errors"

	"github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/vectorindex/common"
)

const (
	DefaultRQEnabled       = false
	DefaultRQBits          = 8
	DefaultRQRescoreLimit  = 20
	DefaultBRQRescoreLimit = 512
	DefaultRQCentering     = false
	// DefaultRQTrainingLimit is deliberately smaller than the PQ/SQ default
	// (100k): centering only fits a coordinate-wise mean, which converges
	// with far fewer samples, and a lower limit activates compression (and
	// its memory savings) much earlier in an import.
	DefaultRQTrainingLimit = 10000
)

type RQConfig struct {
	Enabled      bool  `json:"enabled"`
	Bits         int16 `json:"bits"`
	RescoreLimit int   `json:"rescoreLimit"`
	// Centering subtracts the dataset mean before quantization. It requires a
	// training pass over TrainingLimit vectors to fit the mean, so compression
	// activates like PQ/SQ (deferred) instead of on the first vector. Immutable
	// once set.
	Centering     bool `json:"centering"`
	TrainingLimit int  `json:"trainingLimit"`
}

func ValidateRQConfig(cfg RQConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.Bits != 8 && cfg.Bits != 4 && cfg.Bits != 1 {
		return errors.New("RQ bits must be 8, 4 or 1")
	}
	if cfg.Centering && cfg.Bits != 4 {
		return errors.New("RQ centering requires bits=4")
	}
	if cfg.Centering && cfg.TrainingLimit <= 0 {
		return errors.New("RQ trainingLimit must be positive when centering is enabled")
	}

	return nil
}

func parseRQMap(in map[string]interface{}, rq *RQConfig) error {
	rqConfigValue, ok := in["rq"]
	if !ok {
		return nil
	}

	rqConfigMap, ok := rqConfigValue.(map[string]interface{})
	if !ok {
		return nil
	}

	if err := common.OptionalBoolFromMap(rqConfigMap, "enabled", func(v bool) {
		rq.Enabled = v
	}); err != nil {
		return err
	}

	if err := common.OptionalIntFromMap(rqConfigMap, "bits", func(v int) {
		rq.Bits = int16(v)
	}); err != nil {
		return err
	}

	if err := common.OptionalIntFromMap(rqConfigMap, "rescoreLimit", func(v int) {
		rq.RescoreLimit = v
	}); err != nil {
		return err
	}

	if err := common.OptionalBoolFromMap(rqConfigMap, "centering", func(v bool) {
		rq.Centering = v
	}); err != nil {
		return err
	}

	if err := common.OptionalIntFromMap(rqConfigMap, "trainingLimit", func(v int) {
		rq.TrainingLimit = v
	}); err != nil {
		return err
	}

	if rq.Bits == 1 && rqConfigMap["rescoreLimit"] == nil {
		rq.RescoreLimit = DefaultBRQRescoreLimit
	}

	return nil
}

// GetRQBits returns the bits value for RQ compression, or 0 if not RQ
func GetRQBits(cfg config.VectorIndexConfig) int16 {
	if hnswUserConfig, ok := cfg.(UserConfig); ok && hnswUserConfig.RQ.Enabled {
		return hnswUserConfig.RQ.Bits
	}
	return 0
}
