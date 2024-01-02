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

package hnsw

import (
	"fmt"

	"github.com/weaviate/weaviate/entities/vectorindex/common"
)

const (
	PQEncoderTypeKMeans            = "kmeans"
	PQEncoderTypeTile              = "tile"
	PQEncoderDistributionLogNormal = "log-normal"
	PQEncoderDistributionNormal    = "normal"
)

const (
	DefaultPQEnabled             = false
	DefaultPQBitCompression      = false
	DefaultPQSegments            = 0
	DefaultPQEncoderType         = PQEncoderTypeKMeans
	DefaultPQEncoderDistribution = PQEncoderDistributionLogNormal
	DefaultPQCentroids           = 256
	DefaultPQTrainingLimit       = 100000
)

// Product Quantization encoder configuration
type PQEncoder struct {
	Type         string `json:"type"`
	Distribution string `json:"distribution,omitempty"`
}

// Product Quantization configuration
type PQConfig struct {
	Enabled        bool      `json:"enabled"`
	BitCompression bool      `json:"bitCompression"`
	Segments       int       `json:"segments"`
	Centroids      int       `json:"centroids"`
	TrainingLimit  int       `json:"trainingLimit"`
	Encoder        PQEncoder `json:"encoder"`
}

func validEncoder(v string) error {
	switch v {
	case PQEncoderTypeKMeans:
	case PQEncoderTypeTile:
	default:
		return fmt.Errorf("invalid encoder type %s", v)
	}

	return nil
}

func validEncoderDistribution(v string) error {
	switch v {
	case PQEncoderDistributionLogNormal:
	case PQEncoderDistributionNormal:
	default:
		return fmt.Errorf("invalid encoder distribution %s", v)
	}

	return nil
}

func ValidatePQConfig(cfg PQConfig) error {
	if !cfg.Enabled {
		return nil
	}
	err := validEncoder(cfg.Encoder.Type)
	if err != nil {
		return err
	}

	err = validEncoderDistribution(cfg.Encoder.Distribution)
	if err != nil {
		return err
	}

	return nil
}

func encoderFromMap(in map[string]interface{}, setFn func(v string)) error {
	value, ok := in["type"]
	if !ok {
		return nil
	}

	asString, ok := value.(string)
	if !ok {
		return nil
	}

	err := validEncoder(asString)
	if err != nil {
		return err
	}

	setFn(asString)
	return nil
}

func encoderDistributionFromMap(in map[string]interface{}, setFn func(v string)) error {
	value, ok := in["distribution"]
	if !ok {
		return nil
	}

	asString, ok := value.(string)
	if !ok {
		return nil
	}

	err := validEncoderDistribution(asString)
	if err != nil {
		return err
	}

	setFn(asString)
	return nil
}

func parsePQMap(in map[string]interface{}, pq *PQConfig) error {
	pqConfigValue, ok := in["pq"]
	if !ok {
		return nil
	}

	pqConfigMap, ok := pqConfigValue.(map[string]interface{})
	if !ok {
		return nil
	}

	if err := common.OptionalBoolFromMap(pqConfigMap, "enabled", func(v bool) {
		pq.Enabled = v
	}); err != nil {
		return err
	}

	if err := common.OptionalBoolFromMap(pqConfigMap, "bitCompression", func(v bool) {
		pq.BitCompression = v
	}); err != nil {
		return err
	}

	if err := common.OptionalIntFromMap(pqConfigMap, "segments", func(v int) {
		pq.Segments = v
	}); err != nil {
		return err
	}

	if err := common.OptionalIntFromMap(pqConfigMap, "centroids", func(v int) {
		pq.Centroids = v
	}); err != nil {
		return err
	}

	if err := common.OptionalIntFromMap(pqConfigMap, "trainingLimit", func(v int) {
		pq.TrainingLimit = v
	}); err != nil {
		return err
	}

	pqEncoderValue, ok := pqConfigMap["encoder"]
	if !ok {
		return nil
	}

	pqEncoderMap, ok := pqEncoderValue.(map[string]interface{})
	if !ok {
		return nil
	}

	if err := encoderFromMap(pqEncoderMap, func(v string) {
		pq.Encoder.Type = v
	}); err != nil {
		return err
	}

	if err := encoderDistributionFromMap(pqEncoderMap, func(v string) {
		pq.Encoder.Distribution = v
	}); err != nil {
		return err
	}

	return nil
}
