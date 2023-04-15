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

package hnsw

import (
	"fmt"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
)

const (
	DefaultPQEnabled             = false
	DefaultPQBitCompression      = false
	DefaultPQSegments            = 0
	DefaultPQEncoderType         = "kmeans"
	DefaultPQEncoderDistribution = "log-normal"
	DefaultPQCentroids           = 256
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
	Encoder        PQEncoder `json:"encoder"`
}

func ValidEncoder(encoder string) (ssdhelpers.Encoder, error) {
	switch encoder {
	case "tile":
		return ssdhelpers.UseTileEncoder, nil
	case "kmeans":
		return ssdhelpers.UseKMeansEncoder, nil
	default:
		return 0, fmt.Errorf("invalid encoder type: %s", encoder)
	}
}

func ValidEncoderDistribution(distribution string) (ssdhelpers.EncoderDistribution, error) {
	switch distribution {
	case "log-normal":
		return ssdhelpers.LogNormalEncoderDistribution, nil
	case "normal":
		return ssdhelpers.NormalEncoderDistribution, nil
	default:
		return 0, fmt.Errorf("invalid encoder distribution: %s", distribution)
	}
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

	_, err := ValidEncoder(asString)
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

	_, err := ValidEncoderDistribution(asString)
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

	if err := optionalBoolFromMap(pqConfigMap, "enabled", func(v bool) {
		pq.Enabled = v
	}); err != nil {
		return err
	}

	if err := optionalBoolFromMap(pqConfigMap, "bitCompression", func(v bool) {
		pq.BitCompression = v
	}); err != nil {
		return err
	}

	if err := optionalIntFromMap(pqConfigMap, "segments", func(v int) {
		pq.Segments = v
	}); err != nil {
		return err
	}

	if err := optionalIntFromMap(pqConfigMap, "centroids", func(v int) {
		pq.Centroids = v
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
