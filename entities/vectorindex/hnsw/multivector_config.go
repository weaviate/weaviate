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
	MultivectorAggregationMaxSim = "maxSim"
)

const (
	DefaultMultivectorEnabled     = false
	DefaultMultivectorAggregation = "maxSim"
)

// Multivector configuration
type MultivectorConfig struct {
	Enabled     bool   `json:"enabled"`
	Aggregation string `json:"aggregation"`
}

func validAggregation(v string) error {
	switch v {
	case MultivectorAggregationMaxSim:
	default:
		return fmt.Errorf("invalid aggregation type %s", v)
	}

	return nil
}

func ValidateMultivectorConfig(cfg MultivectorConfig) error {
	if !cfg.Enabled {
		return nil
	}
	err := validAggregation(cfg.Aggregation)
	if err != nil {
		return err
	}

	return nil
}

func parseMultivectorMap(in map[string]interface{}, multivector *MultivectorConfig, isMultiVector bool) error {
	multivectorConfigValue, ok := in["multivector"]
	if !ok {
		return nil
	}

	multivectorConfigMap, ok := multivectorConfigValue.(map[string]interface{})
	if !ok {
		return nil
	}

	if err := common.OptionalBoolFromMap(multivectorConfigMap, "enabled", func(v bool) {
		if isMultiVector {
			// vectorizer set is a multi vector vectorizer, enable multi vector index
			multivector.Enabled = true
		} else {
			multivector.Enabled = v
		}
	}); err != nil {
		return err
	}

	if err := common.OptionalStringFromMap(multivectorConfigMap, "aggregation", func(v string) {
		multivector.Aggregation = v
	}); err != nil {
		return err
	}

	return nil
}
