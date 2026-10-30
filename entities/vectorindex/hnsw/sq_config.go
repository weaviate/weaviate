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

import "github.com/weaviate/weaviate/entities/vectorindex/common"

const (
	DefaultSQEnabled       = false
	DefaultSQTrainingLimit = 100000
	// DefaultSQRescoreLimit is what RescoreLimitAuto resolves to.
	DefaultSQRescoreLimit = 20
	// DefaultSQRescoreFactor is what RescoreFactorAuto resolves to.
	DefaultSQRescoreFactor = 2.0
)

type SQConfig struct {
	Enabled       bool    `json:"enabled"`
	TrainingLimit int     `json:"trainingLimit"`
	RescoreLimit  int     `json:"rescoreLimit"`
	RescoreFactor float64 `json:"rescoreFactor"`
}

func parseSQMap(in map[string]interface{}, sq *SQConfig) error {
	sqConfigValue, ok := in["sq"]
	if !ok {
		return nil
	}

	sqConfigMap, ok := sqConfigValue.(map[string]interface{})
	if !ok {
		return nil
	}

	if err := common.OptionalBoolFromMap(sqConfigMap, "enabled", func(v bool) {
		sq.Enabled = v
	}); err != nil {
		return err
	}

	if err := common.OptionalIntFromMap(sqConfigMap, "trainingLimit", func(v int) {
		sq.TrainingLimit = v
	}); err != nil {
		return err
	}

	if err := parseRescoreLimit(sqConfigMap, "sq", func(v int) {
		sq.RescoreLimit = v
	}); err != nil {
		return err
	}

	if err := common.ParseRescoreFactor(sqConfigMap, "sq", func(v float64) {
		sq.RescoreFactor = v
	}); err != nil {
		return err
	}

	return nil
}
