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

import "github.com/weaviate/weaviate/entities/vectorindex/common"

const (
	DefaultLASQEnabled       = false
	DefaultLASQTrainingLimit = 100000
)

type LASQConfig struct {
	Enabled       bool `json:"enabled"`
	TrainingLimit int  `json:"trainingLimit"`
}

func parseLASQMap(in map[string]interface{}, lasq *LASQConfig) error {
	lasqConfigValue, ok := in["lasq"]
	if !ok {
		return nil
	}

	lasqConfigMap, ok := lasqConfigValue.(map[string]interface{})
	if !ok {
		return nil
	}

	if err := common.OptionalBoolFromMap(lasqConfigMap, "enabled", func(v bool) {
		lasq.Enabled = v
	}); err != nil {
		return err
	}

	if err := common.OptionalIntFromMap(lasqConfigMap, "trainingLimit", func(v int) {
		lasq.TrainingLimit = v
	}); err != nil {
		return err
	}

	return nil
}
