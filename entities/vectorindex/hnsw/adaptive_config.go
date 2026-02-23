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
	"encoding/json"
	"strconv"

	"github.com/weaviate/weaviate/entities/vectorindex/common"
)

const (
	DefaultAdaptiveEFEnabled      = false
	DefaultAdaptiveEFTargetRecall = 0.95
)

type AdaptiveConfig struct {
	Enabled      bool    `json:"enabled"`
	TargetRecall float32 `json:"targetRecall"`
}

func parseAdaptiveEFMap(in map[string]interface{}, config *AdaptiveConfig) error {
	adaptiveEFConfigValue, ok := in["adaptiveEf"]
	if !ok {
		return nil
	}

	adaptiveEFConfigMap, ok := adaptiveEFConfigValue.(map[string]interface{})
	if !ok {
		return nil
	}

	if err := common.OptionalBoolFromMap(adaptiveEFConfigMap, "enabled", func(v bool) {
		config.Enabled = v
	}); err != nil {
		return err
	}

	// Handle targetRecall (float32)
	if targetRecallValue, ok := adaptiveEFConfigMap["targetRecall"]; ok {
		switch typed := targetRecallValue.(type) {
		case float64:
			config.TargetRecall = float32(typed)
		case float32:
			config.TargetRecall = typed
		case json.Number:
			if f, err := strconv.ParseFloat(typed.String(), 32); err == nil {
				config.TargetRecall = float32(f)
			}
		}
	}

	return nil
}
