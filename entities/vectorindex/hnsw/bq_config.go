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

package hnsw

import "github.com/weaviate/weaviate/entities/vectorindex/common"

const (
	DefaultBQEnabled      = false
	DefaultBQRescoreLimit = 512
)

type BQConfig struct {
	Enabled      bool `json:"enabled"`
	RescoreLimit int  `json:"rescoreLimit"`
}

func parseBQMap(in map[string]interface{}, bq *BQConfig) error {
	bqConfigValue, ok := in["bq"]
	if !ok {
		return nil
	}

	bqConfigMap, ok := bqConfigValue.(map[string]interface{})
	if !ok {
		return nil
	}

	if err := common.OptionalBoolFromMap(bqConfigMap, "enabled", func(v bool) {
		bq.Enabled = v
	}); err != nil {
		return err
	}

	if rescoreLimit, ok := bqConfigMap["rescoreLimit"]; ok {
		if v, ok := rescoreLimit.(int); ok {
			bq.RescoreLimit = v
		} else if v, ok := rescoreLimit.(float64); ok {
			bq.RescoreLimit = int(v)
		} else {
			bq.RescoreLimit = DefaultBQRescoreLimit
		}
	} else {
		bq.RescoreLimit = DefaultBQRescoreLimit
	}

	return nil
}
