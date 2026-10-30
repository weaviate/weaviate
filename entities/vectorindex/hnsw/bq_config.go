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
	DefaultBQEnabled = false
	// DefaultBQRescoreLimit is what RescoreLimitAuto resolves to.
	DefaultBQRescoreLimit = 256
	// DefaultBQRescoreFactor is what RescoreFactorAuto resolves to. One bit
	// per dimension ranks candidates coarsely, so BQ keeps more headroom.
	DefaultBQRescoreFactor = 4.0
)

type BQConfig struct {
	Enabled       bool    `json:"enabled"`
	RescoreLimit  int     `json:"rescoreLimit"`
	RescoreFactor float64 `json:"rescoreFactor"`
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

	if err := parseRescoreLimit(bqConfigMap, "bq", func(v int) {
		bq.RescoreLimit = v
	}); err != nil {
		return err
	}

	if err := common.ParseRescoreFactor(bqConfigMap, "bq", func(v float64) {
		bq.RescoreFactor = v
	}); err != nil {
		return err
	}

	return nil
}
