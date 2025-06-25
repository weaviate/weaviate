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

import (
	"errors"

	"github.com/weaviate/weaviate/entities/vectorindex/common"
)

const (
	DefaultRQEnabled      = false
	DefaultRQBits         = 8
	DefaultRQRescoreLimit = 20
)

type RQConfig struct {
	Enabled      bool  `json:"enabled"`
	Bits         int16 `json:"bits"`
	RescoreLimit int   `json:"rescoreLimit"`
}

func ValidateRQConfig(cfg RQConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.Bits != 8 {
		return errors.New("RQ bits must be 8")
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

	return nil
}
