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
	"github.com/weaviate/weaviate/entities/vectorindex/common"
)

const (
	DefaultRQEnabled = false
	DefaultDataBits  = 1
	DefaultQueryBits = 1
)

type RQConfig struct {
	Enabled   bool  `json:"enabled"`
	DataBits  int16 `json:"dataBits"`
	QueryBits int16 `json:"queryBits"`
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

	if err := common.OptionalIntFromMap(rqConfigMap, "dataBits", func(v int) {
		rq.DataBits = int16(v)
	}); err != nil {
		return err
	}

	if err := common.OptionalIntFromMap(rqConfigMap, "queryBits", func(v int) {
		rq.QueryBits = int16(v)
	}); err != nil {
		return err
	}

	return nil
}
