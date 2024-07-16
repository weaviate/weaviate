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
	DefaultFilteredSearchEnabled = false
	DefaultFilteredSearchCache2H = false
)

type FilteredSearchConfig struct {
	Enabled bool `json:"enabled"`
	Cache2H bool `json:"cache2h"`
}

func parseFilteredSearchMap(in map[string]interface{}, fs *FilteredSearchConfig) error {
	fsConfigValue, ok := in["filteredSearch"]
	if !ok {
		return nil
	}

	fsConfigMap, ok := fsConfigValue.(map[string]interface{})
	if !ok {
		return nil
	}

	if err := common.OptionalBoolFromMap(fsConfigMap, "enabled", func(v bool) {
		fs.Enabled = v
	}); err != nil {
		return err
	}

	if err := common.OptionalBoolFromMap(fsConfigMap, "cache2h", func(v bool) {
		fs.Cache2H = v
	}); err != nil {
		return err
	}

	return nil
}
