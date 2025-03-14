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

package config

import (
	"bytes"

	"github.com/weaviate/weaviate/usecases/config/runtime"
	"gopkg.in/yaml.v2"
)

type WeaviateRuntimeConfig struct {
	MaximumAllowedCollectionsCount int `json:"maximum_allowed_collections_count" yaml:"maximum_allowed_collections_count"`

	// config manager that keep the runtime config up to date
	cm *runtime.ConfigManager[WeaviateRuntimeConfig]
}

func NewWeaviateRuntimeConfig(cm *runtime.ConfigManager[WeaviateRuntimeConfig]) *WeaviateRuntimeConfig {
	return &WeaviateRuntimeConfig{
		cm: cm,
	}
}

func (rc *WeaviateRuntimeConfig) GetMaximumAllowedCollectionsCount() *int {
	if cfg, err := rc.cm.Config(); err == nil {
		return &cfg.MaximumAllowedCollectionsCount
	}
	return nil
}

func ParseYaml(buf []byte) (*WeaviateRuntimeConfig, error) {
	var conf WeaviateRuntimeConfig

	dec := yaml.NewDecoder(bytes.NewReader(buf))
	dec.SetStrict(true)
	if err := dec.Decode(&conf); err != nil {
		return nil, err
	}
	return &conf, nil
}
