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
	"errors"
	"io"

	"gopkg.in/yaml.v2"
)

type ConfigManager interface {
	Config() (*WeaviateRuntimeConfig, error)
}

type WeaviateRuntimeConfig struct {
	MaximumAllowedCollectionsCount *int `json:"maximum_allowed_collections_count" yaml:"maximum_allowed_collections_count"`

	// AutoSchemaEnabled marking it as pointer type to differentiate default values.
	AutoSchemaEnabled *bool `json:"auto_schema_enabled" yaml:"auto_schema_enabled"`

	AsyncReplicationDisabled *bool `json:"async_replication_disabled" yaml:"async_replication_disabled"`

	// config manager that keep the runtime config up to date
	cm ConfigManager
}

func NewWeaviateRuntimeConfig(cm ConfigManager) *WeaviateRuntimeConfig {
	return &WeaviateRuntimeConfig{
		cm: cm,
	}
}

func (rc *WeaviateRuntimeConfig) GetMaximumAllowedCollectionsCount() *int {
	if cfg, err := rc.cm.Config(); err == nil {
		return cfg.MaximumAllowedCollectionsCount
	}
	return nil
}

func (rc *WeaviateRuntimeConfig) GetAutoSchemaEnabled() *bool {
	if cfg, err := rc.cm.Config(); err == nil {
		return cfg.AutoSchemaEnabled
	}
	return nil
}

func (rc *WeaviateRuntimeConfig) GetAsyncReplicationDisabled() *bool {
	if cfg, err := rc.cm.Config(); err == nil {
		return cfg.AsyncReplicationDisabled
	}
	return nil
}

func ParseYaml(buf []byte) (*WeaviateRuntimeConfig, error) {
	var conf WeaviateRuntimeConfig

	dec := yaml.NewDecoder(bytes.NewReader(buf))
	dec.SetStrict(true)

	// Am empty runtime yaml file is still a valid file. So treating io.EOF as
	// non-error case returning default values of conf.
	if err := dec.Decode(&conf); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return &conf, nil
}
