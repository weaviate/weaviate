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
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"gopkg.in/yaml.v3"
)

type WeaviateRuntimeConfig struct {
	MaximumAllowedCollectionsCount *runtime.DynamicValue[int]  `json:"maximum_allowed_collections_count" yaml:"maximum_allowed_collections_count"`
	AutoschemaEnabled              *runtime.DynamicValue[bool] `json:"autoschema_enabled" yaml:"autoschema_enabled"`
	AsyncReplicationDisabled       *runtime.DynamicValue[bool] `json:"async_replication_disabled" yaml:"async_replication_disabled"`
}

// ParseRuntimeConfig decode WeaviateRuntimeConfig from raw bytes of YAML.
func ParseRuntimeConfig(buf []byte) (*WeaviateRuntimeConfig, error) {
	var conf WeaviateRuntimeConfig

	dec := yaml.NewDecoder(bytes.NewReader(buf))
	dec.KnownFields(true)

	// Am empty runtime yaml file is still a valid file. So treating io.EOF as
	// non-error case returning default values of conf.
	if err := dec.Decode(&conf); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return &conf, nil
}

// UpdateConfig does in-place update of `registered` config based on values available in
// `parsed` config.
func UpdateRuntimeConfig(registered, parsed *WeaviateRuntimeConfig) error {
	if parsed.MaximumAllowedCollectionsCount != nil {
		registered.MaximumAllowedCollectionsCount.SetValue(parsed.MaximumAllowedCollectionsCount.Get())
	} else {
		registered.MaximumAllowedCollectionsCount.Reset()
	}

	if parsed.AsyncReplicationDisabled != nil {
		registered.AsyncReplicationDisabled.SetValue(parsed.AsyncReplicationDisabled.Get())
	} else {
		registered.AsyncReplicationDisabled.Reset()
	}

	if parsed.AutoschemaEnabled != nil {
		registered.AutoschemaEnabled.SetValue(parsed.AutoschemaEnabled.Get())
	} else {
		registered.AutoschemaEnabled.Reset()
	}

	return nil
}
