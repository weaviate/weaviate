package config

import (
	"bytes"

	"github.com/weaviate/weaviate/usecases/config/runtime"
	"gopkg.in/yaml.v2"
)

type WeaviateRuntimeConfig struct {
	MaximumAllowedCollectionCount int `json:"maximum_collection_count" yaml:"maximum_collection_count"`

	// config manager that keep the runtime config up to date
	cm *runtime.ConfigManager[WeaviateRuntimeConfig]
}

func NewWeaviateRuntimeConfig(cm *runtime.ConfigManager[WeaviateRuntimeConfig]) *WeaviateRuntimeConfig {
	return &WeaviateRuntimeConfig{
		cm: cm,
	}
}

func (rc *WeaviateRuntimeConfig) GetMaximumAllowedCollectionCount() *int {
	if cfg, err := rc.cm.Config(); err == nil {
		return &cfg.MaximumAllowedCollectionCount
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
