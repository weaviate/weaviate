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

package modules

import (
	"maps"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

type ClassBasedModuleConfig struct {
	class        *models.Class
	moduleName   string
	tenant       string
	targetVector string
	cfg          *config.Config
}

func NewClassBasedModuleConfig(class *models.Class,
	moduleName, tenant, targetVector string,
	cfg *config.Config,
) *ClassBasedModuleConfig {
	return &ClassBasedModuleConfig{
		class:        class,
		moduleName:   moduleName,
		tenant:       tenant,
		targetVector: targetVector,
		cfg:          cfg,
	}
}

func NewCrossClassModuleConfig(cfg *config.Config) *ClassBasedModuleConfig {
	// explicitly setting tenant to "" in order to flag that a cross class search
	// is being done without a tenant context
	return &ClassBasedModuleConfig{tenant: "", cfg: cfg}
}

func (cbmc *ClassBasedModuleConfig) Class() map[string]interface{} {
	return cbmc.ClassByModuleName(cbmc.moduleName)
}

func (cbmc *ClassBasedModuleConfig) Tenant() string {
	return cbmc.tenant
}

func (cbmc *ClassBasedModuleConfig) TargetVector() string {
	return cbmc.targetVector
}

func (cbmc *ClassBasedModuleConfig) PropertiesDataTypes() map[string]schema.DataType {
	primitiveProps := map[string]schema.DataType{}
	for _, schemaProp := range cbmc.class.Properties {
		dt, err := schema.GetValueDataTypeFromString(schemaProp.DataType[0])
		if err != nil {
			continue
		}
		primitiveProps[schemaProp.Name] = *dt
	}
	return primitiveProps
}

func (cbmc *ClassBasedModuleConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	defaultConf := map[string]interface{}{}
	asMap, ok := cbmc.getModuleConfig().(map[string]interface{})
	if !ok {
		return defaultConf
	}

	moduleCfg, ok := asMap[moduleName]
	if !ok {
		return defaultConf
	}

	asMap, ok = moduleCfg.(map[string]interface{})
	if !ok {
		return defaultConf
	}

	return asMap
}

func (cbmc *ClassBasedModuleConfig) getModuleConfig() interface{} {
	if cbmc.targetVector != "" {
		if vectorConfig, ok := cbmc.class.VectorConfig[cbmc.targetVector]; ok {
			return vectorConfig.Vectorizer
		}
		return nil
	}
	return cbmc.class.ModuleConfig
}

func (cbmc *ClassBasedModuleConfig) Property(propName string) map[string]interface{} {
	defaultConf := map[string]interface{}{}
	prop, err := schema.GetPropertyByName(cbmc.class, propName)
	if err != nil {
		return defaultConf
	}

	asMap, ok := prop.ModuleConfig.(map[string]interface{})
	if !ok {
		return defaultConf
	}

	moduleCfg, ok := asMap[cbmc.moduleName]
	if !ok {
		return defaultConf
	}

	asMap, ok = moduleCfg.(map[string]interface{})
	if !ok {
		return defaultConf
	}

	return asMap
}

func (cbmc *ClassBasedModuleConfig) Config() *config.Config {
	return cbmc.cfg
}

// RequestModuleConfig is a class config with the settings a request names
// for one module over the class's. The module reads the merged map under
// its name; everything else reads the class.
type RequestModuleConfig struct {
	moduletools.ClassConfig
	module   string
	settings map[string]any
}

// NewRequestModuleConfig returns base when the request names no settings.
func NewRequestModuleConfig(base moduletools.ClassConfig, module string, settings map[string]any) moduletools.ClassConfig {
	if len(settings) == 0 {
		return base
	}
	return &RequestModuleConfig{ClassConfig: base, module: module, settings: settings}
}

func (c *RequestModuleConfig) Class() map[string]any {
	return c.ClassByModuleName(c.module)
}

func (c *RequestModuleConfig) ClassByModuleName(moduleName string) map[string]any {
	fromClass := c.ClassConfig.ClassByModuleName(moduleName)
	if moduleName != c.module {
		return fromClass
	}
	merged := make(map[string]any, len(fromClass)+len(c.settings))
	maps.Copy(merged, fromClass)
	maps.Copy(merged, c.settings)
	return merged
}
