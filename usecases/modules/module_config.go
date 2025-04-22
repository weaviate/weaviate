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

package modules

import (
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

type ClassBasedModuleConfig struct {
	class        *models.Class
	moduleName   string
	tenant       string
	targetVector string
}

func NewClassBasedModuleConfig(class *models.Class,
	moduleName, tenant, targetVector string,
) *ClassBasedModuleConfig {
	return &ClassBasedModuleConfig{
		class:        class,
		moduleName:   moduleName,
		tenant:       tenant,
		targetVector: targetVector,
	}
}

func NewCrossClassModuleConfig() *ClassBasedModuleConfig {
	// explicitly setting tenant to "" in order to flag that a cross class search
	// is being done without a tenant context
	return &ClassBasedModuleConfig{tenant: ""}
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
