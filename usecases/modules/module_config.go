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
	class      *models.Class
	moduleName string
	tenant     string
}

func NewClassBasedModuleConfig(class *models.Class,
	moduleName string, tenant string,
) *ClassBasedModuleConfig {
	return &ClassBasedModuleConfig{
		class:      class,
		moduleName: moduleName,
		tenant:     tenant,
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

func (cbmc *ClassBasedModuleConfig) ClassByModuleName(moduleName string) map[string]interface{} {
	defaultConf := map[string]interface{}{}
	asMap, ok := cbmc.class.ModuleConfig.(map[string]interface{})
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
