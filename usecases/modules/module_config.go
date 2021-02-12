package modules

import (
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

type ClassBasedModuleConfig struct {
	class      *models.Class
	moduleName string
}

func NewClassBasedModuleConfig(class *models.Class,
	moduleName string) *ClassBasedModuleConfig {
	return &ClassBasedModuleConfig{
		class:      class,
		moduleName: moduleName,
	}
}

func (cbmc *ClassBasedModuleConfig) Class() map[string]interface{} {
	defaultConf := map[string]interface{}{}
	asMap, ok := cbmc.class.ModuleConfig.(map[string]interface{})
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
