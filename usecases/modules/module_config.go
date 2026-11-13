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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

type ClassBasedModuleConfig struct {
	class        *models.Class
	moduleName   string
	altNames     []string
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

// WithAltNames registers the legacy names a module is also known by, so
// property-level settings stored under an older key are still found when the
// provider asks for the canonical module name.
func (cbmc *ClassBasedModuleConfig) WithAltNames(altNames []string) *ClassBasedModuleConfig {
	cbmc.altNames = altNames
	return cbmc
}

func NewCrossClassModuleConfig(cfg *config.Config) *ClassBasedModuleConfig {
	// explicitly setting tenant to "" in order to flag that a cross class search
	// is being done without a tenant context
	return &ClassBasedModuleConfig{tenant: "", cfg: cfg}
}

// newClassBasedModuleConfig builds a class config for moduleName and teaches it
// the module's registered legacy names, so property settings written under an
// older key are still honoured. Canonical keys keep precedence.
func (p *Provider) newClassBasedModuleConfig(class *models.Class,
	moduleName, tenant, targetVector string,
) *ClassBasedModuleConfig {
	var altNames []string
	if mod := p.GetByName(moduleName); mod != nil {
		altNames = moduleAltNames(mod)
	}
	return NewClassBasedModuleConfig(class, moduleName, tenant, targetVector, &p.cfg).
		WithAltNames(altNames)
}

func moduleAltNames(mod modulecapabilities.Module) []string {
	if modWithAltNames, ok := mod.(modulecapabilities.ModuleHasAltNames); ok {
		return modWithAltNames.AltNames()
	}
	return nil
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
	if cbmc.class == nil {
		return primitiveProps
	}
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
	// a cross class config (e.g. Explore) is not bound to any class
	if cbmc.class == nil {
		return nil
	}
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
	if cbmc.class == nil {
		return defaultConf
	}

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
		// Providers ask for the canonical name, but a collection created under
		// a legacy module key still stores its property settings there. Canonical
		// settings win whenever they exist; the registered aliases are only a
		// fallback.
		for _, altName := range cbmc.altNames {
			if moduleCfg, ok = asMap[altName]; ok {
				break
			}
		}
	}
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
