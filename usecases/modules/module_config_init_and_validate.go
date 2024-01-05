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
	"context"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
)

// SetClassDefaults sets the module-specific defaults for the class itself, but
// also for each prop
func (p *Provider) SetClassDefaults(class *models.Class) {
	if class.Vectorizer == "none" {
		// the class does not use a vectorizer, nothing to do for us
		return
	}

	mod := p.GetByName(class.Vectorizer)
	cc, ok := mod.(modulecapabilities.ClassConfigurator)
	if !ok {
		// the module exists, but is not a class configurator, nothing to do for us
		return
	}

	cfg := NewClassBasedModuleConfig(class, class.Vectorizer, "")

	p.setPerClassConfigDefaults(class, cfg, cc)
	p.setPerPropertyConfigDefaults(class, cfg, cc)
}

// SetSinglePropertyDefaults can be used when a property is added later, e.g.
// as part of merging in a ref prop after a class has already been created
func (p *Provider) SetSinglePropertyDefaults(class *models.Class,
	prop *models.Property,
) {
	if class.Vectorizer == "none" {
		// the class does not use a vectorizer, nothing to do for us
		return
	}

	mod := p.GetByName(class.Vectorizer)
	cc, ok := mod.(modulecapabilities.ClassConfigurator)
	if !ok {
		// the module exists, but is not a class configurator, nothing to do for us
		return
	}

	p.setSinglePropertyConfigDefaults(class, prop, cc)
}

func (p *Provider) setPerClassConfigDefaults(class *models.Class,
	cfg *ClassBasedModuleConfig, cc modulecapabilities.ClassConfigurator,
) {
	modDefaults := cc.ClassConfigDefaults()
	userSpecified := cfg.Class()
	mergedConfig := map[string]interface{}{}

	for key, value := range modDefaults {
		mergedConfig[key] = value
	}
	for key, value := range userSpecified {
		mergedConfig[key] = value
	}

	if class.ModuleConfig == nil {
		class.ModuleConfig = map[string]interface{}{}
	}

	class.ModuleConfig.(map[string]interface{})[class.Vectorizer] = mergedConfig
}

func (p *Provider) setPerPropertyConfigDefaults(class *models.Class,
	cfg *ClassBasedModuleConfig, cc modulecapabilities.ClassConfigurator,
) {
	for _, prop := range class.Properties {
		p.setSinglePropertyConfigDefaults(class, prop, cc)
	}
}

func (p *Provider) setSinglePropertyConfigDefaults(class *models.Class,
	prop *models.Property, cc modulecapabilities.ClassConfigurator,
) {
	dt, _ := schema.GetValueDataTypeFromString(prop.DataType[0])
	modDefaults := cc.PropertyConfigDefaults(dt)
	mergedConfig := map[string]interface{}{}
	userSpecified := make(map[string]interface{})

	if prop.ModuleConfig != nil {
		userSpecified = prop.ModuleConfig.(map[string]interface{})[class.Vectorizer].(map[string]interface{})
	}

	for key, value := range modDefaults {
		mergedConfig[key] = value
	}
	for key, value := range userSpecified {
		mergedConfig[key] = value
	}

	if prop.ModuleConfig == nil {
		prop.ModuleConfig = map[string]interface{}{}
	}

	prop.ModuleConfig.(map[string]interface{})[class.Vectorizer] = mergedConfig
}

func (p *Provider) ValidateClass(ctx context.Context, class *models.Class) error {
	if class.Vectorizer == "none" {
		// the class does not use a vectorizer, nothing to do for us
		return nil
	}

	moduleConfig, ok := class.ModuleConfig.(map[string]interface{})
	if !ok {
		return nil
	}
	for key := range moduleConfig {
		mod := p.GetByName(key)
		cc, ok := mod.(modulecapabilities.ClassConfigurator)
		if !ok {
			// the module exists, but is not a class configurator, nothing to do for us
			return nil
		}

		cfg := NewClassBasedModuleConfig(class, key, "")
		err := cc.ValidateClass(ctx, class, cfg)
		if err != nil {
			return errors.Wrapf(err, "module '%s'", key)
		}
	}

	return nil
}
