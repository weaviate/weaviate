//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modules

import (
	"context"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/schema"
)

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

	cfg := NewClassBasedModuleConfig(class, class.Vectorizer)

	p.setPerClassConfigDefaults(class, cfg, cc)
	p.setPerPropertyConfigDefaults(class, cfg, cc)
}

func (p *Provider) setPerClassConfigDefaults(class *models.Class,
	cfg *ClassBasedModuleConfig, cc modulecapabilities.ClassConfigurator) {
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
	cfg *ClassBasedModuleConfig, cc modulecapabilities.ClassConfigurator) {
	for _, prop := range class.Properties {
		dt, _ := schema.GetPropertyDataType(class, prop.Name)
		modDefaults := cc.PropertyConfigDefaults(dt)
		userSpecified := cfg.Property(prop.Name)
		mergedConfig := map[string]interface{}{}

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
}

func (p *Provider) ValidateClass(ctx context.Context, class *models.Class) error {
	if class.Vectorizer == "none" {
		// the class does not use a vectorizer, nothing to do for us
		return nil
	}

	mod := p.GetByName(class.Vectorizer)
	cc, ok := mod.(modulecapabilities.ClassConfigurator)
	if !ok {
		// the module exists, but is not a class configurator, nothing to do for us
		return nil
	}

	cfg := NewClassBasedModuleConfig(class, class.Vectorizer)
	err := cc.ValidateClass(ctx, class, cfg)
	if err != nil {
		return errors.Wrapf(err, "module '%s'", class.Vectorizer)
	}

	return nil
}
