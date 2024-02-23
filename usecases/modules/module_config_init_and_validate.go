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

	cfg := NewClassBasedModuleConfig(class, class.Vectorizer, "", "")

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
	switch len(class.VectorConfig) {
	case 0:
		// legacy configuration
		if class.Vectorizer == "none" {
			// the class does not use a vectorizer, nothing to do for us
			return nil
		}
		if err := p.validateClassesModuleConfig(ctx, class, "", class.ModuleConfig); err != nil {
			return err
		}
		return nil
	default:
		// named vectors configuration
		for targetVector, vectorConfig := range class.VectorConfig {
			if len(targetVector) > schema.TargetVectorNameMaxLength {
				return errors.Errorf("class.VectorConfig target vector name %q is not valid. "+
					"Target vector name should not be longer than %d characters.",
					targetVector, schema.TargetVectorNameMaxLength)
			}
			if !p.targetVectorNameValidator.MatchString(targetVector) {
				return errors.Errorf("class.VectorConfig target vector name %q is not valid, "+
					"in Weaviate target vector names are restricted to valid GraphQL names, "+
					"which must be “/%s/”.", targetVector, schema.TargetVectorNameRegex)
			}
			vectorizer, ok := vectorConfig.Vectorizer.(map[string]interface{})
			if !ok {
				return errors.Errorf("class.VectorConfig.Vectorizer must be an object, got %T", vectorConfig.Vectorizer)
			}
			if len(vectorizer) != 1 {
				return errors.Errorf("class.VectorConfig.Vectorizer must consist only 1 configuration, got: %v", len(vectorizer))
			}
			for modName := range vectorizer {
				if modName == "none" {
					// the class does not use a vectorizer, nothing to do for us
					return nil
				}
				if mod := p.GetByName(modName); mod == nil {
					return errors.Errorf("class.VectorConfig.Vectorizer module with name %s doesn't exist", modName)
				}
				if err := p.validateClassModuleConfig(ctx, class, modName, targetVector); err != nil {
					return err
				}
			}
		}
		// check module config configuration in case that there are other none vectorizer modules defined
		if err := p.validateClassesModuleConfigNoneVectorizers(ctx, class, "", class.ModuleConfig); err != nil {
			return err
		}
		return nil
	}
}

func (p *Provider) validateClassesModuleConfigNoneVectorizers(ctx context.Context,
	class *models.Class, targetVector string, moduleConfig interface{},
) error {
	modConfig, ok := moduleConfig.(map[string]interface{})
	if !ok {
		return nil
	}
	for modName := range modConfig {
		mod := p.GetByName(modName)
		if !p.isVectorizerModule(mod.Type()) {
			if err := p.validateClassModuleConfig(ctx, class, modName, ""); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Provider) validateClassesModuleConfig(ctx context.Context,
	class *models.Class, targetVector string, moduleConfig interface{},
) error {
	modConfig, ok := moduleConfig.(map[string]interface{})
	if !ok {
		return nil
	}
	for modName := range modConfig {
		if err := p.validateClassModuleConfig(ctx, class, modName, ""); err != nil {
			return err
		}
	}
	return nil
}

func (p *Provider) validateClassModuleConfig(ctx context.Context,
	class *models.Class, moduleName, targetVector string,
) error {
	mod := p.GetByName(moduleName)
	cc, ok := mod.(modulecapabilities.ClassConfigurator)
	if !ok {
		// the module exists, but is not a class configurator, nothing to do for us
		return nil
	}

	cfg := NewClassBasedModuleConfig(class, moduleName, "", targetVector)
	err := cc.ValidateClass(ctx, class, cfg)
	if err != nil {
		return errors.Wrapf(err, "module '%s'", moduleName)
	}
	return nil
}
