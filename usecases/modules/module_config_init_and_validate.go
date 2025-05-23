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
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modelsext"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/schema"
)

// SetClassDefaults sets the module-specific defaults for the class itself, but
// also for each prop
func (p *Provider) SetClassDefaults(class *models.Class) {
	if modelsext.ClassHasLegacyVectorIndex(class) || len(class.VectorConfig) == 0 {
		p.setClassDefaults(class, class.Vectorizer, "", func(vectorizerConfig map[string]interface{}) {
			if class.ModuleConfig == nil {
				class.ModuleConfig = map[string]interface{}{}
			}
			class.ModuleConfig.(map[string]interface{})[class.Vectorizer] = vectorizerConfig
		})
	}

	for targetVector, vectorConfig := range class.VectorConfig {
		if moduleConfig, ok := vectorConfig.Vectorizer.(map[string]interface{}); ok && len(moduleConfig) == 1 {
			for vectorizer := range moduleConfig {
				p.setClassDefaults(class, vectorizer, targetVector, func(vectorizerConfig map[string]interface{}) {
					moduleConfig[vectorizer] = vectorizerConfig
				})
			}
		}
	}
}

func (p *Provider) setClassDefaults(class *models.Class, vectorizer string,
	targetVector string, storeFn func(vectorizerConfig map[string]interface{}),
) {
	if vectorizer == "none" {
		// the class does not use a vectorizer, nothing to do for us
		return
	}

	mod := p.GetByName(vectorizer)
	cc, ok := mod.(modulecapabilities.ClassConfigurator)
	if !ok {
		// the module exists, but is not a class configurator, nothing to do for us
		return
	}

	cfg := NewClassBasedModuleConfig(class, vectorizer, "", targetVector)

	p.setPerClassConfigDefaults(cfg, cc, storeFn)
	for _, prop := range class.Properties {
		p.setSinglePropertyConfigDefaults(prop, vectorizer, cc)
	}
}

func (p *Provider) setPerClassConfigDefaults(cfg *ClassBasedModuleConfig,
	cc modulecapabilities.ClassConfigurator,
	storeFn func(vectorizerConfig map[string]interface{}),
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

	if len(mergedConfig) > 0 {
		storeFn(mergedConfig)
	}
}

// SetSinglePropertyDefaults can be used when a property is added later, e.g.
// as part of merging in a ref prop after a class has already been created
func (p *Provider) SetSinglePropertyDefaults(class *models.Class,
	props ...*models.Property,
) {
	for _, prop := range props {
		if modelsext.ClassHasLegacyVectorIndex(class) || len(class.VectorConfig) == 0 {
			p.setSinglePropertyDefaults(prop, class.Vectorizer)
		}

		for _, vectorConfig := range class.VectorConfig {
			if moduleConfig, ok := vectorConfig.Vectorizer.(map[string]interface{}); ok && len(moduleConfig) == 1 {
				for vectorizer := range moduleConfig {
					p.setSinglePropertyDefaults(prop, vectorizer)
				}
			}
		}
	}
}

func (p *Provider) setSinglePropertyDefaults(prop *models.Property, vectorizer string) {
	if vectorizer == "none" {
		// the class does not use a vectorizer, nothing to do for us
		return
	}

	mod := p.GetByName(vectorizer)
	cc, ok := mod.(modulecapabilities.ClassConfigurator)
	if !ok {
		// the module exists, but is not a class configurator, nothing to do for us
		return
	}

	p.setSinglePropertyConfigDefaults(prop, vectorizer, cc)
}

func (p *Provider) setSinglePropertyConfigDefaults(prop *models.Property,
	vectorizer string, cc modulecapabilities.ClassConfigurator,
) {
	dt, _ := schema.GetValueDataTypeFromString(prop.DataType[0])
	modDefaults := cc.PropertyConfigDefaults(dt)
	userSpecified := map[string]interface{}{}
	mergedConfig := map[string]interface{}{}

	if prop.ModuleConfig != nil {
		if vectorizerConfig, ok := prop.ModuleConfig.(map[string]interface{})[vectorizer]; ok {
			if mcvm, ok := vectorizerConfig.(map[string]interface{}); ok {
				userSpecified = mcvm
			}
		}
	}

	for key, value := range modDefaults {
		mergedConfig[key] = value
	}
	for key, value := range userSpecified {
		mergedConfig[key] = value
	}

	if len(mergedConfig) > 0 {
		if prop.ModuleConfig == nil {
			prop.ModuleConfig = map[string]interface{}{}
		}
		prop.ModuleConfig.(map[string]interface{})[vectorizer] = mergedConfig
	}
}

func (p *Provider) ValidateClass(ctx context.Context, class *models.Class) error {
	switch len(class.VectorConfig) {
	case 0:
		// legacy configuration
		if class.Vectorizer == "none" {
			// the class does not use a vectorizer, nothing to do for us
			return nil
		}
		if err := p.validateClassesModuleConfig(ctx, class, class.ModuleConfig); err != nil {
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
					continue
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
		if mod == nil {
			return errors.Errorf("module with name %s doesn't exist", modName)
		}
		if !p.isVectorizerModule(mod.Type()) {
			if err := p.validateClassModuleConfig(ctx, class, modName, ""); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *Provider) validateClassesModuleConfig(ctx context.Context,
	class *models.Class, moduleConfig interface{},
) error {
	modConfig, ok := moduleConfig.(map[string]interface{})
	if !ok {
		return nil
	}
	configuredVectorizers := make([]string, 0, len(modConfig))
	for modName := range modConfig {
		if err := p.validateClassModuleConfig(ctx, class, modName, ""); err != nil {
			return err
		}
		if err := p.ValidateVectorizer(modName); err == nil {
			configuredVectorizers = append(configuredVectorizers, modName)
		}
	}
	if len(configuredVectorizers) > 1 {
		return fmt.Errorf("multiple vectorizers configured in class's moduleConfig: %v. class.vectorizer is set to %q",
			configuredVectorizers, class.Vectorizer)
	}
	if len(configuredVectorizers) == 1 && p.IsMultiVector(configuredVectorizers[0]) {
		return fmt.Errorf("multi vector vectorizer: %q is only allowed to be defined using named vector configuration", configuredVectorizers[0])
	}
	if p.IsMultiVector(class.Vectorizer) {
		return fmt.Errorf("multi vector vectorizer: %q is only allowed to be defined using named vector configuration", class.Vectorizer)
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

	p.validateVectorConfig(class, moduleName, targetVector)

	return nil
}

func (p *Provider) validateVectorConfig(class *models.Class, moduleName string, targetVector string) {
	mod := p.GetByName(moduleName)

	if class.VectorConfig == nil || !p.implementsVectorizer(mod) {
		return
	}

	// named vector props need to be a string array
	props, ok := class.VectorConfig[targetVector].Vectorizer.(map[string]interface{})[moduleName].(map[string]interface{})["properties"]
	if ok {
		propsTyped := make([]string, len(props.([]interface{})))
		for i, v := range props.([]interface{}) {
			propsTyped[i] = v.(string) // was validated by the module
		}
		class.VectorConfig[targetVector].Vectorizer.(map[string]interface{})[moduleName].(map[string]interface{})["properties"] = propsTyped
	}
}
