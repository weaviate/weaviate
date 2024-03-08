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

package schema

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

var errPropertyNotFound = errors.New("property not found")

// AddClassProperty to an existing Class
func (h *Handler) AddClassProperty(ctx context.Context, principal *models.Principal,
	class string, prop *models.Property,
) error {
	err := h.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	if prop.Name == "" {
		return fmt.Errorf("property must contain name")
	}
	prop.Name = schema.LowercaseFirstLetter(prop.Name)
	if prop.DataType == nil {
		return fmt.Errorf("property must contain dataType")
	}

	cls := h.metaReader.ReadOnlyClass(class)
	if cls == nil {
		return fmt.Errorf("class %q: %w", class, ErrNotFound)
	}

	existingPropertyNames := map[string]bool{}
	for _, existingProperty := range cls.Properties {
		existingPropertyNames[strings.ToLower(existingProperty.Name)] = true
	}

	h.setNewPropDefaults(cls, prop)
	if err := h.validateProperty(prop, cls, existingPropertyNames, false); err != nil {
		return err
	}
	// migrate only after validation in completed
	migratePropertySettings(prop)
	return h.metaWriter.AddProperty(class, prop)
}

// DeleteClassProperty from existing Schema
func (h *Handler) DeleteClassProperty(ctx context.Context, principal *models.Principal,
	class string, property string,
) error {
	err := h.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	return fmt.Errorf("deleting a property is currently not supported, see " +
		"https://github.com/weaviate/weaviate/issues/973 for details.")
	// return h.deleteClassProperty(ctx, class, property, kind.Action)
}

func (m *Handler) setNewPropDefaults(class *models.Class, prop *models.Property) {
	setPropertyDefaults(prop)
	m.moduleConfig.SetSinglePropertyDefaults(class, prop)
}

func (m *Handler) validatePropModuleConfig(class *models.Class, prop *models.Property) error {
	if prop.ModuleConfig == nil {
		return nil
	}
	modconfig, ok := prop.ModuleConfig.(map[string]interface{})
	if !ok {
		return fmt.Errorf("%v property config invalid", prop.Name)
	}

	if !hasTargetVectors(class) {
		configuredVectorizers := make([]string, 0, len(modconfig))
		for modName := range modconfig {
			if err := m.vectorizerValidator.ValidateVectorizer(modName); err == nil {
				configuredVectorizers = append(configuredVectorizers, modName)
			}
		}
		if len(configuredVectorizers) > 1 {
			return fmt.Errorf("multiple vectorizers configured in property's %q moduleConfig: %v. class.vectorizer is set to %q",
				prop.Name, configuredVectorizers, class.Vectorizer)
		}

		vectorizerConfig, ok := modconfig[class.Vectorizer]
		if !ok {
			if class.Vectorizer == "none" {
				return nil
			}
			return fmt.Errorf("%v vectorizer module not part of the property", class.Vectorizer)
		}
		_, ok = vectorizerConfig.(map[string]interface{})
		if !ok {
			return fmt.Errorf("vectorizer config for vectorizer %v, not of type map[string]interface{}", class.Vectorizer)
		}
		return nil
	}

	// TODO reuse for multiple props?
	vectorizersSet := map[string]struct{}{}
	for _, cfg := range class.VectorConfig {
		if vm, ok := cfg.Vectorizer.(map[string]interface{}); ok && len(vm) == 1 {
			for vectorizer := range vm {
				vectorizersSet[vectorizer] = struct{}{}
			}
		}
	}
	for vectorizer, cfg := range modconfig {
		if _, ok := vectorizersSet[vectorizer]; !ok {
			return fmt.Errorf("vectorizer %q not configured for any of target vectors", vectorizer)
		}
		if _, ok := cfg.(map[string]interface{}); !ok {
			return fmt.Errorf("vectorizer config for vectorizer %q not of type map[string]interface{}", vectorizer)
		}
	}

	return nil
}

func mergeObjectProperty(c *models.Class, p *models.Property) error {
	var prop *models.Property
	for i := range c.Properties {
		if c.Properties[i].Name == p.Name {
			prop = c.Properties[i]
			break
		}
	}
	if prop == nil {
		return errPropertyNotFound
	}

	prop.NestedProperties, _ = schema.MergeRecursivelyNestedProperties(prop.NestedProperties, p.NestedProperties)
	return nil
}

// MergeClassObjectProperty of an existing Class
// Merges NestedProperties of incoming object/object[] property into existing one
func (h *Handler) MergeClassObjectProperty(
	ctx context.Context,
	principal *models.Principal,
	className string,
	prop *models.Property,
) error {
	err := h.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	class := h.metaReader.ReadOnlyClass(className)
	prop.Name = schema.LowercaseFirstLetter(prop.Name)

	// reuse setDefaults/validation/migrate methods coming from add property
	// (empty existing names map, to validate existing updated property)
	// TODO nested - refactor / cleanup setDefaults/validation/migrate methods
	h.setNewPropDefaults(class, prop)
	if err := h.validateProperty(prop, class, map[string]bool{}, false); err != nil {
		return err
	}

	// migrate only after validation in completed
	migratePropertySettings(prop)

	err = mergeObjectProperty(class, prop)
	if err != nil {
		return err
	}

	h.logger.WithField("action", "schema.update_object_property").Debugf("updating class %s after property merge", className)
	err = h.metaWriter.UpdateClass(class, nil)
	if err != nil {
		return err
	}

	// TODO: implement MergeObjectProperty (needed for indexing/filtering)
	// will result in a mismatch between schema and index if function below fails
	// return h.migrator.MergeObjectProperty(ctx, className, prop)
	return nil
}
