//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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

	existing := map[string]bool{}
	for _, p := range cls.Properties {
		existing[strings.ToLower(p.Name)] = true
	}

	if err := h.setNewPropDefaults(cls, prop); err != nil {
		return err
	}
	if err := h.validateProperty(prop, class, existing, false); err != nil {
		return err
	}
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

	/// TODO-RAFT START
	/// Implement RAFT based DeleteClassProperty
	/// TODO-RAFT END

	return fmt.Errorf("deleting a property is currently not supported, see " +
		"https://github.com/weaviate/weaviate/issues/973 for details.")
	// return h.deleteClassProperty(ctx, class, property, kind.Action)
}

func (h *Handler) setNewPropDefaults(class *models.Class, prop *models.Property) error {
	setPropertyDefaults(prop)
	if err := validateUserProp(class, prop); err != nil {
		return err
	}
	h.moduleConfig.SetSinglePropertyDefaults(class, prop)
	return nil
}

func validateUserProp(class *models.Class, prop *models.Property) error {
	if prop.ModuleConfig == nil {
		return nil
	} else {
		modconfig, ok := prop.ModuleConfig.(map[string]interface{})
		if !ok {
			return fmt.Errorf("%v property config invalid", prop.Name)
		}
		vectorizerConfig, ok := modconfig[class.Vectorizer]
		if !ok {
			return fmt.Errorf("%v vectorizer module not part of the property", class.Vectorizer)
		}
		_, ok = vectorizerConfig.(map[string]interface{})
		if !ok {
			return fmt.Errorf("vectorizer config for vectorizer %v, not of type map[string]interface{}", class.Vectorizer)
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
	if err := h.setNewPropDefaults(class, prop); err != nil {
		return err
	}
	if err := h.validateProperty(prop, className, map[string]bool{}, false); err != nil {
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
