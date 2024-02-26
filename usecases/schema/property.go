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

func (h *Handler) UpdateClassProperty(ctx context.Context, principal *models.Principal,
	class *models.Class, merge bool, newProps ...*models.Property,
) error {
	err := h.Authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	// validate new props
	for _, prop := range newProps {
		if prop.Name == "" {
			return fmt.Errorf("property must contain name")
		}
		prop.Name = schema.LowercaseFirstLetter(prop.Name)
		if prop.DataType == nil {
			return fmt.Errorf("property must contain dataType")
		}
	}

	existingNames := map[string]bool{}
	for _, p := range class.Properties {
		existingNames[strings.ToLower(p.Name)] = true
	}

	// normalize the new props
	for _, p := range newProps {
		_, ex := existingNames[strings.ToLower(p.Name)]
		existingNames[strings.ToLower(p.Name)] = ex
	}

	if err := h.setNewPropDefaults(class, newProps...); err != nil {
		return err
	}

	if merge {
		// clear in case if we want to merge
		if err := h.validateProperty(class.Class, map[string]bool{}, false, newProps...); err != nil {
			return err
		}
	} else {
		if err := h.validateProperty(class.Class, existingNames, false, newProps...); err != nil {
			return err
		}
	}

	migratePropertySettings(newProps...)

	// TODO-RAFT use UpdateProperty() for adding/merging property when index idempotence exists
	// we don't support updating property in index atm,
	// h.metaWriter.UpdateClass() and h.metaWriter.AddProperty() should be replaced by h.metaWriter.UpdateProperty
	new, old := split(class.Properties, newProps)
	if len(old) > 0 {
		for _, p := range old {
			if existingNames[strings.ToLower(p.Name)] {
				if err = mergeObjectProperty(class, p); err != nil {
					return err
				}
			}
		}
		if err = h.metaWriter.UpdateClass(class, nil); err != nil {
			return err
		}
	}

	if len(new) == 0 {
		return nil
	}
	return h.metaWriter.AddProperty(class.Class, new...)
}

// split does split the passed properties based in their existence
// it shouldn't be needed once we have idempotence on Add/Update Property
// it's used to mark shall the index be updated or not
func split(old, new []*models.Property) (propertiesToAdd, propertiesToUpdate []*models.Property) {
	exPropMap := make(map[string]int, len(old))
	for index := range old {
		exPropMap[old[index].Name] = index
	}

	for _, prop := range new {
		index, exists := exPropMap[schema.LowercaseFirstLetter(prop.Name)]
		if !exists {
			propertiesToAdd = append(propertiesToAdd, prop)
		} else if _, isNested := schema.AsNested(old[index].DataType); isNested {
			mergedNestedProperties, merged := schema.MergeRecursivelyNestedProperties(old[index].NestedProperties,
				prop.NestedProperties)
			if merged {
				prop.NestedProperties = mergedNestedProperties
				propertiesToUpdate = append(propertiesToUpdate, prop)
			}
		}
	}
	return
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

func (h *Handler) setNewPropDefaults(class *models.Class, props ...*models.Property) error {
	setPropertyDefaults(props...)
	if err := validateUserProp(class, props...); err != nil {
		return err
	}

	for _, prop := range props {
		h.moduleConfig.SetSinglePropertyDefaults(class, prop)
	}
	return nil
}

func validateUserProp(class *models.Class, props ...*models.Property) error {
	for _, prop := range props {
		if prop.ModuleConfig == nil {
			continue
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
