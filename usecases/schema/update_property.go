//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

// UpdateClassProperty of an existing Object Property
func (m *Manager) UpdateClassProperty(ctx context.Context, principal *models.Principal,
	class string, name string, property *models.Property) error {
	err := m.authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	return m.updateClassProperty(ctx, class, name, property)
}

// TODO: gh-832: Implement full capabilities, not just keywords/naming
func (m *Manager) updateClassProperty(ctx context.Context, className string, name string,
	property *models.Property) error {
	m.Lock()
	defer m.Unlock()

	var newName *string

	if property.Name != name {
		// the name in the URI and body don't match, so we assume the user wants to rename
		n := lowerCaseFirstLetter(property.Name)
		newName = &n
	}

	semanticSchema := m.state.SchemaFor()
	class, err := schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	prop, err := schema.GetPropertyByName(class, name)
	if err != nil {
		return err
	}

	propNameAfterUpdate := name
	if newName != nil {
		// verify uniqueness
		err = validatePropertyNameUniqueness(*newName, class)
		propNameAfterUpdate = *newName
		if err != nil {
			return err
		}
	}

	// Validate name / keywords in contextionary
	err = m.validatePropertyName(ctx, className, propNameAfterUpdate,
		prop.ModuleConfig)
	if err != nil {
		return err
	}

	// Validated! Now apply the changes.
	prop.Name = propNameAfterUpdate

	err = m.saveSchema(ctx)
	if err != nil {
		return nil
	}

	return m.migrator.UpdateProperty(ctx, className, name, newName)
}

// UpdatePropertyAddDataType adds another data type to a property. Warning: It does not lock on its own, assumes that it is called from when a schema lock is already held!
func (m *Manager) UpdatePropertyAddDataType(ctx context.Context, principal *models.Principal,
	className string, propName string, newDataType string) error {
	err := m.authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	semanticSchema := m.state.SchemaFor()
	class, err := schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	prop, err := schema.GetPropertyByName(class, propName)
	if err != nil {
		return err
	}

	if dataTypeAlreadyContained(prop.DataType, newDataType) {
		return nil
	}

	prop.DataType = append(prop.DataType, newDataType)
	err = m.saveSchema(ctx)
	if err != nil {
		return nil
	}

	return m.migrator.UpdatePropertyAddDataType(ctx, className, propName, newDataType)
}

func dataTypeAlreadyContained(haystack []string, needle string) bool {
	for _, hay := range haystack {
		if hay == needle {
			return true
		}
	}
	return false
}
