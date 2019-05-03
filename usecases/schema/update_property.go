/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package schema

import (
	"context"

	"github.com/creativesoftwarefdn/weaviate/entities/models"
	"github.com/creativesoftwarefdn/weaviate/entities/schema"
	"github.com/creativesoftwarefdn/weaviate/entities/schema/kind"
)

// UpdateActionProperty of an existing Action Property
func (m *Manager) UpdateActionProperty(ctx context.Context, class string, name string,
	property *models.SemanticSchemaClassProperty) error {
	return m.updateClassProperty(ctx, class, name, property, kind.Action)
}

// UpdateThingProperty of an existing Thing Property
func (m *Manager) UpdateThingProperty(ctx context.Context, class string, name string,
	property *models.SemanticSchemaClassProperty) error {
	return m.updateClassProperty(ctx, class, name, property, kind.Thing)
}

// TODO: gh-832: Implement full capabilities, not just keywords/naming
func (m *Manager) updateClassProperty(ctx context.Context, className string, name string,
	property *models.SemanticSchemaClassProperty, k kind.Kind) error {
	unlock, err := m.locks.LockSchema()
	if err != nil {
		return err
	}
	defer unlock()

	var newName *string
	var newKeywords *models.SemanticSchemaKeywords

	if property.Name != name {
		// the name in the URI and body don't match, so we assume the user wants to rename
		newName = &property.Name
	}

	// TODO gh-619: This implies that we can't undo setting keywords, because we can't detect if keywords is not present, or empty.
	if len(property.Keywords) > 0 {
		newKeywords = &property.Keywords
	}

	semanticSchema := m.state.SchemaFor(k)
	class, err := schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	prop, err := schema.GetPropertyByName(class, name)
	if err != nil {
		return err
	}

	propNameAfterUpdate := name
	keywordsAfterUpdate := prop.Keywords
	if newName != nil {
		// verify uniqueness
		err = validatePropertyNameUniqueness(*newName, class)
		propNameAfterUpdate = *newName
		if err != nil {
			return err
		}
	}

	if newKeywords != nil {
		keywordsAfterUpdate = *newKeywords
	}

	// Validate name / keywords in contextionary
	m.validatePropertyNameAndKeywords(className, propNameAfterUpdate, keywordsAfterUpdate)

	// Validated! Now apply the changes.
	prop.Name = propNameAfterUpdate
	prop.Keywords = keywordsAfterUpdate

	err = m.saveSchema(ctx)
	if err != nil {
		return nil
	}

	return m.migrator.UpdateProperty(ctx, k, className, name, newName, newKeywords)
}

// UpdatePropertyAddDataType adds another data type to a property. Warning: It does not lock on its own, assumes that it is called from when a schema lock is already held!
func (m *Manager) UpdatePropertyAddDataType(ctx context.Context, kind kind.Kind, className string, propName string, newDataType string) error {
	semanticSchema := m.state.SchemaFor(kind)
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

	return m.migrator.UpdatePropertyAddDataType(ctx, kind, className, propName, newDataType)
}

func dataTypeAlreadyContained(haystack []string, needle string) bool {
	for _, hay := range haystack {
		if hay == needle {
			return true
		}
	}
	return false
}
