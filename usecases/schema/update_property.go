//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"
	"fmt"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// UpdateActionProperty of an existing Action Property
func (m *Manager) UpdateActionProperty(ctx context.Context, principal *models.Principal,
	class string, name string, property *models.Property) error {

	err := m.authorizer.Authorize(principal, "update", "schema/actions")
	if err != nil {
		return err
	}

	return m.updateClassProperty(ctx, class, name, property, kind.Action)
}

// UpdateThingProperty of an existing Thing Property
func (m *Manager) UpdateThingProperty(ctx context.Context, principal *models.Principal,
	class string, name string, property *models.Property) error {

	err := m.authorizer.Authorize(principal, "update", "schema/things")
	if err != nil {
		return err
	}
	return m.updateClassProperty(ctx, class, name, property, kind.Thing)
}

// TODO: gh-832: Implement full capabilities, not just keywords/naming
func (m *Manager) updateClassProperty(ctx context.Context, className string, name string,
	property *models.Property, k kind.Kind) error {
	unlock, err := m.locks.LockSchema()
	if err != nil {
		return err
	}
	defer unlock()

	var newName *string
	var newKeywords *models.Keywords

	if property.Name != name {
		// the name in the URI and body don't match, so we assume the user wants to rename
		n := lowerCaseFirstLetter(property.Name)
		newName = &n
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
	err = m.validatePropertyNameAndKeywords(ctx, className, propNameAfterUpdate, keywordsAfterUpdate)
	if err != nil {
		return err
	}

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
func (m *Manager) UpdatePropertyAddDataType(ctx context.Context, principal *models.Principal,
	kind kind.Kind, className string, propName string, newDataType string) error {

	err := m.authorizer.Authorize(principal, "update", fmt.Sprintf("schema/%ss", kind.Name()))
	if err != nil {
		return err
	}

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
