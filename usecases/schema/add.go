//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package schema

import (
	"context"
	"fmt"
	"strings"

	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
)

// AddAction Class to the schema
func (m *Manager) AddAction(ctx context.Context, principal *models.Principal,
	class *models.Class) error {

	err := m.authorizer.Authorize(principal, "create", "schema/actions")
	if err != nil {
		return err
	}

	return m.addClass(ctx, principal, class, kind.Action)
}

// AddThing Class to the schema
func (m *Manager) AddThing(ctx context.Context, principal *models.Principal,
	class *models.Class) error {

	err := m.authorizer.Authorize(principal, "create", "schema/things")
	if err != nil {
		return err
	}

	return m.addClass(ctx, principal, class, kind.Thing)
}

func (m *Manager) addClass(ctx context.Context, principal *models.Principal,
	class *models.Class, k kind.Kind) error {
	unlock, err := m.locks.LockSchema()
	if err != nil {
		return err
	}
	defer unlock()

	class.Class = upperCaseClassName(class.Class)
	class.Properties = lowerCaseAllPropertyNames(class.Properties)

	err = m.validateCanAddClass(ctx, principal, k, class)
	if err != nil {
		return err
	}

	semanticSchema := m.state.SchemaFor(k)
	semanticSchema.Classes = append(semanticSchema.Classes, class)
	err = m.saveSchema(ctx)
	if err != nil {
		return err
	}

	return m.migrator.AddClass(ctx, k, class)
	// TODO gh-846: Rollback state upate if migration fails
}

func (m *Manager) validateCanAddClass(ctx context.Context, principal *models.Principal, knd kind.Kind, class *models.Class) error {
	// First check if there is a name clash.
	err := m.validateClassNameUniqueness(class.Class)
	if err != nil {
		return err
	}

	err = m.validateClassNameAndKeywords(ctx, knd, class.Class, class.Keywords, class.VectorizeClassName)
	if err != nil {
		return err
	}

	// Check properties
	foundNames := map[string]bool{}
	for _, property := range class.Properties {
		err = m.validatePropertyNameAndKeywords(ctx, class.Class, property.Name, property.Keywords)
		if err != nil {
			return err
		}

		if foundNames[property.Name] == true {
			return fmt.Errorf("name '%s' already in use as a property name for class '%s'", property.Name, class.Class)
		}

		foundNames[property.Name] = true

		// Validate data type of property.
		schema, err := m.GetSchema(principal)
		if err != nil {
			return err
		}

		_, err = (&schema).FindPropertyDataType(property.DataType)
		if err != nil {
			return fmt.Errorf("property '%s': invalid dataType: %v", property.Name, err)
		}
	}

	// all is fine!
	return nil
}

func upperCaseClassName(name string) string {
	if len(name) < 1 {
		return name
	}

	if len(name) == 1 {
		return strings.ToUpper(name)
	}

	return strings.ToUpper(string(name[0])) + name[1:]
}

func lowerCaseAllPropertyNames(props []*models.Property) []*models.Property {
	for i, prop := range props {
		props[i].Name = lowerCaseFirstLetter(prop.Name)
	}

	return props
}

func lowerCaseFirstLetter(name string) string {
	if len(name) < 1 {
		return name
	}

	if len(name) == 1 {
		return strings.ToLower(name)
	}

	return strings.ToLower(string(name[0])) + name[1:]
}
