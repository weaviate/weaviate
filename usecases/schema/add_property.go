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

// AddActionProperty to an existing Action
func (m *Manager) AddActionProperty(ctx context.Context, principal *models.Principal,
	class string, property *models.Property) error {

	err := m.authorizer.Authorize(principal, "update", "schema/actions")
	if err != nil {
		return err
	}

	return m.addClassProperty(ctx, principal, class, property, kind.Action)
}

// AddThingProperty to an existing Thing
func (m *Manager) AddThingProperty(ctx context.Context, principal *models.Principal,
	class string, property *models.Property) error {

	err := m.authorizer.Authorize(principal, "update", "schema/things")
	if err != nil {
		return err
	}

	return m.addClassProperty(ctx, principal, class, property, kind.Thing)
}

func (m *Manager) addClassProperty(ctx context.Context, principal *models.Principal, className string,
	prop *models.Property, k kind.Kind) error {
	unlock, err := m.locks.LockSchema()
	if err != nil {
		return err
	}
	defer unlock()

	semanticSchema := m.state.SchemaFor(k)
	class, err := schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	prop.Name = lowerCaseFirstLetter(prop.Name)

	err = m.validateCanAddProperty(ctx, principal, prop, class)
	if err != nil {
		return err
	}

	class.Properties = append(class.Properties, prop)

	err = m.saveSchema(ctx)

	if err != nil {
		return nil
	}

	return m.migrator.AddProperty(ctx, k, className, prop)
}

func (m *Manager) validateCanAddProperty(ctx context.Context, principal *models.Principal,
	property *models.Property, class *models.Class) error {
	// Verify format of property.
	_, err := schema.ValidatePropertyName(property.Name)
	if err != nil {
		return err
	}

	// First check if there is a name clash.
	err = validatePropertyNameUniqueness(property.Name, class)
	if err != nil {
		return err
	}

	err = m.validatePropertyNameAndKeywords(ctx, class.Class, property.Name, property.Keywords)
	if err != nil {
		return err
	}

	// Validate data type of property.
	schema, err := m.GetSchema(principal)
	if err != nil {
		return err
	}

	_, err = (&schema).FindPropertyDataType(property.DataType)
	if err != nil {
		return fmt.Errorf("Data type of property '%s' is invalid; %v", property.Name, err)
	}

	if err = m.validateNetworkCrossRefs(property.DataType); err != nil {
		return fmt.Errorf("Data type of property '%s' is invalid; %v", property.Name, err)
	}

	// all is fine!
	return nil
}
