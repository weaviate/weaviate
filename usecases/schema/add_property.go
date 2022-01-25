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
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
)

// AddClassProperty to an existing Class
func (m *Manager) AddClassProperty(ctx context.Context, principal *models.Principal,
	class string, property *models.Property) error {
	err := m.authorizer.Authorize(principal, "update", "schema/objects")
	if err != nil {
		return err
	}

	return m.addClassProperty(ctx, principal, class, property)
}

func (m *Manager) addClassProperty(ctx context.Context, principal *models.Principal, className string,
	prop *models.Property) error {
	m.Lock()
	defer m.Unlock()

	semanticSchema := m.state.SchemaFor()
	class, err := schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	prop.Name = lowerCaseFirstLetter(prop.Name)

	err = m.validateCanAddProperty(ctx, principal, prop, class)
	if err != nil {
		return err
	}

	m.setNewPropDefaults(class, prop)

	tx, err := m.cluster.BeginTransaction(ctx, AddProperty,
		AddPropertyPayload{className, prop})
	if err != nil {
		// possible causes for errors could be nodes down (we expect every node to
		// the up for a schema transaction) or concurrent transactions from other
		// nodes
		return errors.Wrap(err, "open cluster-wide transaction")
	}

	if err := m.cluster.CommitTransaction(ctx, tx); err != nil {
		return errors.Wrap(err, "commit cluster-wide transaction")
	}

	return m.addClassPropertyApplyChanges(ctx, className, prop)
}

func (m *Manager) setNewPropDefaults(class *models.Class, prop *models.Property) {
	m.moduleConfig.SetSinglePropertyDefaults(class, prop)
}

func (m *Manager) addClassPropertyApplyChanges(ctx context.Context,
	className string, prop *models.Property) error {
	semanticSchema := m.state.SchemaFor()
	class, err := schema.GetClassByName(semanticSchema, className)
	if err != nil {
		return err
	}

	class.Properties = append(class.Properties, prop)
	err = m.saveSchema(ctx)
	if err != nil {
		return nil
	}

	return m.migrator.AddProperty(ctx, className, prop)
}

func (m *Manager) validateCanAddProperty(ctx context.Context, principal *models.Principal,
	property *models.Property, class *models.Class) error {
	// Verify format of property.
	_, err := schema.ValidatePropertyName(property.Name)
	if err != nil {
		return err
	}

	// Verify that property name is not a reserved name
	err = schema.ValidateReservedPropertyName(property.Name)
	if err != nil {
		return err
	}

	// First check if there is a name clash.
	err = validatePropertyNameUniqueness(property.Name, class)
	if err != nil {
		return err
	}

	err = m.validatePropertyName(ctx, class.Class, property.Name,
		property.ModuleConfig)
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

	// all is fine!
	return nil
}
