//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package objects

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/objects/validation"
)

// AddObjectReference Class Instance to the connected DB. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) AddObjectReference(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, propertyName string, property *models.SingleRef) error {
	err := m.authorizer.Authorize(principal, "update", fmt.Sprintf("objects/%s", id.String()))
	if err != nil {
		return err
	}

	unlock, err := m.locks.LockSchema()
	if err != nil {
		return NewErrInternal("could not acquire lock: %v", err)
	}
	defer unlock()

	return m.addObjectReferenceToConnectorAndSchema(ctx, principal, id, propertyName, property)
}

func (m *Manager) addObjectReferenceToConnectorAndSchema(ctx context.Context, principal *models.Principal,
	id strfmt.UUID, propertyName string, property *models.SingleRef) error {
	// get object to see if it exists
	objectRes, err := m.getObjectFromRepo(ctx, "", id, additional.Properties{})
	if err != nil {
		return err
	}

	object := objectRes.Object()

	err = m.validateReference(ctx, property)
	if err != nil {
		return err
	}

	err = m.validateCanModifyReference(principal, object.Class, propertyName)
	if err != nil {
		return err
	}

	err = m.vectorRepo.AddReference(ctx, object.Class, object.ID,
		propertyName, property)
	if err != nil {
		return NewErrInternal("add reference to vector repo: %v", err)
	}

	return nil
}

func (m *Manager) validateReference(ctx context.Context, reference *models.SingleRef) error {
	err := validation.New(schema.Schema{}, m.exists, m.config).
		ValidateSingleRef(ctx, reference, "reference not found")
	if err != nil {
		return NewErrInvalidUserInput("invalid reference: %v", err)
	}

	return nil
}

func (m *Manager) validateCanModifyReference(principal *models.Principal,
	className string, propertyName string) error {
	class, err := schema.ValidateClassName(className)
	if err != nil {
		return NewErrInvalidUserInput("invalid class name in reference: %v", err)
	}

	err = schema.ValidateReservedPropertyName(propertyName)
	if err != nil {
		return NewErrInvalidUserInput("invalid property name in reference: %v", err)
	}

	propName, err := schema.ValidatePropertyName(propertyName)
	if err != nil {
		return NewErrInvalidUserInput("invalid property name in reference: %v", err)
	}

	schema, err := m.schemaManager.GetSchema(principal)
	if err != nil {
		return err
	}

	prop, err := schema.GetProperty(class, propName)
	if err != nil {
		return NewErrInvalidUserInput("Could not find property '%s': %v", propertyName, err)
	}

	propertyDataType, err := schema.FindPropertyDataType(prop.DataType)
	if err != nil {
		return NewErrInternal("Could not find datatype of property '%s': %v", propertyName, err)
	}

	if propertyDataType.IsPrimitive() {
		return NewErrInvalidUserInput("property '%s' is a primitive datatype, not a reference-type", propertyName)
	}

	return nil
}
