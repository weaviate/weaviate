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
	"errors"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/objects/validation"
)

// AddObjectReference to an existing object. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) AddObjectReference(
	ctx context.Context,
	principal *models.Principal,
	input *AddReferenceInput,
) *Error {
	deprecatedEndpoint := input.Class == ""
	if deprecatedEndpoint { // for backward compatibility only
		objectRes, err := m.getObjectFromRepo(ctx, "", input.ID, additional.Properties{})
		if err != nil {
			errnf := ErrNotFound{} // treated as StatusBadRequest for backward comp
			if errors.As(err, &errnf) {
				return &Error{"source object deprecated", StatusBadRequest, err}
			}
			return &Error{"source object deprecated", StatusInternalServerError, err}
		}
		input.Class = objectRes.Object().Class
	}
	path := fmt.Sprintf("objects/%s/%s", input.Class, input.ID)
	if err := m.authorizer.Authorize(principal, "update", path); err != nil {
		return &Error{path, StatusForbidden, err}
	}

	unlock, err := m.locks.LockSchema()
	if err != nil {
		return &Error{"cannot lock", StatusInternalServerError, err}
	}
	defer unlock()

	validator := validation.New(schema.Schema{}, m.exists, m.config)
	if err := input.validate(ctx, principal, validator, m.schemaManager); err != nil {
		return &Error{"validate inputs", StatusBadRequest, err}
	}
	if !deprecatedEndpoint {
		ok, err := m.vectorRepo.Exists(ctx, input.Class, input.ID)
		if err != nil {
			return &Error{"source object", StatusInternalServerError, err}
		}
		if !ok {
			return &Error{"source object", StatusNotFound, err}
		}
	}

	if err := m.vectorRepo.AddReference(ctx, input.Class, input.ID, input.Property, &input.Ref); err != nil {
		return &Error{"add reference to repo", StatusInternalServerError, err}
	}
	return nil
}

// AddReferenceInput represents required inputs to add a reference to an existing object.
type AddReferenceInput struct {
	// Class name
	Class string
	// ID of an object
	ID strfmt.UUID
	// Property name
	Property string
	// Ref cross reference
	Ref models.SingleRef
}

func (req *AddReferenceInput) validate(
	ctx context.Context,
	principal *models.Principal,
	v *validation.Validator,
	sm schemaManager,
) error {
	if err := req.validateNames(); err != nil {
		return err
	}
	if err := v.ValidateSingleRef(ctx, &req.Ref, "validate reference"); err != nil {
		return err
	}

	schema, err := sm.GetSchema(principal)
	if err != nil {
		return err
	}
	return req.validateSchema(schema)
}

// validateNames validates class and property names
func (req *AddReferenceInput) validateNames() error {
	if _, err := schema.ValidateClassName(req.Class); err != nil {
		return err
	}

	if err := schema.ValidateReservedPropertyName(req.Property); err != nil {
		return err
	}

	if _, err := schema.ValidatePropertyName(req.Property); err != nil {
		return err
	}

	return nil
}

func (req *AddReferenceInput) validateSchema(sch schema.Schema) error {
	prop, err := sch.GetProperty(schema.ClassName(req.Class), schema.PropertyName(req.Property))
	if err != nil {
		return err
	}

	dt, err := sch.FindPropertyDataType(prop.DataType)
	if err != nil {
		return err
	}

	if dt.IsPrimitive() {
		return fmt.Errorf("property '%s' is a primitive datatype, not a reference-type", req.Property)
	}

	return nil
}

func (m *Manager) validateCanModifyReference(principal *models.Principal,
	className string, propertyName string,
) error {
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
