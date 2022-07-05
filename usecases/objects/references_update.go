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

// PutReferenceInput represents required inputs to add a reference to an existing object.
type PutReferenceInput struct {
	// Class name
	Class string
	// ID of an object
	ID strfmt.UUID
	// Property name
	Property string
	// Ref cross reference
	Refs models.MultipleRef
}

// UpdateObjectReferences of a specific data object. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) UpdateObjectReferences(
	ctx context.Context,
	principal *models.Principal,
	input *PutReferenceInput,
) *Error {
	res, err := m.getObjectFromRepo(ctx, input.Class, input.ID, additional.Properties{})
	if err != nil {
		errnf := ErrNotFound{}
		if errors.As(err, &errnf) {
			if input.Class == "" { // for backward comp reasons
				return &Error{"source object deprecated", StatusBadRequest, err}
			}
			return &Error{"source object", StatusNotFound, err}
		}
		return &Error{"source object", StatusInternalServerError, err}
	}
	input.Class = res.ClassName

	path := fmt.Sprintf("objects/%s/%s", input.Class, input.ID)
	if err := m.authorizer.Authorize(principal, "update", path); err != nil {
		return &Error{path, StatusForbidden, err}
	}

	unlock, err := m.locks.LockSchema()
	if err != nil {
		return &Error{"cannot lock", StatusInternalServerError, err}
	}
	defer unlock()

	validator := validation.New(schema.Schema{}, m.vectorRepo.Exists, m.config)
	if err := input.validate(ctx, principal, validator, m.schemaManager); err != nil {
		return &Error{"bad inputs", StatusBadRequest, err}
	}
	obj := res.Object()
	if obj.Properties == nil {
		obj.Properties = map[string]interface{}{input.Property: input.Refs}
	} else {
		obj.Properties.(map[string]interface{})[input.Property] = input.Refs
	}
	obj.LastUpdateTimeUnix = m.timeSource.Now()
	err = m.vectorRepo.PutObject(ctx, obj, res.Vector)
	if err != nil {
		return &Error{"repo.putobject", StatusInternalServerError, err}
	}
	return nil
}

func (req *PutReferenceInput) validate(
	ctx context.Context,
	principal *models.Principal,
	v *validation.Validator,
	sm schemaManager,
) error {
	if err := validateReferenceName(req.Class, req.Property); err != nil {
		return err
	}
	if err := v.ValidateMultipleRef(ctx, req.Refs, "validate references"); err != nil {
		return err
	}

	schema, err := sm.GetSchema(principal)
	if err != nil {
		return err
	}
	return validateReferenceSchema(req.Class, req.Property, schema)
}

// validateNames validates class and property names
func validateReferenceName(class, property string) error {
	if _, err := schema.ValidateClassName(class); err != nil {
		return err
	}

	if err := schema.ValidateReservedPropertyName(property); err != nil {
		return err
	}

	if _, err := schema.ValidatePropertyName(property); err != nil {
		return err
	}

	return nil
}

func validateReferenceSchema(class, property string, sch schema.Schema) error {
	prop, err := sch.GetProperty(schema.ClassName(class), schema.PropertyName(property))
	if err != nil {
		return err
	}

	dt, err := sch.FindPropertyDataType(prop.DataType)
	if err != nil {
		return err
	}

	if dt.IsPrimitive() {
		return fmt.Errorf("property '%s' is a primitive datatype, not a reference-type", property)
	}

	return nil
}
