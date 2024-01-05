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

package objects

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/usecases/objects/validation"
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
func (m *Manager) UpdateObjectReferences(ctx context.Context, principal *models.Principal,
	input *PutReferenceInput, repl *additional.ReplicationProperties, tenant string,
) *Error {
	m.metrics.UpdateReferenceInc()
	defer m.metrics.UpdateReferenceDec()

	res, err := m.getObjectFromRepo(ctx, input.Class, input.ID, additional.Properties{}, nil, tenant)
	if err != nil {
		errnf := ErrNotFound{}
		if errors.As(err, &errnf) {
			if input.Class == "" { // for backward comp reasons
				return &Error{"source object deprecated", StatusBadRequest, err}
			}
			return &Error{"source object", StatusNotFound, err}
		} else if errors.As(err, &ErrMultiTenancy{}) {
			return &Error{"source object", StatusUnprocessableEntity, err}
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

	validator := validation.New(m.vectorRepo.Exists, m.config, repl)
	parsedTargetRefs, err := input.validate(ctx, principal, validator, m.schemaManager, tenant)
	if err != nil {
		if errors.As(err, &ErrMultiTenancy{}) {
			return &Error{"bad inputs", StatusUnprocessableEntity, err}
		}
		return &Error{"bad inputs", StatusBadRequest, err}
	}

	for i := range input.Refs {
		if parsedTargetRefs[i].Class == "" {
			toClass, toBeacon, replace, err := m.autodetectToClass(ctx, principal, input.Class, input.Property, parsedTargetRefs[i])
			if err != nil {
				return err
			}

			if replace {
				input.Refs[i].Class = toClass
				input.Refs[i].Beacon = toBeacon
				parsedTargetRefs[i].Class = string(toClass)
			}
		}
		if err := input.validateExistence(ctx, validator, tenant, parsedTargetRefs[i]); err != nil {
			return &Error{"validate existence", StatusBadRequest, err}
		}

	}

	obj := res.Object()
	if obj.Properties == nil {
		obj.Properties = map[string]interface{}{input.Property: input.Refs}
	} else {
		obj.Properties.(map[string]interface{})[input.Property] = input.Refs
	}
	obj.LastUpdateTimeUnix = m.timeSource.Now()
	err = m.vectorRepo.PutObject(ctx, obj, res.Vector, repl)
	if err != nil {
		return &Error{"repo.putobject", StatusInternalServerError, err}
	}
	return nil
}

func (req *PutReferenceInput) validate(
	ctx context.Context,
	principal *models.Principal,
	v *validation.Validator,
	sm schemaManager, tenant string,
) ([]*crossref.Ref, error) {
	if err := validateReferenceName(req.Class, req.Property); err != nil {
		return nil, err
	}
	refs, err := v.ValidateMultipleRef(ctx, req.Refs, "validate references", tenant)
	if err != nil {
		return nil, err
	}

	schema, err := sm.GetSchema(principal)
	if err != nil {
		return nil, err
	}
	return refs, validateReferenceSchema(req.Class, req.Property, schema)
}

func (req *PutReferenceInput) validateExistence(
	ctx context.Context,
	v *validation.Validator, tenant string, ref *crossref.Ref,
) error {
	return v.ValidateExistence(ctx, ref, "validate reference", tenant)
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

	if !dt.IsReference() {
		return fmt.Errorf("property '%s' is not a reference-type", property)
	}

	return nil
}
