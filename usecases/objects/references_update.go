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

	"github.com/weaviate/weaviate/usecases/auth/authorization"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/dto"
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

	ctx = classcache.ContextWithClassCache(ctx)
	input.Class = schema.UppercaseClassName(input.Class)

	if err := m.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.ShardsData(input.Class, tenant)...); err != nil {
		return &Error{err.Error(), StatusForbidden, err}
	}

	if input.Class == "" {
		if err := m.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Collections()...); err != nil {
			return &Error{err.Error(), StatusForbidden, err}
		}
	}

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

	if err := validateReferenceName(input.Class, input.Property); err != nil {
		return &Error{err.Error(), StatusBadRequest, err}
	}

	class, schemaVersion, _, typedErr := m.getAuthorizedFromClass(ctx, principal, input.Class)
	if typedErr != nil {
		return typedErr
	}

	validator := validation.New(m.vectorRepo.Exists, m.config, repl)
	parsedTargetRefs, err := input.validate(validator, class)
	if err != nil {
		if errors.As(err, &ErrMultiTenancy{}) {
			return &Error{"bad inputs", StatusUnprocessableEntity, err}
		}
		return &Error{"bad inputs", StatusBadRequest, err}
	}

	previouslyAuthorized := map[string]struct{}{}
	for i := range input.Refs {
		if parsedTargetRefs[i].Class == "" {
			toClass, toBeacon, replace, err := m.autodetectToClass(class, input.Property, parsedTargetRefs[i])
			if err != nil {
				return err
			}

			if replace {
				input.Refs[i].Class = toClass
				input.Refs[i].Beacon = toBeacon
				parsedTargetRefs[i].Class = string(toClass)
			}
		}

		// only check authZ once per class/tenant combination
		checkName := parsedTargetRefs[i].Class + "#" + tenant
		if _, ok := previouslyAuthorized[checkName]; !ok {
			if err := m.authorizer.Authorize(ctx, principal, authorization.READ, authorization.ShardsData(parsedTargetRefs[i].Class, tenant)...); err != nil {
				return &Error{err.Error(), StatusForbidden, err}
			}
			previouslyAuthorized[checkName] = struct{}{}
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

	// Ensure that the local schema has caught up to the version we used to validate
	if err := m.schemaManager.WaitForUpdate(ctx, schemaVersion); err != nil {
		return &Error{
			Msg:  fmt.Sprintf("error waiting for local schema to catch up to version %d", schemaVersion),
			Code: StatusInternalServerError,
			Err:  err,
		}
	}
	vectors, multiVectors, err := dto.GetVectors(res.Vectors)
	if err != nil {
		return &Error{"repo.putobject", StatusInternalServerError, fmt.Errorf("cannot get vectors: %w", err)}
	}
	err = m.vectorRepo.PutObject(ctx, obj, res.Vector, vectors, multiVectors, repl, schemaVersion)
	if err != nil {
		return &Error{"repo.putobject", StatusInternalServerError, err}
	}
	return nil
}

func (req *PutReferenceInput) validate(v *validation.Validator, class *models.Class) ([]*crossref.Ref, error) {
	refs, err := v.ValidateMultipleRef(req.Refs)
	if err != nil {
		return nil, err
	}

	return refs, validateReferenceSchema(class, req.Property)
}

func (req *PutReferenceInput) validateExistence(
	ctx context.Context,
	v *validation.Validator, tenant string, ref *crossref.Ref,
) error {
	return v.ValidateExistence(ctx, ref, "validate reference", tenant)
}

func validateReferenceSchema(c *models.Class, property string) error {
	prop, err := schema.GetPropertyByName(c, property)
	if err != nil {
		return err
	}

	classGetterFunc := func(string) *models.Class { return c }
	dt, err := schema.FindPropertyDataTypeWithRefs(classGetterFunc, prop.DataType, false, "")
	if err != nil {
		return err
	}

	if !dt.IsReference() {
		return fmt.Errorf("property '%s' is not a reference-type", property)
	}

	return nil
}
