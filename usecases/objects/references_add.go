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

	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"

	"github.com/weaviate/weaviate/usecases/auth/authorization"

	"github.com/go-openapi/strfmt"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/usecases/objects/validation"
)

// AddObjectReference to an existing object. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) AddObjectReference(ctx context.Context, principal *models.Principal,
	input *AddReferenceInput, repl *additional.ReplicationProperties, tenant string,
) *Error {
	m.metrics.AddReferenceInc()
	defer m.metrics.AddReferenceDec()

	ctx = classcache.ContextWithClassCache(ctx)
	input.Class = schema.UppercaseClassName(input.Class)

	if err := m.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.ShardsData(input.Class, tenant)...); err != nil {
		return &Error{err.Error(), StatusForbidden, err}
	}

	deprecatedEndpoint := input.Class == ""
	if deprecatedEndpoint { // for backward compatibility only
		if err := m.authorizer.Authorize(ctx, principal, authorization.READ, authorization.Collections()...); err != nil {
			return &Error{err.Error(), StatusForbidden, err}
		}
		objectRes, err := m.getObjectFromRepo(ctx, "", input.ID,
			additional.Properties{}, nil, tenant)
		if err != nil {
			errnf := ErrNotFound{} // treated as StatusBadRequest for backward comp
			if errors.As(err, &errnf) {
				return &Error{"source object deprecated", StatusBadRequest, err}
			} else if errors.As(err, &ErrMultiTenancy{}) {
				return &Error{"source object deprecated", StatusUnprocessableEntity, err}
			}
			return &Error{"source object deprecated", StatusInternalServerError, err}
		}
		input.Class = objectRes.Object().Class
	}

	if err := validateReferenceName(input.Class, input.Property); err != nil {
		return &Error{err.Error(), StatusBadRequest, err}
	}

	class, schemaVersion, fetchedClass, typedErr := m.getAuthorizedFromClass(ctx, principal, input.Class)
	if typedErr != nil {
		return typedErr
	}

	validator := validation.New(m.vectorRepo.Exists, m.config, repl)
	targetRef, err := input.validate(validator, class)
	if err != nil {
		if errors.As(err, &ErrMultiTenancy{}) {
			return &Error{"validate inputs", StatusUnprocessableEntity, err}
		}
		var forbidden autherrs.Forbidden
		if errors.As(err, &forbidden) {
			return &Error{"validate inputs", StatusForbidden, err}
		}

		return &Error{"validate inputs", StatusBadRequest, err}
	}

	if input.Class != "" && targetRef.Class == "" {
		toClass, toBeacon, replace, err := m.autodetectToClass(class, input.Property, targetRef)
		if err != nil {
			return err
		}
		if replace {
			input.Ref.Class = toClass
			input.Ref.Beacon = toBeacon
			targetRef.Class = string(toClass)
		}
	}

	if err := m.authorizer.Authorize(ctx, principal, authorization.READ, authorization.ShardsData(targetRef.Class, tenant)...); err != nil {
		return &Error{err.Error(), StatusForbidden, err}
	}
	if err := input.validateExistence(ctx, validator, tenant, targetRef); err != nil {
		return &Error{"validate existence", StatusBadRequest, err}
	}

	if !deprecatedEndpoint {
		ok, err := m.vectorRepo.Exists(ctx, input.Class, input.ID, repl, tenant)
		if err != nil {
			switch {
			case errors.As(err, &ErrMultiTenancy{}):
				return &Error{"source object", StatusUnprocessableEntity, err}
			default:
				return &Error{"source object", StatusInternalServerError, err}
			}
		}
		if !ok {
			return &Error{"source object", StatusNotFound, err}
		}
	}

	source := crossref.NewSource(schema.ClassName(input.Class),
		schema.PropertyName(input.Property), input.ID)

	target, err := crossref.ParseSingleRef(&input.Ref)
	if err != nil {
		return &Error{"parse target ref", StatusBadRequest, err}
	}

	if shouldValidateMultiTenantRef(tenant, source, target) {
		_, err = validateReferenceMultiTenancy(ctx, principal,
			m.schemaManager, m.vectorRepo, source, target, tenant, fetchedClass)
		if err != nil {
			switch {
			case errors.As(err, &autherrs.Forbidden{}):
				return &Error{"validation", StatusForbidden, err}
			default:
				return &Error{"multi-tenancy violation", StatusInternalServerError, err}
			}
		}
	}

	// Ensure that the local schema has caught up to the version we used to validate
	if err := m.schemaManager.WaitForUpdate(ctx, schemaVersion); err != nil {
		return &Error{
			Msg:  fmt.Sprintf("error waiting for local schema to catch up to version %d", schemaVersion),
			Code: StatusInternalServerError,
			Err:  err,
		}
	}
	if err := m.vectorRepo.AddReference(ctx, source, target, repl, tenant, schemaVersion); err != nil {
		return &Error{"add reference to repo", StatusInternalServerError, err}
	}

	if err := m.updateRefVector(ctx, principal, input.Class, input.ID, tenant, class, schemaVersion); err != nil {
		return &Error{"update ref vector", StatusInternalServerError, err}
	}

	return nil
}

func shouldValidateMultiTenantRef(tenant string, source *crossref.RefSource, target *crossref.Ref) bool {
	return tenant != "" || (source != nil && target != nil && source.Class != "" && target.Class != "")
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
	v *validation.Validator,
	class *models.Class,
) (*crossref.Ref, error) {
	ref, err := v.ValidateSingleRef(&req.Ref)
	if err != nil {
		return nil, err
	}

	return ref, validateReferenceSchema(class, req.Property)
}

func (req *AddReferenceInput) validateExistence(
	ctx context.Context,
	v *validation.Validator, tenant string, ref *crossref.Ref,
) error {
	return v.ValidateExistence(ctx, ref, "validate reference", tenant)
}
