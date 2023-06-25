//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
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

// AddObjectReference to an existing object. If the class contains a network
// ref, it has a side-effect on the schema: The schema will be updated to
// include this particular network ref class.
func (m *Manager) AddObjectReference(ctx context.Context, principal *models.Principal,
	input *AddReferenceInput, repl *additional.ReplicationProperties, tenantKey string,
) *Error {
	m.metrics.AddReferenceInc()
	defer m.metrics.AddReferenceDec()

	deprecatedEndpoint := input.Class == ""
	if deprecatedEndpoint { // for backward compatibility only
		objectRes, err := m.getObjectFromRepo(ctx, "", input.ID,
			additional.Properties{}, nil, "")
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

	validator := validation.New(m.vectorRepo.Exists, m.config, repl, tenantKey)
	if err := input.validate(ctx, principal, validator, m.schemaManager); err != nil {
		return &Error{"validate inputs", StatusBadRequest, err}
	}
	if !deprecatedEndpoint {
		ok, err := m.vectorRepo.Exists(ctx, input.Class, input.ID, repl, tenantKey)
		if err != nil {
			return &Error{"source object", StatusInternalServerError, err}
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

	if shouldValidateMultiTenantRef(tenantKey, source, target) {
		err = validateReferenceMultiTenancy(ctx, principal,
			m.schemaManager, m.vectorRepo, source, target, tenantKey)
		if err != nil {
			return &Error{"multi-tenancy violation", StatusInternalServerError, err}
		}
	}

	if err := m.vectorRepo.AddReference(ctx, source, target, repl, tenantKey); err != nil {
		return &Error{"add reference to repo", StatusInternalServerError, err}
	}

	if err := m.updateRefVector(ctx, principal, input.Class, input.ID); err != nil {
		return &Error{"update ref vector", StatusInternalServerError, err}
	}

	return nil
}

func shouldValidateMultiTenantRef(tenantKey string, source *crossref.RefSource, target *crossref.Ref) bool {
	return tenantKey != "" || (source != nil && target != nil && source.Class != "" && target.Class != "")
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
	if err := validateReferenceName(req.Class, req.Property); err != nil {
		return err
	}
	if err := v.ValidateSingleRef(ctx, &req.Ref, "validate reference"); err != nil {
		return err
	}

	schema, err := sm.GetSchema(principal)
	if err != nil {
		return err
	}
	return validateReferenceSchema(req.Class, req.Property, schema)
}
