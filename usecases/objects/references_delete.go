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

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
)

// DeleteReferenceInput represents required inputs to delete a reference from an existing object.
type DeleteReferenceInput struct {
	// Class name
	Class string
	// ID of an object
	ID strfmt.UUID
	// Property name
	Property string
	// Reference cross reference
	Reference models.SingleRef
}

func (m *Manager) DeleteObjectReference(ctx context.Context, principal *models.Principal,
	input *DeleteReferenceInput, repl *additional.ReplicationProperties, tenant string,
) *Error {
	m.metrics.DeleteReferenceInc()
	defer m.metrics.DeleteReferenceDec()

	ctx = classcache.ContextWithClassCache(ctx)
	input.Class = schema.UppercaseClassName(input.Class)

	// We are fetching the existing object and get to know if the UUID exists
	if err := m.authorizer.Authorize(ctx, principal, authorization.READ, authorization.ShardsData(input.Class, tenant)...); err != nil {
		return &Error{err.Error(), StatusForbidden, err}
	}
	if err := m.authorizer.Authorize(ctx, principal, authorization.UPDATE, authorization.ShardsData(input.Class, tenant)...); err != nil {
		return &Error{err.Error(), StatusForbidden, err}
	}

	deprecatedEndpoint := input.Class == ""
	// we need to know which collection an object belongs to, so for the deprecated case we first need to fetch the
	// object from any collection, to then know its collection to check for the correct permissions after wards
	if deprecatedEndpoint {
		if err := m.authorizer.Authorize(ctx, principal, authorization.READ, authorization.CollectionsData()...); err != nil {
			return &Error{err.Error(), StatusForbidden, err}
		}
		res, err := m.getObjectFromRepo(ctx, input.Class, input.ID, additional.Properties{}, nil, tenant)
		if err != nil {
			errnf := ErrNotFound{}
			if errors.As(err, &errnf) {
				return &Error{"source object", StatusNotFound, err}
			} else if errors.As(err, &ErrMultiTenancy{}) {
				return &Error{"source object", StatusUnprocessableEntity, err}
			}

			return &Error{"source object", StatusInternalServerError, err}
		}
		input.Class = res.ClassName
	}

	if err := validateReferenceName(input.Class, input.Property); err != nil {
		return &Error{err.Error(), StatusBadRequest, err}
	}

	class, schemaVersion, _, typedErr := m.getAuthorizedFromClass(ctx, principal, input.Class)
	if typedErr != nil {
		return typedErr
	}

	res, err := m.getObjectFromRepo(ctx, input.Class, input.ID, additional.Properties{}, nil, tenant)
	if err != nil {
		errnf := ErrNotFound{}
		if errors.As(err, &errnf) {
			return &Error{"source object", StatusNotFound, err}
		} else if errors.As(err, &ErrMultiTenancy{}) {
			return &Error{"source object", StatusUnprocessableEntity, err}
		}

		return &Error{"source object", StatusInternalServerError, err}
	}

	beacon, err := crossref.Parse(input.Reference.Beacon.String())
	if err != nil {
		return &Error{"cannot parse beacon", StatusBadRequest, err}
	}
	if input.Class != "" && beacon.Class == "" {
		toClass, toBeacon, replace, err := m.autodetectToClass(class, input.Property, beacon)
		if err != nil {
			return err
		}
		if replace {
			input.Reference.Class = toClass
			input.Reference.Beacon = toBeacon
		}
	}

	if err := input.validateSchema(class); err != nil {
		if deprecatedEndpoint { // for backward comp reasons
			return &Error{"bad inputs deprecated", StatusNotFound, err}
		}
		if errors.As(err, &ErrMultiTenancy{}) {
			return &Error{"bad inputs", StatusUnprocessableEntity, err}
		}
		return &Error{"bad inputs", StatusBadRequest, err}
	}

	obj := res.Object()
	obj.Tenant = tenant
	ok, errmsg := removeReference(obj, input.Property, &input.Reference)
	if errmsg != "" {
		return &Error{errmsg, StatusInternalServerError, nil}
	}
	if !ok {
		return nil
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

	if err := m.updateRefVector(ctx, principal, input.Class, input.ID, tenant, class, schemaVersion); err != nil {
		return &Error{"update ref vector", StatusInternalServerError, err}
	}

	return nil
}

func (req *DeleteReferenceInput) validateSchema(class *models.Class) error {
	return validateReferenceSchema(class, req.Property)
}

// removeReference removes ref from object obj with property prop.
// It returns ok (removal took place) and an error message
func removeReference(obj *models.Object, prop string, ref *models.SingleRef) (ok bool, errmsg string) {
	properties := obj.Properties.(map[string]interface{})
	if properties == nil || properties[prop] == nil {
		return false, ""
	}

	refs, ok := properties[prop].(models.MultipleRef)
	if !ok {
		return false, "source list is not well formed"
	}

	newrefs := make(models.MultipleRef, 0, len(refs))
	for _, r := range refs {
		if r.Beacon != ref.Beacon {
			newrefs = append(newrefs, r)
		}
	}
	properties[prop] = newrefs
	return len(refs) != len(newrefs), ""
}
