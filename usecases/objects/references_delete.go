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

	deprecatedEndpoint := input.Class == ""
	beacon, err := crossref.Parse(input.Reference.Beacon.String())
	if err != nil {
		return &Error{"cannot parse beacon", StatusBadRequest, err}
	}
	if input.Class != "" && beacon.Class == "" {
		toClass, toBeacon, replace, err := m.autodetectToClass(ctx, principal, input.Class, input.Property, beacon)
		if err != nil {
			return err
		}
		if replace {
			input.Reference.Class = toClass
			input.Reference.Beacon = toBeacon
		}
	}

	res, err := m.getObjectFromRepo(ctx, input.Class, input.ID,
		additional.Properties{}, nil, tenant)
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

	path := fmt.Sprintf("objects/%s/%s", input.Class, input.ID)
	if err := m.authorizer.Authorize(principal, "update", path); err != nil {
		return &Error{path, StatusForbidden, err}
	}

	unlock, err := m.locks.LockSchema()
	if err != nil {
		return &Error{"cannot lock", StatusInternalServerError, err}
	}
	defer unlock()

	if err := input.validate(ctx, principal, m.schemaManager); err != nil {
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

	err = m.vectorRepo.PutObject(ctx, obj, res.Vector, repl)
	if err != nil {
		return &Error{"repo.putobject", StatusInternalServerError, err}
	}

	if err := m.updateRefVector(ctx, principal, input.Class, input.ID, tenant); err != nil {
		return &Error{"update ref vector", StatusInternalServerError, err}
	}

	return nil
}

func (req *DeleteReferenceInput) validate(
	ctx context.Context,
	principal *models.Principal,
	sm schemaManager,
) error {
	if err := validateReferenceName(req.Class, req.Property); err != nil {
		return err
	}

	schema, err := sm.GetSchema(principal)
	if err != nil {
		return err
	}
	return validateReferenceSchema(req.Class, req.Property, schema)
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
