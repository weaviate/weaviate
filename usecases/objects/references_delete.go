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

func (m *Manager) DeleteObjectReference(
	ctx context.Context,
	principal *models.Principal,
	input *DeleteReferenceInput,
) *Error {
	deprecatedEndpoint := input.Class == ""
	res, err := m.getObjectFromRepo(ctx, input.Class, input.ID, additional.Properties{})
	if err != nil {
		errnf := ErrNotFound{}
		if errors.As(err, &errnf) {
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

	if err := input.validate(ctx, principal, m.schemaManager); err != nil {
		if deprecatedEndpoint { // for backward comp reasons
			return &Error{"bad inputs deprecated", StatusNotFound, err}
		}
		return &Error{"bad inputs", StatusBadRequest, err}
	}

	obj := res.Object()
	ok, errmsg := removeReference(obj, input.Property, &input.Reference)
	if errmsg != "" {
		return &Error{errmsg, StatusInternalServerError, nil}
	}
	if !ok {
		return nil
	}
	obj.LastUpdateTimeUnix = m.timeSource.Now()

	err = m.vectorRepo.PutObject(ctx, obj, res.Vector)
	if err != nil {
		return &Error{"repo.putobject", StatusInternalServerError, err}
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
