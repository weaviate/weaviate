//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/weaviate/weaviate/usecases/auth/authorization"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/classcache"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
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
	if input.Class != "" {
		class, _, err := m.resolveNS(principal, input.Class)
		if err != nil {
			return &Error{err.Error(), StatusUnprocessableEntity, err}
		}
		input.Class = class
	}

	// Parse and prefix-validate the target beacon up front so a namespaced
	// caller submitting "<otherNS>:Animal" gets a 422 before any object
	// lookup work — same contract as add/update.
	beacon, err := crossref.Parse(input.Reference.Beacon.String())
	if err != nil {
		return &Error{"cannot parse beacon", StatusBadRequest, err}
	}
	if beacon.Class != "" {
		if err := namespacing.ValidateNamespacePrefix(principal, m.config.Config.Namespaces.Enabled, beacon.Class, "class"); err != nil {
			return &Error{err.Error(), StatusUnprocessableEntity, err}
		}
	}

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

	if input.Class != "" && beacon.Class == "" {
		toClass, toBeacon, replace, err := m.autodetectToClass(class, input.Property, beacon)
		if err != nil {
			return err
		}
		if replace {
			input.Reference.Class = toClass
			input.Reference.Beacon = toBeacon
			beacon.Class = string(toClass)
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
	ok, errmsg := removeReference(obj, input.Property, beacon)
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

// removeReference removes from obj.prop every ref whose target matches
// `remove`. Match is structural on (Class, TargetID), with class names
// compared in their short form on both sides so an admin-submitted
// qualified beacon ("weaviate://localhost/customer1:Animal/<id>")
// matches a stored short one ("weaviate://localhost/Animal/<id>"). A
// stored or supplied beacon with no class part matches any class for
// that TargetID — preserves the legacy short-only-beacon contract.
//
// Returns ok=true iff at least one ref was removed, and errmsg when the
// property is present but not a MultipleRef.
func removeReference(obj *models.Object, prop string, remove *crossref.Ref) (ok bool, errmsg string) {
	properties := obj.Properties.(map[string]interface{})
	if properties == nil || properties[prop] == nil {
		return false, ""
	}

	refs, ok := properties[prop].(models.MultipleRef)
	if !ok {
		return false, fmt.Sprintf("property %s of type %T is not a valid cross-reference", prop, refs)
	}

	removeShortClass := namespacing.StripQualification(remove.Class)
	var removed bool
	properties[prop] = slices.DeleteFunc(refs, func(ref *models.SingleRef) bool {
		stored, err := crossref.Parse(ref.Beacon.String())
		if err != nil {
			// Skip malformed stored beacons rather than panicking — the
			// rest of the multi-ref still needs to be evaluated.
			return false
		}
		if stored.TargetID != remove.TargetID {
			return false
		}
		storedShortClass := namespacing.StripQualification(stored.Class)
		// Either side empty → legacy short-only match on TargetID alone.
		if storedShortClass == "" || removeShortClass == "" || storedShortClass == removeShortClass {
			removed = true
			return true
		}
		return false
	})
	return removed, ""
}
