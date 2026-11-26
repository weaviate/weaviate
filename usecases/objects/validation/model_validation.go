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

package validation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

type exists func(_ context.Context, class string, _ strfmt.UUID, _ *additional.ReplicationProperties, _ string) (bool, error)

type referenceAuthorizationError struct {
	err error
}

func (e *referenceAuthorizationError) Error() string {
	return e.err.Error()
}

func (e *referenceAuthorizationError) Unwrap() error {
	return e.err
}

func wrapReferenceAuthorizationError(err error) error {
	return &referenceAuthorizationError{err: err}
}

// IsReferenceAuthorizationError reports whether reference validation failed
// because its target permission check could not complete.
func IsReferenceAuthorizationError(err error) bool {
	var authErr *referenceAuthorizationError
	return errors.As(err, &authErr)
}

// ReferenceTargetTenant uses the tenantless scope required by classless lookups.
func ReferenceTargetTenant(class, tenant string) string {
	if class == "" {
		return ""
	}
	return tenant
}

const (
	// ErrorMissingActionObjects message
	ErrorMissingActionObjects string = "no objects, object and subject, are added. Add 'objects' by using the 'objects' key in the root of the JSON"
	// ErrorMissingActionObjectsObject message
	ErrorMissingActionObjectsObject string = "no object-thing is added. Add the 'object' inside the 'objects' part of the JSON"
	// ErrorMissingActionObjectsSubject message
	ErrorMissingActionObjectsSubject string = "no subject-thing is added. Add the 'subject' inside the 'objects' part of the JSON"
	// ErrorMissingActionObjectsObjectLocation message
	ErrorMissingActionObjectsObjectLocation string = "no 'locationURL' is found in the object-thing. Add the 'locationURL' inside the 'object-thing' part of the JSON"
	// ErrorMissingActionObjectsObjectType message
	ErrorMissingActionObjectsObjectType string = "no 'type' is found in the object-thing. Add the 'type' inside the 'object-thing' part of the JSON"
	// ErrorInvalidActionObjectsObjectType message
	ErrorInvalidActionObjectsObjectType string = "object-thing requires one of the following values in 'type': '%s', '%s' or '%s'"
	// ErrorMissingActionObjectsSubjectLocation message
	ErrorMissingActionObjectsSubjectLocation string = "no 'locationURL' is found in the subject-thing. Add the 'locationURL' inside the 'subject-thing' part of the JSON"
	// ErrorMissingActionObjectsSubjectType message
	ErrorMissingActionObjectsSubjectType string = "no 'type' is found in the subject-thing. Add the 'type' inside the 'subject-thing' part of the JSON"
	// ErrorInvalidActionObjectsSubjectType message
	ErrorInvalidActionObjectsSubjectType string = "subject-thing requires one of the following values in 'type': '%s', '%s' or '%s'"
	// ErrorMisingObject message
	ErrorMissingObject = "the given object is empty"
	// ErrorMissingClass message
	ErrorMissingClass string = "the given class is empty"
	// ErrorMissingContext message
	ErrorMissingContext string = "the given context is empty"
	// ErrorNoExternalCredentials message
	ErrorNoExternalCredentials string = "no credentials available for the Weaviate instance for %s given in the %s"
	// ErrorExternalNotFound message
	ErrorExternalNotFound string = "given statuscode of '%s' is '%d', but 200 was expected for LocationURL given in the %s"
	// ErrorInvalidCRefType message
	ErrorInvalidCRefType string = "'cref' type '%s' does not exists"
	// ErrorNotFoundInDatabase message
	ErrorNotFoundInDatabase string = "%s: no object with id %s found"
	// ErrorInvalidProperties message
	ErrorInvalidProperties string = "properties of object %v must be of type map[string]interface"
)

// ErrMultiTargetRefWithoutClass is what a write returns when it refuses a beacon
// without a class on a reference property with several target classes.
var ErrMultiTargetRefWithoutClass = errors.New("multi-target references require the class name in the target beacon url")

// MultiTargetRefCounter counts the beacons CheckMultiTargetRefWithoutClass sees.
type MultiTargetRefCounter interface {
	MultiTargetRefWithoutClassInc()
}

// CheckMultiTargetRefWithoutClass counts a beacon without a class on a
// reference property with several target classes. It refuses the beacon while
// namespaces are enabled or DisableMultiTargetReferencesWithoutClass is set.
func CheckMultiTargetRefWithoutClass(cfg *config.WeaviateConfig, counter MultiTargetRefCounter) error {
	counter.MultiTargetRefWithoutClassInc()
	if cfg.Config.Namespaces.Enabled || cfg.Config.DisableMultiTargetReferencesWithoutClass.Get() {
		return ErrMultiTargetRefWithoutClass
	}
	return nil
}

type Validator struct {
	exists           exists
	config           *config.WeaviateConfig
	replicationProps *additional.ReplicationProperties
	principal        *models.Principal
	authorizer       authorization.Authorizer
	qualifier        namespacing.Qualifier
	refCounter       MultiTargetRefCounter
}

// New constructs a Validator. principal and qualifier qualify the target class
// of each reference in an object's properties, and refuse one in another
// namespace. authorizer checks that principal may read each reference target
// before its existence is probed.
func New(exists exists, config *config.WeaviateConfig,
	repl *additional.ReplicationProperties,
	principal *models.Principal, authorizer authorization.Authorizer,
	qualifier namespacing.Qualifier, refCounter MultiTargetRefCounter,
) *Validator {
	return &Validator{
		exists:           exists,
		config:           config,
		replicationProps: repl,
		principal:        principal,
		authorizer:       authorizer,
		qualifier:        qualifier,
		refCounter:       refCounter,
	}
}

func (v *Validator) Object(ctx context.Context, class *models.Class,
	incoming *models.Object, existing *models.Object,
) error {
	if incoming.Class == "" {
		return errors.New(ErrorMissingClass)
	}

	if err := v.vector(ctx, class, incoming); err != nil {
		return err
	}

	return v.properties(ctx, class, incoming, existing)
}

// ValidateSingleRef validates a single ref based on location URL and existence of the object in the database
func (v *Validator) ValidateSingleRef(cref *models.SingleRef) (*crossref.Ref, error) {
	ref, err := crossref.ParseSingleRef(cref)
	if err != nil {
		return nil, fmt.Errorf("invalid reference: %w", err)
	}

	// target id must be lowercase
	ref.TargetID = strfmt.UUID(strings.ToLower(ref.TargetID.String()))

	if !ref.Local {
		return nil, fmt.Errorf("unrecognized cross-ref ref format")
	}

	return ref, nil
}

func (v *Validator) ValidateExistence(ctx context.Context, ref *crossref.Ref, errorVal string, tenant string) error {
	// locally check for object existence
	ok, err := v.exists(ctx, ref.Class, ref.TargetID, v.replicationProps, tenant)
	if err != nil {
		if tenant == "" || !errors.Is(err, enterrors.ErrTenantForNonMultiTenantClass) {
			return err
		}
		if authErr := v.authorizer.Authorize(ctx, v.principal, authorization.READ,
			authorization.ShardsData(ref.Class, "")...); authErr != nil {
			return wrapReferenceAuthorizationError(authErr)
		}
		ok2, err2 := v.exists(ctx, ref.Class, ref.TargetID, v.replicationProps, "")
		if err2 != nil {
			// A target class without multi-tenancy fails the tenant call with a
			// tenant error. Only this retry reaches the target shard, so only it
			// can report a replica shortage.
			if errors.Is(err2, replicaerrors.ErrReplicas) {
				return err2
			}
			// return orig error
			return err
		}
		ok = ok2
	}
	if !ok {
		return fmt.Errorf(ErrorNotFoundInDatabase, errorVal, ref.TargetID)
	}

	return nil
}

func (v *Validator) ValidateMultipleRef(refs models.MultipleRef) ([]*crossref.Ref, error) {
	parsedRefs := make([]*crossref.Ref, len(refs))

	if refs == nil {
		return parsedRefs, nil
	}

	for i, ref := range refs {
		parsedRef, err := v.ValidateSingleRef(ref)
		if err != nil {
			return nil, err
		}
		parsedRefs[i] = parsedRef

	}
	return parsedRefs, nil
}
