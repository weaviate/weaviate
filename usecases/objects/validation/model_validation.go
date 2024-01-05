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

package validation

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/usecases/config"
)

type exists func(_ context.Context, class string, _ strfmt.UUID, _ *additional.ReplicationProperties, _ string) (bool, error)

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

type Validator struct {
	exists           exists
	config           *config.WeaviateConfig
	replicationProps *additional.ReplicationProperties
}

func New(exists exists, config *config.WeaviateConfig,
	repl *additional.ReplicationProperties,
) *Validator {
	return &Validator{
		exists:           exists,
		config:           config,
		replicationProps: repl,
	}
}

func (v *Validator) Object(ctx context.Context, class *models.Class,
	incoming *models.Object, existing *models.Object,
) error {
	if err := validateClass(incoming.Class); err != nil {
		return err
	}

	return v.properties(ctx, class, incoming, existing)
}

func validateClass(class string) error {
	// If the given class is empty, return an error
	if class == "" {
		return fmt.Errorf(ErrorMissingClass)
	}

	// No error
	return nil
}

// ValidateSingleRef validates a single ref based on location URL and existence of the object in the database
func (v *Validator) ValidateSingleRef(cref *models.SingleRef) (*crossref.Ref, error) {
	ref, err := crossref.ParseSingleRef(cref)
	if err != nil {
		return nil, fmt.Errorf("invalid reference: %w", err)
	}

	if !ref.Local {
		return nil, fmt.Errorf("unrecognized cross-ref ref format")
	}

	return ref, nil
}

func (v *Validator) ValidateExistence(ctx context.Context, ref *crossref.Ref, errorVal string, tenant string) error {
	// locally check for object existence
	ok, err := v.exists(ctx, ref.Class, ref.TargetID, v.replicationProps, tenant)
	if err != nil {
		if tenant == "" {
			return err
		}
		// since refs can be created to non-MT classes, check again if non-MT object exists
		// (use empty tenant, if previously was given)
		ok2, err2 := v.exists(ctx, ref.Class, ref.TargetID, v.replicationProps, "")
		if err2 != nil {
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

func (v *Validator) ValidateMultipleRef(ctx context.Context, refs models.MultipleRef,
	errorVal string, tenant string,
) ([]*crossref.Ref, error) {
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
