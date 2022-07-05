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

package validation

import (
	"context"
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/crossref"
	"github.com/semi-technologies/weaviate/usecases/config"
)

type exists func(_ context.Context, class string, _ strfmt.UUID) (bool, error)

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
)

type Validator struct {
	schema schema.Schema
	exists exists
	config *config.WeaviateConfig
}

func New(schema schema.Schema, exists exists,
	config *config.WeaviateConfig) *Validator {
	return &Validator{
		schema: schema,
		exists: exists,
		config: config,
	}
}

func (v *Validator) Object(ctx context.Context, object *models.Object) error {
	if err := validateClass(object.Class); err != nil {
		return err
	}

	return v.properties(ctx, object)
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
func (v *Validator) ValidateSingleRef(ctx context.Context, cref *models.SingleRef,
	errorVal string) error {
	ref, err := crossref.ParseSingleRef(cref)
	if err != nil {
		return fmt.Errorf("invalid reference: %s", err)
	}

	if !ref.Local {
		return fmt.Errorf("unrecognized cross-ref ref format")
	}

	// locally check for object existence
	ok, err := v.exists(ctx, ref.Class, ref.TargetID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf(ErrorNotFoundInDatabase, errorVal, ref.TargetID)
	}

	return nil
}

func (v *Validator) ValidateMultipleRef(ctx context.Context, refs models.MultipleRef,
	errorVal string) error {
	if refs == nil {
		return nil
	}

	for _, ref := range refs {
		err := v.ValidateSingleRef(ctx, ref, errorVal)
		if err != nil {
			return err
		}
	}
	return nil
}
