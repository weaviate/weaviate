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

	"github.com/weaviate/weaviate/entities/versioned"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

func (m *Manager) autodetectToClass(class *models.Class, fromProperty string, beaconRef *crossref.Ref) (strfmt.URI, strfmt.URI, bool, *Error) {
	// autodetect to class from schema if not part of reference
	prop, err := schema.GetPropertyByName(class, schema.LowercaseFirstLetter(fromProperty))
	if err != nil {
		return "", "", false, &Error{"cannot get property", StatusInternalServerError, err}
	}
	if len(prop.DataType) > 1 {
		return "", "", false, nil // can't autodetect for multi target
	}

	// Stored DataType is qualified on NS clusters; strip to short so the
	// output matches user-submitted shape (storage-shape rule).
	toClass := namespacing.StripQualification(prop.DataType[0])
	toBeacon := crossref.NewLocalhost(toClass, beaconRef.TargetID).String()

	return strfmt.URI(toClass), strfmt.URI(toBeacon), true, nil
}

func (m *Manager) getAuthorizedFromClass(ctx context.Context, principal *models.Principal, className string) (*models.Class, uint64, versioned.Classes, *Error) {
	fetchedClass, err := m.schemaManager.GetCachedClass(ctx, principal, className)
	if err != nil {
		if errors.As(err, &autherrs.Forbidden{}) {
			return nil, 0, nil, &Error{err.Error(), StatusForbidden, err}
		}

		return nil, 0, nil, &Error{err.Error(), StatusBadRequest, err}
	}
	if _, ok := fetchedClass[className]; !ok {
		err := fmt.Errorf("collection %q not found in schema", className)
		return nil, 0, nil, &Error{"collection not found", StatusBadRequest, err}
	}

	return fetchedClass[className].Class, fetchedClass[className].Version, fetchedClass, nil
}

// validateReferenceName validates class and property names. The class
// portion accepts the qualified "<namespace>:<Class>" form produced by
// namespacing.Resolve upstream.
func validateReferenceName(class, property string) error {
	if _, err := schema.ValidateQualifiedClassName(class); err != nil {
		return err
	}

	if err := schema.ValidateReservedPropertyName(property); err != nil {
		return err
	}

	if _, err := schema.ValidatePropertyName(property); err != nil {
		return err
	}

	return nil
}
