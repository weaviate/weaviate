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
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func (m *Manager) autodetectToClass(ctx context.Context, principal *models.Principal, fromClass, fromProperty string, beaconRef strfmt.URI) (strfmt.URI, strfmt.URI, bool, *Error) {
	// autodetect to class from schema if not part of reference
	class, err := m.schemaManager.GetClass(ctx, principal, fromClass)
	if err != nil {
		return "", "", false, &Error{"cannot get class", StatusInternalServerError, err}
	}
	prop, err := schema.GetPropertyByName(class, schema.LowercaseFirstLetter(fromProperty))
	if err != nil {
		return "", "", false, &Error{"cannot get property", StatusInternalServerError, err}
	}
	if len(prop.DataType) > 1 {
		return "", "", false, nil // can't autodetect for multi target
	}

	toClass := prop.DataType[0] // datatype is the name of the class that is referenced
	beaconElements := strings.Split(string(beaconRef), "/")
	toUUID := beaconElements[len(beaconElements)-1]
	toBeacon := "weaviate://localhost/" + string(toClass) + "/" + toUUID // datatype is the name of the class that is referenced
	return strfmt.URI(toClass), strfmt.URI(toBeacon), true, nil
}
