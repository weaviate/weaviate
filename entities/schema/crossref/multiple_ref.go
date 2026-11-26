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

package crossref

import (
	"fmt"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/models"
)

// AsMultipleRef returns the references held by the value of a reference
// property, in any of the shapes it reaches the code in:
//
//   - models.MultipleRef, the parsed form;
//   - an empty []interface{}: storage cannot tell the type of an empty array,
//     so a reference property whose references were all removed is read back
//     as this placeholder (see storobj's enrichSchemaTypes). It means "no
//     references" and must be handled exactly like an empty MultipleRef;
//   - a []interface{} of {"beacon": "..."} maps, as decoded from generic JSON.
//
// Any other value is an error.
func AsMultipleRef(value interface{}) (models.MultipleRef, error) {
	switch typed := value.(type) {
	case models.MultipleRef:
		return typed, nil
	case []interface{}:
		refs := make(models.MultipleRef, len(typed))
		for i, elem := range typed {
			asMap, ok := elem.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("expected ref element %d to be a map, got %T", i, elem)
			}
			beacon, ok := asMap["beacon"].(string)
			if !ok {
				return nil, fmt.Errorf("expected ref element %d to have a string %q, got %T", i, "beacon", asMap["beacon"])
			}
			refs[i] = &models.SingleRef{Beacon: strfmt.URI(beacon)}
		}
		return refs, nil
	default:
		return nil, fmt.Errorf("expected models.MultipleRef, got %T", value)
	}
}
