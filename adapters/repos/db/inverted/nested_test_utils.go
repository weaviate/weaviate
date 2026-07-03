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

package inverted

import "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"

// NewNestedPropertyForTest constructs a NestedProperty with a caller-supplied
// *AssignResult and Values slice for use in tests outside the inverted package.
// The unexported result field is set here via the package-internal struct
// literal; configs is left nil so all Exists/Anchors entries pass the gate.
// This constructor exists solely to let db-package oracle tests supply a real
// AssignResult without re-exposing the result field on the public struct.
func NewNestedPropertyForTest(name string, result *nested.AssignResult, values []NestedValue) *NestedProperty {
	var numFilterable int
	for _, v := range values {
		if v.HasFilterableIndex {
			numFilterable++
		}
	}
	return &NestedProperty{
		Name:          name,
		result:        result,
		values:        values,
		numFilterable: numFilterable,
	}
}
